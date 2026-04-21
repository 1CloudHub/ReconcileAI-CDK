import json
import os
import boto3
import time
import requests
from textractor import Textractor
from textractor.data.constants import TextractFeatures
import base64
import fitz  # PyMuPDF
from PIL import Image
import io

from db import select_db, insert_db, update_db
import utils 
import helpers
import job_config
import doc_config
import sop_config
import agent_records
import token_logs

from dashboard import (
    get_total_reconciliations,
    get_total_jobs,
    get_total_sops,
    get_total_documents,
    get_day_wise_reconciliation_breakdown,
    get_reconciliation_summary,
    get_top_jobs_by_documents
)


# All config from utils (Secrets Manager)
cfg = utils.get_config()
S3_BUCKET_NAME = cfg["bucket_name_no_sap"]
job_table = cfg["job_table"]
schema = cfg["erp_no_sap_schema"]
# Use config region with fallback to avoid invalid endpoint errors
_region = (cfg.get("region_name") or "").strip() or "us-west-2"
s3_client = boto3.client("s3", region_name=_region)
bedrock_client = boto3.client("bedrock-runtime", region_name=_region)

DB_SCHEMA = "erp_no_sap"
DOC_TABLE = "document_table"

TEXT_EXTRACT_PROMPT = (
    "Extract and provide the text present in the image in a neat formatted manner "
    "which can be used for NLP tasks. The answer format should only be all extracted "
    "text in a neat formatted manner from the image without any other information. "
    "Ensure to double check the numbers extracted from the image."
)

BASE_PROMPT = (
    "System: You are a specialized document information extraction system. "
    "Your task is to extract specific fields from the input document and return them "
    "in a strictly formatted JSON object.\n\n"
    "Rules:\n"
    "1. Return ONLY valid JSON\n"
    "2. Use double quotes\n"
    "3. Use NOT_AVAILABLE for missing values\n"
    "4. Do not fabricate data\n"
    "5. Do not include escape characters"
)

AI_FIELD_EXTRACT_PROMPT = (
    "System: You are a specialized document field extraction system.\n\n"
    "Rules:\n"
    "1. Return ONLY valid JSON in the format: "
    '{\"fields\": [{\"name\": \"field_name\", \"description\": \"field description\"}]}\n'
    "2. Extract only fields present in the document\n"
    "3. Do not include biometric or sensitive fields\n"
    "4. Preserve case sensitivity\n"
    "5. No comments or extra text"
)

JSON_FORMATTING_PROMPT = (
    "Please generate detailed field descriptions for the following document type. "
    "The output should be a JSON with the format:\n\n"
    "{\n"
    '  "documentType": "[Document Type]",\n'
    '  "documentDesc": "[General description of what this document is, its purpose, and who issues it]",\n'
    '  "fields": [\n'
    "    {\n"
    '      "name": "[Field Name]",\n'
    '      "description": "[Clear, concise description of what this field represents]"\n'
    "    }\n"
    "  ]\n"
    "}\n\n"
    "CRITICAL INSTRUCTION: Always give only the JSON, DO NOT provide any other text with it."
)


# ---------------------------------------------------------------------------
# Token logging helper – writes one row into token_log_table.
#
# module         : 'doc_type', 'sop_control_center', 'reconcile_agent'
# log_type       : 'ai_key_extraction', 'document_testing', etc.
# ref_table      : name of the primary table the operation relates to, or None
#                  when no DB record exists yet (e.g. ai_key_extraction before
#                  document_table insert, sop test before sop_table insert)
# ref_id         : integer PK of the related row, or None (same cases as above)
# document_names : list of actual filenames that were passed to the AI
#
# This function NEVER raises – a logging failure must not break the main flow.
# ---------------------------------------------------------------------------

def _insert_token_log(
    module: str,
    log_type: str,
    ref_table,
    ref_id,
    document_names: list,
    input_tokens: int,
    output_tokens: int,
    created_by: str = "admin",
):
    try:
        insert_db(
            f"""
            INSERT INTO {DB_SCHEMA}.token_log_table
                (module, type, ref_table, ref_id, document_name,
                 input_tokens, output_tokens, created_by)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            """,
            (
                module,
                log_type,
                ref_table,
                ref_id,
                json.dumps(document_names),
                input_tokens,
                output_tokens,
                created_by,
            ),
        )
        print(
            f"TOKEN LOG INSERTED: module={module}, type={log_type}, "
            f"ref_table={ref_table}, ref_id={ref_id}, "
            f"input={input_tokens}, output={output_tokens}"
        )
    except Exception as e:
        print(f"_insert_token_log failed (non-fatal): {e}")


def _get_doc_type_pk(doc_type_name: str):
    """
    Fetch the integer PK of a document_table row by document_name.
    Returns None if not found (e.g. ai_key_extraction is called before
    the document type is saved to document_table).
    """
    try:
        rows = select_db(
            f"SELECT id FROM {DB_SCHEMA}.{DOC_TABLE} WHERE document_name = %s LIMIT 1",
            (doc_type_name,),
        )
        return rows[0][0] if rows else None
    except Exception as e:
        print(f"_get_doc_type_pk error (non-fatal): {e}")
        return None



def _update_metadata_tokens(input_tokens: int, output_tokens: int) -> None:
    """
    Increment total_input_token_used and total_output_token_used in
    metadata_table. The table uses a key-value row structure:
        key_id = 'application'
        meta_key = 'total_input_token_used' or 'total_output_token_used'
        meta_value = running total stored as TEXT

    The DB trigger (trg_update_total_tokens_used) auto-updates
    total_tokens_used whenever either row is changed.

    Never raises - a metadata update failure must not break the main flow.
    """
    try:
        insert_db(
            f"""
            UPDATE {DB_SCHEMA}.metadata_table
            SET meta_value = (
                COALESCE(NULLIF(TRIM(meta_value), '')::BIGINT, 0) + %s
            )::TEXT
            WHERE key_id = 'application'
              AND meta_key = 'total_input_token_used'
            """,
            (int(input_tokens),),
        )
        insert_db(
            f"""
            UPDATE {DB_SCHEMA}.metadata_table
            SET meta_value = (
                COALESCE(NULLIF(TRIM(meta_value), '')::BIGINT, 0) + %s
            )::TEXT
            WHERE key_id = 'application'
              AND meta_key = 'total_output_token_used'
            """,
            (int(output_tokens),),
        )
        print(
            f"METADATA TOKENS UPDATED: +input={input_tokens}, +output={output_tokens}"
        )
    except Exception as e:
        print(f"_update_metadata_tokens failed (non-fatal): {e}")


def pdf_to_base64_images(s3_path):
    """Convert PDF from S3 to list of base64 PNG images (uses helpers)."""
    return helpers.pdf_to_base64_images(s3_client, S3_BUCKET_NAME, s3_path)


def key_extraction_funtion_llm(page_texts, doc_type, doc_name, doc_id, file_extension):
    try:
        print("PAGE TEXT EXTRACTED : ", page_texts)

        select_query = f"SELECT needed_fields, document_description FROM {DB_SCHEMA}.{DOC_TABLE} WHERE document_name = %s"
        row = select_db(select_query, (doc_type,))
        if not row:
            raise Exception(f"Document type '{doc_type}' not found in {DB_SCHEMA}.{DOC_TABLE}")
        needed_fields = row[0][0]
        document_description = row[0][1] or ""

        if isinstance(needed_fields, str):
            needed_fields = json.loads(needed_fields)

        final_prompt = build_key_extraction_final_prompt(doc_type, document_description, needed_fields, page_texts)
        print("FINAL PROMPT : ", final_prompt)
        final, input_tokens, output_tokens = invoke_model_function(final_prompt)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            raw_text = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", raw_text)
            extracted_json = parse_llm_json(raw_text)
            if extracted_json is None:
                extracted_json = {}
            else:
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
        else:
            extracted_json = {}
        return extracted_json, input_tokens, output_tokens

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        raise e


def text_extract_llm(base_64_array, file_extension):
    """Extract text from images via LLM using hardcoded prompt.
    Returns (page_results, total_input_tokens, total_output_tokens)."""
    print("TEXT EXTRACT LLM CALLED")
    input_prompt = TEXT_EXTRACT_PROMPT
    page_results = []
    total_input_tokens = 0
    total_output_tokens = 0
    for i in base_64_array:
        response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 3000,
            "temperature": 0,
            "top_p": 0.8,
            "top_k":100,
            "system":input_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": i
                            }
                        },
                    ]
                }
            ]
        }), modelId=cfg["model_id"])

        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        total_input_tokens  += int(headers.get("x-amzn-bedrock-input-token-count", 0))
        total_output_tokens += int(headers.get("x-amzn-bedrock-output-token-count", 0))

        inference_result = response['body'].read().decode('utf-8')
        final = json.loads(inference_result)
        print("FINAL : ",final)
        extracted_content = final['content'][0]['text']
        page_results.append(extracted_content)
    return page_results, total_input_tokens, total_output_tokens


def encode_image_to_base64(doc_type, doc_id, file_extension):
    """Encode image from S3 to base64 (uses helpers)."""
    return helpers.encode_image_to_base64(s3_client, S3_BUCKET_NAME, doc_type, doc_id, file_extension)


def create_s3_folder(bucket_name, folder_name):
    """Create a folder (prefix) in S3 bucket (uses helpers)."""
    helpers.create_s3_folder(s3_client, S3_BUCKET_NAME, folder_name)


def build_key_extraction_final_prompt(doc_type, document_description, document_key_schema, page_texts):
    """Build final key-extraction prompt using hardcoded BASE_PROMPT."""
    return f"""{BASE_PROMPT}

Document Type: {doc_type}
Document Description: {document_description}
Required Fields: {document_key_schema}

Input Document:
{page_texts}
"""


def parse_llm_json(text):
    """
    Parse JSON from LLM response. Strips markdown code fences (e.g. ```json ... ```)
    so that wrapped responses still parse correctly.
    """
    if not text or not isinstance(text, str):
        return None
    s = text.strip()
    if s.startswith("```"):
        first_newline = s.find("\n")
        if first_newline != -1:
            s = s[first_newline + 1 :]
        else:
            s = s[3:].strip()
            if s.startswith("json"):
                s = s[4:].strip()
        s = s.strip()
    if s.endswith("```"):
        s = s[: s.rindex("```")].strip()
    s = s.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def invoke_model_function(final_prompt):
    """Invoke Bedrock model. Returns (result, input_tokens, output_tokens)."""
    max_retries = 4
    retries = 1
    while retries <= max_retries:
        try:
            response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 3000,
                "temperature": 0,
                "top_p": 0.999,
                "top_k":250,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": final_prompt
                            }
                        ]
                    }
                ]
            }), modelId=cfg["model_id"])

            headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            input_tokens  = int(headers.get("x-amzn-bedrock-input-token-count", 0))
            output_tokens = int(headers.get("x-amzn-bedrock-output-token-count", 0))

            if 'body' in response:
                inference_result = response['body'].read().decode('utf-8')
                final = json.loads(inference_result)
            else:
                final = {}
            return final, input_tokens, output_tokens

        except Exception as e:
            print("ERROR OCCURRED IN INVOKE LLM FUNCTION")
            print(f"An error occurred: {e}")
            print("Retrying...")
            time.sleep(1)
            retries += 1
    print("Maximum retries exceeded. Unable to retrieve response.")
    return {}, 0, 0


def key_extraction_funtion(doc_type, doc_name, doc_id, file_extension):
    try:
        path = f"s3://{S3_BUCKET_NAME}/era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"
        print(path)
        extractor = Textractor(region_name="us-west-2")
        document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False)

        page_texts = []
        for i, page in enumerate(document.pages):
            page_texts.append(page.get_text())

        print("PAGE TEXT EXTRACTED")

        select_query = f"SELECT needed_fields, document_description FROM {DB_SCHEMA}.{DOC_TABLE} WHERE document_name = %s"
        row = select_db(select_query, (doc_type,))
        if not row:
            raise Exception(f"Document type '{doc_type}' not found")
        needed_fields = row[0][0]
        document_description = row[0][1] or ""
        if isinstance(needed_fields, str):
            needed_fields = json.loads(needed_fields)

        final_prompt = build_key_extraction_final_prompt(doc_type, document_description, needed_fields, page_texts)
        print("FINAL PROMPT : ", final_prompt)
        final, _, _ = invoke_model_function(final_prompt)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            raw_text = final['content'][0]['text']
            extracted_json = parse_llm_json(raw_text)
            if extracted_json is None:
                extracted_json = {}
        else:
            extracted_json = {}
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        raise e


def test_key_extraction_funtion(doc_type, doc_name, doc_id, file_extension, document_prompt_details):
    try:
        path = f"s3://{S3_BUCKET_NAME}/era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"
        print(path)
        extractor = Textractor(region_name="us-west-2")
        document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False)

        page_texts = []
        for i, page in enumerate(document.pages):
            page_texts.append(page.get_text())

        print("PAGE TEXT EXTRACTED : ", page_texts)

        json_document_details = json.loads(document_prompt_details) if isinstance(document_prompt_details, str) else document_prompt_details
        document_key_schema = json_document_details.get('fields', [])
        document_description = json_document_details.get('documentDesc', '')

        final_prompt = build_key_extraction_final_prompt(doc_type, document_description, document_key_schema, page_texts)
        final, _, _ = invoke_model_function(final_prompt)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = parse_llm_json(final['content'][0]['text'])
            if extracted_json is None:
                extracted_json = {}
        else:
            extracted_json = {}
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        raise e


def test_encode_image_to_base64(doc_type,doc_id,file_extension):
    print("TEST PNG->BASE64")
    key = f"era_demo/Temp/INPUT/{doc_id}.{file_extension}"
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    image_data = response['Body'].read()
    encoded_image = base64.b64encode(image_data).decode("utf-8")
    return encoded_image


def test_key_extraction_funtion_llm(page_texts, doc_type, doc_name, doc_id, file_extension, document_prompt_details=None):
    try:
        print("TEST KEY EXTRACTION FUNCTION LLM")
        print("PAGE TEXT EXTRACTED : ", page_texts)

        if document_prompt_details:
            json_document_details = json.loads(document_prompt_details) if isinstance(document_prompt_details, str) else document_prompt_details
        else:
            json_document_details = {}
        document_key_schema = json_document_details.get('fields', [])
        document_description = json_document_details.get('documentDesc', '')

        final_prompt = build_key_extraction_final_prompt(doc_type, document_description, document_key_schema, page_texts)
        final, input_tokens, output_tokens = invoke_model_function(final_prompt)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = parse_llm_json(final['content'][0]['text'])
            if extracted_json is None:
                extracted_json = {}
        else:
            extracted_json = {}
        return extracted_json, input_tokens, output_tokens

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        raise e


def ai_key_extractiont_function(doc_name, doc_id, file_extension, document_type, document_description):
    """Extract fields from a document using AI. Uses hardcoded AI_FIELD_EXTRACT_PROMPT."""
    try:
        s3_path = f"era_demo/{document_type}/INPUT/{doc_id}.{file_extension}"

        text_input_tokens = text_output_tokens = 0

        if file_extension == 'pdf':
            base64_array = pdf_to_base64_images(s3_path)
            page_texts, text_input_tokens, text_output_tokens = text_extract_llm(base64_array, file_extension)
        elif file_extension in ['png', 'jpg', 'jpeg']:
            key = f"era_demo/{document_type}/INPUT/{doc_id}.{file_extension}"
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            image_data = response['Body'].read()
            encoded = base64.b64encode(image_data).decode("utf-8")
            page_texts, text_input_tokens, text_output_tokens = text_extract_llm([encoded], file_extension)
        else:
            return {'documentType': document_type, 'documentDesc': document_description, 'fields': []}

        query = f"""
        SELECT meta_value
        FROM {DB_SCHEMA}.metadata_table
        WHERE key_id = 'doc_type'
          AND meta_key = 'field_count';
        """
        result = select_db(query, ())
        field_count = result[0][0] if result else 20
        print(field_count)

        final_prompt = f"""{AI_FIELD_EXTRACT_PROMPT}

        retrun only the top {str(field_count)} fields ignore the rest. 

User Given Document Type: {document_type}
User Given Document Description: {document_description}

Input Document:
{page_texts}
"""
        print("FINAL PROMPT : ", final_prompt)
        final, extract_input_tokens, extract_output_tokens = invoke_model_function(final_prompt)

        total_input  = text_input_tokens  + extract_input_tokens
        total_output = text_output_tokens + extract_output_tokens

        # ------------------------------------------------------------------
        # CHANGED: Log tokens into token_log_table instead of metadata_table.
        # ref_table = "document_table", ref_id = PK of the document type row.
        # ai_key_extraction is called when creating a NEW doc type so the row
        # may not exist yet in document_table → ref_id will be None in that case.
        # ------------------------------------------------------------------
        ref_id = _get_doc_type_pk(document_type)
        _insert_token_log(
            module="doc_type",
            log_type="ai_key_extraction",
            ref_table="document_table",
            ref_id=ref_id,
            document_names=[doc_name],
            input_tokens=total_input,
            output_tokens=total_output,
        )
        _update_metadata_tokens(total_input, total_output)
        print(f"ai_key_extraction tokens — input: {total_input}, output: {total_output}")

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = parse_llm_json(final['content'][0]['text'])
            if extracted_json is not None:
                extracted_json['documentType'] = document_type
                extracted_json['documentDesc'] = document_description
            else:
                extracted_json = {'documentType': document_type, 'documentDesc': document_description, 'fields': []}
        else:
            extracted_json = {'documentType': document_type, 'documentDesc': document_description, 'fields': []}
        return extracted_json

    except Exception as e:
        print("Exception occurred while AI key extraction: ", e)
        raise e


def generate_presigned_url(object_key, expiration=3600):
    s3_client = boto3.client('s3',region_name = "us-west-2")

    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None

def convert_response(key, value):
    keys = key.split(".") 
    nested_dict = value

    for k in reversed(keys):
        nested_dict = {k: nested_dict}

    return nested_dict


def lambda_handler(event, context):
    print("EVENT : ",event)
    start_time = time.time()
    print("START TIME : ",start_time)

    event_type = event['event_type']
    if event_type == "ping":
        return "pong"

    elif event_type == "login_api":
        email = event.get("email")
        password = event.get("password")
        print(email,"emaill")
        print(password,"password")

        if not email or not password:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Email and password are required."})
            }

        try:
            cognito_client = boto3.client(
                    'cognito-idp',
                    region_name='us-west-2' 
                )
            COGNITO_USER_POOL_ID = cfg.get("COGNITO_USER_POOL_ID")
            COGNITO_CLIENT_ID = cfg.get("COGNITO_CLIENT_ID")
            
            print(COGNITO_USER_POOL_ID,"COGNITO_USER_POOL_ID")
            print(COGNITO_CLIENT_ID,"COGNITO_CLIENT_ID")
            auth_response = cognito_client.admin_initiate_auth(
                UserPoolId=COGNITO_USER_POOL_ID,
                ClientId=COGNITO_CLIENT_ID,
                AuthFlow="ADMIN_USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": email,
                    "PASSWORD": password
                }
            )
            print(auth_response,"auth_response")

            tokens = auth_response["AuthenticationResult"]
            access_token = tokens["AccessToken"]
            id_token = tokens["IdToken"]
            refresh_token = tokens.get("RefreshToken", "")

            user_attributes_response = cognito_client.admin_get_user(
                UserPoolId=COGNITO_USER_POOL_ID,
                Username=email,
            )
            user_attributes = {
                attr["Name"]: attr["Value"]
                for attr in user_attributes_response.get("UserAttributes", [])
            }

            username = user_attributes.get("name", "")
            emailid = user_attributes.get("email", email)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Login successful",
                    "username": username,
                    "email": emailid,
                    "tokens": {
                        "access_token": access_token,
                        "id_token": id_token,
                        "refresh_token": refresh_token
                    },
                    "user_attributes": user_attributes
                })
            }

        except cognito_client.exceptions.UserNotConfirmedException:
            return {
                "statusCode": 403,
                "body": json.dumps({"message": "UserNotConfirmed"})
            }

        except cognito_client.exceptions.NotAuthorizedException:
            return {
                "statusCode": 401,
                "body": json.dumps({"error": "Invalid email or password."})
            }

        except cognito_client.exceptions.UserNotFoundException:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "User not found."})
            }

        except Exception as e:
            print(f"Error in login_api: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps({"error": f"Internal server error: {str(e)}"})
            }

    elif event_type == 'ai_key_extraction':
        # -----------------------------------------------------------------------
        # module=doc_type, type=ai_key_extraction
        # Token log is inserted inside ai_key_extractiont_function() itself.
        # ref_id = document_table PK fetched by doc type name (may be None if the
        # doc type row doesn't exist yet at the time this is called).
        # -----------------------------------------------------------------------
        document_type = event['document_type']
        document_description = event['document_description']
        doc_id = event['doc_id']
        doc_name = event['doc_name']

        file_extension = doc_name.split('.')[-1]
        if file_extension not in ['pdf', 'jpg', 'png']:
            return {"statusCode": 200, "message": "Invalid file type"}

        try:
            final_output_json = ai_key_extractiont_function(doc_name, doc_id, file_extension, document_type, document_description)
            print("AI KEY EXTRACTION SUCCESSFUL")

            output_path = f"era_demo/{document_type}/OUTPUT/{doc_id}.json"
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=output_path,
                                 Body=json.dumps(final_output_json, indent=4, ensure_ascii=False),
                                 ContentType='application/json')
            return {"statusCode": 200, "message": "Key extraction successful"}

        except Exception as e:
            print("AI key extraction failed: ", e)
            fallback = {'documentType': document_type, 'documentDesc': document_description, 'fields': []}
            output_path = f"era_demo/{document_type}/OUTPUT/{doc_id}.json"
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=output_path,
                                 Body=json.dumps(fallback, indent=4, ensure_ascii=False),
                                 ContentType='application/json')
            return {"statusCode": 200, "message": "Key extraction failed"}

    elif event_type == 'check_ai_field_extract_status':
        doc_name = event['doc_name']
        doc_id = event['document_id']
        doc_type = event['document_type']
        json_file_path = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"

        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=json_file_path)
            json_data = json.loads(response['Body'].read().decode('utf-8'))

            file_extension = doc_name.split('.')[-1]
            presigned_url = generate_presigned_url(f"era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}")

            return {"statusCode": 200, "status": "Completed", "data": json_data, "presigned_url": presigned_url}
        except s3_client.exceptions.NoSuchKey:
            return {"statusCode": 200, "status": "In Progress"}
        except Exception:
            return {"statusCode": 200, "status": "In Progress"}  
    
    elif event_type == "add_document_type":
        try:
            document_type = event['document_type'].lower().strip()
            document_json = event.get('document_json', '{}')
            document_description = event.get('document_description', '')
            document_key = event.get("document_key")
            document_key_is_enabled=event.get("document_key_is_enabled")

            if not document_type:
                return {"status_code": 400, "message": "Document Type Name Can't be Empty"}

            dup_query = f"SELECT id FROM {DB_SCHEMA}.{DOC_TABLE} WHERE document_name = %s"
            if select_db(dup_query, (document_type,)):
                return {"status_code": 403, "message": "Document Type Already Exists"}

            parsed = json.loads(document_json) if isinstance(document_json, str) else document_json
            fields = parsed.get('fields', []) if isinstance(parsed, dict) else parsed
            desc = document_description or (parsed.get('documentDesc', '') if isinstance(parsed, dict) else '')

            insert_query = f"INSERT INTO {DB_SCHEMA}.{DOC_TABLE} (document_name, needed_fields, document_description, document_key, document_key_is_enabled) VALUES (%s, %s, %s, %s, %s)"
            insert_db(insert_query, (document_type, json.dumps(fields), desc, document_key, document_key_is_enabled))

            create_s3_folder(S3_BUCKET_NAME, f"era_demo/{document_type}/INPUT/")
            create_s3_folder(S3_BUCKET_NAME, f"era_demo/{document_type}/OUTPUT/")

            return {"status_code": 200, "message": "Document Type Added Successfully"}
        except Exception as e:
            print(f"Error in {event_type}: {e}")
            return {"status_code": 500, "message": "An Error Occurred While Adding Document Type"}

    elif event_type == "list_document_type":
        try:
            schema = cfg.get("erp_no_sap_schema")
            doc_table = cfg.get("document_table")

            return doc_config.list_document_types(
                schema,
                doc_table,
                page=event.get("page"),
                page_size=event.get("page_size"),
                document_name=event.get("document_name") or event.get("document_type") or None,
                document_description=event.get("document_description"),
                created_from=event.get("created_from"),
                created_to=event.get("created_to"),
            )
        except Exception as e:
            print(f"Error in {event_type}: {e}")
            return {"status_code": 500, "message": "An error occurred while retrieving document types"}

    elif event_type == "list_jobs":
        schema = cfg.get("erp_no_sap_schema")
        job_table = cfg.get("job_table")
        doc_table = cfg.get("document_table")

        payload = job_config.list_jobs(
            schema,
            job_table,
            DOC_TABLE,
            page=event.get("page"),
            page_size=event.get("page_size"),
            job_name=event.get("job_name"),
            created_by=event.get("created_by"),
            document_name=event.get("document_name"),
        )
        return {
            "status_code": 200,
            "result": payload["jobs"],
            "jobs": payload["jobs"],
            "pagination": payload.get("pagination"),
        }

    elif event_type == "create_job":
        try:
            schema = cfg.get("erp_no_sap_schema")
            job_table = cfg.get("job_table")
            if not schema or not job_table:
                return {
                    "status_code": 400,
                    "message": "Missing config: erp_no_sap_schema/schema or job_table",
                }
            job_name = (event.get("job_name") or "").strip()
            document_ids = event.get("document_id") or []
            reference_document_id = event.get("reference_document_id")
            created_by = event.get("created_by") or "demo@1cloudhub.com"
            if not job_name:
                return {"status_code": 400, "message": "job_name is required"}
            if not isinstance(document_ids, list) or len(document_ids) < 2:
                return {"status_code": 400, "message": "At least 2 document ids are required"}
            result = job_config.create_job(schema, job_table, job_name, document_ids, created_by, reference_document_id)
            return {
                "status_code": 200,
                "message": "Job created successfully",
                "job_id": result.get("job_id"),
            }

        except ValueError as e:
            return {
                "status_code": 409,
                "message": str(e),
            }
        except Exception as e:
            print(f"Error occurred in {event_type}: {e}")
            return {
                "status_code": 500,
                "message": "An error occurred while creating job",
            }

    elif event_type == "edit_job":
        try:
            schema = cfg.get("erp_no_sap_schema")
            job_table = cfg.get("job_table")
            if not schema or not job_table:
                return {
                    "status_code": 400,
                    "message": "Missing config: erp_no_sap_schema/schema or job_table",
                }
            job_id = event.get("id")
            job_name = (event.get("job_name") or "").strip()
            document_ids = event.get("document_id") or []
            reference_document_id = event.get("reference_document_id")
            updated_by = event.get("updated_by") or "demo@1cloudhub.com"
            if not job_id:
                return {"status_code": 400, "message": "id is required"}
            if not job_name:
                return {"status_code": 400, "message": "job_name is required"}
            if not isinstance(document_ids, list) or len(document_ids) < 2:
                return {"status_code": 400, "message": "At least 2 document ids are required"}
            job_config.edit_job(schema, job_table, int(job_id), job_name, document_ids, updated_by, reference_document_id)
            return {
                "status_code": 200,
                "message": "Job updated successfully",
            }
        except Exception as e:
            print(f"Error occurred in {event_type}: {e}")
            return {
                "status_code": 500,
                "message": "An error occurred while updating job",
            }

    elif event_type == "delete_job":
        try:
            schema = cfg.get("erp_no_sap_schema") or cfg.get("schema")
            print("schema",schema)
            job_table = cfg.get("job_table")
            print("job_table",job_table)
            sop_table = cfg.get("sop_table")
            print("sop_table",sop_table)
            if not schema or not job_table or not sop_table:
                return {
                    "status_code": 400,
                    "message": "Missing config: erp_no_sap_schema/schema, job_table, or sop_table",
                }
            job_id = event.get("id")
            if not job_id:
                return {"status_code": 400, "message": "id is required"}

            check_sop_query = f"SELECT id, exception_name FROM {schema}.{sop_table} WHERE COALESCE(delete_status, FALSE) = FALSE AND job_id @> %s"
            sops = select_db(check_sop_query, (json.dumps([int(job_id)]),))
            if sops:
                sop_list = [f"{{'id': {row[0]}, 'name': '{row[1]}'}}" for row in sops]
                return {
                    "status_code": 409,
                    "message": f"Job is mapped to sop(s): {sop_list}. Remove the sop(s) before deleting this job.",
                    "sops": sop_list
                }

            job_config.delete_job(schema, job_table, int(job_id))
            return {
                "status_code": 200,
                "message": "Job deleted successfully",
            }
        except Exception as e:
            print(f"Error occurred in {event_type}: {e}")
            return {
                "status_code": 500,
                "message": "An error occurred while deleting job",
            }

    elif event_type == "suggest_description":
        try:
            document_json = event['document_json']
            user_input = event.get("user_input", "")
            final_prompt = f"""{JSON_FORMATTING_PROMPT}
{user_input}
Now, please generate a similar JSON with field descriptions for the document type:
{json.dumps(document_json)}
"""
            response = bedrock_client.invoke_model(contentType='application/json',
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": final_prompt}]}],
                }), modelId=cfg["model_id"])

            response_body = json.loads(response['body'].read().decode('utf-8'))
            return {"status_code": 200, "response": response_body['content'][0]['text']}
        except Exception as e:
            print(f"Error in {event_type}: {e}")
            return {"status_code": 500, "message": "An Error Occurred While Generating Suggestion"}

    elif event_type == "edit_document_type":
        try:
            document_type = event['document_type'].lower().strip()
            document_json = event.get('document_json', '{}')
            document_description = event.get('document_description', '')
            document_key = event.get('document_key')
            document_key_is_enabled = event.get('document_key_is_enabled')

            exists_query = f"SELECT id FROM {DB_SCHEMA}.{DOC_TABLE} WHERE document_name = %s"
            if not select_db(exists_query, (document_type,)):
                return {"status_code": 404, "message": "Document Type Doesn't Exist"}

            parsed = json.loads(document_json) if isinstance(document_json, str) else document_json
            fields = parsed.get('fields', []) if isinstance(parsed, dict) else parsed
            desc = document_description or (parsed.get('documentDesc', '') if isinstance(parsed, dict) else '')

            update_query = f"UPDATE {DB_SCHEMA}.{DOC_TABLE} SET needed_fields = %s, document_description = %s, document_key = %s, document_key_is_enabled = %s WHERE document_name = %s"
            insert_db(update_query, (json.dumps(fields), desc, document_key, document_key_is_enabled, document_type))

            return {"status_code": 200, "message": "Document Updated Successfully"}
        except Exception as e:
            print(f"Error in {event_type}: {e}")
            return {"status_code": 500, "message": "An Error Occurred While Updating Document Type"}

    elif event_type == "delete_document_type":
        try:
            job_table = cfg.get("job_table")
            doc_id = event.get('id')
            if not doc_id:
                return {"status_code": 400, "message": "id is required"}

            exists_query = f"SELECT id, document_name FROM {DB_SCHEMA}.{DOC_TABLE} WHERE id = %s"
            doc_row = select_db(exists_query, (doc_id,))
            if not doc_row:
                return {"status_code": 404, "message": "Document Type Doesn't Exist"}
            document_name = doc_row[0][1]

            check_job_query = f"""
                SELECT id, job_name FROM {DB_SCHEMA}.{job_table}
                WHERE COALESCE(delete_status, FALSE) = FALSE
                  AND (
                      document_id @> %s
                      OR reference_document_id = %s
                  )
            """
            jobs = select_db(check_job_query, (json.dumps([int(doc_id)]), int(doc_id)))
            if jobs:
                job_list = [f"{{'id': {row[0]}, 'name': '{row[1]}'}}" for row in jobs]
                return {
                    "status_code": 409,
                    "message": f"Doc type is mapped to job(s): {job_list}. Remove the job(s) before deleting this doc type.",
                    "jobs": job_list
                }

            delete_query = f"DELETE FROM {DB_SCHEMA}.{DOC_TABLE} WHERE id = %s"
            insert_db(delete_query, (doc_id,))

            return {"status_code": 200, "message": "Document Type Deleted Successfully"}
        except Exception as e:
            print(f"Error in {event_type}: {e}")
            return {"status_code": 500, "message": "An Error Occurred While Deleting Document Type"}

    elif event_type == 'test_doc_upload':
        # -----------------------------------------------------------------------
        # module=doc_type, type=document_testing
        # This is called when testing a doc type BEFORE it is saved to document_table
        # so ref_id will be None (no DB record for this doc type yet).
        # document_name = the uploaded test file name.
        # -----------------------------------------------------------------------
        doc_id = event['document_id']
        doc_name = event['document_name']
        doc_type = event['document_type']
        document_prompt_details = event.get('document_prompt_details')

        file_extension = doc_name.split('.')[-1]
        if file_extension not in ['pdf', 'jpg', 'png']:
            return {"statusCode": 200, "message": "Invalid file type"}

        try:
            if file_extension == 'pdf':
                s3_path = f"era_demo/Temp/INPUT/{doc_id}.{file_extension}"
                base64_array = pdf_to_base64_images(s3_path)
                page_results, text_input_tokens, text_output_tokens = text_extract_llm(base64_array, file_extension)
                final_output_json, extract_input_tokens, extract_output_tokens = test_key_extraction_funtion_llm(page_results, doc_type, doc_name, doc_id, file_extension, document_prompt_details)
            elif file_extension in ['png', 'jpg']:
                encoded_image = test_encode_image_to_base64(doc_type, doc_id, file_extension)
                page_results, text_input_tokens, text_output_tokens = text_extract_llm([encoded_image], file_extension)
                final_output_json, extract_input_tokens, extract_output_tokens = test_key_extraction_funtion_llm(page_results, doc_type, doc_name, doc_id, file_extension, document_prompt_details)
            else:
                return {"statusCode": 200, "message": "Invalid file type"}

            total_input  = text_input_tokens  + extract_input_tokens
            total_output = text_output_tokens + extract_output_tokens

            # ------------------------------------------------------------------
            # CHANGED: Log tokens into token_log_table instead of metadata_table.
            # ref_id is None because test_doc_upload is done before the doc type
            # record is inserted into document_table.
            # ------------------------------------------------------------------
            _insert_token_log(
                module="doc_type",
                log_type="document_testing",
                ref_table="document_table",
                ref_id=None,
                document_names=[doc_name],
                input_tokens=total_input,
                output_tokens=total_output,
            )
            _update_metadata_tokens(total_input, total_output)
            print(f"document_testing tokens — input: {total_input}, output: {total_output}")

            output_path = f"era_demo/Temp/OUTPUT/{doc_id}.json"
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=output_path,
                                 Body=json.dumps(final_output_json, indent=4, ensure_ascii=False),
                                 ContentType='application/json')
            return {"statusCode": 200, "message": "Key extraction successful"}

        except Exception as e:
            print("Test doc upload failed: ", e)
            output_path = f"era_demo/Temp/OUTPUT/{doc_id}.json"
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=output_path,
                                 Body=json.dumps({}, indent=4, ensure_ascii=False),
                                 ContentType='application/json')
            return {"statusCode": 200, "message": "Key extraction failed"}
    
    elif event_type == "list_documents":
        try:
            schema = cfg.get("list_documents_schema") or cfg.get("schema") or cfg.get("ERP_SCHEMA")
            table = cfg.get("list_documents_table") or cfg.get("document_processing_table")
            if not schema or not table:
                return {
                    "status_code": 400,
                    "message": "Missing config: schema or document_processing_table (or list_documents_schema / list_documents_table) not set in Secrets Manager"
                }
            query = f"select json_agg(row_to_json(row_values)) from (SELECT * FROM {schema}.{table} WHERE delete_status = 0 order by created_on desc) as row_values;"
            response = select_db(query)
            print(response)

            if not response:
                return {
                    "status_code": 200,
                    "result": []
                }
            result = response[0][0] if response and len(response) > 0 and len(response[0]) > 0 else None
            return {
                "status_code": 200,
                "result": result if result is not None else []
            }
        except Exception as e:
            print(f"Error occurred in {event_type}: {e}")
            import traceback
            traceback.print_exc()
            err_msg = str(e)
            if "does not exist" in err_msg:
                err_msg = f"{err_msg}. Check Secrets Manager: list_documents_schema / list_documents_table (or schema / document_processing_table) must point to an existing table."
            return {
                "status_code": 500,
                "message": "An error occurred while retrieving documents",
                "error_detail": err_msg
            }

    elif event_type == "view_document":
        try:
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            file_extension = doc_name.split('.')[-1]
            key = f"era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"

            presigned_url = generate_presigned_url(key)   

            json_file_path = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"
            
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=json_file_path)
            json_content = response['Body'].read().decode('utf-8')  
            json_data = json.loads(json_content)            

            return {
                "statusCode":200,
                "presigned_url":presigned_url,
                "json_data":json_data,
                "document_id":doc_id   
            }    
        except Exception as e:
            print("Failed to process doc_view api due to : ",e)
            return {"statusCode":500,"presigned_url":"","json_data":{}}
    
    elif event_type == 'doc_upload':
        doc_id = event['document_id']
        doc_name = event['document_name']
        doc_type = event['document_type']
        uploaded_by = event['uploaded_by']
        file_extension = doc_name.split('.')[-1]

        if file_extension not in ['pdf' ,'jpg', 'png']:
            print("INVALID FILE EXTENSION")
            return {"statusCode":200,"message":"Invalid file type"}

        query = f'''
            SELECT connector_type, config, human_intervention, extraction_method FROM {cfg["schema"]}.{cfg["document_type_table"]} WHERE delete_status = 0  and name = '{doc_type}'
        '''
        response = select_db(query)
        print("RESPONSE: ",response)    

        if not response:
            return {
                "status_code" : 404,
                "message" : "Document Type Can't be Found"
            }
        
        connector_type = response[0][0]
        connector_config = response[0][1]
        human_intervention = response[0][2]
        extraction_method = response[0][3]
        verified = "NO_HUMAN_INTERVENTION" if human_intervention == 0 or human_intervention == "0" else "NOT_VERIFIED"
        connector_config = json.loads(connector_config)

        select_query = f'''SELECT doc_id from {cfg["schema"]}.{cfg["document_processing_table"]} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result != []:
            print("DOCUMENT ALREADY EXISTS")
            return {"statusCode":200,"message":"Document already exists"}
        else:
            print("DOCUMENT PROCESSING INITIATED")

        insert_query = f'''INSERT INTO {cfg["schema"]}.{cfg["document_processing_table"]}   
                        (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
                        VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
        insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, verified)
        insert_result = insert_db(insert_query, insert_values)

        print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY", extraction_method)

        extraction_method = "LLM"

        try:
            if extraction_method == 'Textract':
                print("TEXTRACT METHOD CALLED")
                final_output_json = key_extraction_funtion(doc_type,doc_name,doc_id,file_extension)
                print("KEY EXTRACTION SUCCESSFULLY")
            if extraction_method == 'LLM':
                print("LLM CALLED")
                if file_extension == 'pdf':
                    print("PDF CALLED")
                    s3_path= f"era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"
                    base64_array = pdf_to_base64_images(s3_path)
                    print("BASE_64_ARRAY: ",base64_array)
                    page_results, text_input_tokens, text_output_tokens = text_extract_llm(base64_array,file_extension)
                    print("PAGE RESULTS : ",page_results)
                    final_output_json, extract_input_tokens, extract_output_tokens = key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension)
                    print("FINAL_JSON: ",final_output_json)
                    
                elif file_extension in ['png','jpg']:
                    print("PNG CALLED")
                    encoded_image = encode_image_to_base64(doc_type,doc_id,file_extension)
                    base64_array = [encoded_image]
                    page_results, text_input_tokens, text_output_tokens = text_extract_llm(base64_array,file_extension)
                    final_output_json, extract_input_tokens, extract_output_tokens = key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension)

                else:
                    print("INVALID FILE EXTENSION")
                    return {"statusCode":200,"message":"Invalid file type"}

                total_input  = text_input_tokens  + extract_input_tokens
                total_output = text_output_tokens + extract_output_tokens

                # ------------------------------------------------------------------
                # CHANGED: Log tokens into token_log_table instead of metadata_table.
                # doc_upload → ai_key_extraction type (actual document extraction).
                # ref_table = "document_table", ref_id = PK by doc_type name.
                # The document_table row is guaranteed to exist here (doc_type was
                # looked up successfully above).
                # ------------------------------------------------------------------
                ref_id = _get_doc_type_pk(doc_type)
                _insert_token_log(
                    module="doc_type",
                    log_type="ai_key_extraction",
                    ref_table="document_table",
                    ref_id=ref_id,
                    document_names=[doc_name],
                    input_tokens=total_input,
                    output_tokens=total_output,
                )
                _update_metadata_tokens(total_input, total_output)
                print(f"doc_upload ai_key_extraction tokens — input: {total_input}, output: {total_output}")

            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            if connector_type == "API":
                api_url = connector_config['api_url']
                api_key = connector_config['api_key']
                output_key = connector_config['output_key']

                headers = {
                    "x-api-key": api_key,
                    "Content-Type": "application/json"
                }

                data = convert_response(output_key, final_output_json)

                response = requests.post(api_url, json=data, headers=headers)
                print("Response for API : ", response)

            end_time = time.time()
            latency = end_time-start_time
            status = "Not Verified" if verified == "NOT_VERIFIED" else "Completed"
            update_query = f'''UPDATE {cfg["schema"]}.{cfg["document_processing_table"]} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction successful"}
        except Exception as e:
            print("An exception occurred while key extraction : ",e)   
            end_time = time.time()
            latency = end_time-start_time
            final_output_json = {}     
            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            update_query = f'''UPDATE {cfg["schema"]}.{cfg["document_processing_table"]} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction failed"} 

    elif event_type == 'check_document_status':
        doc_name = event['doc_name']
        doc_id = event['document_id']
        json_file_path = f"era_demo/Temp/OUTPUT/{doc_id}.json"

        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=json_file_path)
            json_data = json.loads(response['Body'].read().decode('utf-8'))

            file_extension = doc_name.split('.')[-1]
            presigned_url = generate_presigned_url(f"era_demo/Temp/INPUT/{doc_id}.{file_extension}")

            return {"statusCode": 200, "status": "Completed", "data": json_data, "presigned_url": presigned_url}
        except s3_client.exceptions.NoSuchKey:
            return {"statusCode": 200, "status": "In Progress"}
        except Exception:
            return {"statusCode": 200, "status": "In Progress"}

    elif event_type == "change_status":
        doc_id = event.get("doc_id")
        status = event.get("status")
        schema = cfg.get("list_documents_schema") or cfg.get("schema") or cfg.get("ERP_SCHEMA")
        table = cfg.get("list_documents_table") or cfg.get("document_processing_table")
        if not schema or not table:
            return {"statusCode": 400, "message": "Missing config: list_documents_schema / list_documents_table (or schema / document_processing_table) not set"}
        query = f"""UPDATE {schema}.{table} SET doc_status = '{status}' WHERE doc_id = '{doc_id}';"""
        update_db(query)
        return {"statusCode": 200}
    
    elif event_type == 'doc_delete':
        doc_id = event['doc_id']
        doc_name = event['doc_name']   
        doc_type = event['doc_type']
        uploaded_by = event['uploaded_by']
        file_extension = doc_name.split('.')[-1]

        select_query = f'''SELECT doc_id from {cfg["schema"]}.{cfg["document_processing_table"]} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result == []:
            print("INVALID DOCUMENT DELETION REQUEST")
            return {"statusCode":200,"message":"Invalid document deletion request"}
        
        else:
            input_folder_path = f"era_demo/{doc_type}/INPUT/{doc_id}.{file_extension}"
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=input_folder_path)

            output_folder_path = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=output_folder_path)

            delete_query = f'''UPDATE {cfg["schema"]}.{cfg["document_processing_table"]} SET delete_status = 1, updated_on = CURRENT_TIMESTAMP, updated_by = '{uploaded_by}' where doc_id = '{doc_id}';'''
            update_db(delete_query)
            return {"statusCode":200,"message":"Document deleted successfully"}
    
    elif event_type == 'doc_edit':
        try:
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            updated_json = event['updated_json']

            update_query = f'''update {cfg["schema"]}.{cfg["document_processing_table"]} set updated_on = CURRENT_TIMESTAMP, updated_by = '{uploaded_by}' where doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)

            final_output_json_content = json.dumps(updated_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"era_demo/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            return {"statusCode":200,"message":"Document updated successfully"}
               
        except Exception as e:
            print("Failed to process doc_edit api due to : ", e)
            return {"statusCode":500,"message":"Failed to update document"}    

    elif event_type == "verify_document":
        try: 
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            
            query = f'''
            UPDATE {cfg["schema"]}.{cfg["document_processing_table"]} 
            SET
            doc_status = 'Verified',
            verified = 'VERIFIED',
            updated_by = '{uploaded_by}'
            where doc_id = '{doc_id}' and delete_status = 0
            '''
            
            update_db(query)

            return {
                "statusCode":200,
                "message":"Document Verified"
            }

        except Exception as e:
            print("Failed to process verify_document api due to : ", e)
            return {"statusCode":500,"message":"Failed to verify document"}
    
    elif event_type == "generate_presigned_url":
        exception_id = event.get("exception_id")
        filename = event.get("filename")
        expiry = event.get("expiry", 3600)

        result = utils.generate_presigned_upload_url(
            exception_id,
            filename,
            expiry,
            "sop_control"
        )

        return {
            "statusCode": 200,
            "body": result
        }

    elif event_type == "reconcile_generate_url":
        session_id = event.get("session_id")
        filename = event.get("filename")
        expiry = event.get("expiry", 3600)

        result = utils.generate_presigned_upload_url(
            session_id,
            filename,
            expiry,
            "reconcile_agent"
        )
        print("resultof reconcile_generate__url",result)

        return {
            "statusCode": 200,
            "body": result
        }

    elif event_type == "get_session_documents": 
        try:
            session_id = event.get("session_id")
            if not session_id:
                return {"statusCode": 400, "message": "session_id is required"}

            result = agent_records.get_session_documents(session_id)
            return result

        except Exception as e:
            print(f"Failed to process get_session_documents api due to: {e}")
            return {"statusCode": 500, "message": "Failed to retrieve session documents"}

    elif event_type == "set_active_sop_document":
        try:
            sop_id = int(event['sop_id'])
            version = int(event['version'])

            result = sop_config.set_active_sop_document(sop_id, version)
            return result

        except Exception as e:
            print("Failed to process set_active_sop_document api due to : ", e)
            return {"statusCode": 500, "message": "Failed to set active SOP document"}

    elif event_type == "upload_sop_test":
        required = ["exception_name", "filename", "created_by", "job_ids"]
        missing = [f for f in required if not event.get(f)]
        if missing:
            return {"statusCode": 400, "message": f"Missing required fields: {missing}"}

        return sop_config.add_sop(
            exception_name=event["exception_name"],
            job_ids=event["job_ids"],
            filenames=event["filename"],
            created_by=event["created_by"],
            test_sop=True,
        )

    elif event_type == "add_sop":
        try:
            exception_name = event['exception_name']
            job_ids = event['job_ids']
            filenames = event['filename']
            created_by = event['created_by']

            result = sop_config.add_sop(exception_name, job_ids, filenames, created_by)
            return result

        except Exception as e:
            print("Failed to process add_sop api due to : ", e)
            return {"statusCode": 500, "message": "Failed to add SOP"}

    elif event_type == "update_sop":
        try:
            sop_id = event['sop_id']
            updated_by = event['updated_by']
            exception_name = event.get('exception_name')
            job_ids = event.get('job_ids')
            filename = event.get('filename')

            result = sop_config.update_sop(sop_id, updated_by, exception_name, job_ids, filename)
            return result

        except Exception as e:
            print("Failed to process update_sop api due to : ", e)
            return {"statusCode": 500, "message": "Failed to update SOP"}

    elif event_type == "delete_sop":
        try: 
            return sop_config.delete_sop(
                sop_id=event.get("sop_id"),
                updated_by=event.get("updated_by"),
                s3_uri=event.get("s3_uri"),
                version=event.get("version")
            )

        except Exception as e:
            print("Failed to process delete_sop api due to : ", e)
            return {"statusCode": 500, "message": "Failed to delete SOP"}
    
    elif event_type == "delete_test_sop":
        required = ["exception_name", "updated_by"]
        missing = [f for f in required if not event.get(f)]
        if missing:
            return {"statusCode": 400, "message": f"Missing required fields: {missing}"}

        return sop_config.delete_sop(
            sop_id=None,
            updated_by=event["updated_by"],
            exception_name=event["exception_name"],
            test_sop=True,
        )

    elif event_type == "list_sops":
        try:
            page           = event.get("page", 1)
            page_size      = event.get("page_size", 10)
            exception_name = event.get("exception_name")
            job_id         = event.get("job_id")
            created_at     = event.get("created_at")
            s3_uri         = event.get("s3_uri")

            result = sop_config.list_sops(page, page_size, exception_name, job_id, created_at, s3_uri)
            return result

        except Exception as e:
            print("Failed to process list_sops api due to : ", e)
            return {"statusCode": 500, "message": "Failed to list SOPs"}

    elif event_type == "generate_presigned_download_url":
        try:
            s3_uri = event['s3_uri']
            expiry = event.get('expiry', 3600)

            result = utils.generate_presigned_download_url(s3_uri, expiry)
            return {"statusCode": 200, "body": result}

        except Exception as e:
            print("Failed to process generate_presigned_download_url api due to : ", e)
            return {"statusCode": 500, "message": "Failed to generate download url"}
    
    elif event_type == "kpi_cards":
        date = event.get("date")

        recon = get_total_reconciliations(date)
        jobs  = get_total_jobs(date)
        sops  = get_total_sops(date)
        docs  = get_total_documents(date)

        for result in [recon, jobs, sops, docs]:
            if result["statusCode"] != 200:
                return result

        return {
            "statusCode": 200,
            "body": {
                "total_reconciliations": recon["body"]["total_reconciliations"],
                "total_jobs":            jobs["body"]["total_jobs"],
                "total_sops":            sops["body"]["total_sops"],
                "total_documents":       docs["body"]["total_documents_processed"],
            }
        }
    
    elif event_type == "get_day_wise_reconciliation_breakdown":
        date = event.get("date")
        return get_day_wise_reconciliation_breakdown(date)

    elif event_type == "get_reconciliation_summary":
        date = event.get("date")
        return get_reconciliation_summary(date)

    elif event_type == "get_top_jobs_by_documents":
        date = event.get("date")
        return get_top_jobs_by_documents(date)

    elif event_type == "list_all_sessions":
        try:
            page = event.get("page", 1)
            page_size = event.get("page_size", 10)
            job_name = event.get("job_name")
            reconcile_status = event.get("reconcile_status")
            document_type = event.get("document_type")
            session_id = event.get("session_id")
            created_by = event.get("created_by")
            created_from = event.get("created_from")
            created_to = event.get("created_to")

            result = agent_records.list_all_sessions(
                page=page,
                page_size=page_size,
                job_name=job_name,
                reconcile_status=reconcile_status,
                document_type=document_type,
                session_id=session_id,
                created_by=created_by,
                created_from=created_from,
                created_to=created_to,
            )
            return result

        except Exception as e:
            print("Failed to process list_all_sessions api due to : ", e)
            return {"statusCode": 500, "message": "Failed to list sessions"}

    elif event_type == "get_doc_upload_url":
        try:
            s3_path = event["s3_path"]
            content_type = event.get("content_type", "application/pdf")
            expiry = event.get("expiry", 3600)

            upload_url = s3_client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": S3_BUCKET_NAME,
                    "Key": s3_path,
                    "ContentType": content_type,
                },
                ExpiresIn=expiry,
            )

            return {
                "statusCode": 200,
                "body": {
                    "upload_url": upload_url,
                    "s3_path": s3_path,
                    "expiry": expiry,
                },
            }

        except Exception as e:
            print("Failed to process get_doc_upload_url api due to : ", e)
            return {
                "statusCode": 500,
                "message": "Failed to generate upload URL"
            }

    elif event_type == "create_presigned_url":
        try:
            s3_uri = event["s3_uri"]
            expiry = event.get("expiry", 3600)

            if not s3_uri.startswith("s3://"):
                raise ValueError("Invalid S3 URI format")

            parts = s3_uri.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""

            if not key:
                raise ValueError("S3 key is missing in URI")

            presigned_url = s3_client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                },
                ExpiresIn=expiry,
            )

            return {
                "status": "success",
                "event_type": event_type,
                "s3_uri": s3_uri,
                "download_url": presigned_url,
                "expiry": expiry
            }

        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }

    elif event_type == "test_session_removal":
        try:
            session_id = event.get("session_id")
            if not session_id:
                return {"statusCode": 400, "message": "session_id is required"}

            result = agent_records.test_session_removal(session_id)
            if result is None:
                return {"statusCode": 500, "message": "Failed to remove session"}

            affected_rows = result.get("affected_rows", 0)
            return {
                "statusCode": 200,
                "body": {
                    "message": "Session removed successfully" if affected_rows > 0 else "Session not found",
                    "affected_rows": affected_rows,
                },
            }
        except Exception as e:
            print("Failed to process test_session_removal api due to : ", e)
            return {"statusCode": 500, "message": "Failed to remove session"}

    elif event_type == "show_tokens":
        try:
            query = """
                SELECT meta_key, meta_value
                FROM erp_no_sap.metadata_table
                WHERE key_id = 'application'
                  AND meta_key IN ('total_token_limit', 'total_token_used')
            """
            result = select_db(query)

            if result:
                meta = {row[0]: row[1] for row in result}
                return {
                    "statusCode": 200,
                    "available_tokens": int(meta.get('total_token_limit', -1)),
                    "total_tokens_used": int(meta.get('total_token_used', 0))
                }
            else:
                return {"statusCode": 404, "message": "No metadata row found"}

        except Exception as e:
            print(f"Failed to process show_tokens api due to: {e}")
            return {"statusCode": 500, "message": "Failed to fetch token data"}

    elif event_type == "document_poll-status":
        session_id = event.get("session_id")
        use_session_table = bool(event.get("use_session_table"))
        return sop_config.document_poll_status(session_id, use_session_table=use_session_table)

    elif event_type == "list_token_usage":
        return token_logs.token_usage_listing(
            page=event.get("page", 1),
            page_size=event.get("page_size", 10),
            module=event.get("module"),
            type=event.get("type"),
            start_date=event.get("start_date"),
            end_date=event.get("end_date")
        )

    elif event_type == "testtt":
        print(cfg)
        return {"statusCode":200,"message":"success","body":cfg}

    else:
        return {
            "statusCode": 400,
            "message": f"Unknown event_type '{event_type}'"
        }