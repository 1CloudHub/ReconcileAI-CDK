from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

import boto3

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
textract_client = boto3.client("textract", region_name="us-west-2")
s3_client = boto3.client("s3", region_name="us-west-2")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-west-2")

logger = logging.getLogger(__name__)

db_schema = "erp_no_sap"

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_db_connection():
    import os
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("db_host", "sap-erp.czka2e64ehbk.us-west-2.rds.amazonaws.com"),
        port=5432,
        dbname=os.environ.get("db_database", "postgres"),
        user=os.environ.get("db_user", "postgres"),
        password=os.environ.get("db_password", "postgres123"),
        options=f"-c search_path={db_schema}",
    )


def _query(sql: str, params: tuple = ()) -> list[dict]:
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _execute(sql: str, params: tuple = ()) -> int | None:
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            try:
                row = cur.fetchone()
                return row[0] if row else None
            except Exception:
                return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Token logging helpers
# ---------------------------------------------------------------------------

def _extract_filename_from_s3_uri(s3_uri: str) -> str:
    """Extract the filename (last segment) from an S3 URI."""
    try:
        return s3_uri.rstrip("/").split("/")[-1]
    except Exception:
        return s3_uri


def _get_session_pk(session_id: str, session_table: str) -> int | None:
    """
    Fetch the integer primary key (id) of a session row using its session_id string.
    Returns None if not found.
    """
    try:
        rows = _query(
            f"SELECT id FROM {db_schema}.{session_table} WHERE session_id = %s LIMIT 1",
            (session_id,),
        )
        return rows[0]["id"] if rows else None
    except Exception as e:
        print(f"_get_session_pk error: {e}")
        return None


def _insert_token_log(
    module: str,
    log_type: str,
    ref_table: str | None,
    ref_id: int | None,
    document_names: list[str],
    input_tokens: int,
    output_tokens: int,
    created_by: str = "admin",
) -> None:
    """
    Insert one row into token_log_table.

    Args:
        module:         e.g. "reconcile_agent", "doc_type", "sop_control_center"
        log_type:       e.g. "reconciliation", "reconcile_reupload", "ai_key_extraction"
        ref_table:      Name of the related primary table, or None
        ref_id:         PK of the related row, or None
        document_names: List of actual filenames passed to AI (extracted from S3 URIs)
        input_tokens:   Total input tokens consumed
        output_tokens:  Total output tokens consumed
        created_by:     User identifier (default "admin")
    """
    try:
        _execute(
            f"""
            INSERT INTO {db_schema}.token_log_table
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
        print(f"TOKEN LOG INSERTED: module={module}, type={log_type}, "
              f"ref_table={ref_table}, ref_id={ref_id}, "
              f"input={input_tokens}, output={output_tokens}")
    except Exception as e:
        # Token logging must never break the main flow
        print(f"_insert_token_log error (non-fatal): {e}")


def _update_metadata_tokens() -> None:
    """
    Recompute total_input_token_used and total_output_token_used in
    metadata_table by summing across session_table + session_table_test.
    The DB trigger then auto-updates total_tokens_used.
    """
    _execute(
        f"""
        UPDATE {db_schema}.metadata_table
        SET meta_value = (
            SELECT COALESCE(SUM(input_tokens_used), 0)::TEXT
            FROM (
                SELECT input_tokens_used FROM {db_schema}.session_table
                UNION ALL
                SELECT input_tokens_used FROM {db_schema}.session_table_test
            ) combined
        )
        WHERE key_id = 'application'
          AND meta_key = 'total_input_token_used'
        """,
    )

    _execute(
        f"""
        UPDATE {db_schema}.metadata_table
        SET meta_value = (
            SELECT COALESCE(SUM(output_tokens_used), 0)::TEXT
            FROM (
                SELECT output_tokens_used FROM {db_schema}.session_table
                UNION ALL
                SELECT output_tokens_used FROM {db_schema}.session_table_test
            ) combined
        )
        WHERE key_id = 'application'
          AND meta_key = 'total_output_token_used'
        """,
    )


# ---------------------------------------------------------------------------
# Textract helpers
# ---------------------------------------------------------------------------

def _run_textract(bucket: str, key: str) -> dict:
    try:
        response = textract_client.analyze_document(
            Document={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["FORMS", "TABLES"],
        )
        return _parse_textract_response(response)
    except textract_client.exceptions.UnsupportedDocumentException:
        logger.info("Document %s may be multi-page, using async API", key)
        return _run_textract_async(bucket, key)


def _run_textract_async(bucket: str, key: str) -> dict:
    start_resp = textract_client.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["FORMS", "TABLES"],
    )
    job_id = start_resp["JobId"]
    while True:
        result = textract_client.get_document_analysis(JobId=job_id)
        status = result["JobStatus"]
        if status == "SUCCEEDED":
            return _parse_textract_response(result)
        if status == "FAILED":
            raise RuntimeError(f"Textract job failed for s3://{bucket}/{key}")
        time.sleep(2)


def _parse_textract_response(response: dict) -> dict:
    blocks = response.get("Blocks", [])
    block_map: dict[str, dict] = {b["Id"]: b for b in blocks}

    key_values: dict[str, str] = {}
    for block in blocks:
        if block["BlockType"] == "KEY_VALUE_SET" and "KEY" in block.get("EntityTypes", []):
            key_text = _get_text_from_block(block, block_map)
            value_block = _get_value_block(block, block_map)
            value_text = _get_text_from_block(value_block, block_map) if value_block else ""
            if key_text:
                key_values[key_text.strip()] = value_text.strip()

    tables: list[list[list[str]]] = []
    for block in blocks:
        if block["BlockType"] == "TABLE":
            table = _parse_table(block, block_map)
            tables.append(table)

    raw_lines = [b.get("Text", "") for b in blocks if b["BlockType"] == "LINE"]

    return {
        "key_values": key_values,
        "tables": tables,
        "raw_text": "\n".join(raw_lines),
    }


def _get_text_from_block(block: dict, block_map: dict) -> str:
    text_parts = []
    for rel in block.get("Relationships", []):
        if rel["Type"] == "CHILD":
            for child_id in rel["Ids"]:
                child = block_map.get(child_id, {})
                if child.get("BlockType") == "WORD":
                    text_parts.append(child.get("Text", ""))
    return " ".join(text_parts)


def _get_value_block(key_block: dict, block_map: dict) -> dict | None:
    for rel in key_block.get("Relationships", []):
        if rel["Type"] == "VALUE":
            for vid in rel["Ids"]:
                vb = block_map.get(vid, {})
                if vb.get("BlockType") == "KEY_VALUE_SET" and "VALUE" in vb.get("EntityTypes", []):
                    return vb
    return None


def _parse_table(table_block: dict, block_map: dict) -> list[list[str]]:
    cells: list[dict] = []
    for rel in table_block.get("Relationships", []):
        if rel["Type"] == "CHILD":
            for cid in rel["Ids"]:
                cell = block_map.get(cid, {})
                if cell.get("BlockType") == "CELL":
                    cells.append(cell)

    max_row = max((c.get("RowIndex", 1) for c in cells), default=1)
    max_col = max((c.get("ColumnIndex", 1) for c in cells), default=1)
    table = [["" for _ in range(max_col)] for _ in range(max_row)]

    for cell in cells:
        ri = cell.get("RowIndex", 1) - 1
        ci = cell.get("ColumnIndex", 1) - 1
        table[ri][ci] = _get_text_from_block(cell, block_map)
    return table


def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    path = s3_path.replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


# ---------------------------------------------------------------------------
# Core agent functions
# ---------------------------------------------------------------------------

def textract_documents(documents: dict) -> dict:
    """
    Run Amazon Textract on uploaded documents and return structured output.

    Args:
        documents: A dict where keys are document names (from document_table)
                   and values are S3 keys (e.g. "s3://bucket/path/to/file.pdf").

    Returns:
        Dict keyed by document name with Textract-extracted raw_text.
    """
    results: dict[str, Any] = {}
    textract_cache: dict[str, str] = {}

    for doc_name, s3_path in documents.items():
        if s3_path in textract_cache:
            raw_text = textract_cache[s3_path]
        else:
            bucket, key = _parse_s3_path(s3_path)
            logger.info("Running Textract on %s  ->  s3://%s/%s", doc_name, bucket, key)
            extracted = _run_textract(bucket, key)
            raw_text = extracted.get("raw_text", "")
            textract_cache[s3_path] = raw_text

        results[doc_name] = {"raw_text": raw_text}
    print("TEXTRACT_RESULTS: ", results)
    return results


def fetch_sop_documents(job_id: int, sub_flag: str = "no") -> dict:
    """
    Fetch SOP documents linked to a job, then Textract them.

    Args:
        job_id: The job identifier.
        sub_flag: If "testing", queries sop_table_test instead of sop_table.
    """
    try:
        print("JOBBBBBBB: ", job_id)
        print("SUB_FLAGGGG: ", sub_flag)
        table_name = "sop_table_test" if sub_flag == "testing" else "sop_table"

        print("TABLEEEEEEEEE: ", table_name)

        rows = _query(
            f"""
            SELECT id, exception_id, exception_name, s3_path
            FROM {db_schema}.{table_name}
            WHERE job_id @> %s::jsonb
            AND (delete_status IS NULL OR delete_status = false)
            """,
            (json.dumps([job_id]),),
        )
        print("SOPPPPPPPP: ", rows)
        if not rows:
            return {"message": f"No SOP documents found for job_id {job_id} in {table_name}"}

        results: dict[str, Any] = {}
        textract_cache: dict[str, str] = {}

        for row in rows:
            sop_key = f"sop_{row['id']}"
            s3_path_json = row.get("s3_path")

            s3_uri = None
            if s3_path_json:
                docs = (
                    json.loads(s3_path_json)
                    if isinstance(s3_path_json, str)
                    else s3_path_json
                )
                active_doc = next(
                    (
                        d for d in docs
                        if d.get("active_status") is True
                        and d.get("document_delete_status") is not True
                    ),
                    None,
                )
                if active_doc:
                    s3_uri = active_doc.get("s3_uri")

            if not s3_uri:
                results[sop_key] = {
                    "exception_id": row.get("exception_id"),
                    "exception_name": row.get("exception_name"),
                    "raw_text": None,
                    "error": "No active s3_uri found for this SOP",
                }
                continue

            if s3_uri in textract_cache:
                raw_text = textract_cache[s3_uri]
            else:
                bucket, key = _parse_s3_path(s3_uri)
                extracted = _run_textract(bucket, key)
                raw_text = extracted.get("raw_text", "")
                textract_cache[s3_uri] = raw_text

            results[sop_key] = {
                "exception_id": row.get("exception_id"),
                "exception_name": row.get("exception_name"),
                "raw_text": raw_text,
            }
        print("RESULTS: ", results)
        return results

    except Exception as e:
        print("ERROR: ", e)
        return {"error": str(e)}


def get_job_and_documents(job_id: int) -> dict:
    """
    Fetch job metadata and its linked document details from the database.

    Args:
        job_id: The primary key of the job in job_table.
    """
    job_rows = _query(
        f"SELECT id, job_name, document_id, reference_document_id FROM {db_schema}.job_table WHERE id = %s",
        (job_id,),
    )
    if not job_rows:
        return {"error": f"Job with id {job_id} not found"}

    job = job_rows[0]
    document_ids: list[int] = job["document_id"] or []
    reference_document_id = job.get("reference_document_id")

    all_ids = list(document_ids)
    if reference_document_id and reference_document_id not in all_ids:
        all_ids.append(reference_document_id)

    if not all_ids:
        return {
            "job_id": job_id,
            "job_name": job["job_name"],
            "document_ids": [],
            "documents": {},
        }

    placeholders = ",".join(["%s"] * len(all_ids))
    doc_rows = _query(
        f"SELECT id, document_name, needed_fields, document_key, document_key_is_enabled FROM {db_schema}.document_table WHERE id IN ({placeholders})",
        tuple(all_ids),
    )

    documents = {}
    for doc in doc_rows:
        is_reference = doc["id"] == reference_document_id
        key = "reference_document" if is_reference else doc["document_name"]
        documents[key] = {
            "id": doc["id"],
            "document_name": doc["document_name"],
            "needed_fields": doc["needed_fields"],
            "document_key": doc.get("document_key", ""),
            "document_key_is_enabled": doc.get("document_key_is_enabled", False),
        }

    return {
        "job_id": job_id,
        "job_name": job["job_name"],
        "document_ids": all_ids,
        "documents": documents,
    }


def extract_fields_from_documents(
    needed_fields: dict,
    document_textract_outputs: dict,
    sop_textract_outputs: dict,
) -> dict:

    print("NEEDED_FIELDS: ", needed_fields)
    print("INPUT DOCUMENT FIELDS: ", document_textract_outputs)

    try:
        sop_names = [
            v.get("exception_name", k)
            for k, v in sop_textract_outputs.items()
            if isinstance(v, dict)
        ]

        prompt = f"""You are a document field extraction specialist.

You will be given:
1. NEEDED FIELDS – the specific fields that must be extracted from each document.
2. DOCUMENT CONTENTS – Textract outputs of the uploaded documents.
3. SOP (Standard Operating Procedure) – instructions that describe what fields matter and how they should be compared.

Your task:
- Extract ONLY the needed fields from each document.
- Read each SOP carefully and produce a refined, structured summary of each SOP's comparison rule.
- Identify which fields each SOP requires for comparison.

Return ONLY valid JSON (no markdown, no explanation) in this exact structure:
{{
  "extracted_documents": {{
    "<document_name>": {{
      "<field_name>": "<extracted_value>",
      ...
    }},
    ...
  }},
  "sop_instructions": [
    {{
      "exception_name": "<sop_exception_name>",
      "fields_to_compare": ["<field1>", "<field2>", ...],
      "documents_to_compare": ["<doc_name_1>", "<doc_name_2>", ...],
      "rule": "<clear, concise description of what this SOP checks and how to determine pass/fail>"
    }},
    ...
  ]
}}

RULES:
- "extracted_documents" must have one key per document provided.
- Extract every field listed in NEEDED FIELDS, plus any additional fields referenced by the SOPs.
- If a field cannot be found in a document, set its value to "NOT_FOUND".
- "sop_instructions" must have one entry per SOP.
- The exception names to use are: {json.dumps(sop_names, default=str)}
- "fields_to_compare" should list the exact field names that this SOP needs to check.
- "documents_to_compare" should list which documents are involved in the comparison.
- "rule" should be a clear, actionable description of the comparison logic.

--- NEEDED FIELDS ---
{json.dumps(needed_fields, indent=2)}

--- DOCUMENT CONTENTS ---
{json.dumps(document_textract_outputs, indent=2, default=str)}

--- SOP DOCUMENTS ---
{json.dumps(sop_textract_outputs, indent=2, default=str)}
"""

        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 16384,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            }
        )

        response = bedrock_runtime.invoke_model(
            modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        input_tokens = int(headers.get("x-amzn-bedrock-input-token-count", 0))
        output_tokens = int(headers.get("x-amzn-bedrock-output-token-count", 0))

        print("EXTRACTTTTT FIELDSSSSSSSS HEADERS: ", headers)
        print("INPUTTTTT: ", input_tokens)
        print("OUTPUTTTT: ", output_tokens)

        resp_body = json.loads(response["body"].read())
        raw_text = resp_body["content"][0]["text"]

        try:
            result = json.loads(raw_text)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", raw_text, re.DOTALL)
            if match:
                result = json.loads(match.group())
            else:
                return {
                    "error": "Failed to parse LLM extraction response",
                    "raw_response": raw_text[:2000],
                }

        result["input_token"] = input_tokens
        result["output_token"] = output_tokens

        if "extracted_documents" not in result:
            result["extracted_documents"] = {}
        if "sop_instructions" not in result:
            result["sop_instructions"] = []

        print("EXTRACT_FIELDS_RESULT: ", result)
        return result

    except Exception as e:
        print("EXTRACT_FIELDS_ERROR: ", e)
        return {"error": str(e)}


def reconcile_documents(
    extracted_documents: dict,
    sop_instructions: list,
    session_id: str,
    job_id: int,
    created_by: str,
    sub_flag: str = "no",
) -> dict:
    """
    Compare the extracted field values across documents using the refined SOP
    instructions. Determines match/mismatch for each SOP check and saves the
    result to session_table (or session_table_test if sub_flag is "testing").
    """
    session_table = "session_table_test" if sub_flag == "testing" else "session_table"

    try:
        prompt = f"""You are a document reconciliation specialist.

You will be given:
1. EXTRACTED DOCUMENTS – field values already extracted from each document.
2. SOP INSTRUCTIONS – refined rules describing what to compare and how.

Your task:
- For each SOP instruction, compare the relevant fields across the specified documents.
- Determine if each SOP check passes or fails based on the rule provided.

Return ONLY valid JSON (no markdown, no explanation) in this exact structure:
{{
  "match_status": "yes" or "no",
  "reason_for_failure": [
    {{
      "<exception_name>": "none" or "<clear reason for mismatch>",
      "status": "yes" or "no",
      "matching": {{
        "<field_name>": {{
          "<document_name>": "<value_from_that_document>",
          ...,
          "matched": "yes" or "no"
        }},
        ...
      }}
    }},
    ...
  ]
}}

RULES:
- "reason_for_failure" MUST be a JSON array with one object per SOP instruction.
- Each object has exactly three keys: the exception_name, "status", and "matching".
- If a SOP check passes: set the exception_name value to "none" and "status" to "yes".
- If a SOP check fails: set the exception_name value to a clear reason string and "status" to "no".
- "match_status" should be "yes" ONLY if ALL SOP checks pass (all statuses are "yes").
- "matching" shows the actual field values from each document that were compared.
  Keys are field names, values are objects mapping document_name to extracted value.
  Include ALL fields that were compared, whether they matched or not.
- IMPORTANT: Each field object inside "matching" MUST include a "matched" key set to "yes" if the values across all documents are equal/consistent for that field, or "no" if there is any mismatch.
- NUMERIC COMPARISON: When comparing numerical values, ignore trailing decimal zeros — values like 1600, 1600.0, 1600.00, and 1600.000 are considered identical. Compare only the significant numeric value.

--- EXTRACTED DOCUMENTS ---
{json.dumps(extracted_documents, indent=2, default=str)}

--- SOP INSTRUCTIONS ---
{json.dumps(sop_instructions, indent=2, default=str)}
"""

        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 16384,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
            }
        )

        response = bedrock_runtime.invoke_model(
            modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        input_tokens = int(headers.get("x-amzn-bedrock-input-token-count", 0))
        output_tokens = int(headers.get("x-amzn-bedrock-output-token-count", 0))

        print("RECONCILEEEE HEADERS: ", headers)
        print("INPUTTTTT: ", input_tokens)
        print("OUTPUTTTT: ", output_tokens)

        resp_body = json.loads(response["body"].read())
        raw_text = resp_body["content"][0]["text"]

        try:
            result = json.loads(raw_text)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", raw_text, re.DOTALL)
            if match:
                result = json.loads(match.group())
            else:
                result = {
                    "match_status": "no",
                    "reason_for_failure": [{"parse_error": "Failed to parse LLM response", "status": "no", "matching": {}}],
                }

        if not isinstance(result.get("reason_for_failure"), list):
            result["reason_for_failure"] = [{"unknown": str(result.get("reason_for_failure", "none")), "status": "no", "matching": {}}]

        for entry in result["reason_for_failure"]:
            if "matching" not in entry:
                entry["matching"] = {}

        # Convert extracted_documents to list format for DB storage
        extracted_fields = [
            {doc_name: fields}
            for doc_name, fields in extracted_documents.items()
        ]

        print("REASON FOR FAILUREEEE", result["reason_for_failure"])
        _execute(
            f"""
            UPDATE {db_schema}.{session_table}
            SET
                reconcile_status   = %s,
                reason_for_failure = %s::jsonb,
                extracted_fields   = %s::jsonb,
                updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
            WHERE session_id = %s
            """,
            (
                result["match_status"],
                json.dumps(result["reason_for_failure"]),
                json.dumps(extracted_fields),
                session_id,
            ),
        )

        result["input_token"] = input_tokens
        result["output_token"] = output_tokens
        result["session_id"] = session_id
        result["job_id"] = job_id
        result["created_by"] = created_by
        result["extracted_fields"] = extracted_fields
        result["message"] = "Session updated successfully"

        print("RECONCILE_RESULT: ", result)
        return result

    except Exception as e:
        print("RECONCILE_ERROR: ", e)
        error_result = {
            "match_status": "no",
            "reason_for_failure": [{"error": str(e), "status": "no", "matching": {}}],
        }

        _execute(
            f"""
            UPDATE {db_schema}.{session_table}
            SET
                reconcile_status   = 'failed',
                reason_for_failure = %s::jsonb,
                updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
            WHERE session_id = %s
            """,
            (json.dumps(error_result["reason_for_failure"]), session_id),
        )

        return error_result


# ---------------------------------------------------------------------------
# Lambda / entry-point handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context: Any = None) -> dict:
    event_type = event.get("event_type")

    if event_type == "reconcile_documents":
        job_id     = event.get("job_id")
        documents  = event.get("documents")
        created_by = event.get("created_by", "system")
        session_id = event.get("session_id")
        sub_flag   = event.get("sub_flag", "")

        session_table = "session_table_test" if sub_flag == "testing" else "session_table"

        # ---------------------------------------------------------------------------
        # Determine module and log_type for token_log_table:
        #
        #   sub_flag == "testing"  →  module = "sop_control_center"
        #                             log_type = value of event["type"] if provided,
        #                             one of: new_sop_testing | existing_sop_testing | sop_version_testing
        #                             fallback: "new_sop_testing"
        #
        #   anything else          →  module = "reconcile_agent"
        #                             log_type = "reconcile_reupload" if session already exists,
        #                             else "reconciliation"
        # ---------------------------------------------------------------------------
        existing = _query(
            f"SELECT id FROM {db_schema}.{session_table} WHERE session_id = %s",
            (session_id,),
        )

        VALID_SOP_TYPES = {"new_sop_testing", "existing_sop_testing", "sop_version_testing"}

        if sub_flag == "testing":
            log_module = "sop_control_center"
            requested_type = event.get("type", "")
            log_type = requested_type if requested_type in VALID_SOP_TYPES else "new_sop_testing"
        else:
            log_module = "reconcile_agent"
            log_type = "reconcile_reupload" if existing else "reconciliation"

        if existing:
            _execute(
                f"""
                UPDATE {db_schema}.{session_table}
                SET
                    job_id             = %s,
                    documents          = %s::jsonb,
                    reconcile_status   = 'processing',
                    reason_for_failure = %s::jsonb,
                    created_by         = %s,
                    updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
                WHERE session_id = %s
                """,
                (
                    job_id,
                    json.dumps(documents),
                    json.dumps({"status": "processing"}),
                    created_by,
                    session_id,
                ),
            )
        else:
            _execute(
                f"""
                INSERT INTO {db_schema}.{session_table}
                    (session_id, job_id, documents, reconcile_status,
                    reason_for_failure, created_by, created_at)
                VALUES (
                    %s, %s, %s::jsonb, 'processing',
                    %s::jsonb, %s,
                    (now() AT TIME ZONE 'Asia/Kolkata')
                )
                """,
                (
                    session_id, job_id, json.dumps(documents),
                    json.dumps({"status": "processing"}), created_by,
                ),
            )

        if not job_id or not documents or not session_id:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {"error": "job_id, session_id, and documents are required"}
                ),
            }

        # Upsert to ensure processing row always exists
        _execute(
            f"""
            INSERT INTO {db_schema}.{session_table}
                (session_id, job_id, documents, reconcile_status,
                reason_for_failure, created_by, created_at)
            VALUES (
                %s, %s, %s::jsonb, 'processing',
                %s::jsonb, %s,
                (now() AT TIME ZONE 'Asia/Kolkata')
            )
            ON CONFLICT (session_id) DO UPDATE SET
                reconcile_status   = 'processing',
                reason_for_failure = %s::jsonb,
                updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
            """,
            (
                session_id, job_id, json.dumps(documents),
                json.dumps({"status": "processing"}), created_by,
                json.dumps({"status": "processing"}),
            ),
        )

        # Extract actual filenames from the documents S3 URIs for token logging
        doc_filenames = [
            _extract_filename_from_s3_uri(s3_uri)
            for s3_uri in documents.values()
        ]

        # Initialise token counters so the except block can always reference them
        ex_input = ex_output = re_input = re_output = 0

        try:
            # STEP 1 – Fetch job + document metadata
            job_info = get_job_and_documents(job_id)
            if "error" in job_info:
                raise RuntimeError(f"get_job_and_documents failed: {job_info['error']}")
            print("STEP 1 - job_info:", job_info)

            needed_fields = {
                doc_name: doc_meta["needed_fields"]
                for doc_name, doc_meta in job_info["documents"].items()
            }

            # STEP 2 – Textract the uploaded documents
            textract_results = textract_documents(documents)
            print("STEP 2 - textract_results:", textract_results)

            reference_document_textract = None
            if "reference_document" in textract_results:
                reference_document_textract = textract_results.pop("reference_document")
                print("STEP 2.1 - reference_document separated:", reference_document_textract)

            # STEP 2.5 – Validate document_key presence in Textract output
            mismatches = []
            for doc_name, doc_meta in job_info["documents"].items():
                if doc_name == "reference_document":
                    continue
                if not doc_meta.get("document_key_is_enabled"):
                    continue
                document_key = doc_meta.get("document_key", "")
                if not document_key:
                    continue
                raw_text = textract_results.get(doc_name, {}).get("raw_text", "")
                if document_key.upper() not in raw_text.upper():
                    mismatches.append({
                        "document_name": doc_name,
                        "expected_key": document_key,
                        "message": f"Document '{doc_name}' does not contain expected key '{document_key}'. Possibly wrong document uploaded."
                    })
            print("STEP 2.5 - VALIDATION:", mismatches)

            if mismatches:
                print("STEP 2.5 - VALIDATION FAILED:", mismatches)
                _execute(
                    f"""
                    UPDATE {db_schema}.{session_table}
                    SET
                        reconcile_status   = 'Validation Failed',
                        reason_for_failure = %s::jsonb,
                        updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
                    WHERE session_id = %s
                    """,
                    (json.dumps({"validation_errors": mismatches}), session_id),
                )
                return {
                    "statusCode": 400,
                    "body": json.dumps({
                        "error": "Document validation failed",
                        "mismatches": mismatches,
                        "session_id": session_id,
                    }),
                }

            print("STEP 2.5 - All document keys validated successfully")

            # STEP 3 – Fetch and Textract SOP documents
            sop_results = fetch_sop_documents(job_id, sub_flag=sub_flag)
            if "error" in sop_results:
                raise RuntimeError(f"fetch_sop_documents failed: {sop_results['error']}")
            print("STEP 3 - sop_results:", sop_results)

            # STEP 4 – Extract fields using LLM
            if reference_document_textract is not None:
                print("REFERENCE DOCUMENT DOES EXIST")
                textract_results["reference_document"] = reference_document_textract

            print("TEXTRACT AFTER adding reference document: ", textract_results)

            extraction = extract_fields_from_documents(
                needed_fields=needed_fields,
                document_textract_outputs=textract_results,
                sop_textract_outputs=sop_results,
            )
            if "error" in extraction:
                raise RuntimeError(f"extract_fields_from_documents failed: {extraction['error']}")
            print("STEP 4 - extraction:", extraction)

            textract_results.pop("reference_document", None)

            reference_document_extracted = None
            if "reference_document" in extraction.get("extracted_documents", {}):
                reference_document_extracted = extraction["extracted_documents"].pop("reference_document")
                print("STEP 4.1 - reference_document extracted fields separated:", reference_document_extracted)

            ex_input  = extraction["input_token"]
            ex_output = extraction["output_token"]

            # STEP 5 – Reconcile and save to DB
            reconcile_result = reconcile_documents(
                extracted_documents=extraction["extracted_documents"],
                sop_instructions=extraction["sop_instructions"],
                session_id=session_id,
                job_id=job_id,
                created_by=created_by,
                sub_flag=sub_flag,
            )
            print("STEP 5 - reconcile_result:", reconcile_result)

            re_input  = reconcile_result["input_token"]
            re_output = reconcile_result["output_token"]

            # Re-insert reference_document into extracted_fields for DB storage
            if reference_document_extracted is not None:
                ref_actual_name = job_info["documents"]["reference_document"]["document_name"]
                reconcile_result["extracted_fields"].append(
                    {ref_actual_name: reference_document_extracted}
                )

                updated_documents = dict(documents)
                if "reference_document" in updated_documents:
                    updated_documents[ref_actual_name] = updated_documents.pop("reference_document")

                _execute(
                    f"""
                    UPDATE {db_schema}.{session_table}
                    SET
                        extracted_fields = %s::jsonb,
                        documents        = %s::jsonb
                    WHERE session_id = %s
                    """,
                    (
                        json.dumps(reconcile_result["extracted_fields"]),
                        json.dumps(updated_documents),
                        session_id,
                    ),
                )
                print("STEP 5.1 - reference_document re-inserted as:", ref_actual_name)

            total_input  = ex_input + re_input
            total_output = ex_output + re_output

            # Update session row with token counts
            _execute(
                f"""
                UPDATE {db_schema}.{session_table}
                SET
                    input_tokens_used  = %s,
                    output_tokens_used = %s
                WHERE session_id = %s
                """,
                (total_input, total_output, session_id),
            )

            # ---------------------------------------------------------------
            # Insert token log row into token_log_table.
            # Fetch the session PK using session_id (guaranteed to exist now).
            # module and log_type are resolved earlier based on sub_flag + type.
            # ---------------------------------------------------------------
            session_pk = _get_session_pk(session_id, session_table)
            _insert_token_log(
                module=log_module,
                log_type=log_type,
                ref_table=session_table,
                ref_id=session_pk,
                document_names=doc_filenames,
                input_tokens=total_input,
                output_tokens=total_output,
            )

            # Update metadata_table totals (trigger auto-updates total_tokens_used)
            _update_metadata_tokens()

            return {
                "statusCode": 200,
                "body": json.dumps(reconcile_result, default=str),
            }

        except Exception as exc:
            logger.exception("Direct reconcile failed for session_id=%s", session_id)

            total_input  = ex_input + re_input
            total_output = ex_output + re_output

            _execute(
                f"""
                UPDATE {db_schema}.{session_table}
                SET
                    reconcile_status   = 'failed',
                    reason_for_failure = %s::jsonb,
                    input_tokens_used  = %s,
                    output_tokens_used = %s,
                    updated_at         = (now() AT TIME ZONE 'Asia/Kolkata')
                WHERE session_id = %s
                """,
                (
                    json.dumps({"error": str(exc)[:1000]}),
                    total_input,
                    total_output,
                    session_id,
                ),
            )

            # Log tokens even on failure (tokens were still consumed)
            session_pk = _get_session_pk(session_id, session_table)
            _insert_token_log(
                module=log_module,
                log_type=log_type,
                ref_table=session_table,
                ref_id=session_pk,
                document_names=doc_filenames,
                input_tokens=total_input,
                output_tokens=total_output,
            )

            # Update metadata_table totals
            _update_metadata_tokens()

            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(exc), "session_id": session_id}),
            }

    return {
        "statusCode": 400,
        "body": json.dumps({"error": f"Unknown event_type: {event_type}"}),
    }