"""
Fetch environment values and credentials from AWS Secrets Manager.
All config is loaded from a fixed secret name.
"""
import json
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

secret_cache: Optional[Dict[str, Any]] = None
config_cache: Optional[Dict[str, Any]] = None


def get_secret() -> Dict[str, Any]:
    """Fetch secrets from AWS Secrets Manager. Cached after first call."""
    global secret_cache
    if secret_cache is not None:
        return secret_cache

    region_name = os.getenv("AWS_REGION_NAME") or "us-west-2"
    client = boto3.client("secretsmanager", region_name=region_name)

    secret_name = os.environ.get("SECRET_NAME")
    if not secret_name:
        raise RuntimeError("SECRET_NAME environment variable not set")

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString")
        if not secret_string:
            raise RuntimeError("SecretString is empty")
        secret_cache = json.loads(secret_string)
    except ClientError as e:
        raise RuntimeError(f"Unable to fetch secret: {e}")

    return secret_cache


def get_config_value(key: str) -> str:
    """Fetch config value from secret. Returns empty string if key missing."""
    secret = get_secret()
    if key in secret and secret[key] is not None:
        return str(secret[key]).strip()
    return ""


def get_config() -> Dict[str, Any]:
    """Return application configuration from secret. Cached after first call."""
    global config_cache
    if config_cache is not None:
        return config_cache

    def val(k: str) -> str:
        v = get_config_value(k)
        if not v:
            print(f"Missing config key: {k}")
        return v

    aws_region = val("REGION_AWS") or val("AWS_REGION")

    config_cache = {
        "db_host": val("db_host"),
        "db_port": val("db_port"),
        "db_user": val("db_user"),
        "db_password": val("db_password"),
        "db_database": val("db_database"),
        "schema": val("Textops_schema"),
        "bucket_name_no_sap": val("bucket_name_no_sap"),
        "region_name": val("REGION_AWS") ,
        "BEDROCK_MODEL_ID": val("BEDROCK_MODEL_ID"),
        "model_id": val("BEDROCK_MODEL_ID") ,
        "document_type_table": val("document_type_table"),
        "document_table": val("document_table"),
        "job_table": val("job_table"),
        "sop_table": val("sop_table"),
        "erp_no_sap_schema": val("erp_no_sap_schema"),
        "document_processing_table": val("document_processing_table"),
        "prompt_metadata_table": val("prompt_metadata_table"),
        "ai_suggestion_table": val("ai_suggestion_table"),
        "temp_document_processing_table": val("temp_document_processing_table"),
        "cexp_ocr_ai_key_extraction_details_table": val("cexp_ocr_ai_key_extraction_details_table"),
        "list_documents_schema": val("Textops_schema"),
        "list_documents_table": val("document_processing_table"),
        "list_document_type_schema": val("Textops_schema"),
        "list_document_type_table":  val("document_type_table"),
        "COGNITO_USER_POOL_ID": val("COGNITO_USER_POOL_ID_NO_SAP"),
        "COGNITO_CLIENT_ID": val("COGNITO_CLIENT_ID_NO_SAP"),
    }
    print(config_cache,"config_cache")

    print("Loaded config from Secrets Manager")
    return config_cache


def get_db_config() -> Dict[str, Any]:
    """Database connection config for db module."""
    c = get_config()
    port = c.get("db_port")
    try:
        port = int(port) if port else 5432
    except (TypeError, ValueError):
        port = 5432
    return {
        "host": c.get("db_host", ""),
        "port": port,
        "user": c.get("db_user", ""),
        "password": c.get("db_password", ""),
        "database": c.get("db_database", ""),
    }

# def generate_presigned_upload_url(exception_id: str, filename: str, expiry: int = 3600):
#     """
#     Generate a presigned S3 upload URL for a SOP document.
#     Returns upload_url, s3_uri, s3_key, and expiry.
#     """
#     config = get_config()  # no need for utils.get_config() since we're inside utils.py
#     region = config.get("region_name", "us-west-2")
#     bucket_name = config.get("bucket_name_no_sap")

#     s3_key = f"sop_documents/{exception_id}/{filename}"
#     s3_uri = f"s3://{bucket_name}/{s3_key}"

#     s3_client = boto3.client("s3", region_name=region)

#     upload_url = s3_client.generate_presigned_url(
#         "put_object",
#         Params={"Bucket": bucket_name, "Key": s3_key, "ContentType": "application/pdf"},
#         ExpiresIn=expiry,
#     )

#     return {
#         "upload_url": upload_url,
#         "s3_uri": s3_uri,
#         "s3_key": s3_key,
#         "expiry": expiry,
#     }
def generate_presigned_upload_url(exception_id: str,filename: str,expiry: int = 3600,reconcile_type: str = "sop_control"):
    try:
        config = get_config()
        region = config.get("region_name", "us-west-2")
        bucket_name = config.get("bucket_name_no_sap")

        if not exception_id or not filename:
            raise ValueError("exception_id and filename are required")

        if not filename.lower().endswith(".pdf"):
            raise ValueError("Only PDF files are allowed")

        if reconcile_type == "sop_control":
            folder = "sop_documents"
        elif reconcile_type == "reconcile_agent":
            folder = "reconcile_agent"
        elif reconcile_type == "sop_test":
            folder = "sop_test"
        else:
            raise ValueError(f"Invalid reconcile_type: {reconcile_type}")

        s3_key = f"{folder}/{exception_id}/{filename}"
        s3_uri = f"s3://{bucket_name}/{s3_key}"

        s3_client = boto3.client("s3", region_name=region)
        upload_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": bucket_name,
                "Key": s3_key,
                "ContentType": "application/pdf"
            },
            ExpiresIn=expiry,
        )

        return {
            "upload_url": upload_url,
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "expiry": expiry,
        }

    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        raise
          

def generate_presigned_download_url(s3_uri: str, expiry: int = 3600):
    """
    Generate a presigned S3 download URL for a SOP document.
    Parses s3_uri to extract bucket and key.
    """
    try:
        # Parse s3_uri → s3://bucket-name/key
        s3_uri_stripped = s3_uri.replace("s3://", "")
        bucket_name, s3_key = s3_uri_stripped.split("/", 1)

        config = get_config()
        region = config.get("region_name", "us-west-2")

        s3_client = boto3.client("s3", region_name=region)

        download_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket_name,
                "Key": s3_key,
            },
            ExpiresIn=expiry,
        )

        return {
            "download_url": download_url,
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "expiry": expiry,
        }

    except Exception as e:
        print(f"Failed to generate download presigned url: {e}")
        return None