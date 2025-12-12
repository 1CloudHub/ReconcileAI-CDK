import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, BotoCoreError
import os
import shutil
import glob
import re
import secrets
from typing import List, Dict, Tuple, Any, Optional
import math
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from strands import Agent, tool
from strands.models import BedrockModel

import os
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from strands import Agent, tool
from strands.models import BedrockModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
import random
import requests

# Global variable to store final response text
final_text_ff = ""

# ------------------------------------------------------------
# WebSocket and Callback Handler Functions
# ------------------------------------------------------------

def send_ws_message(connection_id, data):
    """
    Send WebSocket message to the connected client
    
    Args:
        connection_id: WebSocket connection ID
        data: Message data (dict or string)
    """
    client = boto3.client(
        'apigatewaymanagementapi',
        endpoint_url="https://0if9awq559.execute-api.us-west-2.amazonaws.com/production"
    )

    if isinstance(data, dict):
        data = json.dumps(data)

    try:
        response = client.post_to_connection(
            ConnectionId=connection_id,
            Data=data
        )
        return response
    except client.exceptions.GoneException:
        print(f"Connection {connection_id} is gone.")
    except Exception as e:
        print(f"Error sending message: {e}")

def make_callback_handler(connectionId, message_id):
    global final_text_ff
    """
    Create a callback handler for streaming agent responses via WebSocket
   
    Args:
        connectionId: WebSocket connection ID
        message_id: Message ID for tracking
       
    Returns:
        Callback handler function
    """

    state = {"buffer": []}
   
    def custom_callback_handler(**kwargs):
        """
        Custom callback handler to process streaming data from the agent.
        Streams intermediate tokens and final response via WebSocket.
        """
        # Handle streaming data (intermediate tokens)
        if "data" in kwargs:
            token = kwargs["data"]
            state["buffer"].append(token)
            send_ws_message(connectionId, {
                "type": "thinking",
                "message": token,
                "message_id": message_id
            })
       
        # Handle final assistant message
        if "message" in kwargs and kwargs["message"].get("role") == "assistant":
            final_text = kwargs["message"].get("content", "")
            final_text_ff = final_text
            send_ws_message(connectionId, {
                "type": "answer",
                "message": final_text,
                "message_id": message_id
            })
            state["buffer"].clear()
   
    return custom_callback_handler


# ------------------------------------------------------------
# Fetch and store metadata locally
# ------------------------------------------------------------

@tool
def fetch_metadata(
    bucket_name: str = "sap-mcp-data-server",
    object_key: str = "kb_data/metadata.txt",
    folder_name: str = "ragbot_documents",
    file_name: str = "metadata.json",
) -> List[Dict[str, Any]]:
    """
    Fetch metadata.txt from S3, parse its JSON array, and store it as
    ragbot_documents/metadata.json locally. Returns the parsed list.
    """
    s3_client = boto3.client("s3")

    # Fetch S3 file
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        text = response["Body"].read().decode("utf-8")
    except ClientError as e:
        raise RuntimeError(f"Failed to fetch {object_key} from {bucket_name}: {e}")

    try:
        metadata_list = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"metadata.txt content is not valid JSON: {e}")

    if not isinstance(metadata_list, list):
        raise ValueError("metadata.txt must contain a JSON array.")

    # Ensure folder exists
    os.makedirs(folder_name, exist_ok=True)

    # Write metadata.json
    output_path = os.path.join(folder_name, file_name)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metadata_list, f, indent=2, ensure_ascii=False)

    return metadata_list


# ------------------------------------------------------------
# Generic reusable S3 fetcher
# Saves files locally + returns contents
# ------------------------------------------------------------

@tool
def fetch_s3_data(
    object_names: List[str],
    bucket_name: str = "sap-mcp-data-server",
    base_prefix: str = "kb_data",
    output_folder: str = "ragbot_documents",
) -> Dict[str, str]:
    """
    Fetch multiple text files from S3 under base_prefix, save them locally
    into output_folder, and return a dict of {object_name: file_content}.
    """
    s3_client = boto3.client("s3")

    os.makedirs(output_folder, exist_ok=True)

    file_contents: Dict[str, str] = {}

    for obj_name in object_names:
        s3_key = f"{base_prefix}/{obj_name}"
        local_path = os.path.join(output_folder, obj_name)

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            text = response["Body"].read().decode("utf-8")
        except ClientError as e:
            raise RuntimeError(f"Failed to fetch {s3_key} from {bucket_name}: {e}")

        # Save locally
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(text)

        # Record content
        file_contents[obj_name] = text

    return file_contents


# ------------------------------------------------------------
# Specific File Fetchers (PO / GRN / Invoice)
# ------------------------------------------------------------

@tool
def fetch_po_files(
    po_numbers: List[str],
    bucket_name: str = "sap-mcp-data-server",
    base_prefix: str = "kb_data",
    root_folder: str = "ragbot_documents",
) -> Dict[str, str]:
    """
    Fetch multiple PO text files (PO_<PO>.txt) from S3, save them under
    ragbot_documents/po, and return {po_number: file_content}.
    """
    folder = os.path.join(root_folder, "po")
    object_names = [f"PO_{po}.txt" for po in po_numbers]

    result = fetch_s3_data(
        object_names=object_names,
        bucket_name=bucket_name,
        base_prefix=base_prefix,
        output_folder=folder,
    )

    return {po: result[f"PO_{po}.txt"] for po in po_numbers}


@tool
def fetch_grn_files(
    grn_numbers: List[str],
    bucket_name: str = "sap-mcp-data-server",
    base_prefix: str = "kb_data",
    root_folder: str = "ragbot_documents",
) -> Dict[str, str]:
    """
    Fetch multiple GRN text files (GRN_<GRN>.txt) from S3, save them under
    ragbot_documents/grn, and return {grn_number: file_content}.
    """
    folder = os.path.join(root_folder, "grn")
    object_names = [f"GRN_{grn}.txt" for grn in grn_numbers]

    result = fetch_s3_data(
        object_names=object_names,
        bucket_name=bucket_name,
        base_prefix=base_prefix,
        output_folder=folder,
    )

    return {grn: result[f"GRN_{grn}.txt"] for grn in grn_numbers}


@tool
def fetch_invoice_files(
    invoice_numbers: List[str],
    bucket_name: str = "sap-mcp-data-server",
    base_prefix: str = "kb_data",
    root_folder: str = "ragbot_documents",
) -> Dict[str, str]:
    """
    Fetch multiple Invoice text files (INV_<INV>.txt) from S3, save them under
    ragbot_documents/invoice, and return {invoice_number: file_content}.
    """
    folder = os.path.join(root_folder, "invoice")
    object_names = [f"INV_{inv}.txt" for inv in invoice_numbers]

    result = fetch_s3_data(
        object_names=object_names,
        bucket_name=bucket_name,
        base_prefix=base_prefix,
        output_folder=folder,
    )

    return {inv: result[f"INV_{inv}.txt"] for inv in invoice_numbers}


# ------------------------------------------------------------
# Date/Time Utility Tool
# ------------------------------------------------------------

@tool
def get_current_date_info() -> Dict[str, str]:
    """
    Get the current date and time information for filtering documents by date.
    
    Returns a dictionary with:
    - current_date: Current date in YYYY-MM-DD format
    - current_datetime: Current date and time in ISO format
    - today_start: Start of today (00:00:00) in YYYY-MM-DD format
    - today_end: End of today (23:59:59) in YYYY-MM-DD format
    - week_start: Start of current week (Monday) in YYYY-MM-DD format
    - week_end: End of current week (Sunday) in YYYY-MM-DD format
    - month_start: Start of current month in YYYY-MM-DD format
    - month_end: End of current month in YYYY-MM-DD format
    
    Use this tool when questions involve:
    - "today", "posted today", "created today"
    - "this week", "this month"
    - Date comparisons or filtering by date ranges
    """
    now = datetime.now()
    today = now.date()
    
    # Calculate week start (Monday) and end (Sunday)
    days_since_monday = today.weekday()
    week_start = today - timedelta(days=days_since_monday)
    week_end = week_start + timedelta(days=6)
    
    # Calculate month start and end
    month_start = today.replace(day=1)
    # Get last day of month
    if today.month == 12:
        month_end = today.replace(day=31)
    else:
        next_month = today.replace(month=today.month + 1, day=1)
        month_end = next_month - timedelta(days=1)
    
    return {
        "current_date": today.strftime("%Y-%m-%d"),
        "current_datetime": now.isoformat(),
        "today_start": today.strftime("%Y-%m-%d"),
        "today_end": today.strftime("%Y-%m-%d"),
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end": week_end.strftime("%Y-%m-%d"),
        "month_start": month_start.strftime("%Y-%m-%d"),
        "month_end": month_end.strftime("%Y-%m-%d"),
    }


# ------------------------------------------------------------
# STRANDS AGENT DEFINITION
# ------------------------------------------------------------

AGENT_SYSTEM_PROMPT = """
You are a procurement and document analysis agent for SAP-like data.

You work with three types of JSON documents stored in S3 and mirrored locally
under 'ragbot_documents':

1) Purchase Orders (PO)
   - Header fields (examples):
       PurchaseOrder, Vendor, CompanyCode, DocumentCurrency,
       CreatedOn, POHeaderNetValue
   - Items[] fields (examples):
       PurchaseOrderItem, Material, MaterialDescription, Plant,
       OrderQuantity, OrderUnit, OrderPrice, NetAmount, DeliveryDate

2) Goods Receipts (GR / GRN)
   - Header fields (examples):
       MaterialDocument, PurchaseOrder, PostingDate, CompanyCode
   - Items[] fields (examples):
       MaterialDocumentItem, PurchaseOrderItem, Material,
       QuantityInEntryUnit, EntryUnit, AmountInDocumentCurrency, GRStatus

3) Invoices
   - Header fields (examples):
       BillingDocument, Supplier, CompanyCode, DocumentCurrency, PostingDate
   - Items[] fields (examples):
       BillingDocumentItem, ReferencePurchaseOrder, ReferencePurchaseOrderItem,
       Material, BillingQuantity, BillingQuantityUnit,
       InvoiceUnitPrice, NetValueAmount

Metadata:
- The metadata file in S3 ('kb_data/metadata.txt') is a JSON array of entries:
    { "po": "<PO>", "grn": "<GRN>", "invoice": "<inv>" }
  or:
    { "po": "<PO>", "grn": "<GRN>", "invoice": ["<inv1>", "<inv2>", ...] }
- Use this to map relationships between POs, GRNs, and invoices.

TOOLS:
- fetch_metadata
- fetch_po_files
- fetch_grn_files
- fetch_invoice_files
- fetch_s3_data
- get_current_date_info: Use this when questions involve "today", "this week", "this month", or any date-based filtering

BEHAVIOR:
- You must be able to answer any question related to:
    * Any single field in PO / GRN / Invoice documents
    * Comparisons within a document set (e.g., compare POs by vendor, value, date)
    * Cross-document logic (e.g., PO vs GRN vs Invoice for 3-way checks)
    * Counts and aggregates (e.g., number of POs, sum of NetValueAmount, etc.)
    * Date-based filtering (e.g., "GRs posted today", "invoices this week", "last GR for PO")
    
- For date-based questions (today, this week, this month), first call get_current_date_info to get the current date ranges, then filter documents by comparing PostingDate, CreatedOn, or DeliveryDate fields with the returned date ranges.

- When a question involves relationships (PO vs GR vs Invoice), typically:
    1) Call fetch_metadata to identify which GRNs and invoices relate to the POs.
    2) Use fetch_po_files / fetch_grn_files / fetch_invoice_files with the relevant IDs.
    3) Parse the returned JSON strings into structured data in your reasoning.
    4) Perform comparisons on quantities, amounts, dates, vendors, plants, etc.

ANSWER STYLE:
- Be precise, structured, and data-driven.
- Use paragraph-based or bullet-point formatting. DO NOT use tables or tabular formats as they do not display well in streaming responses.
- Clearly show comparisons in natural language, e.g.:
    "PO 4600..., item 00010: ordered 120 EA, received 80 EA, invoiced 120 EA."
- When summarizing sets (e.g., list of POs), use bullet points or numbered lists with clear descriptions for each item.
- Format multi-item responses as:
    • Item 1: [description with key fields]
    • Item 2: [description with key fields]
- If certain data (PO, GRN, or Invoice) is missing, state that explicitly.
"""

model = BedrockModel(
    model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
)

procurement_agent = Agent(
    model=model,
    tools=[
        fetch_metadata,
        fetch_s3_data,
        fetch_po_files,
        fetch_grn_files,
        fetch_invoice_files,
        get_current_date_info,
    ],
    system_prompt=AGENT_SYSTEM_PROMPT,
)


# ------------------------------------------------------------
# FastAPI Application Setup
# ------------------------------------------------------------

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)


@app.get("/health_check")
async def check_health():
    """Health check endpoint"""
    return {"status": "ok"}


@app.post("/process")
async def create_item(request: Request):
    """
    Process procurement analysis requests with streaming support
   
    Receives user question, connection_id, message_id, and userid.
    Streams intermediate results and final response via WebSocket.
    """
    global final_text_ff
    event = await request.json()

    user_input = event.get("user_question")
    connection_id = event.get("connection_id")
    message_id = event.get("message_id")
    userid = event.get("userid")
   
    # Log request details
    logger.info(f"Processing request - Connection: {connection_id}, Message: {message_id}, UserID: {userid}")
    logger.info(f"User question: {user_input}")

    try:
        # Call agent with streaming callback handler
        response = procurement_agent(
            user_input,
            callback_handler=make_callback_handler(connection_id, message_id)
        )

        # Extract response text
        response_text = response.message["content"][0]["text"]
        logger.info(f"Agent processing completed. Response length: {len(response_text)}")

        # Send final response as backup (in case streaming didn't capture everything)
        # This ensures the message is delivered even if callback handler had issues
        if response_text and response_text != final_text_ff:
            send_ws_message(
                connection_id,
                {"message": response_text, "message_id": message_id}
            )
       
        return {"status": "success", "message": "Processing completed"}

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        # Send error message to frontend
        send_ws_message(
            connection_id,
            {
                "message": f"Error processing your request: {str(e)}",
                "message_id": message_id,
                "type": "error"
            }
        )
        return {"status": "error", "message": str(e)}


# ---------------- TEST ENTRY =================
if __name__ == "__main__":
    uvicorn.run("mcp_alternative_agent_main_code:app", host="0.0.0.0", port=8083, reload=True)
    
    # Interactive console (commented out - use /process endpoint instead)
    # print("Interactive Procurement Agent console")
    # print("Type your question about POs / GRNs / Invoices.")
    # print("Type 'exit' or 'quit' to stop.\n")
    #
    # # Interactive loop
    # running = True
    # while running:
    #     ask = input("\nEnter your question (type 'exit' to quit): ")
    #     if ask.lower() == "exit":
    #         running = False
    #         break
    #     response = procurement_agent(ask)
    #     print("\n[SAP Agent Response]:")













    # Basic metadata test (requires AWS credentials and S3 objects)
    # fetch_metadata()

    # # Example tests for the new functions (will work if S3 objects exist and creds are set)
    # po_example = ["4600011198", "4600078912"]
    # grn_example = ["5100077211", "5100083229"]
    # inv_example = ["90018881", "90019921", "90025501", "90025522"]

    # fetch_po_files(po_example)
    # fetch_grn_files(grn_example)
    # fetch_invoice_files(inv_example)

    # Example agent usage (uncomment if your Bedrock/Strands environment is configured):
    # response = procurement_agent.chat("Has PO 4600011198 been fully delivered?")
    # print(response.message)
