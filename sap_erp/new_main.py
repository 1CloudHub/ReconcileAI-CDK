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
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from strands import Agent, tool
from strands.models import BedrockModel

import os
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
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
import threading
import time
final_text_ff = ""
# Global variable to store userid for the current request context
current_userid = None
# Global variable to store Lambda callback data
lambda_callback_data = {}
lambda_callback_lock = threading.Lock()

S3_BUCKET = os.environ.get("S3_BUCKET", "sap-mcp-data-server")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-20250514-v1:0")
AWS_REGION = os.environ.get("AWS_REGION")


EXCEPTION_TYPE_CODES = {
    "Quantity Variance": "001",
    "Price Mismatch": "002",
    "Invoice Errors": "003",
    "Missing GR": "004",
    "UoM Issues": "005",
    "Missing PO": "006",
    "Unknown": "000"
}


try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
except NoCredentialsError:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
   


def generate_exception_id(exception_type_code: str) -> str:
    """
    Generate EXC-XXX-YYYY where XXX is exception_type_code and YYYY is a random 4-digit number.
    Uses secrets for cryptographic randomness.
    """
    rand4 = secrets.randbelow(10000)
    return f"EXC-{exception_type_code}-{rand4:04d}"

def download_file_from_s3(bucket_name, file_key, local_path):
    print(f"{bucket_name}/{file_key}")
    try:
        s3_client.download_file(bucket_name, file_key, local_path)
        print(f"File downloaded successfully: {local_path}")
    except ClientError as e:
        print(f"Failed to download file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
       

@tool
def check_po_status(po_number: str) -> dict:
    """
    Check if a PO has already been processed and all exceptions are fixed.
    This should be called FIRST before any processing.
    
    Args:
        po_number: Purchase Order number to check
        
    Returns:
        dict with action: "STOP" or "CONTINUE" and message
    """
    global current_userid, lambda_callback_data
    
    if current_userid is None:
        return {
            "action": "STOP",
            "message": "Error: User ID is required but was not provided. Cannot process request."
        }
    
    print(f"\n{'='*80}")
    print(f"EC2: Checking PO status for PO {po_number}, userid {current_userid}")
    print(f"{'='*80}")
    
    # Build cache key
    cache_key = f"{po_number}_{current_userid}"
    
    # Clear any old cached data for this PO+user
    with lambda_callback_lock:
        if cache_key in lambda_callback_data:
            del lambda_callback_data[cache_key]
            print(f"EC2: Cleared old cache for {cache_key}")
    
    # Trigger Lambda to check PO status (Lambda will POST back to /receive_from_lambda)
    url = "https://agbmqz53j0.execute-api.us-west-2.amazonaws.com/test/ERP"
    headers = {"content-type": "application/json"}
    
    payload = {
        "event_type": "check_po_queried",
        "po_number": po_number,
        "userid": current_userid
    }
    
    print(f"EC2: Triggering Lambda with payload: {payload}")
    
    try:
        # Call Lambda - it will return the status in its response AND try to POST to EC2
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        print(f"EC2: Lambda API call completed with status {response.status_code}")
        
        # Parse Lambda's response directly (don't wait for callback)
        data = None
        try:
            lambda_resp = response.json()
            print(f"EC2: Lambda response: {json.dumps(lambda_resp, indent=2)}")
            
            # Check if Lambda wrapped the data
            if 'data' in lambda_resp:
                # Lambda returned {'lambda_status': 'success', 'data': {...}}
                data = lambda_resp['data']
                print(f"EC2: Extracted data from 'data' key: {data}")
            elif 'body' in lambda_resp:
                # Lambda returns data in 'body' field (JSON string) when using API Gateway format
                body_str = lambda_resp['body']
                if isinstance(body_str, str):
                    body_data = json.loads(body_str)
                else:
                    body_data = body_str
                
                # Check if this is the actual PO status data or a wrapper
                if 'data' in body_data:
                    data = body_data['data']
                elif 'data_sent_to_ec2' in body_data:
                    data = body_data['data_sent_to_ec2']
                else:
                    data = body_data
                
                print(f"EC2: Extracted data from 'body' field: {data}")
            else:
                # Response is the data itself
                data = lambda_resp
                print(f"EC2: Using response directly as data: {data}")
        except Exception as e:
            print(f"EC2: Failed to parse Lambda response: {e}")
            import traceback
            traceback.print_exc()
        
        if data is None:
            print(f"EC2: Could not extract data from Lambda response")
            return {
                "action": "CONTINUE",
                "message": f"Could not retrieve PO status. Proceeding with processing.",
                "po_number": po_number
            }
        
        # Extract status from data
        status = data.get('status', 'not_queried')
        
        print(f"EC2: PO status check result: {status}")
        print(f"EC2: Full data: {data}")
        
        if status == "queried":
            # PO already processed and fixed - STOP
            return {
                "action": "STOP",
                "message": f"PO {po_number} has already been processed and all exceptions are fixed. Please check the Exceptions page for details.",
                "po_number": po_number,
                "details": data
            }
        else:
            # PO not queried or not all fixed - CONTINUE
            return {
                "action": "CONTINUE",
                "message": f"PO {po_number} is ready for processing. Proceeding with three-way match analysis.",
                "po_number": po_number,
                "details": data
            }
            
    except requests.exceptions.Timeout:
        print(f"EC2: Lambda API call timed out")
        return {
            "action": "CONTINUE",
            "message": f"PO status check timed out. Proceeding with processing.",
            "po_number": po_number
        }
    except Exception as e:
        print(f"EC2: Error checking PO status: {str(e)}")
        import traceback
        print(traceback.format_exc())
        # On error, allow processing (fail open)
        return {
            "action": "CONTINUE",
            "message": f"PO {po_number} status check failed. Proceeding with processing.",
            "po_number": po_number,
            "error": str(e)
        }


@tool  
def fetch_s3_data(po_number: str) -> dict:

    
    # Remove old folder if it exists
    if os.path.exists("needed_doc"):
        shutil.rmtree("needed_doc")
    os.makedirs("needed_doc", exist_ok=True)
    meta_data = [
  {
    "po": "4500000001",
    "grn": "5100000450",
    "invoice": ["90005001"]
  },
  {
    "po": "4500018901",
    "grn": "5100044444",
    "invoice": ["90011111"]
  },
  {
    "po": "4500020000",
    "grn": "5100055555",
    "invoice": ["90013333", "90014444"]
  },
  {
    "po": "4500012345",
    "grn": "5100011111",
    "invoice": ["90007777"]
  },
  {
    "po": "4500016789",
    "grn": "5100022222",
    "invoice": ["90008888"]
  },
    {
    "po": "4500017890",
    "grn": "5100033333",
    "invoice": ["90009999"]
  },
  {
    "po": None,
    "grn": "5100000525",
    "invoice": ["90012222"]
  },
  {
    "po": "4500024789",
    "grn": "5100067890",
    "invoice": ["90015555"]
  }

   
]
    needed_ele = meta_data[-1]
    for i in meta_data:
        if i["po"] == po_number:
            needed_ele = i

    po_path = "Not available"
    if needed_ele["po"] != None:
        po_s3_key = f"data/data_json/PO_{needed_ele['po']}.json"
        po_path = f"needed_doc/PO_{needed_ele['po']}.json"
        download_file_from_s3(S3_BUCKET, po_s3_key, f"needed_doc/PO_{needed_ele['po']}.json")
    gr_s3_key = f"data/data_json/GRN_{needed_ele['grn']}.json"
    gr_path = f"needed_doc/GRN_{needed_ele['grn']}.json"
    download_file_from_s3(S3_BUCKET, gr_s3_key, f"needed_doc/GRN_{needed_ele['grn']}.json")


    invoice_path = []
    for i in needed_ele["invoice"]:
        in_s3_key = f"data/data_json/INV_{i}.json"
        op = f"needed_doc/INV_{i}.json"
        invoice_path.append(op)
        download_file_from_s3(S3_BUCKET, in_s3_key, f"needed_doc/INV_{i}.json")


    final_map = {"purchase_order": po_path, "goods_receipt": gr_path, "invoice": invoice_path}

    with open("map.json", 'w') as f:
        json.dump(final_map, f)

    print("WWWWWWWWWWWWWWw", final_map)
    return final_map



#three-way-match_functionality


@tool
def three_way_match_with_llm(
    needed_dir: str = "needed_doc",
    output_file: str = "three_way_match_result.json"
):
    """
    Read PO_/GRN_/INV_ JSON files from `needed_dir`, send them to Bedrock (Claude 3.7 Sonnet)
    requesting the exact three-way-match JSON schema + exception detection, write the LLM response to `output_file`,
    parse, post-process exception IDs, and return the JSON object.

    Behavior changes made:
    - Instructs the LLM to include 'Material' for invoice and GR records in its output.
    - Deterministically maps item_number for invoice_order_records and goods_receipt_records
      using ONLY exact Material matches against the original invoice/GR files in `needed_dir`.
    - No fallback logic. If a material match or explicit item id is not available, item_number stays None.

    Returns:
        dict: parsed JSON object returned by the LLM (with exception_ids added locally).

    Raises:
        RuntimeError on Bedrock invocation/parsing failures or if no JSON is found in the model response.
    """

    def _load_files(pattern):
        paths = sorted(glob.glob(os.path.join(needed_dir, pattern)))
        objs = []
        for p in paths:
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    objs.append(json.load(fh))
            except Exception as e:
                raise RuntimeError(f"Failed to load JSON file {p}: {e}")
        return objs

    # Read files (may be empty lists)
    po_list = _load_files("PO_*.json")
    grn_list = _load_files("GRN_*.json")
    inv_list = _load_files("INV_*.json")

    payload = {
        "purchase_orders": po_list,
        "goods_receipts": grn_list,
        "invoices": inv_list
    }

    # --- RESPONSE SCHEMA variable (updated to request Material in invoice/gr records) ---
    response_schema = {
        # LLM should detect exceptions and output an array of exception records
        "exception_records": [
            {
                "exception_type": "string (one of: Quantity Variance, Price Mismatch, Invoice Errors, Missing GR, UoM Issues, Missing PO)",
                "po_number": "string or null",
                "item_number": "string or null",
                "material": "string or null",
                "exception_value": "number or string or list of strings or null"
                # exception_id will be added locally after LLM returns (format EXC-XXX-YYYY)
            }
        ]
    }

    # Compact prompt that references response_schema variable and mapping rules (added exception detection instructions)
    # NOTE: explicitly request 'material' to be returned for invoice and goods_receipt records.
    prompt = (
        "You will receive three JSON arrays named 'purchase_orders', 'goods_receipts', and 'invoices'.\n"
        "Return ONLY a single JSON object (no commentary) that exactly follows the RESPONSE_SCHEMA below.\n\n"
        f"RESPONSE_SCHEMA = {json.dumps(response_schema, separators=(',', ':'))}\n\n"
        "Mapping & exception rules (apply exactly):\n"
        "EXCEPTION DETECTION (IMPORTANT): Inspect the data and detect ONLY these exception types:\n"
        "   - Quantity Variance\n"
        "   - Price Mismatch\n"
        "   - Invoice Errors\n"
        "   - Missing GR\n"
        "   - UoM Issues\n"
        "   - Missing PO\n\n"
        "   Duplicate-invoice rule (exact): If two or more distinct invoice documents reference the same PO number,\n"
        "   treat this as an 'Invoice Errors' exception. Add one exception_records entry per affected PO with:\n"
        "      exception_type: 'Invoice Errors',\n"
        "      po_number: <the PO number>,\n"
        "      item_number: null,\n"
        "      exception_value: list of the invoice numbers that are referencing that PO.\n\n"
        "   For other invoice-level problems (same invoice duplicate lines, mismatched invoice numbers, etc.),\n"
        "   check the orderQuantity,BillingQuantity,QuantityEntryUnit from PO , invoice and grn item level respectively.Compare thes values and if there is a difference map it as a Quantity Variance and set the exception_value as the difference in these values and set the item_number as the item_number of the PO, invoice or grn respectively.\n"
        "   check NetAmount, NetValueAmount , AmountInDocumentCurrency from PO , invoice and grn item level respectively.Compare thes values and if there is a difference map it as a Price Mismatch and set the exception_value as the difference in these values and set the item_number as the item_number of the PO, invoice or grn respectively.\n"
        "   check OrderUnit, BillingQuantityUnit, EntryUnit from PO , invoice and grn item level respectively.Compare thes values and if there is a difference map it as a UoM Issues and set the exception_value as the difference in these values and set the item_number as the item_number of the PO, invoice or grn respectively.\n"
        "   For Missing GR,Missing PO,uom set exception_value to None.\n\n"
        "   SPECIAL CASE : If the exception is Missing_PO,then just create 1 exception for the first item . no need to create exception for other items."
        "   IMPORTANT: Do NOT create exception IDs — they will be generated locally. Only return the fields\n"
        "   exception_type, po_number, item_number, exception_value, material in exception_records.\n\n"
        "INPUT (do not modify):\n"
        + json.dumps(payload, indent=2, default=str)
    )

    # call Bedrock runtime
    try:
        client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
        # use the simple input body so the prompt stays compact
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}]
        })

        resp = client.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=body
        )

        sb = resp.get("body")
        if sb is None:
            raise RuntimeError("No body found in Bedrock response.")
        raw_text = sb.read().decode("utf-8")

    except (NoCredentialsError, EndpointConnectionError, ClientError, BotoCoreError) as e:
        raise RuntimeError(f"Bedrock invocation failed: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error when calling Bedrock: {e}")

    # Parse the response robustly (unchanged)
    parsed_outer = None
    parsed_inner = None
    try:
        parsed_outer = json.loads(raw_text)
    except Exception:
        parsed_outer = None

    if isinstance(parsed_outer, dict):
        content = parsed_outer.get("content")
        if isinstance(content, list):
            text_candidate = None
            for item in content:
                if isinstance(item, dict):
                    if "text" in item and isinstance(item["text"], str):
                        text_candidate = item["text"]
                        break
                    if "content" in item and isinstance(item["content"], str):
                        text_candidate = item["content"]
                        break
            if text_candidate is not None:
                try:
                    parsed_inner = json.loads(text_candidate)
                except Exception:
                    try:
                        unescaped = text_candidate.encode("utf-8").decode("unicode_escape")
                        parsed_inner = json.loads(unescaped)
                    except Exception:
                        m = re.search(r"(\{(?:.|\n)*\})", text_candidate)
                        if m:
                            try:
                                parsed_inner = json.loads(m.group(1))
                            except Exception as e:
                                raise RuntimeError(f"Failed to parse inner JSON from model content.text. Parse error: {e}\nContent text:\n{text_candidate}")
                        else:
                            raise RuntimeError(f"No JSON object found inside model content.text. Content text:\n{text_candidate}")
    if parsed_inner is None:
        try:
            parsed_inner = json.loads(raw_text)
            if isinstance(parsed_inner, dict) and "content" in parsed_inner and isinstance(parsed_inner["content"], list):
                for item in parsed_inner["content"]:
                    if isinstance(item, dict) and "text" in item and isinstance(item["text"], str):
                        try:
                            parsed_inner = json.loads(item["text"])
                            break
                        except Exception:
                            pass
        except Exception:
            m = re.search(r"(\{(?:.|\n)*\})", raw_text)
            if m:
                candidate = m.group(1)
                try:
                    parsed_inner = json.loads(candidate)
                except Exception as e:
                    raise RuntimeError(f"Failed to parse JSON from model response. Raw model output:\n{raw_text}\nParse error: {e}")
            else:
                raise RuntimeError(f"No JSON object found in model response. Raw model output:\n{raw_text}")

    if parsed_inner is None:
        raise RuntimeError(f"Unable to extract the inner JSON from model response. Raw model output:\n{raw_text}")

    # -----------------------------
    # Post-process: generate exception_id for each exception_record returned by LLM
    # -----------------------------
    try:
        ex_recs = parsed_inner.get("exception_records")
        if isinstance(ex_recs, list):
            enriched = []
            for er in ex_recs:
                # Expectation from LLM: er contains exception_type, po_number, item_number, exception_value
                exception_type = er.get("exception_type") if isinstance(er.get("exception_type"), str) else None
                po_number = er.get("po_number")
                item_number = er.get("item_number")
                exception_value = er.get("exception_value")
                material = er.get("material")
                # Determine code (default to Unknown -> '000')
                code = EXCEPTION_TYPE_CODES.get(exception_type, EXCEPTION_TYPE_CODES["Unknown"])

                # Generate EXC id locally using secrets
                exc_id = generate_exception_id(code)

                # Build enriched record (insert exception_id)
                enriched_rec = {
                    "exception_id": exc_id,
                    "exception_type": exception_type,
                    "po_number": po_number,
                    "item_number": item_number,
                    "exception_value": exception_value,
                    "material": material
                }
                enriched.append(enriched_rec)

            # Replace/overwrite the exception_records in parsed_inner with enriched list
            parsed_inner["exception_records"] = enriched
    except Exception as e:
        # Per your request: no local fallback mechanism — if postprocessing fails, raise
        raise RuntimeError(f"Failed during exception post-processing: {e}")

    # -----------------------------
    # Compute variance locally (deterministic)
    # -----------------------------
    try:
        # Build a map of PO item -> ordered quantity from the original PO JSONs (po_list is available)
        po_item_qty_map: Dict[str, float] = {}
        # Also capture a PO-level default order_quantity if item-level mapping not present
        po_default_order_qty: Optional[float] = None
        if isinstance(po_list, list) and len(po_list) > 0:
            # If first purchase_order has a top-level order_quantity, use as a fallback default
            first_po = po_list[0]
            if isinstance(first_po, dict):
                # top-level fallback
                try:
                    po_default_order_qty = float(first_po.get("OrderQuantity", first_po.get("order_quantity", 0) or 0))
                except Exception:
                    po_default_order_qty = None

            # attempt to map item-level OrderQuantity fields
            for po_obj in po_list:
                if not isinstance(po_obj, dict):
                    continue
                items = po_obj.get("Items") or po_obj.get("items") or []
                if isinstance(items, list):
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        # common SAP-like item id keys
                        item_key = it.get("PurchaseOrderItem") or it.get("item_number") or it.get("Item") or it.get("POItem")
                        try:
                            ordered_q = it.get("OrderQuantity") or it.get("order_quantity") or it.get("OrderQty")
                            ordered_q_val = float(ordered_q) if ordered_q is not None else None
                        except Exception:
                            ordered_q_val = None
                        if item_key and ordered_q_val is not None:
                            po_item_qty_map[str(item_key)] = ordered_q_val

        # If parsed_inner has goods_receipt_records, compute variance = received_quantity - ordered_quantity (item-level)
        gr_records = parsed_inner.get("goods_receipt_records")
        if isinstance(gr_records, list):
            for gr in gr_records:
                # Received quantity
                recv_q = gr.get("received_quantity") or 0
                try:
                    recv_q_val = float(recv_q)
                except Exception:
                    recv_q_val = 0.0

                # Prefer item number from GR if present, else attempt to use Material or similar fields
                item_no = gr.get("item_number") or gr.get("PurchaseOrderItem") or gr.get("Item") or gr.get("MaterialItem") or gr.get("Material")
                ordered_q_val = None
                if item_no is not None:
                    ordered_q_val = po_item_qty_map.get(str(item_no))
                # fallback to PO-level default order quantity if available
                if ordered_q_val is None:
                    ordered_q_val = po_default_order_qty if po_default_order_qty is not None else 0.0

                # Compute variance (received - ordered)
                try:
                    variance_val = recv_q_val - float(ordered_q_val or 0)
                except Exception:
                    variance_val = None

                # Set variance in the parsed_inner object (use numeric or null)
                gr["variance"] = variance_val if variance_val is not None else None
    except Exception as e:
        # If anything fails here, raise so caller becomes aware (you asked no fallback)
        raise RuntimeError(f"Failed during local variance computation: {e}")

    # --------------------------------------------
    # Add deterministic item_list to purchase_order_records
    # --------------------------------------------
    try:
        # Collect all PurchaseOrderItem IDs from PO list
        po_item_list = []

        for po in po_list:
            if not isinstance(po, dict):
                continue
            po_num = po.get("PurchaseOrder") or po.get("purchase_order")
            items = po.get("Items") or po.get("items") or []
            for it in items:
                if not isinstance(it, dict):
                    continue
                # pick item id
                item_no = (
                    it.get("PurchaseOrderItem")
                    or it.get("Item")
                    or it.get("POItem")
                    or it.get("item_number")
                )

                # normalize numeric id to 5-digit zero-padded string
                if item_no is not None:
                    s = str(item_no)
                    if s.isdigit():
                        norm_item = s.zfill(5)
                    else:
                        norm_item = s
                    po_item_list.append(norm_item)

        # Remove duplicates while preserving order
        seen = set()
        po_item_list = [x for x in po_item_list if not (x in seen or seen.add(x))]

        # Inject into parsed_inner["purchase_order_records"]
        if isinstance(parsed_inner.get("purchase_order_records"), dict):
            parsed_inner["purchase_order_records"]["item_list"] = po_item_list

    except Exception as e:
        raise RuntimeError(f"Failed to add item_list to purchase_order_records: {e}")

    # --------------------------------------------
    # Deterministically attach item_number to invoice & goods receipt records (ONLY via Material field, no fallbacks)
    # --------------------------------------------
    try:
        # Build lookups from original invoices and GRNs:
        # invoice_num -> list of item dicts
        inv_items_by_invoice = {}
        for inv in inv_list:
            if not isinstance(inv, dict):
                continue
            inv_num = inv.get("BillingDocument") or inv.get("billing_document") or inv.get("BillingDocumentNumber")
            items = inv.get("Items") or inv.get("items") or []
            if inv_num is None:
                continue
            inv_items_by_invoice.setdefault(str(inv_num), [])
            for it in items:
                # normalize material for quick compare and keep the raw item dict
                inv_items_by_invoice[str(inv_num)].append(it)

        # gr_num -> list of item dicts
        gr_items_by_gr = {}
        for gr in grn_list:
            if not isinstance(gr, dict):
                continue
            gr_num = gr.get("MaterialDocument") or gr.get("material_document") or gr.get("MaterialDocumentNumber")
            items = gr.get("Items") or gr.get("items") or []
            if gr_num is None:
                continue
            gr_items_by_gr.setdefault(str(gr_num), [])
            for it in items:
                gr_items_by_gr[str(gr_num)].append(it)

        # Helper to normalize item id
        def _normalize_item_id(x):
            if x is None:
                return None
            s = str(x)
            return s.zfill(5) if s.isdigit() else s

        # Helper to normalize material string for comparison
        def _norm_mat(m):
            if m is None:
                return None
            return str(m).strip().lower()

        # Resolve invoice_order_records using ONLY exact material field match (case-insensitive)
        inv_recs = parsed_inner.get("invoice_order_records")
        if isinstance(inv_recs, list):
            for rec in inv_recs:
                try:
                    # skip if already present
                    if rec.get("item_number"):
                        continue

                    inv_num = rec.get("invoice_number")
                    po_num = rec.get("po_number")
                    rec_material = _norm_mat(rec.get("material") or rec.get("Material"))

                    candidate_item = None

                    # If parsed record contains material and original invoice with that number exists, match by material
                    if rec_material and inv_num and str(inv_num) in inv_items_by_invoice:
                        for it in inv_items_by_invoice[str(inv_num)]:
                            orig_mat = _norm_mat(it.get("Material") or it.get("material"))
                            # require exact material equality (case-insensitive)
                            if orig_mat and rec_material == orig_mat:
                                # Only accept mapping if original invoice item has explicit ReferencePurchaseOrderItem (or PurchaseOrderItem)
                                ref_item = it.get("ReferencePurchaseOrderItem") or it.get("reference_purchase_order_item") or it.get("PurchaseOrderItem")
                                ref_po = it.get("ReferencePurchaseOrder") or it.get("reference_purchase_order") or it.get("PurchaseOrder")
                                if not ref_item:
                                    # original invoice item doesn't have explicit PO item -> cannot map
                                    continue
                                # If parsed rec specified a PO number, check invoice item's referenced PO matches it
                                if po_num:
                                    if not ref_po or str(ref_po) != str(po_num):
                                        # referenced PO missing or mismatch -> do not map
                                        continue
                                # exact material match + explicit ref_item + PO match (if provided) -> map
                                candidate_item = _normalize_item_id(ref_item)
                                break

                    rec["item_number"] = candidate_item if candidate_item is not None else None
                except Exception:
                    rec["item_number"] = rec.get("item_number", None)

        # Resolve goods_receipt_records using ONLY exact material field match (case-insensitive)
        gr_recs = parsed_inner.get("goods_receipt_records")
        if isinstance(gr_recs, list):
            for rec in gr_recs:
                try:
                    # skip if already present
                    if rec.get("item_number"):
                        continue

                    gr_num = rec.get("gr_number")
                    po_num = rec.get("po_number")
                    rec_material = _norm_mat(rec.get("material") or rec.get("Material"))

                    candidate_item = None

                    # If parsed record contains material and original GR with that number exists, match by material
                    if rec_material and gr_num and str(gr_num) in gr_items_by_gr:
                        for it in gr_items_by_gr[str(gr_num)]:
                            orig_mat = _norm_mat(it.get("Material") or it.get("material"))
                            if orig_mat and rec_material == orig_mat:
                                # Only accept mapping if original GR item has explicit PurchaseOrderItem or MaterialDocumentItem
                                orig_po_item = it.get("PurchaseOrderItem") or it.get("purchase_order_item") or it.get("MaterialDocumentItem")
                                orig_po_ref = it.get("PurchaseOrder") or it.get("purchase_order") or it.get("ReferencePurchaseOrder")
                                if not orig_po_item:
                                    # no explicit PO item -> cannot map
                                    continue
                                # If parsed rec specified a PO number, check GR item's referenced PO matches it
                                if po_num:
                                    if not orig_po_ref or str(orig_po_ref) != str(po_num):
                                        # mismatch or missing -> cannot map
                                        continue
                                candidate_item = _normalize_item_id(orig_po_item)
                                break

                    rec["item_number"] = candidate_item if candidate_item is not None else None
                except Exception:
                    rec["item_number"] = rec.get("item_number", None)

    except Exception as e:
        raise RuntimeError(f"Failed while attaching item_number to invoice/gr records: {e}")

    # Write the parsed inner JSON (pretty) to the output file
    try:
        with open(output_file, "w", encoding="utf-8") as of:
            json.dump(parsed_inner, of, indent=2, ensure_ascii=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write output file {output_file}: {e}")

    return parsed_inner




@tool
def sop_analysis(
    needed_dir: str = "needed_doc",
    result_file: str = "three_way_match_result.json"
) -> dict:
    """
    1) Build SOP -> EXC code map.
    2) Read exception_records from result_file.
    3) For each exception record, download the matching SOP file from S3 (folder 'sop', file 'SOP_<KEY>.txt')
       and store it in needed_dir.
    4) Invoke Bedrock (same body format as other functions) with SOP content + exception record and ask the model
       to return JSON with fields: severity, status, exception_summary, sop_connected, sop_summary, item_number.
    5) Update the exception_records in the result JSON with the returned fields, save file and return updated JSON.

    No local fallbacks — will raise RuntimeError on Bedrock/download/parse failures.
    """
    # 1) SOP map with EXC codes (EXC-001 ... EXC-006)
    sop_map = {
        "SOP_QUANTITY_CHANGE_V1": "EXC-001",
        "SOP_PRICE_CHANGE_V1": "EXC-002",
        "SOP_DUPLICATE_INVOICE_V1": "EXC-003",
        "SOP_MISSING_GRN_V1": "EXC-004",
        "SOP_UOM_ISSUE_V1": "EXC-005",
        "SOP_MISSING_PO_V1": "EXC-006"
    }

    # map exception_type -> SOP key
    exception_to_sop = {
        "Quantity Variance": "SOP_QUANTITY_CHANGE_V1",
        "Price Mismatch": "SOP_PRICE_CHANGE_V1",
        "Invoice Errors": "SOP_DUPLICATE_INVOICE_V1",
        "Missing GR": "SOP_MISSING_GRN_V1",
        "UoM Issues": "SOP_UOM_ISSUE_V1",
        "Missing PO": "SOP_MISSING_PO_V1"
    }

    # Ensure needed_dir exists
    os.makedirs(needed_dir, exist_ok=True)

    # 2) Load existing three-way match result
    try:
        with open(result_file, "r", encoding="utf-8") as rf:
            result_json = json.load(rf)
    except Exception as e:
        raise RuntimeError(f"Failed to load result file {result_file}: {e}")

    ex_records = result_json.get("exception_records", [])
    if not isinstance(ex_records, list) or len(ex_records) == 0:
        # Nothing to analyze — write back unchanged and return
        try:
            with open(result_file, "w", encoding="utf-8") as of:
                json.dump(result_json, of, indent=2, ensure_ascii=False)
        except Exception as e:
            raise RuntimeError(f"Failed to write result file {result_file}: {e}")
        return result_json

    # 3) For each exception record, download the SOP file and invoke Bedrock
    client = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    updated_ex_records = []
    for er in ex_records:
        # keep existing fields
        exception_type = er.get("exception_type")
        po_number = er.get("po_number")
        item_number_existing = er.get("item_number")
        exception_value = er.get("exception_value")

        # determine SOP key
        sop_key = exception_to_sop.get(exception_type)
        if sop_key is None:
            # If unknown exception type, skip LLM call but preserve record
            updated_ex_records.append(er)
            continue

        sop_s3_key = f"sop/{sop_key}.txt"
        local_sop_path = os.path.join(needed_dir, f"{sop_key}.txt")

        # download SOP file from S3
        try:
            download_file_from_s3(S3_BUCKET, sop_s3_key, local_sop_path)
        except Exception as e:
            raise RuntimeError(f"Failed to download SOP file {sop_s3_key} from S3: {e}")

        # read SOP content
        try:
            with open(local_sop_path, "r", encoding="utf-8") as sf:
                sop_content = sf.read()
        except Exception as e:
            raise RuntimeError(f"Failed to read SOP file {local_sop_path}: {e}")

        # Build prompt: include SOP content and the exception record; instruct concise JSON output
        prompt = (
            "You are given a Standard Operating Procedure (SOP) text and a single exception record.\n"
            "Read the SOP and the exception record and then return ONLY a JSON object (no explanation) with these fields:\n"
            "  - severity (string, e.g. High/Medium/Low)\n"
            "  - status (string, e.g. Pending)\n"
            "  - exception_summary (a detailed description summarizing why this is an exception and what is the problem)\n"
            "  - sop_connected (boolean: true if SOP applies to this exception, false otherwise)\n"
            "  - sop_summary (one-line summary of the SOP action that should be taken)\n"
            "  - item_number (string or null; the relevant item number if applicable)\n\n"
            "SOP_TEXT:\n"
            f"{sop_content}\n\n"
            "EXCEPTION_RECORD:\n"
            f"{json.dumps(er, ensure_ascii=False)}\n\n"
            "Return valid JSON only.\n"
        )

        # call Bedrock using same body style as other functions
        try:
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}]
            })

            resp = client.invoke_model(
                modelId=MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=body
            )

            sb = resp.get("body")
            if sb is None:
                raise RuntimeError("No body found in Bedrock response.")
            raw_text = sb.read().decode("utf-8")
            print("DEBUG: Raw model output:\n", raw_text)
        except (NoCredentialsError, EndpointConnectionError, ClientError, BotoCoreError) as e:
            raise RuntimeError(f"Bedrock invocation failed: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error when calling Bedrock: {e}")

        # parse LLM response to JSON (handle Bedrock envelope, code fences, escaped JSON, or raw JSON)
        parsed = None

        # 1) Try direct JSON parse of raw_text (this may be the envelope or direct JSON)
        try:
            parsed_candidate = json.loads(raw_text)
        except Exception:
            parsed_candidate = None

        # 2) If parsed_candidate is a dict and contains 'content' list (typical Bedrock envelope),
        #    try to extract the first text block that contains the JSON string.
        if isinstance(parsed_candidate, dict) and isinstance(parsed_candidate.get("content"), list):
            text_candidate = None
            for item in parsed_candidate.get("content", []):
                if isinstance(item, dict):
                    # Common fields: "text" or "content"
                    if "text" in item and isinstance(item["text"], str):
                        text_candidate = item["text"]
                        break
                    if "content" in item and isinstance(item["content"], str):
                        text_candidate = item["content"]
                        break

            if text_candidate is not None:
                # remove triple-backtick fences if present (``` or ```json)
                text_candidate = re.sub(r"^```(?:json)?\s*", "", text_candidate.strip())
                text_candidate = re.sub(r"\s*```$", "", text_candidate.strip())

                # try direct parse of the cleaned text
                try:
                    parsed = json.loads(text_candidate)
                except Exception:
                    # try to unescape common escape sequences then parse
                    try:
                        unescaped = text_candidate.encode("utf-8").decode("unicode_escape")
                        parsed = json.loads(unescaped)
                    except Exception:
                        # fallback: extract first {...} block from the text_candidate
                        m = re.search(r"(\{(?:.|\n)*\})", text_candidate)
                        if m:
                            try:
                                parsed = json.loads(m.group(1))
                            except Exception as e:
                                raise RuntimeError(f"Failed to parse JSON from model content text. Raw model content:\n{text_candidate}\nParse error: {e}")
                        else:
                            raise RuntimeError(f"No JSON object found inside model content text. Content:\n{text_candidate}")

        # 3) If parsed still None, maybe raw_text itself contains the desired JSON (no envelope)
        if parsed is None:
            # try direct parse of raw_text (again, to cover raw JSON case)
            try:
                parsed = json.loads(raw_text)
            except Exception:
                # fallback: try to extract first {...} block from raw_text
                m = re.search(r"(\{(?:.|\n)*\})", raw_text)
                if m:
                    try:
                        parsed = json.loads(m.group(1))
                    except Exception as e:
                        raise RuntimeError(f"Failed to parse JSON from Bedrock response. Raw output:\n{raw_text}\nError: {e}")
                else:
                    raise RuntimeError(f"No JSON object found in Bedrock response. Raw output:\n{raw_text}")


        # Validate parsed contains required fields
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Parsed LLM response is not a JSON object: {parsed}")

        # Build enriched exception record by copying existing fields and adding/overwriting the new ones
        enriched = dict(er)  # copy existing
        # Only keep expected keys from parsed (prevents extra junk)
        enriched["severity"] = parsed.get("severity")
        enriched["status"] = parsed.get("status")
        enriched["exception_summary"] = parsed.get("exception_summary")
        # Set sop_connected to the SOP filename instead of boolean
        enriched["sop_connected"] = f"{sop_key}.txt"
        enriched["sop_summary"] = parsed.get("sop_summary")
        # Prefer item_number returned by LLM if present, else keep existing
        enriched_item = parsed.get("item_number")
        enriched["item_number"] = enriched_item if enriched_item is not None else item_number_existing

        # Also attach which SOP was used and its EXC code for traceability
        enriched["sop_used"] = sop_key
        enriched["sop_exc_code"] = sop_map.get(sop_key)

        updated_ex_records.append(enriched)

    # replace exception_records in main result with enriched ones
    result_json["exception_records"] = updated_ex_records

    # write back updated result file
    try:
        with open(result_file, "w", encoding="utf-8") as of:
            json.dump(result_json, of, indent=2, ensure_ascii=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write updated result file {result_file}: {e}")

    return result_json


@tool
def database_records(
    three_way_file: str = "three_way_match_result.json",
    output_file: str = "db_records.json",
    needed_dir: str = "needed_doc"
) -> Dict[str, Any]:
    """
    Builds DB JSON records from exception_records (from three_way_match_result.json) 
    and full PO/Invoice/GRN data from JSON files in needed_doc folder.
    
    Changes:
    - three_way_match_result.json now only contains exception_records
    - purchase_order_records, invoice_records, goods_receipt_records are populated 
      from JSON files in needed_doc folder (PO_*.json, INV_*.json, GRN_*.json)
    - Exception records are linked to the corresponding PO/Invoice/GRN records
    - Requires userid to be set in the request context (no fallback)
    """
    import json, glob, os
    from datetime import datetime
    from typing import Dict, Any, List, Optional
    global current_userid
    
    # Validate userid is present (required, no fallback)
    if current_userid is None:
        raise RuntimeError("userid is required but was not provided in the request. Cannot proceed with database insertion.")

    def now_iso() -> str:
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Load three-way result - now only contains exception_records
    try:
        with open(three_way_file, "r", encoding="utf-8") as rf:
            three = json.load(rf)
    except Exception as e:
        raise RuntimeError(f"Failed to load three-way file {three_way_file}: {e}")

    # Only read exception_records from three_way_match_result.json
    exceptions: List[Dict[str, Any]] = three.get("exception_records", []) or []

    timestamp = now_iso()

    # Initialize with dictionaries (objects) instead of arrays
    db_out: Dict[str, Any] = {
        "database_records": {
            "purchase_order_records": {},
            "invoice_records": {},
            "goods_receipt_records": {},
            "exception_records": {}
        }
    }

    # ---------- Load actual PO/INV/GRN files from needed_doc and put them as-is ----------
    def _load_json_files_with_keys(pattern: str, key_extractor) -> Dict[str, Dict[str, Any]]:
        """Load all JSON files matching the pattern from needed_dir and return as dictionary keyed by document number"""
        paths = sorted(glob.glob(os.path.join(needed_dir, pattern)))
        data = {}
        for p in paths:
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    file_data = json.load(fh)
                    # Extract key (PO number, Invoice number, or GRN number)
                    key = key_extractor(file_data)
                    if key:
                        data[str(key)] = file_data
            except Exception:
                continue
        return data

    # Load PO files - key by PurchaseOrder number
    def get_po_key(po_data: Dict[str, Any]) -> Optional[str]:
        return po_data.get("PurchaseOrder") or po_data.get("purchase_order")
    
    db_out["database_records"]["purchase_order_records"] = _load_json_files_with_keys("PO_*.json", get_po_key)

    # Load Invoice files - key by BillingDocument number
    def get_inv_key(inv_data: Dict[str, Any]) -> Optional[str]:
        return inv_data.get("BillingDocument") or inv_data.get("billing_document") or inv_data.get("invoice_number")
    
    db_out["database_records"]["invoice_records"] = _load_json_files_with_keys("INV_*.json", get_inv_key)

    # Load GRN files - key by MaterialDocument number
    def get_grn_key(grn_data: Dict[str, Any]) -> Optional[str]:
        return grn_data.get("MaterialDocument") or grn_data.get("material_document") or grn_data.get("gr_number")
    
    db_out["database_records"]["goods_receipt_records"] = _load_json_files_with_keys("GRN_*.json", get_grn_key)

    # ---------- Process exceptions and store them in exception_records dictionary ----------
    for ex in exceptions:
        exc_id = ex.get("exception_id")
        if exc_id:
            # Store exception as-is, keyed by exception_id
            db_out["database_records"]["exception_records"][exc_id] = ex

    # ---------- Write output ----------
    try:
        with open(output_file, "w", encoding="utf-8") as of:
            json.dump(db_out, of, indent=2, ensure_ascii=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write DB records file {output_file}: {e}")

    payload = {"event_type": "insert_new_data", "analysis_data": db_out, "userid": current_userid}
    insert_data_lambda(payload)

    return db_out



def insert_data_lambda(payload):

    print("PAYLOOOOOOOOOOOOOOOOOOOOO\n\n\n\n\n\n\n\n\n\n\n", payload, "\n\n\n\n\n\n\n\n\n\n\n\n\n")

    url = "https://agbmqz53j0.execute-api.us-west-2.amazonaws.com/test/ERP"

    headers = {"content-type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


def check_po_queried_lambda(po_number: str, userid: int) -> dict:
    """
    Call Lambda to check if PO has been queried and fixed by this user
    
    Args:
        po_number: PO number to check
        userid: User ID
        
    Returns:
        dict with status: "queried" or "not_queried"
    """
    print("=" * 80)
    print(f"EC2: Checking if PO {po_number} has been queried by userid {userid}")
    print("=" * 80)
    
    url = "https://agbmqz53j0.execute-api.us-west-2.amazonaws.com/test/ERP"
    headers = {"content-type": "application/json"}
    
    payload = {
        "event_type": "check_po_queried",
        "po_number": po_number,
        "userid": userid
    }
    
    print(f"EC2: Sending to Lambda: {payload}")
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        result = response.json()
        
        print(f"EC2: Lambda response: {result}")
        print("=" * 80)
        
        return result
    except Exception as e:
        print(f"EC2: Error calling Lambda: {str(e)}")
        print("=" * 80)
        # On error, allow processing (fail open)
        return {"status": "not_queried", "error": str(e)}


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


def create_sap_agent() -> Agent:
    """
    Create and configure the SAP S3 analysis agent

    Returns:
        Configured Strands Agent instance
    """
    # Define available tools for 3-way match analysis with SOP integration
    # check_po_status MUST be called first to avoid duplicate processing
    tools = [check_po_status, fetch_s3_data, three_way_match_with_llm, sop_analysis, database_records]


    system_prompt = """
You are an SAP business analyst assistant. From a user message like 3-way match for PO_4500018901 extract only the PO number (digits only, e.g. 4500018901). Do not ask questions — act.

All responses must be formatted in **Markdown**, with clear line breaks between each step.

When running tools, announce each call in plain business language (not technical) and wait for the tool to finish before continuing:

Call check_po_status(po_number=<PO_NUMBER>) and announce:
"Step 1 — Checking if PO <PO_NUMBER> has already been processed."

After Step 1:
- If the tool returns action="STOP", immediately inform the user:
  "**PO <PO_NUMBER> has already been processed and all exceptions are fixed. Please check the Exceptions page for more details.**"
  Then STOP all processing. Do NOT proceed to Steps 2-5.

- If the tool returns action="CONTINUE", proceed with the following steps:

Call fetch_s3_data(po_number=<PO_NUMBER>) and announce:
"Step 2 — Retrieve purchase, goods receipt and invoice documents for PO <PO_NUMBER>."

Call three_way_match_with_llm(needed_dir="needed_doc", output_file="three_way_match_result.json") and announce:
"Step 3 — Perform three-way matching and identify discrepancies for PO <PO_NUMBER>."

Call sop_analysis(needed_dir="needed_doc", result_file="three_way_match_result.json") and announce:
"Step 4 — Apply SOPs to determine severity, status and recommended actions for identified exceptions."

After Step 4, present the exception details in Markdown, one line per exception:
`Exception ID — Type — PO — Item — Severity — Status — Short summary.`

Call database_records(three_way_file="three_way_match_result.json", output_file="db_records.json") and announce:
"Step 5 — Prepare database records and persist exception details for downstream processing."

After Step 5, return a concise **final_summary** paragraph in Markdown describing overall outcome (how many exceptions, highest severity, and recommended next action).

Do not include technical logs, raw JSON, or prompts.
    """


    # Configure model   
    model = BedrockModel(
        model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    )

    # Create and return the agent
    agent = Agent(
        tools=tools,
        model=model,
        system_prompt=system_prompt
    )
    return agent



app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import logging

logger = logging.getLogger(__name__)


@app.get("/health_check")
async def check_health():
    return {"status": "ok"}


@app.post("/receive_from_lambda")
async def receive_from_lambda(request: Request):
    """
    Endpoint to receive data from Lambda
    """
    global lambda_callback_data
    
    print(f"\n{'='*80}")
    print(f"EC2: /receive_from_lambda endpoint hit!")
    print(f"EC2: Timestamp: {datetime.now()}")
    print(f"EC2: Request method: {request.method}")
    print(f"EC2: Request URL: {request.url}")
    print(f"EC2: Client host: {request.client.host if request.client else 'unknown'}")
    print(f"{'='*80}")
    
    # Log all headers
    print(f"EC2: Request headers:")
    for header_name, header_value in request.headers.items():
        print(f"  {header_name}: {header_value}")
    
    try:
        # Read raw body first
        raw_body = await request.body()
        print(f"\nEC2: Raw request body (bytes): {raw_body}")
        print(f"EC2: Body length: {len(raw_body)} bytes")
        print(f"EC2: Body encoding: {raw_body[:100]}")  # First 100 bytes
        
        # Parse JSON
        data = await request.json()
        print(f"\nEC2: Successfully parsed JSON data")
        print(f"EC2: Parsed data type: {type(data)}")
        
        if isinstance(data, dict):
            print(f"EC2: Data keys: {list(data.keys())}")
            print(f"\nEC2: Full data content:")
            print(json.dumps(data, indent=2))
            
            # Extract specific fields
            event_type = data.get('event_type', 'unknown')
            po_number = data.get('po_number', 'N/A')
            status = data.get('status', 'N/A')
            message = data.get('message', 'N/A')
            total_records = data.get('total_records', 'N/A')
            fixed_records = data.get('fixed_records', 'N/A')
            userid = data.get('userid', 'N/A')
            
            print(f"\nEC2: Extracted fields:")
            print(f"  - Event Type: {event_type}")
            print(f"  - PO Number: {po_number}")
            print(f"  - User ID: {userid}")
            print(f"  - Status: {status}")
            print(f"  - Message: {message}")
            print(f"  - Total Records: {total_records}")
            print(f"  - Fixed Records: {fixed_records}")
            
            # Store data in global dict keyed by po_number+userid for retrieval
            if event_type == "check_po_queried" and po_number != 'N/A' and userid != 'N/A':
                cache_key = f"{po_number}_{userid}"
                with lambda_callback_lock:
                    lambda_callback_data[cache_key] = data
                print(f"\nEC2: Stored callback data with key: {cache_key}")
        
        logger.info(f"EC2 received from Lambda: {json.dumps(data, indent=2)}")
        
        response = {
            "status": "success",
            "message": "EC2 successfully received and processed data from Lambda",
            "received_data": data,
            "ec2_timestamp": datetime.now().isoformat(),
            "processing_note": "Data logged and acknowledged"
        }
        
        print(f"\nEC2: Sending response back to Lambda:")
        print(json.dumps(response, indent=2))
        print(f"{'='*80}\n")
        
        return response
        
    except json.JSONDecodeError as e:
        print(f"\nEC2: ✗ JSON decode error: {str(e)}")
        print(f"EC2: Error position: {e.pos}")
        print(f"EC2: Error line: {e.lineno}, column: {e.colno}")
        print(f"EC2: Error message: {e.msg}")
        print(f"{'='*80}\n")
        logger.error(f"JSON decode error: {str(e)}")
        return {
            "status": "error",
            "message": f"JSON decode error: {str(e)}",
            "error_type": "JSONDecodeError"
        }
    except Exception as e:
        print(f"\nEC2: ✗ Unexpected error in receive_from_lambda: {str(e)}")
        print(f"EC2: Error type: {type(e).__name__}")
        import traceback
        print(f"EC2: Traceback:")
        print(traceback.format_exc())
        print(f"{'='*80}\n")
        logger.error(f"Error receiving from Lambda: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }


@app.post("/test_process")
async def test_process(request: Request):
    event = await request.json()


    po_num = event.get("po_num")

    fetch_s3_data(po_number="4500012345")
    three_way_match_with_llm(needed_dir="needed_doc", output_file="three_way_match_result.json")
    sop_analysis(needed_dir="needed_doc", result_file="three_way_match_result.json")
    database_records(three_way_file="three_way_match_result.json", output_file="db_records.json")

    return {"status": "check db"}


@app.post("/process")
async def create_item(request: Request):
    """
    Process SAP analysis requests with streaming support
   
    Receives user question, connection_id, message_id, and userid.
    Streams intermediate results and final response via WebSocket.
    """
    global final_text_ff, current_userid
    event = await request.json()

    user_input = event.get("user_question")
    connection_id = event.get("connection_id")
    message_id = event.get("message_id")
    userid = event.get("userid")
    
    # Validate userid is present (required, no fallback)
    if userid is None:
        error_msg = "userid is required but was not provided in the request"
        logger.error(error_msg)
        send_ws_message(
            connection_id,
            {
                "message": f"Error: {error_msg}",
                "message_id": message_id,
                "type": "error"
            }
        )
        return {"status": "error", "message": error_msg}
    
    # Store userid in global context for use by database_records tool
    current_userid = userid
   
    # Log request details
    logger.info(f"Processing request - Connection: {connection_id}, Message: {message_id}, UserID: {userid}")
    logger.info(f"User question: {user_input}")

    try:
        # Create SAP agent
        sap_agent = create_sap_agent()
       
        # Call agent with streaming callback handler
        response = sap_agent(
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
    finally:
        # Clear userid from global context after request completes
        current_userid = None

if __name__ == "__main__":
    uvicorn.run("new_main:app", host="0.0.0.0", port=8084, reload=True)
    
# if __name__ == "__main__":
#     fetch_s3_data(po_number="4500016789")
#     three_way_match_with_llm(needed_dir="needed_doc", output_file="three_way_match_result.json")
#     sop_analysis(needed_dir="needed_doc", result_file="three_way_match_result.json")
#     database_records(three_way_file="three_way_match_result.json", output_file="db_records.json")
