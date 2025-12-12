"""
SAP S3 Data Analysis Agent using Strands
========================================

This module creates a Strands agent that can fetch and analyze SAP data from S3,
performing 3-way matching and other financial analysis tasks.

Author: Generated for SAP AP Analysis
Date: 2025-01-17
"""

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sap_agent.log')
    ]
)
logger = logging.getLogger(__name__)

# Exception type to code mapping based on the exception dashboard
EXCEPTION_TYPE_CODES = {
    "Quantity Variance": "001",
    "Price Mismatch": "002",
    "Invoice Errors": "003",
    "Missing GR": "004",
    "UoM Issues": "005",
    "Missing PO": "006",
    "Unknown": "000"
}

# Load environment variables from .env file
load_dotenv()


# Environment configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "sap-mcp-data-server")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Database configuration
DB_HOST = os.environ.get("db_host")
DB_PORT = int(os.environ.get("db_port", 5432))
DB_USER = os.environ.get("db_user")
DB_PASSWORD = os.environ.get("db_password")
DB_DATABASE = os.environ.get("db_database")

# Initialize S3 client
try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
except NoCredentialsError:
    s3_client = boto3.client('s3', region_name=AWS_REGION)



def insert_data_lambda(payload):

    print("PAYLOOOOOOOOOOOOOOOOOOOOO\n\n\n\n\n\n\n\n\n\n\n", payload, "\n\n\n\n\n\n\n\n\n\n\n\n\n")

    url = "https://agbmqz53j0.execute-api.us-west-2.amazonaws.com/test/ERP"

    headers = {"content-type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    return response.json()

class SAPDataParser:
    """Utility class for parsing SAP OData XML responses"""
    
    @staticmethod
    def parse_odata_xml(xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse OData XML feed and extract entity data
        
        Args:
            xml_content: Raw XML content from S3
            
        Returns:
            List of parsed entity records
        """
        try:
            root = ET.fromstring(xml_content)
            records = []
            
            # Find all entry elements
            for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                record = {}
                
                # Extract properties from content
                content = entry.find('.//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties')
                if content is not None:
                    for prop in content:
                        tag_name = prop.tag.split('}')[-1]  # Remove namespace
                        # Always add the field, even if empty, to preserve structure
                        if prop.text is not None and prop.text.strip():
                            record[tag_name] = prop.text.strip()
                        else:
                            record[tag_name] = prop.text if prop.text is not None else None
                
                # Add record if it has any content (even with None values)
                if record:
                    records.append(record)
            
            return records
            
        except ET.ParseError as e:
            print(f"XML Parse Error: {e}")
            return []
        except Exception as e:
            print(f"XML Processing Error: {e}")
            return []


@tool
def fetch_metadata() -> dict:
    """
    Fetch metadata.json file to understand data relationships
    
    Returns:
        Dictionary with metadata information
    """
    try:
        logger.info(f"========== METADATA RETRIEVAL INITIATED ==========")
        logger.info(f"S3 Bucket: {S3_BUCKET}")
        logger.info(f"Metadata File: data/metadata.json")
        
        response = s3_client.get_object(Bucket=S3_BUCKET, Key="data/metadata.json")
        content = response['Body'].read().decode('utf-8')
        
        # Log metadata file details
        logger.info(f"✓ Metadata file retrieved successfully")
        logger.info(f"File Size: {len(content)} characters")
        logger.info(f"Raw Metadata Content:\n{content}")
        
        metadata = json.loads(content)
        
        # Log parsed metadata details
        logger.info(f"✓ Metadata JSON parsed successfully")
        logger.info(f"Total Metadata Records: {len(metadata)}")
        logger.info(f"Metadata Details:")
        for idx, meta in enumerate(metadata, 1):
            logger.info(f"  Record {idx}: {meta}")
        logger.info(f"========== METADATA RETRIEVAL COMPLETED ==========\n")
        
        return {
            "status": "success",
            "content": [{
                "text": f"Successfully fetched metadata with {len(metadata)} records",
                "data": {
                    "metadata": metadata,
                    "record_count": len(metadata)
                }
            }]
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            return {
                "status": "error",
                "content": [{"text": "Metadata file not found"}]
            }
        else:
            return {
                "status": "error", 
                "content": [{"text": f"S3 error: {str(e)}"}]
            }
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error fetching metadata: {str(e)}"}]
        }


@tool
def fetch_s3_data(file_path: str) -> dict:
    """
    Fetch SAP data file from S3 bucket
    
    Args:
        file_path: S3 object key 
        
    Returns:
        Dictionary with status and parsed data
    """
    try:
        # Fetch object from S3
        logger.info(f"========== S3 DATA RETRIEVAL INITIATED ==========")
        logger.info(f"S3 Bucket: {S3_BUCKET}")
        logger.info(f"File Path: {file_path}")
        
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        
        # Log S3 file details immediately after retrieval
        logger.info(f"✓ S3 file retrieved successfully")
        logger.info(f"File Size: {len(content)} characters")
        logger.info(f"Content Type: {response.get('ContentType', 'Unknown')}")
        logger.info(f"Last Modified: {response.get('LastModified', 'Unknown')}")
        logger.info(f"\n========== FULL FILE CONTENT ==========")
        logger.info(f"\n{content}")
        logger.info(f"========== END OF FILE CONTENT ==========\n")
        
        # Parse the XML content
        logger.info(f"Starting XML parsing...")
        parser = SAPDataParser()
        records = parser.parse_odata_xml(content)
        
        # Log parsed data details
        logger.info(f"✓ XML parsing completed successfully")
        logger.info(f"Total Records Parsed: {len(records)}")
        
        if records:
            logger.info(f"First Record Keys: {list(records[0].keys())}")
            logger.info(f"Sample Records (first 3):")
            for idx, record in enumerate(records[:3], 1):
                logger.info(f"  Record {idx}: {record}")
        else:
            logger.warning(f"No records found in the parsed data")
        
        logger.info(f"========== S3 DATA RETRIEVAL COMPLETED ==========\n")
        
        return {
            "status": "success",
            "content": [{
                "text": f"Successfully fetched {len(records)} records from {file_path}",
                "data": {
                    "file_path": file_path,
                    "record_count": len(records),
                    "sample_records": records[:5] if records else [],  # First 5 records as sample
                    "all_records": records  # Include all records for debugging
                }
            }]
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            return {
                "status": "error",
                "content": [{"text": f"File not found: {file_path}"}]
            }
        else:
            return {
                "status": "error", 
                "content": [{"text": f"S3 error: {str(e)}"}]
            }
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error fetching data: {str(e)}"}]
        }


@tool
def list_s3_files(folder_path: str = "data/") -> dict:
    """
    List all files in S3 bucket folder
    
    Args:
        folder_path: S3 folder path to list (default: 'data/')
        
    Returns:
        Dictionary with list of available files
    """
    try:
        logger.info(f"========== S3 FILE LISTING INITIATED ==========")
        logger.info(f"S3 Bucket: {S3_BUCKET}")
        logger.info(f"Folder Path: {folder_path}")
        
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=folder_path)
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
        
        # Log file listing details
        logger.info(f"✓ S3 file listing retrieved successfully")
        logger.info(f"Total Files Found: {len(files)}")
        if files:
            logger.info(f"File Details:")
            for idx, file in enumerate(files, 1):
                logger.info(f"  File {idx}:")
                logger.info(f"    Key: {file['key']}")
                logger.info(f"    Size: {file['size']} bytes")
                logger.info(f"    Last Modified: {file['last_modified']}")
        else:
            logger.warning(f"No files found in folder: {folder_path}")
        logger.info(f"========== S3 FILE LISTING COMPLETED ==========\n")
        
        return {
            "status": "success",
            "content": [{
                "text": f"Found {len(files)} files in {folder_path}",
                "data": {
                    "files": files,
                    "folder_path": folder_path
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error listing files: {str(e)}"}]
        }


@tool
def fetch_purchase_order_data(po_number: str) -> dict:
    """
    Fetch Purchase Order data by PO number
    
    Args:
        po_number: Purchase Order number to search for
        
    Returns:
        Dictionary with PO header and item data
    """
    try:
        logger.info(f"========== FETCH PURCHASE ORDER DATA INITIATED ==========")
        logger.info(f"PO Number: {po_number}")
        
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            logger.error(f"Failed to fetch metadata")
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            logger.warning(f"PO {po_number} not found in metadata")
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        logger.info(f"PO Record found in metadata: {po_record}")
        
        # Fetch PO data from the specific file
        po_file_path = f"data/PurchaseOrder/PO_{po_number}.txt"
        logger.info(f"Fetching PO data from: {po_file_path}")
        
        po_data = fetch_s3_data(po_file_path)
        if po_data["status"] != "success":
            logger.error(f"Failed to fetch PO data from {po_file_path}")
            return po_data
        
        # Parse the records
        records = po_data["content"][0]["data"]["all_records"]
        logger.info(f"Total PO records retrieved: {len(records)}")
        
        # Separate header and item records
        header_records = [r for r in records if r.get('PurchaseOrder') and not r.get('PurchaseOrderItem')]
        item_records = [r for r in records if r.get('PurchaseOrderItem')]
        
        logger.info(f"✓ PO Data parsed successfully")
        logger.info(f"Header Records: {len(header_records)}")
        logger.info(f"Item Records: {len(item_records)}")
        
        if header_records:
            logger.info(f"PO Header Details:")
            for idx, header in enumerate(header_records, 1):
                logger.info(f"  Header {idx}: {header}")
        
        if item_records:
            logger.info(f"PO Item Details:")
            for idx, item in enumerate(item_records, 1):
                logger.info(f"  Item {idx}: {item}")
        
        logger.info(f"========== FETCH PURCHASE ORDER DATA COMPLETED ==========\n")
        with open("purchase_order.txt", "w") as ww:
            ww.write(str(records))
            ww.close()  

            
        return {
            "status": "success",
            "content": [{
                "text": f"Purchase Order {po_number} data retrieved",
                "data": {
                    "po_number": po_number,
                    "header_records": header_records,
                    "item_records": item_records,
                    "header_count": len(header_records),
                    "item_count": len(item_records),
                    "metadata_status": po_record.get('status', 'unknown'),
                    "all_records": records
                }
            }]
        }
        
    except Exception as e:
        logger.error(f"Error in fetch_purchase_order_data: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "content": [{"text": f"Error fetching PO data: {str(e)}"}]
        }


@tool
def fetch_goods_receipt_data(po_number: str) -> dict:
    """
    Fetch Goods Receipt data by understanding the metadata.json file
    
    Args:
        po_number: Purchase Order number to search for
        
    Returns:
        Dictionary with GR header and item data
    """
    try:
        logger.info(f"========== FETCH GOODS RECEIPT DATA INITIATED ==========")
        logger.info(f"PO Number: {po_number}")
        
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            logger.error(f"Failed to fetch metadata")
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            logger.warning(f"PO {po_number} not found in metadata")
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        logger.info(f"PO Record found in metadata: {po_record}")
        
        # Check if GRN exists for this PO
        grn_number = po_record.get('grn')
        if not grn_number:
            logger.warning(f"No GRN found for PO {po_number}")
            return {
                "status": "success",
                "content": [{
                    "text": f"No Goods Receipt found for PO {po_number}",
                    "data": {
                        "po_number": po_number,
                        "header_records": [],
                        "item_records": [],
                        "header_count": 0,
                        "item_count": 0,
                        "status": "no_grn"
                    }
                }]
            }
        
        logger.info(f"GRN Number identified: {grn_number}")
        
        # Fetch GR data from the specific file
        gr_file_path = f"data/GoodsReceipt/GRN_{grn_number}.txt"
        logger.info(f"Fetching GR data from: {gr_file_path}")
        
        gr_data = fetch_s3_data(gr_file_path)
        if gr_data["status"] != "success":
            logger.error(f"Failed to fetch GR data from {gr_file_path}")
            return gr_data
        
        # Parse the records
        records = gr_data["content"][0]["data"]["all_records"]
        logger.info(f"Total GR records retrieved: {len(records)}")
        
        # Separate header and item records
        # Many GR feeds do not include MaterialDocumentItem at item level.
        # Treat records with typical item-level fields as items even if MaterialDocumentItem is absent.
        # Header: Has MaterialDocument and lacks common item fields
        header_records = [
            r for r in records
            if r.get('MaterialDocument') and not (
                r.get('PurchaseOrderItem') or r.get('MaterialDocumentItem') or r.get('QuantityInEntryUnit') or r.get('PurchaseOrder')
            )
        ]
        # Items: Recognize by presence of PurchaseOrderItem OR MaterialDocumentItem OR QuantityInEntryUnit
        item_records = [
            r for r in records
            if r.get('PurchaseOrderItem') or r.get('MaterialDocumentItem') or r.get('QuantityInEntryUnit')
        ]
        
        logger.info(f"DEBUG: Checking GR record structure...")
        for idx, record in enumerate(records[:3], 1):
            logger.info(f"  GR Record {idx} keys: {list(record.keys())}")
            logger.info(f"  Has MaterialDocument: {bool(record.get('MaterialDocument'))}")
            logger.info(f"  Has MaterialDocumentItem: {bool(record.get('MaterialDocumentItem'))}")
        
        logger.info(f"✓ GR Data parsed successfully")
        logger.info(f"Header Records: {len(header_records)}")
        logger.info(f"Item Records: {len(item_records)}")
        
        if header_records:
            logger.info(f"GR Header Details:")
            for idx, header in enumerate(header_records, 1):
                logger.info(f"  Header {idx}: {header}")
        
        if item_records:
            logger.info(f"GR Item Details:")
            for idx, item in enumerate(item_records, 1):
                logger.info(f"  Item {idx}: {item}")
        
        logger.info(f"========== FETCH GOODS RECEIPT DATA COMPLETED ==========\n")
        with open("goods_receipt.txt", "w") as ww:
            ww.write(str(records))
            ww.close()

        return {
            "status": "success",
            "content": [{
                "text": f"Goods Receipt data for PO {po_number} retrieved",
                "data": {
                    "po_number": po_number,
                    "grn_number": grn_number,
                    "header_records": header_records,
                    "item_records": item_records,
                    "header_count": len(header_records),
                    "item_count": len(item_records),
                    "all_records": records
                }
            }]
        }
        
    except Exception as e:
        logger.error(f"Error in fetch_goods_receipt_data: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "content": [{"text": f"Error fetching GR data: {str(e)}"}]
        }


@tool
def fetch_sop_document(sop_id: str) -> dict:
    """
    Fetch SOP document from S3 bucket
    
    Args:
        sop_id: SOP ID (e.g., 'SOP_PRICE_CHANGE_V1')
        
    Returns:
        Dictionary with SOP document content
    """
    try:
        # Construct S3 key for SOP document
        sop_file_path = f"sop/{sop_id}.txt"
        
        # Fetch object from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=sop_file_path)
        content = response['Body'].read().decode('utf-8')
        
        return {
            "status": "success",
            "content": [{
                "text": f"Successfully fetched SOP document {sop_id}",
                "data": {
                    "sop_id": sop_id,
                    "sop_content": content,
                    "file_path": sop_file_path
                }
            }]
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            return {
                "status": "error",
                "content": [{"text": f"SOP document not found: {sop_id}"}]
            }
        else:
            return {
                "status": "error", 
                "content": [{"text": f"S3 error: {str(e)}"}]
            }
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error fetching SOP document: {str(e)}"}]
        }


@tool
def fetch_supplier_invoice_data(po_number: str) -> dict:
    """
    Fetch Supplier Invoice data by PO number
    
    Args:
        po_number: Purchase Order number to search for
        
    Returns:
        Dictionary with invoice header and item data
    """
    try:
        logger.info(f"========== FETCH SUPPLIER INVOICE DATA INITIATED ==========")
        logger.info(f"PO Number: {po_number}")
        
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            logger.error(f"Failed to fetch metadata")
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            logger.warning(f"PO {po_number} not found in metadata")
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        logger.info(f"PO Record found in metadata: {po_record}")
        
        # Check if Invoice exists for this PO
        invoice_number = po_record.get('invoice')
        if not invoice_number:
            logger.warning(f"No Invoice found for PO {po_number}")
            return {
                "status": "success",
                "content": [{
                    "text": f"No Invoice found for PO {po_number}",
                    "data": {
                        "po_number": po_number,
                        "header_records": [],
                        "item_records": [],
                        "header_count": 0,
                        "item_count": 0,
                        "status": "no_invoice"
                    }
                }]
            }
        
        logger.info(f"Invoice Number identified: {invoice_number}")
        
        # Fetch Invoice data from the specific file
        invoice_file_path = f"data/SupplierInvoice/INV_{invoice_number}.txt"
        logger.info(f"Fetching Invoice data from: {invoice_file_path}")
        
        invoice_data = fetch_s3_data(invoice_file_path)
        if invoice_data["status"] != "success":
            logger.error(f"Failed to fetch Invoice data from {invoice_file_path}")
            return invoice_data
        
        # Parse the records
        records = invoice_data["content"][0]["data"]["all_records"]
        logger.info(f"Total Invoice records retrieved: {len(records)}")
        
        # Separate header and item records
        # Header: Has BillingDocument but NO ReferencePurchaseOrderItem
        # Items: Have ReferencePurchaseOrderItem but NO BillingDocument
        header_records = [r for r in records if r.get('BillingDocument') and not r.get('ReferencePurchaseOrderItem')]
        item_records = [r for r in records if r.get('ReferencePurchaseOrderItem')]
        
        logger.info(f"DEBUG: Checking Invoice record structure...")
        for idx, record in enumerate(records[:3], 1):
            logger.info(f"  Invoice Record {idx} keys: {list(record.keys())}")
            logger.info(f"  Has BillingDocument: {bool(record.get('BillingDocument'))}")
            logger.info(f"  Has ReferencePurchaseOrderItem: {bool(record.get('ReferencePurchaseOrderItem'))}")
        
        logger.info(f"✓ Invoice Data parsed successfully")
        logger.info(f"Header Records: {len(header_records)}")
        logger.info(f"Item Records: {len(item_records)}")
        
        if header_records:
            logger.info(f"Invoice Header Details:")
            for idx, header in enumerate(header_records, 1):
                logger.info(f"  Header {idx}: {header}")
        
        if item_records:
            logger.info(f"Invoice Item Details:")
            for idx, item in enumerate(item_records, 1):
                logger.info(f"  Item {idx}: {item}")

        
        logger.info(f"========== FETCH SUPPLIER INVOICE DATA COMPLETED ==========\n")
        with open("invoice.txt", "w") as ww:
            ww.write(str(records))
            ww.close()
        return {
            "status": "success",
            "content": [{
                "text": f"Supplier Invoice data for PO {po_number} retrieved",
                "data": {
                    "po_number": po_number,
                    "invoice_number": invoice_number,
                    "header_records": header_records,
                    "item_records": item_records,
                    "header_count": len(header_records),
                    "item_count": len(item_records),
                    "all_records": records
                }
            }]
        }
        
    except Exception as e:
        logger.error(f"Error in fetch_supplier_invoice_data: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "content": [{"text": f"Error fetching invoice data: {str(e)}"}]
        }


@tool
def categorize_exception(exception_data: dict) -> dict:
    """
    Categorize exception into one of the 6 SOP types and generate complete database records.
    This tool now RELIES on the agent-provided analysis for exception_type and related fields.
    Severity will be assigned ONLY during SOP analysis; it is not set here.
    
    Args:
        exception_data: Dictionary containing agent-derived exception analysis data
        Expected keys (agent-provided where applicable):
          - exception_type (REQUIRED): "Quantity Variance", "Price Mismatch", "Missing GR", "Missing PO", "UoM Issues", "Invoice Errors"
          - exception_code (optional): e.g., "EXC-001"; if missing, derived from EXCEPTION_TYPE_CODES
          - exception_summary (optional)
          - exception_value (optional, numeric)
          - confidence_score (optional, numeric)
          - po_number (REQUIRED)
          - item_number (optional)
          - records_processed (optional, default 1)
          - agent_reasoning_summary (optional)
    
    Returns:
        Dictionary containing:
        - exception_records: Complete database records for exceptions table with ALL required fields:
          * exception_id: Unique exception identifier (EXC-XXX-XXXX format)
          * exception_type: Type of exception (Quantity Variance, Price Mismatch, etc.)
          * severity: Severity level (Low, Medium, High)
          * status: Exception status (Pending, Resolved, Escalated)
          * po_number: Related Purchase Order number
          * item_number: Related item number
          * created_at: Creation timestamp
          * updated_at: Update timestamp
          * sop_connected: Connected SOP document ID
          * exception_summary: Detailed exception summary
          * exception_value: Numeric value of the exception (variance amount)
          * records_processed: Number of records processed
          * agent_reasoning_summary: AI reasoning for the exception
          * sop_summary: SOP-based summary
    """
    try:
        # Extract required/optional fields provided by the agent
        exception_type = exception_data.get('exception_type')
        if not exception_type:
            return {
                "status": "error",
                "content": [{"text": "categorize_exception requires 'exception_type' decided by agent."}]
            }
        po_number = exception_data.get('po_number', 'Unknown')
        item_number = exception_data.get('item_number')
        exception_code = exception_data.get('exception_code')
        if not exception_code:
            code_suffix = EXCEPTION_TYPE_CODES.get(exception_type, "000")
            exception_code = f"EXC-{code_suffix}"
        exception_summary = exception_data.get('exception_summary', '')
        exception_value = float(exception_data.get('exception_value', 0.0) or 0.0)
        confidence_score = float(exception_data.get('confidence_score', 0.0) or 0.0)
        records_processed = int(exception_data.get('records_processed', 1) or 1)
        agent_reasoning = exception_data.get('agent_reasoning_summary', '')
        
        # IDs and timestamps
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        random_suffix = random.randint(1000, 9999)
        exception_id = f"EXC-{exception_code.replace('EXC-', '')}-{random_suffix:04d}" if exception_code != "none" else f"NO-EXC-{random_suffix:04d}"
        sop_connected = f"SOP_{exception_type.upper().replace(' ', '_')}_V1" if exception_type != "No Exception" else "N/A"
        
        # Build exception record WITHOUT severity; severity handled by SOP analysis later
        exception_record = {
            "exception_id": exception_id,
            "exception_type": exception_type,
            "severity": severity,
            "status": "Pending",
            "po_number": po_number,
            "created_at": current_timestamp,
            "updated_at": current_timestamp,
            "sop_connected": sop_connected,
            "exception_summary": exception_summary,
            "exception_value": exception_value,
            "records_processed": records_processed,
            "agent_reasoning_summary": agent_reasoning
        }
        
        return {
            "status": "success",
            "content": [{
                "text": f"Exception prepared for SOP analysis: {exception_type}",
                "data": {
                    "exception_type": exception_type,
                    "exception_code": exception_code,
                    "severity": severity,
                    "confidence_score": confidence_score,
                    "exception_record": exception_record,
                    "analysis_data": exception_data
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error categorizing exception: {str(e)}"}]
        }


def _get_price_change_action(severity: str) -> str:
    """Get recommended action for price change based on severity"""
    if severity == "Low":
        return "Informational only - monitor for trends."
    elif severity == "Medium":
        return "Review vendor pricing before payment."
    else:
        return "Escalate to procurement/AP lead - significant variance detected."

def _get_quantity_change_action(severity: str) -> str:
    """Get recommended action for quantity change based on severity"""
    if severity == "Low":
        return "Note only - minor quantity variance."
    elif severity == "Medium":
        return "Reconfirm with warehouse - verify received quantities."
    else:
        return "Hold until receipt confirmation."

@tool
def sop_based_analysis(exceptions_data: List[dict]) -> dict:
    """
    Perform comprehensive SOP-based analysis for single or multiple exceptions.
    This tool automatically fetches relevant SOP documents and provides detailed
    recommendations based on SOP procedures and guidelines.
    
    The agent will:
    1. Analyze exception data to identify exception types
    2. Fetch relevant SOP documents from S3 bucket
    3. Apply SOP procedures to each exception
    4. Generate detailed recommendations and action items
    5. Provide reasoning summaries based on SOP guidelines
    6. Return comprehensive analysis with all required information
    
    Args:
        exceptions_data: List of exception dictionaries containing:
            - exception_type: Type of exception (Quantity Variance, Price Mismatch, etc.)
            - severity: Severity level (Low, Medium, High)
            - variance_data: Detailed variance information
            - po_number: Purchase Order number
            - item_number: Item number
            - Any other relevant analysis data
        
    Returns:
        Dictionary containing:
        - sop_analysis: Complete SOP-based analysis results
        - sop_documents: Fetched SOP documents with content
        - recommendations: Detailed recommendations for each exception
        - action_items: Specific action items based on SOP procedures
        - reasoning_summaries: AI reasoning based on SOP guidelines
        - confidence_scores: Confidence levels for each recommendation
        - json_outputs: Structured JSON outputs for each exception
        - consolidated_summary: Overall analysis summary
        
    Available SOP Documents:
    - SOP_QUANTITY_CHANGE_V1: For quantity variance exceptions
    - SOP_PRICE_CHANGE_V1: For price mismatch exceptions
    - SOP_DUPLICATE_INVOICE_V1: For invoice error exceptions
    - SOP_MISSING_GRN_V1: For missing goods receipt exceptions
    - SOP_UOM_ISSUE_V1: For unit of measure issues
    - SOP_MISSING_PO_V1: For missing purchase order exceptions
    """
    try:
        # Let the agent handle the complete SOP-based analysis automatically
        # The agent will use other tools to fetch SOPs, analyze exceptions, and generate recommendations
        
        return {
            "status": "success",
            "content": [{
                "text": f"SOP-based analysis initiated for {len(exceptions_data)} exception(s). The agent will automatically fetch relevant SOP documents, analyze each exception against SOP procedures, and provide comprehensive recommendations and action items.",
                "data": {
                    "exceptions_count": len(exceptions_data),
                    "analysis_type": "comprehensive_sop_analysis",
                    "agent_instructions": "Perform complete SOP-based analysis including SOP document fetching, exception analysis against SOP procedures, recommendation generation, and comprehensive reporting",
                    "expected_outputs": [
                        "SOP documents fetched and analyzed",
                        "Exception-specific recommendations",
                        "Action items based on SOP procedures",
                        "Reasoning summaries following SOP guidelines",
                        "Confidence scores for recommendations",
                        "Structured JSON outputs",
                        "Consolidated analysis summary"
                    ],
                    "sop_documents_available": [
                        "SOP_QUANTITY_CHANGE_V1",
                        "SOP_PRICE_CHANGE_V1", 
                        "SOP_DUPLICATE_INVOICE_V1",
                        "SOP_MISSING_GRN_V1",
                        "SOP_UOM_ISSUE_V1",
                        "SOP_MISSING_PO_V1"
                    ],
                    "analysis_scope": "Single or multiple exceptions with comprehensive SOP integration"
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error initiating SOP-based analysis: {str(e)}"}]
        }


@tool
def insert_to_database(analysis_data: dict) -> dict:
    """
    Insert 3-way match analysis results into PostgreSQL database
    
    Args:
        analysis_data: Dictionary containing analysis results with database_records
                      MUST include ALL four tables:
                      - purchase_order_records: PO data with all required fields
                      - invoice_records: Invoice data with all required fields
                      - goods_receipt_records: GRN data with all required fields
                      - exception_records: Exception data with ALL required fields
        
    Returns:
        Dictionary with insertion status and results

    CRITICAL: The analysis_data MUST contain database_records with ALL FOUR record types:
    {
        "database_records": {
            "purchase_order_records": [...],
            "invoice_records": [...],
            "goods_receipt_records": [...],
            "exception_records": [...]  # MUST BE INCLUDED
        }
    }
    
    Exception records MUST include ALL fields from db_column_names.txt:
    - exception_id, exception_type, severity, status, po_number
    - created_at, updated_at, sop_connected, exception_summary
    - exception_value, sop_summary, records_processed
    """

    payload = {"event_type": "insert_new_data", "analysis_data": analysis_data}

    insert_data_lambda(payload)




def _generate_database_records(po_number: str, po_record: dict, analysis_results: list, match_status: str) -> dict:
    """
    Generate database-compatible JSON records for PO, Invoice, and GRN tables based on analysis results.
    Exception records are handled separately by the categorize_exception tool.
    
    Args:
        po_number: Purchase Order number
        po_record: PO metadata record
        analysis_results: 3-way match analysis results
        match_status: Overall match status
        
    Returns:
        Dictionary with database records for PO, Invoice, GRN, and Exception tables.
        
        Structure:
        {
            "purchase_order_records": [
                {
                    "po_id": str,              # Unique PO record ID (e.g., "PO_001_4500000001")
                    "po_number": str,          # Purchase Order number (e.g., "4500000001")
                    "vendor": str,             # Vendor name (e.g., "Vendor ABC Corp")
                    "order_quantity": int,     # Ordered quantity
                    "unit_price": float,       # Unit price from PO
                    "uom": str,                # Unit of measure (e.g., "EA")
                    "total_value": float,      # Total order value
                    "created_at": str,         # Timestamp (ISO 8601 format)
                    "updated_at": str,         # Timestamp (ISO 8601 format)
                    "exception_id": str,       # Exception ID if applicable (e.g., "EXC-001-7832")
                    "po_date": str,            # PO date (YYYY-MM-DD)
                    "currency": str,           # Currency code (e.g., "USD")
                    "delivery_date": str       # Expected delivery date (YYYY-MM-DD)
                }
            ],
            "invoice_records": [
                {
                    "invoice_id": str,         # Unique invoice record ID (e.g., "INV_001_SI001")
                    "invoice_number": str,     # Invoice number (e.g., "SI001")
                    "po_number": str,          # Related PO number
                    "invoiced_quantity": int,  # Invoiced quantity
                    "unit_price": float,       # Unit price from invoice
                    "invoice_date": str,       # Invoice date (YYYY-MM-DD)
                    "currency": str,           # Currency code (e.g., "USD")
                    "created_at": str,         # Timestamp (ISO 8601 format)
                    "updated_at": str,         # Timestamp (ISO 8601 format)
                    "exception_id": str,       # Exception ID if applicable
                    "uom": str,                # Unit of measure
                    "total_value": float       # Total invoice value
                }
            ],
            "goods_receipt_records": [
                {
                    "gr_id": str,              # Unique GR record ID (e.g., "GR_001_GR001")
                    "gr_number": str,          # Goods Receipt number (e.g., "GR001")
                    "po_number": str,          # Related PO number
                    "received_quantity": int,  # Received quantity
                    "receipt_date": str,       # Receipt date (YYYY-MM-DD)
                    "variance": float,         # Quantity variance
                    "created_at": str,         # Timestamp (ISO 8601 format)
                    "updated_at": str,         # Timestamp (ISO 8601 format)
                    "exception_id": str,       # Exception ID if applicable
                    "uom": str,                # Unit of measure
                    "currency": str            # Currency code
                }
            ],
            "exception_records": [
                {
                    "exception_id": str,                # Unique exception ID (e.g., "EXC-001-7832")
                    "exception_type": str,              # Exception type (e.g., "Quantity Variance")
                    "severity": str,                    # Severity level (Low/Medium/High)
                    "status": str,                      # Status (Pending/Resolved/Cancelled)
                    "po_number": str,                   # Related PO number
                    "created_at": str,                  # Timestamp (ISO 8601 format)
                    "updated_at": str,                  # Timestamp (ISO 8601 format)
                    "sop_connected": str,               # Connected SOP procedure (e.g., "SOP_QUANTITY_CHANGE_V1")
                    "exception_summary": str,           # Brief summary of the exception
                    "exception_value": float,           # Financial impact value
                    "records_processed": int,           # Number of records processed
                    "agent_reasoning_summary": str,     # Detailed AI agent reasoning and analysis
                    "sop_summary": str                  # SOP procedure summary and resolution steps
                }
            ]
        }
    """
    logger.info(f"Generating database records for PO {po_number}")
    logger.info(f"Analysis results count: {len(analysis_results)}")
    
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    # Generate Purchase Order, Invoice, and Goods Receipt records
    purchase_order_records = []
    invoice_records = []
    goods_receipt_records = []
    
    # Process each analysis result to generate database records
    for i, item in enumerate(analysis_results):
        item_number = item.get('item_number', '')
        logger.info(f"Processing analysis result {i+1}: item {item_number}")
        
        # Purchase Order Record - Include ALL required fields from database schema (db_column_names.txt)
        po_record_data = {  
            "po_number": po_number,  # varchar(20) NOT NULL
            "vendor": item.get('vendor', 'Unknown Vendor'),  # varchar(150) NOT NULL
            "order_quantity": item.get('ordered_qty', 0),  # int4 NOT NULL
            "unit_price": item.get('ordered_unit_price', 0.0),  # numeric(15, 2) NOT NULL
            "uom": item.get('po_uom', 'EA'),  # varchar(10) NOT NULL
            "total_value": item.get('ordered_amount', 0.0),  # numeric(15, 2) NOT NULL
            "created_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
            "updated_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
            "exception_id": item.get('exception_id', None),  # varchar(20) NULL - Will be populated by agent if exception exists
            "po_date": item.get('po_date', datetime.now().strftime('%Y-%m-%d')),  # date NULL
            "currency": item.get('currency', 'INR'),  # varchar(10) NULL
            "delivery_date": item.get('delivery_date', None)  # date NULL
        }
        purchase_order_records.append(po_record_data)
        logger.info(f"Generated PO record for PO {po_number} item {item_number}")
        
        # Invoice Record (if invoice exists) - Include ALL required fields from database schema (db_column_names.txt)
        if item.get('has_invoice', False):
            invoice_record_data = {  # Auto-increment ID (serial4)
                "invoice_number": item.get('invoice_number', f"INV-{po_number}-{item_number}"),  # varchar(20) NOT NULL
                "po_number": po_number,  # varchar(20) NOT NULL
                "invoiced_quantity": item.get('invoiced_qty', 0),  # int4 NOT NULL
                "unit_price": item.get('invoiced_unit_price', 0.0),  # numeric(15, 2) NOT NULL
                "invoice_date": item.get('invoice_date', datetime.now().strftime('%Y-%m-%d')),  # date NOT NULL
                "currency": item.get('currency', 'INR'),  # varchar(10) NOT NULL
                "created_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
                "updated_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
                "exception_id": item.get('exception_id', None),  # varchar(20) NULL - Will be populated by agent if exception exists
                "uom": item.get('invoice_uom', 'EA'),  # varchar(10) NULL
                "total_value": item.get('invoiced_amount', item.get('invoiced_qty', 0) * item.get('invoiced_unit_price', 0.0))  # numeric(15, 2) NULL
            }
            invoice_records.append(invoice_record_data)
            logger.info(f"Generated invoice record for PO {po_number} item {item_number}")
        
        # Goods Receipt Record (if GR exists) - Include ALL required fields from database schema (db_column_names.txt)
        if item.get('has_gr', False):
            gr_record_data = {  # Auto-increment ID (serial4)
                "gr_number": item.get('grn_number', f"GR-{po_number}-{item_number}"),  # varchar(20) NOT NULL
                "po_number": po_number,  # varchar(20) NOT NULL
                "received_quantity": item.get('received_qty', 0),  # int4 NOT NULL
                "receipt_date": item.get('receipt_date', datetime.now().strftime('%Y-%m-%d')),  # date NOT NULL
                "variance": item.get('qty_variance', 0.0),  # numeric(5, 2) NULL
                "created_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
                "updated_at": current_timestamp,  # timestamp DEFAULT CURRENT_TIMESTAMP
                "exception_id": item.get('exception_id', None),  # varchar(20) NULL - Will be populated by agent if exception exists
                "uom": item.get('gr_uom', item.get('po_uom', 'EA')),  # varchar(10) NULL
                "currency": item.get('currency', 'INR')  # varchar(10) NULL
            }
            goods_receipt_records.append(gr_record_data)
            logger.info(f"Generated GR record for PO {po_number} item {item_number}")
    
    result = {
        "purchase_order_records": purchase_order_records,
        "invoice_records": invoice_records,
        "goods_receipt_records": goods_receipt_records
    }
    
    logger.info(f"Database records generation completed:")
    logger.info(f"  - Purchase Orders: {len(purchase_order_records)}")
    logger.info(f"  - Invoices: {len(invoice_records)}")
    logger.info(f"  - Goods Receipts: {len(goods_receipt_records)}")
    logger.info("  - Exceptions: Handled separately by categorize_exception tool")
    
    return result


@tool
def perform_three_way_match(po_all_records: Optional[List[Dict[str, Any]]] = None,gr_all_records: Optional[List[Dict[str, Any]]] = None,invoice_all_records:Optional[List[Dict[str, Any]]] = None) -> dict:  

    with open("log.txt", "w") as ww:

        
        ww.write(f"\nPO_ALL_RECORDS: {po_all_records}, GR_ALL_RECORDS: {gr_all_records}, INVOICE_ALL_RECORDS: {invoice_all_records}")

        ww.close()
   
    with open("purchase_order.txt", "r") as ww:
        po_all_records = ww.read()
        ww.close()

    with open("goods_receipt.txt", "r") as ww:
        gr_all_records = ww.read()
        ww.close()

    with open("invoice.txt", "r") as ww:
        invoice_all_records = ww.read()
        ww.close()
    with open("three_way_match_response.txt", "w") as ww:
        ww.write("response")
        ww.close()

    client = boto3.client('bedrock-runtime', region_name='us-west-2')
    prompt = f"""You are a 3-Way Match Analysis Agent. Your task is to perform a detailed 3-way match analysis between Purchase Orders (PO), Goods Receipts (GR), and Supplier Invoices using ONLY the provided datasets ({po_all_records}, {gr_all_records}, {invoice_all_records})"""
    response = client.invoke_model(
        modelId='us.anthropic.claude-sonnet-4-20250514-v1:0',
        contentType='application/json',
        accept='application/json',
        body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": prompt}]
    })

    )
    response_body = json.loads(response.get("body").read())
    final_text = response_body["content"][0]["text"]

    with open("three_way_match_response.txt", "w") as ww:
        ww.write(final_text)
        ww.close()



    print("Final Response:", final_text)

    return final_text




        
        # return {
        #     "status": "success",
        #     "content": [{
        #         "text": (
        #             f"3-way match analysis initiated for PO {po_number}. Use ONLY the provided datasets "
        #             f"(PO records: {po_count}, GR records: {gr_count}, Invoice records: {inv_count}). "
        #             "Perform item-level matching and prepare PO/Invoice/GRN records. Exception categorization is out of scope."
        #         ),
        #         "data": {
        #             "po_number": po_number,
        #             "analysis_type": "three_way_match_only",
        #             "agent_instructions": (
        #                 "Use the provided po_all_records, gr_all_records, and invoice_all_records. "
        #                 "Do NOT fetch data. Perform 3-way matching only and generate database-ready records. "
        #                 "Do not handle SOP or exceptions."
        #             ),
        #             "inputs_summary": {
        #                 "po_records_count": po_count,
        #                 "gr_records_count": gr_count,
        #                 "invoice_records_count": inv_count
        #             },
        #             "expected_outputs": [
        #                 "Purchase Order records with all required fields",
        #                 "Invoice records with all required fields", 
        #                 "Goods Receipt records with all required fields",
        #                 "3-way match analysis results",
        #                 "Variance data for exception categorization"
        #             ],
        #             "database_tables_covered": [
        #                 "purchase_order",
        #                 "invoice", 
        #                 "goods_receipt"
        #             ],
        #             "exception_handling": "Delegated to categorize_exception tool"
        #         }
        #     }]
        # }
        
    # except Exception as e:
    #     return {
    #         "status": "error",
    #         "content": [{"text": f"Error initiating 3-way match analysis: {str(e)}"}]
    #     }


def create_sap_agent() -> Agent:
    """
    Create and configure the SAP S3 analysis agent
    
    Returns:
        Configured Strands Agent instance
    """
    
    # Define available tools for 3-way match analysis with SOP integration
    tools = [
        fetch_metadata,
        fetch_s3_data,
        list_s3_files,
        fetch_purchase_order_data,
        fetch_goods_receipt_data,
        fetch_supplier_invoice_data,
        fetch_sop_document,
        categorize_exception,
        sop_based_analysis,
        perform_three_way_match,
        insert_to_database
    ]
    
    # Configure model
    model = BedrockModel(
        model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    )
    
    # System prompt for 3-way match analysis with SOP integration
    system_prompt = """

You are SAP 3-Way Match Agent with SOP Integration, a specialized assistant for performing 3-way matching between Purchase Orders, Goods Receipts, and Supplier Invoices with automated exception categorization and SOP-based recommendations.
  
Tool call guidelines:
- ALWAYS start by fetching metadata.json using fetch_metadata() tool
- Use fetch_s3_data to get PO, GRN, and Invoice data based on metadata
- CALL FETCH TOOLS ONLY ONCE EACH THEN MOVE ON TO CALL THE PERFORM THREE WAY MATCH TOOL
- PLEASE Use perform_three_way_match() tool to do 3-way matching  after fetching the data.
- Use categorize_exception to categorize exceptions into defined types
- Use sop_based_analysis to perform SOP-based analysis for exceptions
- Finally, use insert_to_database to store results in PostgreSQL database

⚠️ CRITICAL: METADATA.JSON IS THE SOURCE OF TRUTH FOR DOCUMENT RELATIONSHIPS ⚠️
- ALWAYS start by fetching metadata.json using fetch_metadata() tool
- metadata.json contains the relationships between PO numbers, GRN numbers, and Invoice numbers
- metadata.json structure: [{"po": "4500000001", "grn": "5100000450", "invoice": "90005001", "status": "matched"}, ...]
- From metadata.json, extract the PO number, GRN number (if available), and Invoice number (if available)
- Use these numbers to fetch ALL THREE documents:
- Even if metadata shows grn: null or invoice: null, still attempt to fetch - the tools handle missing documents gracefully
- The metadata.json file is located at: data/metadata.json in S3 bucket

⚠️ THE 6 EXCEPTION TYPES - EXPLICITLY DEFINED ⚠️
The system categorizes exceptions into exactly 6 types, each with a specific code and SOP document:

1. QUANTITY VARIANCE (EXC-001) - SOP_QUANTITY_CHANGE_V1
   - Description: Mismatch between ordered quantity, received quantity, and/or invoiced quantity
   - Detection: When OrderQuantity ≠ ReceivedQuantity OR OrderQuantity ≠ InvoicedQuantity OR ReceivedQuantity ≠ InvoicedQuantity
   - Examples: PO ordered 100 units, GRN received 95 units, Invoice billed 100 units
   - SOP Document: SOP_QUANTITY_CHANGE_V1.txt in S3 sop/ folder

2. PRICE MISMATCH (EXC-002) - SOP_PRICE_CHANGE_V1
   - Description: Unit price variance between Purchase Order and Supplier Invoice
   - Detection: When PO unit price ≠ Invoice unit price (difference > $0.01)
   - Examples: PO unit price $10.50, Invoice unit price $11.00
   - SOP Document: SOP_PRICE_CHANGE_V1.txt in S3 sop/ folder

3. INVOICE ERRORS (EXC-003) - SOP_DUPLICATE_INVOICE_V1
   - Description: Duplicate invoices detected (same vendor, PO number, and amount)
   - Detection: Multiple invoices with identical vendor, PO reference, and total amount
   - Examples: Two invoices with same invoice number or same vendor/PO/amount combination
   - SOP Document: SOP_DUPLICATE_INVOICE_V1.txt in S3 sop/ folder

4. MISSING GR (EXC-004) - SOP_MISSING_GRN_V1 - ⚠️ HIGHEST PRIORITY ⚠️
   - Description: ITEM-LEVEL CHECK - If ANY PO item has Invoice item but NO corresponding GRN item found
   - Detection: For EACH PO item, check item-by-item:
     * If PO Item X exists AND Invoice Item X exists BUT no GRN Item X found → EXC-004 for that item
     * If Invoice Item X exists but no GRN Item X found → EXC-004 for that item
   - Examples:
     * PO Item 10 has Invoice Item 10 but no GRN Item 10 → Create EXC-004 exception for Item 10
     * PO Item 20 has Invoice Item 20 but GRN Item 20 missing → Create EXC-004 exception for Item 20
   - Severity: High (invoice should not be paid without goods receipt confirmation)
   - Priority: HIGHEST - Check this FIRST before other exceptions
   - SOP Document: SOP_MISSING_GRN_V1.txt in S3 sop/ folder

5. UOM ISSUES (EXC-005) - SOP_UOM_ISSUE_V1
   - Description: Unit of Measure (UoM) inconsistency without valid conversion between documents
   - Detection: Different UoM values across PO, GRN, or Invoice without conversion factor
   - Examples: PO uses "PC" (pieces), Invoice uses "EA" (each) without conversion
   - SOP Document: SOP_UOM_ISSUE_V1.txt in S3 sop/ folder

6. MISSING PO (EXC-006) - SOP_MISSING_PO_V1 - ⚠️ HIGHEST PRIORITY ⚠️
   - Description: ITEM-LEVEL CHECK - If ANY Invoice item exists without valid Purchase Order item reference
   - Detection: For EACH Invoice item, check item-by-item:
     * If Invoice Item X exists but no corresponding PO Item X found → EXC-006 for that item
     * If Invoice references PO number but PO Item X doesn't exist → EXC-006 for that item
   - Examples:
     * Invoice Item 30 exists but PO Item 30 doesn't exist → Create EXC-006 exception for Item 30
     * Invoice posted for PO 4500000001 Item 40, but PO Item 40 not found → Create EXC-006 exception for Item 40
   - Priority: HIGHEST - Check this FIRST before other exceptions
   - SOP Document: SOP_MISSING_PO_V1.txt in S3 sop/ folder

⚠️ MANDATORY CARDINALITY RULE - EQUAL RECORD COUNTS ACROSS ALL TABLES ⚠️
- PO record count = Invoice record count = GRN record count = Exception record count
- If 3 exceptions exist → 3 PO records + 3 Invoice records + 3 GRN records + 3 Exception records
- If Item 10 has 2 exceptions (quantity + price) → Create 2 PO records for Item 10, 2 Invoice records for Item 10, 2 GRN records for Item 10
- Each exception gets its own complete set of PO/Invoice/GRN records
- This is a 1:1:1:1 relationship - every exception has exactly one PO, one Invoice, one GRN record

CRITICAL ANTI-HALLUCINATION RULES - MUST FOLLOW:
- NEVER make up, invent, or fabricate any data that you cannot verify from actual fetched sources
- NEVER assume data exists if you cannot successfully fetch it from S3
- NEVER provide analysis on data that was not successfully retrieved
- ALWAYS explicitly state when data is missing, unavailable, or could not be fetched
- ALWAYS report tool execution failures and their impact on analysis
- NEVER fill in missing data with placeholder values or estimates
- NEVER proceed with analysis if critical data sources are unavailable
- ALWAYS validate that all required data was successfully fetched before analysis
- NEVER make assumptions about document relationships without metadata confirmation
- ALWAYS report "DATA NOT AVAILABLE" or "FETCH FAILED" when tools return errors
- NEVER provide recommendations based on incomplete or missing data
- ALWAYS distinguish between "no data found" and "data not fetched due to error"

MANDATORY ANALYSIS FLOW - FOLLOW EXACTLY:
Every time you run, your goal is to follow this exact sequence:

1. RETRIEVE STRUCTURED DATA FROM S3
    call the fetch_metadata tool first . then use the metadata to call the fetch data tools
   - Fetch metadata to understand document relationships
   - Retrieve PO data from S3 bucket
   - Retrieve GRN data from S3 bucket  
   - Retrieve Invoice data from S3 bucket
   - VERIFY ALL DATA FETCHES SUCCEEDED before proceeding

2. PERFORM 3-WAY MATCHING ACROSS ALL RELATED RECORDS (ITEM-BY-ITEM)
     call the perform_three_way_match tool with fetched data
   - CRITICAL: Match items ITEM-BY-ITEM, not document-level
   - For EACH PO item (identified by PurchaseOrderItem number):
     * Find corresponding GRN item (match by PurchaseOrderItem number in GRN)
     * Find corresponding Invoice item (match by ReferencePurchaseOrderItem number in Invoice)
   - For EACH Invoice item (identified by ReferencePurchaseOrderItem):
     * Verify corresponding PO item exists (match by PurchaseOrderItem number)
     * Verify corresponding GRN item exists (match by PurchaseOrderItem number)
   - Identify all relationships and connections between documents at ITEM LEVEL
   - Create a mapping of which items exist in which documents

3. IDENTIFY ANY MISMATCHES OR MISSING LINKS (ITEM-LEVEL CHECKING - HIGHEST PRIORITY)
   - CRITICAL FIRST STEP: Check for missing documents ITEM-BY-ITEM (not document-level):
     * For EACH PO item, verify if corresponding GRN item AND Invoice item exist
     * If ANY PO item has Invoice but NO GRN item → Flag as EXC-004 (Missing GR) - HIGHEST PRIORITY
     * If ANY Invoice item exists but NO PO item → Flag as EXC-006 (Missing PO) - HIGHEST PRIORITY
     * Missing document exceptions take precedence over all other exceptions
   - THEN check for other mismatches (only if items exist in all documents):
     * Detect quantity variances between ordered/received/invoiced (EXC-001)
     * Detect price mismatches between PO and Invoice (EXC-002)
     * Detect UoM inconsistencies (EXC-005)
     * Identify duplicate invoices (EXC-003)
     * Check for data quality issues

4. DETERMINE WHICH EXCEPTION TYPE HAS OCCURRED (ITEM-LEVEL PRIORITY ORDER)
    - Use categorize_exception tool to classify EACH identified mismatch
   - CRITICAL: Categorize exceptions in THIS EXACT ORDER of priority:
     * FIRST PRIORITY: Missing GR (EXC-004) - ITEM-LEVEL check if ANY PO item has Invoice but NO GRN item
     * FIRST PRIORITY: Missing PO (EXC-006) - ITEM-LEVEL check if ANY Invoice item exists without PO item
     * SECOND PRIORITY: Check other variances ONLY if items exist in all documents:
       - Quantity Variance (EXC-001) - Ordered vs Received vs Invoiced mismatch
       - Price Mismatch (EXC-002) - PO vs Invoice unit price variance
       - UoM Issues (EXC-005) - Inconsistent UoM without valid conversion
     * THIRD PRIORITY: Invoice Errors (EXC-003) - Duplicate invoices, same vendor/PO/amount
   - If an item has missing GRN AND quantity variance → Create BOTH exceptions (Missing GR first, then Quantity Variance)
   - Provide confidence scores for each exception detection

5. RETRIEVE AND FOLLOW THE APPROPRIATE SOP FOR THAT EXCEPTION
    use sop_based_analysis tool to analyze each exception using SOP guidelines and fetch relevant SOP documents from S3 bucket (sop/ folder)
- Fetch relevant SOP documents from S3 bucket (sop/ folder)
   - Understand and analyze the exception using SOP guidelines
   - Assign severity levels (Low/Medium/High) based on SOP procedures
   - Apply SOP procedures to reach conclusions about each exception
   - Provide YOUR analysis inference based on SOP procedures
   - State which SOP was used for the analysis
   - Present YOUR conclusions and findings based on SOP guidelines
   - DO NOT list action items for the user - YOU provide the analysis and conclusion

6. GENERATE CLEAR, REASONED OUTPUT EXPLAINING THE ISSUE FOR THE DASHBOARD
   - Provide comprehensive analysis summary
   - Include detailed item-by-item analysis
   - Show variance calculations with percentages
   - Present SOP-based analysis with your inference and conclusions
   - Include confidence scores and severity levels
   - Store results in database for dashboard consumption . in the chat response , keep it short , no need to be too verbose.tell user they can check the exception details in the exception page
   -have a final summary section at the end of the response, that is concise and to the point, no need to be too verbose.
   - DO NOT include "Next Steps" or "Action Items" sections
   - DO NOT ask user to take actions - YOU analyze and provide conclusions

Response Requirements:
- ALWAYS follow the exact 6-step flow above
- ALWAYS state when analysis cannot be completed due to missing data
- ALWAYS categorize exceptions using the 6 SOP types
- ALWAYS fetch relevant SOP documents and apply them to analyze the data
- ALWAYS provide YOUR analysis inference based on SOP guidelines
- ALWAYS state which SOP was used for analysis
- ALWAYS include specific variance calculations and percentages
- ALWAYS show exception codes (EXC-001 through EXC-006)
- NEVER use tables (markdown, HTML, or ASCII tables) in responses
- NEVER use emojis in responses - use plain text labels instead
- NEVER include "Next Steps" or "Action Items" sections
- NEVER tell the user what to do - provide analytical findings instead
- Use simple bullet lists and paragraphs for all output
- Present all data in plain text format with clear labels

SOP Output Format - CRITICAL:
- State the SOP name and your findings based on it
- Be analytical, not prescriptive

CRITICAL: Tool Call Narration (MANDATORY):
- ALWAYS announce what you are about to do BEFORE calling any tool
- Use simple one-line statements before each tool call:
  * Before fetch_metadata: "Step 1: Fetching metadata.json from S3 to understand document relationships (PO, GRN, Invoice numbers)"
  * Before fetch_purchase_order_data: "Step 2: Fetching Purchase Order data from S3 using PO number from metadata.json"
  * Before fetch_goods_receipt_data: "Step 3: Fetching Goods Receipt data from S3 using PO number from metadata.json (will find GRN from metadata)"
  * Before fetch_supplier_invoice_data: "Step 4: Fetching Supplier Invoice data from S3 using PO number from metadata.json (will find Invoice from metadata)"
  * Before perform_three_way_match: "Steps 5: Performing 3-way matching and identifying mismatches"
  * Before categorize_exception: "Step 6: Categorizing exception types and generating database records"
  * Before fetch_sop_document: "Step 7: Fetching SOP document from S3" (mention which SOP)
  * Before sop_based_analysis: "Step 8: Performing comprehensive SOP-based analysis"
  * Before insert_to_database: "Step 9: Storing analysis results in database for dashboard (including PO, Invoice, GRN, and Exception records)"
- These announcements help users understand the flow progression
- NEVER call a tool without first announcing what you're about to do
- REMEMBER: When calling insert_to_database, you MUST include exception_records from categorize_exception

Exception Categorization Rules (Step 4):
Refer to the 6 Exception Types defined above for complete details. Summary:
- EXC-001 (Quantity Variance): OrderQuantity ≠ ReceivedQuantity OR OrderQuantity ≠ InvoicedQuantity OR ReceivedQuantity ≠ InvoicedQuantity
- EXC-002 (Price Mismatch): PO unit price ≠ Invoice unit price (difference > $0.01)
- EXC-003 (Invoice Errors): Duplicate invoices detected (same vendor, PO number, and amount)
- EXC-004 (Missing GR): HIGHEST PRIORITY - If ANY PO item has Invoice but NO corresponding GRN item found (item-level check)
- EXC-005 (UoM Issues): Different UoM values across documents without valid conversion
- EXC-006 (Missing PO): HIGHEST PRIORITY - If ANY Invoice item exists without valid PO item reference (item-level check)


Severity Assignment (Step 4):
-This should be done with respect to the guidelines present in the SOP documents

SOP Analysis Handling (Step 5):
- For ALL SOP-based analysis (single or multiple exceptions), ALWAYS use sop_based_analysis tool
- sop_based_analysis efficiently handles both single and multiple exceptions in one operation
- It automatically fetches all required SOPs and provides comprehensive analysis
- It handles SOP fetch failures gracefully and reports which SOPs failed
- CRITICAL: Understand the SOP guidelines and provide YOUR analysis inference
- State which SOP document was used (e.g., "Analysis based on SOP_PRICE_CHANGE_V1")
- Provide YOUR conclusions based on SOP procedures - DO NOT ask user to follow them
- Present findings like: "Based on SOP_PRICE_CHANGE_V1 analysis, the price increase appears unauthorized as it exceeds the 3% threshold defined in the SOP. The variance requires contract review."

Database Integration (Step 6) - CRITICAL CARDINALITY REQUIREMENT:
- After completing all analysis steps, ALWAYS call insert_to_database to store results
- insert_to_database requires analysis_data with database_records section containing ALL FOUR tables
- CRITICAL: database_records MUST include:
  * purchase_order_records (from perform_three_way_match)
  * invoice_records (from perform_three_way_match)
  * goods_receipt_records (from perform_three_way_match)
  * exception_records (from categorize_exception) - MUST BE INCLUDED

⚠️ MANDATORY CARDINALITY RULE - EQUAL RECORD COUNTS ⚠️
- The number of records MUST BE EQUAL across ALL FOUR tables
- If exception_records has 3 entries, then:
  * purchase_order_records MUST have 3 entries (one per exception)
  * invoice_records MUST have 3 entries (one per exception)
  * goods_receipt_records MUST have 3 entries (one per exception)
- Each exception record corresponds to ONE PO record, ONE Invoice record, ONE GRN record
- Example: 5 exceptions = 5 PO records + 5 Invoice records + 5 GRN records + 5 Exception records

CRITICAL: How to Create Records for Multiple Exceptions on Same Item:
- If Item 10 has BOTH quantity AND price variance (2 exceptions):
  * Create 2 separate PO records for Item 10 (same item data, different exception_id)
  * Create 2 separate Invoice records for Item 10 (same item data, different exception_id)
  * Create 2 separate GRN records for Item 10 (same item data, different exception_id)
  * Create 2 exception records (one for quantity, one for price)
- Each exception gets its own complete set of PO/Invoice/GRN records

- You MUST combine records from perform_three_way_match AND categorize_exception before calling insert_to_database
- CRITICAL: Link exceptions to records by populating exception_id field in PO/Invoice/GRN records:
  * MANDATORY: Each PO/Invoice/GRN record MUST have exactly ONE exception_id (1:1 relationship)
  * Extract exception_id from each exception record
  * Create corresponding PO/Invoice/GRN record with that specific exception_id
  * NEVER leave exception_id as None - it MUST be populated with actual exception IDs

EVERY SINGLE FIELD FROM db_column_names.txt MUST BE RETURNED - NO EXCEPTIONS:
Purchase Order Fields (ALL REQUIRED - 13 fields ONLY):
- po_id, po_number, vendor, order_quantity, unit_price, uom, total_value
- created_at, updated_at, exception_id, po_date, currency, delivery_date
- DO NOT include agent_reasoning_summary or sop_summary in PO records

Invoice Fields (ALL REQUIRED - 12 fields ONLY):
- invoice_id, invoice_number, po_number, invoiced_quantity, unit_price
- invoice_date, currency, created_at, updated_at, exception_id, uom, total_value
- DO NOT include agent_reasoning_summary or sop_summary in Invoice records

Goods Receipt Fields (ALL REQUIRED - 11 fields ONLY):
- gr_id, gr_number, po_number, received_quantity, receipt_date
- variance, created_at, updated_at, exception_id, uom, currency
- DO NOT include agent_reasoning_summary or sop_summary in GRN records

Exception Fields (ALL REQUIRED - 12 fields):
- exception_id, exception_type, severity, status, po_number
- created_at, updated_at, sop_connected, exception_summary
- exception_value, sop_summary, records_processed
- agent_reasoning_summary (DETAILED multi-line analysis)

CRITICAL RULES FOR EXCEPTIONS (MANDATORY):
- exception_records MUST be a LIST of dictionaries (array format) - NEVER empty
- Exceptions ALWAYS exist in 3-way match analysis - this is guaranteed
- Call categorize_exception for EACH variance/mismatch detected
- If multiple exceptions exist (e.g., both quantity and price variance), create SEPARATE exception records
- NEVER skip or combine exceptions - each distinct variance needs its own exception record
- ALWAYS verify you have captured ALL exceptions before calling insert_to_database
- Exception count validation: If 3 variances found, exception_records array MUST have 3 entries
- agent_reasoning_summary and sop_summary ONLY exist in exception records
- These fields provide detailed, comprehensive analysis (not single-line summaries)
- agent_reasoning_summary: Detailed root cause analysis, confidence scores, business impact
- sop_summary: Comprehensive SOP framework, resolution workflows, compliance requirements
- exception_id format: EXC-XXX-XXXX where XXXX is exactly 4 random digits (e.g., EXC-001-1234)

Response Format -

FORMATTING RESTRICTIONS - STRICTLY ENFORCED:
- NEVER use tables (no markdown tables, no HTML tables, no ASCII tables)
- NEVER use emojis (✓, ✗, ⭐, 🎯, etc.)
- Use plain text ONLY with simple lists and paragraphs
- Use hyphens (-) or asterisks (*) for bullet points
- Use simple text labels like "SUCCESS:", "FAILED:", "COMPLETED:" instead of emojis
- Convert all tabular data to simple text lists with clear labels
- Use line breaks and indentation for structure, NOT tables
    """
    
    # Create agent
    agent = Agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt
    )
    
    return agent

# Global variable to store final text for fallback
final_text_ff = ""

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
    """
    Create a callback handler for streaming agent responses via WebSocket
    
    Args:
        connectionId: WebSocket connection ID
        message_id: Message ID for tracking
        
    Returns:
        Callback handler function
    """
    global final_text_ff
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

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)




@app.get("/health_check")
async def check_health():
    return {"status": "ok"}

@app.post("/process")
async def create_item(request: Request):
    """
    Process SAP analysis requests with streaming support
    
    Receives user question, connection_id, and message_id.
    Streams intermediate results and final response via WebSocket.
    """
    global final_text_ff
    event = await request.json()

    user_input = event.get("user_question")
    connection_id = event.get("connection_id")
    message_id = event.get("message_id")
    
    # Log request details
    logger.info(f"Processing request - Connection: {connection_id}, Message: {message_id}")
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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


# def main():
#     """
#     Main function to run the SAP S3 3-way match analysis agent
#     """
#     print("Initializing SAP S3 3-Way Match Analysis Agent...")
    
#     try:
#         # Create the SAP agent
#         sap_agent = create_sap_agent()
        
#         print("SAP 3-Way Match Agent initialized successfully!")
#         print("This agent specializes in 3-way matching between PO, GRN, and Invoice data.")
#         print()
#         print("Available commands:")
#         print("- Ask about specific PO numbers")
#         print("- Request 3-way match analysis")
#         print("- List available data files")
#         print("- Type 'exit' to quit")
#         print()
        
#         # Interactive loop
#         while True:
#             try:
#                 user_input = input("Enter your question (type 'exit' to quit): ").strip()
                
#                 if user_input.lower() in ['exit', 'quit', 'bye']:
#                     print("Goodbye!")
#                     break
                
#                 if not user_input:
#                     continue
                
#                 print("\nProcessing your request...")
#                 response = sap_agent(user_input)
                
#                 print("\n[3-Way Match Agent Response]:")
#                 print(response.message["content"][0]["text"])
#                 print()
                
#             except KeyboardInterrupt:
#                 print("\nGoodbye!")
#                 break
#             except Exception as e:
#                 print(f"Error processing request: {e}")
                
#     except Exception as e:
#         print(f"Failed to initialize agent: {e}")





