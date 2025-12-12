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

# Exception type to code mapping based on the images
EXCEPTION_TYPE_CODES = {
    "Missing Purchase Order": "001",
    "Missing Goods Receipt": "002", 
    "Quantity Change": "003",
    "Price Change": "004",
    "Unit of Measure Issue": "005",
    "Duplicate Invoice": "006",
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
        response = s3_client.get_object(Bucket=S3_BUCKET, Key="data/metadata.json")
        content = response['Body'].read().decode('utf-8')
        metadata = json.loads(content)
        
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
        file_path: S3 object key (e.g., 'data/PurchaseOrder/PO_4500000001.txt')
        
    Returns:
        Dictionary with status and parsed data
    """
    try:
        # Fetch object from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_path)
        content = response['Body'].read().decode('utf-8')
        
        # Parse the XML content
        parser = SAPDataParser()
        records = parser.parse_odata_xml(content)
        
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
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=folder_path)
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
        
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
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        # Fetch PO data from the specific file
        po_file_path = f"data/PurchaseOrder/PO_{po_number}.txt"
        po_data = fetch_s3_data(po_file_path)
        if po_data["status"] != "success":
            return po_data
        
        # Parse the records
        records = po_data["content"][0]["data"]["all_records"]
        
        # Separate header and item records
        header_records = [r for r in records if r.get('PurchaseOrder') and not r.get('PurchaseOrderItem')]
        item_records = [r for r in records if r.get('PurchaseOrderItem')]
        
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
                    "metadata_status": po_record.get('status', 'unknown')
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error fetching PO data: {str(e)}"}]
        }


@tool
def fetch_goods_receipt_data(po_number: str) -> dict:
    """
    Fetch Goods Receipt data by PO number
    
    Args:
        po_number: Purchase Order number to search for
        
    Returns:
        Dictionary with GR header and item data
    """
    try:
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        # Check if GRN exists for this PO
        grn_number = po_record.get('grn')
        if not grn_number:
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
        
        # Fetch GR data from the specific file
        gr_file_path = f"data/GoodsReceipt/GRN_{grn_number}.txt"
        gr_data = fetch_s3_data(gr_file_path)
        if gr_data["status"] != "success":
            return gr_data
        
        # Parse the records
        records = gr_data["content"][0]["data"]["all_records"]
        
        # Separate header and item records
        header_records = [r for r in records if r.get('MaterialDocument') and not r.get('MaterialDocumentItem')]
        item_records = [r for r in records if r.get('MaterialDocumentItem')]
        
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
                    "item_count": len(item_records)
                }
            }]
        }
        
    except Exception as e:
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
        # First check metadata to understand relationships
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        
        # Find the PO in metadata
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        # Check if Invoice exists for this PO
        invoice_number = po_record.get('invoice')
        if not invoice_number:
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
        
        # Fetch Invoice data from the specific file
        invoice_file_path = f"data/SupplierInvoice/INV_{invoice_number}.txt"
        invoice_data = fetch_s3_data(invoice_file_path)
        if invoice_data["status"] != "success":
            return invoice_data
        
        # Parse the records
        records = invoice_data["content"][0]["data"]["all_records"]
        
        # Separate header and item records
        header_records = [r for r in records if r.get('BillingDocument') and not r.get('BillingDocumentItem')]
        item_records = [r for r in records if r.get('BillingDocumentItem')]
        
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
                    "item_count": len(item_records)
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error fetching invoice data: {str(e)}"}]
        }


@tool
def categorize_exception(exception_data: dict) -> dict:
    """
    Categorize exception into one of the 6 SOP types based on analysis data
    
    Args:
        exception_data: Dictionary containing exception analysis data
        
    Returns:
        Dictionary with categorized exception information
    """
    try:
        # Extract key data for categorization
        has_po = exception_data.get('has_po', False)
        has_grn = exception_data.get('has_grn', False)
        has_invoice = exception_data.get('has_invoice', False)
        price_variance = exception_data.get('price_variance', 0)
        qty_variance = exception_data.get('qty_variance', 0)
        uom_issue = exception_data.get('uom_issue', False)
        duplicate_invoice = exception_data.get('duplicate_invoice', False)
        
        # Enhanced categorization logic with data quality validation
        exception_type = None
        exception_code = None
        severity = "Low"
        confidence_score = 0.0
        
        # Check for data quality issues first
        data_quality_issues = exception_data.get('data_quality_issues', [])
        material_mismatch = any('Material mismatch' in issue for issue in data_quality_issues)
        
        # CORRECTED: Material mismatches are informational, not blocking exceptions
        # They should be investigated but don't prevent 3-way match analysis
            
        # 1. Missing Purchase Order (exp-001)
        if not has_po and has_invoice:
            exception_type = "Missing Purchase Order"
            exception_code = "exp-001"
            severity = "High"
            confidence_score = 0.95
            
        # 2. Missing Goods Receipt (exp-002)
        elif has_po and has_invoice and not has_grn:
            exception_type = "Missing Goods Receipt"
            exception_code = "exp-002"
            severity = "High"
            confidence_score = 0.90
            
        # 3. Quantity Change (exp-003) - Check quantity variance first
        elif abs(qty_variance) > 0.01:  # More than 0.01 quantity variance
            exception_type = "Quantity Change"
            exception_code = "exp-003"
            variance_percent = abs(qty_variance) * 100
            if variance_percent <= 5:
                severity = "Low"
                confidence_score = 0.85
            elif variance_percent <= 15:
                severity = "Medium"
                confidence_score = 0.90
            else:
                severity = "High"
                confidence_score = 0.95
                
        # 4. Price Change (exp-004)
        elif abs(price_variance) > 0.01:  # More than 1 cent variance
            exception_type = "Price Change"
            exception_code = "exp-004"
            variance_percent = abs(price_variance) * 100
            if variance_percent <= 5:
                severity = "Low"
                confidence_score = 0.85
            elif variance_percent <= 10:
                severity = "Medium"
                confidence_score = 0.90
            else:
                severity = "High"
                confidence_score = 0.95
                
        # 5. Unit of Measure Issue (exp-005)
        elif uom_issue:
            exception_type = "Unit of Measure Issue"
            exception_code = "exp-005"
            severity = "High"
            confidence_score = 0.88
            
        # 6. Duplicate Invoice (exp-006)
        elif duplicate_invoice:
            exception_type = "Duplicate Invoice"
            exception_code = "exp-006"
            severity = "High"
            confidence_score = 0.97
            
        # No exception found
        else:
            exception_type = "No Exception"
            exception_code = "none"
            severity = "None"
            confidence_score = 1.0
        
        return {
            "status": "success",
            "content": [{
                "text": f"Exception categorized as: {exception_type}",
                "data": {
                    "exception_type": exception_type,
                    "exception_code": exception_code,
                    "severity": severity,
                    "confidence_score": confidence_score,
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
def get_sop_recommendations(exception_type: str, severity: str, variance_data: dict = None) -> dict:
    """
    Get SOP-based recommendations for specific exception types
    
    Args:
        exception_type: Type of exception (e.g., 'Price Change', 'Quantity Change')
        severity: Severity level (Low, Medium, High)
        variance_data: Optional variance data for detailed recommendations
        
    Returns:
        Dictionary with SOP-based recommendations
    """
    try:
        # Map exception types to SOP IDs (only existing SOPs)
        sop_mapping = {
            "Price Change": "SOP_PRICE_CHANGE_V1",
            "Quantity Change": "SOP_QUANTITY_CHANGE_V1", 
            "Unit of Measure Issue": "SOP_UOM_ISSUE_V1",
            "Missing Goods Receipt": "SOP_MISSING_GRN_V1",
            "Missing Purchase Order": "SOP_MISSING_PO_V1",
            "Duplicate Invoice": "SOP_DUPLICATE_INVOICE_V1"
        }
        
        sop_id = sop_mapping.get(exception_type)
        if not sop_id:
            return {
                "status": "error",
                "content": [{"text": f"Unknown exception type: {exception_type}"}]
            }
        
        # Fetch SOP document
        sop_result = fetch_sop_document(sop_id)
        if sop_result["status"] != "success":
            return sop_result
        
        sop_content = sop_result["content"][0]["data"]["sop_content"]
        
        # Generate JSON output matching SOP format
        json_output = {}
        
        if exception_type == "Price Change":
            variance_percent = abs(variance_data.get('price_variance', 0)) * 100 if variance_data else 0
            po_number = variance_data.get('po_number', 'Unknown')
            invoice_number = variance_data.get('invoice_number', 'Unknown')
            
            json_output = {
                "exception_type": "Price Change",
                "variance_percent": round(variance_percent, 1),
                "severity": severity,
                "recommended_action": _get_price_change_action(severity),
                "agent_reasoning_summary": f"Invoice {invoice_number} is priced {variance_percent:.1f}% {'higher' if variance_data.get('price_variance', 0) > 0 else 'lower'} than PO {po_number}. Possible vendor rate update or entry error.",
                "status": "Pending",
                "confidence_score": 0.91
            }
                
        elif exception_type == "Quantity Change":
            qty_variance_percent = abs(variance_data.get('qty_variance', 0)) * 100 if variance_data else 0
            po_number = variance_data.get('po_number', 'Unknown')
            invoice_number = variance_data.get('invoice_number', 'Unknown')
            grn_number = variance_data.get('grn_number', 'Unknown')
            
            json_output = {
                "exception_type": "Quantity Change",
                "variance_percent": round(qty_variance_percent, 1),
                "severity": severity,
                "recommended_action": _get_quantity_change_action(severity),
                "agent_reasoning_summary": f"Invoice shows {variance_data.get('invoiced_qty', 0)} units billed, but {variance_data.get('received_qty', 0)} units were received under GRN {grn_number}. This {qty_variance_percent:.1f}% variance may indicate early billing or incomplete delivery.",
                "status": "Pending",
                "confidence_score": 0.9
            }
                
        elif exception_type == "Unit of Measure Issue":
            po_number = variance_data.get('po_number', 'Unknown')
            po_uom = variance_data.get('po_uom', 'Unknown')
            invoice_uom = variance_data.get('invoice_uom', 'Unknown')
            
            json_output = {
                "exception_type": "Unit of Measure Issue",
                "severity": severity,
                "recommended_action": "Verify UoM conversion and correct master data.",
                "agent_reasoning_summary": f"Invoice uses '{invoice_uom}' while PO uses '{po_uom}' with no valid conversion. Could affect valuation accuracy.",
                "status": "Pending",
                "confidence_score": 0.88
            }
            
        elif exception_type == "Missing Goods Receipt":
            po_number = variance_data.get('po_number', 'Unknown')
            invoice_number = variance_data.get('invoice_number', 'Unknown')
            
            json_output = {
                "exception_type": "Missing Goods Receipt",
                "severity": "High",
                "recommended_action": "Hold until GRN is posted.",
                "agent_reasoning_summary": f"Invoice {invoice_number} was billed before any goods receipt recorded for PO {po_number}. Delivery confirmation required.",
                "status": "Pending",
                "confidence_score": 0.95
            }
            
        elif exception_type == "Missing Purchase Order":
            invoice_number = variance_data.get('invoice_number', 'Unknown')
            
            json_output = {
                "exception_type": "Missing Purchase Order",
                "severity": "High",
                "recommended_action": "Procurement review for PO creation or vendor clarification.",
                "agent_reasoning_summary": f"Invoice {invoice_number} was posted without a PO reference. Potential unapproved purchase requires validation.",
                "status": "Pending",
                "confidence_score": 0.93
            }
            
        elif exception_type == "Duplicate Invoice":
            invoice_numbers = variance_data.get('duplicate_invoices', ['Unknown'])
            po_number = variance_data.get('po_number', 'Unknown')
            
            json_output = {
                "exception_type": "Duplicate Invoice",
                "severity": "High",
                "recommended_action": "Block duplicate until verification.",
                "agent_reasoning_summary": f"Invoices {', '.join(invoice_numbers)} have identical PO and amount. Possible duplicate entry requiring review.",
                "status": "Pending",
                "confidence_score": 0.97
            }
        
        # Generate reasoning summary based on SOP
        reasoning_summary = f"{exception_type} exception detected with {severity} severity. "
        if variance_data:
            if 'price_variance' in variance_data:
                reasoning_summary += f"Price variance: {variance_data['price_variance']:.4f}. "
            if 'qty_variance' in variance_data:
                reasoning_summary += f"Quantity variance: {variance_data['qty_variance']:.2f}. "
        
        reasoning_summary += "Please refer to SOP document for detailed procedures."
        
        return {
            "status": "success",
            "content": [{
                "text": f"SOP-based recommendations for {exception_type}",
                "data": {
                    "exception_type": exception_type,
                    "severity": severity,
                    "sop_id": sop_id,
                    "sop_content": sop_content,
                    "reasoning_summary": reasoning_summary,
                    "json_output": json_output,
                    "confidence_score": json_output.get("confidence_score", 0.9)
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error getting SOP recommendations: {str(e)}"}]
        }


@tool
def get_multiple_sop_recommendations(exceptions_list: List[dict]) -> dict:
    """
    Get SOP-based recommendations for multiple exception types
    
    Args:
        exceptions_list: List of exception dictionaries with type, severity, and variance_data
        
    Returns:
        Dictionary with consolidated SOP-based recommendations for all exceptions
    """
    try:
        if not exceptions_list:
            return {
                "status": "error",
                "content": [{"text": "No exceptions provided for SOP analysis"}]
            }
        
        # Map exception types to SOP IDs (only existing SOPs)
        sop_mapping = {
            "Price Change": "SOP_PRICE_CHANGE_V1",
            "Quantity Change": "SOP_QUANTITY_CHANGE_V1", 
            "Unit of Measure Issue": "SOP_UOM_ISSUE_V1",
            "Missing Goods Receipt": "SOP_MISSING_GRN_V1",
            "Missing Purchase Order": "SOP_MISSING_PO_V1",
            "Duplicate Invoice": "SOP_DUPLICATE_INVOICE_V1"
        }
        
        # Collect unique exception types and their SOPs
        unique_exception_types = list(set([exc.get('exception_type') for exc in exceptions_list]))
        sop_recommendations = []
        failed_sops = []
        successful_sops = []
        
        # Fetch SOPs for each unique exception type
        for exception_type in unique_exception_types:
            sop_id = sop_mapping.get(exception_type)
            if not sop_id:
                failed_sops.append(f"Unknown exception type: {exception_type}")
                continue
            
            # Fetch SOP document
            sop_result = fetch_sop_document(sop_id)
            if sop_result["status"] != "success":
                failed_sops.append(f"Failed to fetch SOP for {exception_type}: {sop_result['content'][0]['text']}")
                continue
            
            sop_content = sop_result["content"][0]["data"]["sop_content"]
            successful_sops.append(sop_id)
            
            # Get exceptions of this type
            type_exceptions = [exc for exc in exceptions_list if exc.get('exception_type') == exception_type]
            
            # Generate JSON outputs for each exception of this type
            json_outputs = []
            reasoning_summary = f"{exception_type} exceptions detected: {len(type_exceptions)} items. "
            
            # Process each exception of this type
            for exc in type_exceptions:
                severity = exc.get('severity', 'Medium')
                variance_data = exc.get('variance_data', {})
                
                # Generate JSON output matching SOP format
                json_output = {}
                
                if exception_type == "Price Change":
                    variance_percent = abs(variance_data.get('price_variance', 0)) * 100
                    po_number = variance_data.get('po_number', 'Unknown')
                    invoice_number = variance_data.get('invoice_number', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Price Change",
                        "variance_percent": round(variance_percent, 1),
                        "severity": severity,
                        "recommended_action": _get_price_change_action(severity),
                        "agent_reasoning_summary": f"Invoice {invoice_number} is priced {variance_percent:.1f}% {'higher' if variance_data.get('price_variance', 0) > 0 else 'lower'} than PO {po_number}. Possible vendor rate update or entry error.",
                        "status": "Pending",
                        "confidence_score": 0.91
                    }
                        
                elif exception_type == "Quantity Change":
                    qty_variance_percent = abs(variance_data.get('qty_variance', 0)) * 100
                    po_number = variance_data.get('po_number', 'Unknown')
                    invoice_number = variance_data.get('invoice_number', 'Unknown')
                    grn_number = variance_data.get('grn_number', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Quantity Change",
                        "variance_percent": round(qty_variance_percent, 1),
                        "severity": severity,
                        "recommended_action": _get_quantity_change_action(severity),
                        "agent_reasoning_summary": f"Invoice shows {variance_data.get('invoiced_qty', 0)} units billed, but {variance_data.get('received_qty', 0)} units were received under GRN {grn_number}. This {qty_variance_percent:.1f}% variance may indicate early billing or incomplete delivery.",
                        "status": "Pending",
                        "confidence_score": 0.9
                    }
                        
                elif exception_type == "Unit of Measure Issue":
                    po_number = variance_data.get('po_number', 'Unknown')
                    po_uom = variance_data.get('po_uom', 'Unknown')
                    invoice_uom = variance_data.get('invoice_uom', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Unit of Measure Issue",
                        "severity": severity,
                        "recommended_action": "Verify UoM conversion and correct master data.",
                        "agent_reasoning_summary": f"Invoice uses '{invoice_uom}' while PO uses '{po_uom}' with no valid conversion. Could affect valuation accuracy.",
                        "status": "Pending",
                        "confidence_score": 0.88
                    }
                    
                elif exception_type == "Missing Goods Receipt":
                    po_number = variance_data.get('po_number', 'Unknown')
                    invoice_number = variance_data.get('invoice_number', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Missing Goods Receipt",
                        "severity": "High",
                        "recommended_action": "Hold until GRN is posted.",
                        "agent_reasoning_summary": f"Invoice {invoice_number} was billed before any goods receipt recorded for PO {po_number}. Delivery confirmation required.",
                        "status": "Pending",
                        "confidence_score": 0.95
                    }
                    
                elif exception_type == "Missing Purchase Order":
                    invoice_number = variance_data.get('invoice_number', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Missing Purchase Order",
                        "severity": "High",
                        "recommended_action": "Procurement review for PO creation or vendor clarification.",
                        "agent_reasoning_summary": f"Invoice {invoice_number} was posted without a PO reference. Potential unapproved purchase requires validation.",
                        "status": "Pending",
                        "confidence_score": 0.93
                    }
                    
                elif exception_type == "Duplicate Invoice":
                    invoice_numbers = variance_data.get('duplicate_invoices', ['Unknown'])
                    po_number = variance_data.get('po_number', 'Unknown')
                    
                    json_output = {
                        "exception_type": "Duplicate Invoice",
                        "severity": "High",
                        "recommended_action": "Block duplicate until verification.",
                        "agent_reasoning_summary": f"Invoices {', '.join(invoice_numbers)} have identical PO and amount. Possible duplicate entry requiring review.",
                        "status": "Pending",
                        "confidence_score": 0.97
                    }
                
                json_outputs.append(json_output)
                
                # Add variance details to reasoning
                if variance_data:
                    if 'price_variance' in variance_data:
                        reasoning_summary += f"Price variance: {variance_data['price_variance']:.4f}. "
                    if 'qty_variance' in variance_data:
                        reasoning_summary += f"Quantity variance: {variance_data['qty_variance']:.2f}. "
            
            reasoning_summary += "Please refer to SOP document for detailed procedures."
            
            # Store SOP recommendation for this exception type
            sop_recommendations.append({
                "exception_type": exception_type,
                "sop_id": sop_id,
                "sop_content": sop_content,
                "json_outputs": json_outputs,
                "reasoning_summary": reasoning_summary,
                "exception_count": len(type_exceptions),
                "severities": list(set([exc.get('severity') for exc in type_exceptions]))
            })
        
        # Generate consolidated summary
        total_exceptions = len(exceptions_list)
        unique_types = len(unique_exception_types)
        successful_sop_count = len(successful_sops)
        
        consolidated_text = f"MULTIPLE EXCEPTION SOP ANALYSIS\n"
        consolidated_text += f"Total Exceptions: {total_exceptions}\n"
        consolidated_text += f"Unique Exception Types: {unique_types}\n"
        consolidated_text += f"SOPs Successfully Fetched: {successful_sop_count}\n"
        consolidated_text += f"Failed SOP Fetches: {len(failed_sops)}\n\n"
        
        if failed_sops:
            consolidated_text += "SOP FETCH FAILURES:\n"
            for failure in failed_sops:
                consolidated_text += f"- {failure}\n"
            consolidated_text += "\n"
        
        consolidated_text += "EXCEPTION TYPE ANALYSIS:\n"
        for sop_rec in sop_recommendations:
            consolidated_text += f"\n{sop_rec['exception_type']}:\n"
            consolidated_text += f"  Count: {sop_rec['exception_count']}\n"
            consolidated_text += f"  Severities: {', '.join(sop_rec['severities'])}\n"
            consolidated_text += f"  SOP: {sop_rec['sop_id']}\n"
            consolidated_text += f"  JSON Outputs: {len(sop_rec['json_outputs'])}\n"
            
            # Add JSON outputs for each exception
            for i, json_output in enumerate(sop_rec['json_outputs']):
                consolidated_text += f"\n  Exception {i+1} JSON:\n"
                consolidated_text += f"    {json.dumps(json_output, indent=4)}\n"
        
        return {
            "status": "success",
            "content": [{
                "text": consolidated_text,
                "data": {
                    "total_exceptions": total_exceptions,
                    "unique_exception_types": unique_exception_types,
                    "successful_sops": successful_sops,
                    "failed_sops": failed_sops,
                    "sop_recommendations": sop_recommendations,
                    "consolidated_analysis": True
                }
            }]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error getting multiple SOP recommendations: {str(e)}"}]
        }


@tool
def insert_to_database(analysis_data: dict) -> dict:
    """
    Insert 3-way match analysis results into PostgreSQL database
    
    Args:
        analysis_data: Dictionary containing analysis results with database_records
        
    Returns:
        Dictionary with insertion status and results

    """

    payload = {"event_type": "insert_data", "analysis_data": analysis_data}

    return insert_data_lambda(payload)

    # print("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQq\n\n\n\n\n\n\n\n", analysis_data, "\n\n\n\n\n\n\n\n\n")
    # try:
    #     # Validate database credentials
    #     logger.info("Validating database credentials...")
    #     if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE]):
    #         logger.error("Database credentials not configured")
    #         return {
    #             "status": "error",
    #             "content": [{"text": "Database credentials not configured. Please set db_host, db_user, db_password, and db_database environment variables."}]
    #         }
        
    #     logger.info(f"Database credentials validated - Host: {DB_HOST}, Database: {DB_DATABASE}, User: {DB_USER}")
        
    #     # Debug: Print the entire analysis_data structure
    #     logger.info("DEBUG: Full analysis_data structure:")
    #     logger.info(json.dumps(analysis_data, indent=2, default=str))
        
    #     # Extract database records from analysis data
    #     if 'database_records' not in analysis_data:
    #         logger.error("No database_records found in analysis_data")
    #         return {
    #             "status": "error",
    #             "content": [{"text": "No database_records found in analysis_data. Please ensure analysis includes database-compatible records."}]
    #         }
        
    #     db_records = analysis_data['database_records']
    #     current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
    #     # Handle different data structure formats
    #     # Check if we have the old format (purchase_orders, invoices, etc.) and convert to new format
    #     if 'purchase_orders' in db_records and 'purchase_order_records' not in db_records:
    #         logger.info("Converting old data format to new format")
            
    #         # Convert purchase_orders to purchase_order_records with correct field mapping
    #         purchase_order_records = []
    #         for po in db_records.get('purchase_orders', []):
    #             purchase_order_records.append({
    #                 'po_id': f"PO-{po.get('po_number', 'unknown')}-001",
    #                 'po_number': po.get('po_number', ''),
    #                 'vendor': po.get('vendor', 'Unknown Vendor'),
    #                 'order_quantity': 0,  # Not available in old format
    #                 'unit_price': 0,      # Not available in old format
    #                 'uom': 'EA',
    #                 'total_value': po.get('total_amount', 0),
    #                 'created_at': current_timestamp,
    #                 'updated_at': current_timestamp,
    #                 'agent_reasoning_summary': po.get('agent_reasoning_summary', ''),
    #                 'sop_summary': po.get('sop_summary', '')
    #             })
            
    #         # Convert invoices to invoice_records with correct field mapping
    #         invoice_records = []
    #         for inv in db_records.get('invoices', []):
    #             invoice_records.append({
    #                 'invoice_id': f"INV-{inv.get('po_number', 'unknown')}-001",
    #                 'invoice_number': inv.get('invoice_number', ''),
    #                 'po_number': inv.get('po_number', ''),
    #                 'invoiced_quantity': 0,  # Not available in old format
    #                 'unit_price': 0,         # Not available in old format
    #                 'invoice_date': datetime.now().strftime('%Y-%m-%d'),
    #                 'currency': inv.get('currency', 'USD'),
    #                 'created_at': current_timestamp,
    #                 'updated_at': current_timestamp,
    #                 'agent_reasoning_summary': inv.get('agent_reasoning_summary', ''),
    #                 'sop_summary': inv.get('sop_summary', '')
    #             })
            
    #         # Convert goods_receipts to goods_receipt_records with correct field mapping
    #         goods_receipt_records = []
    #         for gr in db_records.get('goods_receipts', []):
    #             goods_receipt_records.append({
    #                 'gr_id': f"GR-{gr.get('po_number', 'unknown')}-001",
    #                 'gr_number': gr.get('gr_number', ''),
    #                 'po_number': gr.get('po_number', ''),
    #                 'received_quantity': gr.get('total_quantity', 0),
    #                 'receipt_date': datetime.now().strftime('%Y-%m-%d'),
    #                 'variance': 0,  # Not available in old format
    #                 'created_at': current_timestamp,
    #                 'updated_at': current_timestamp,
    #                 'agent_reasoning_summary': gr.get('agent_reasoning_summary', ''),
    #                 'sop_summary': gr.get('sop_summary', '')
    #             })
            
    #         # Convert exceptions to exception_records with correct field mapping
    #         exception_records = []
    #         for exc in db_records.get('exceptions', []):
    #             exception_records.append({
    #                 'exception_id': exc.get('exception_id', f"EXC-{exc.get('exception_code', '000').replace('exp-', '')}-{random.randint(1000, 9999):04d}"),
    #                 'exception_type': exc.get('exception_type', 'Unknown'),
    #                 'severity': exc.get('severity', 'Medium'),
    #                 'status': exc.get('status', 'Pending'),
    #                 'po_number': exc.get('po_number', ''),
    #                 'created_at': current_timestamp,
    #                 'updated_at': current_timestamp,
    #                 'sop_connected': f"SOP_{exc.get('exception_type', 'UNKNOWN').upper().replace(' ', '_')}_V1",
    #                 'exception_summary': exc.get('exception_summary') or exc.get('agent_reasoning_summary', ''),
    #                 'exception_value': exc.get('exception_value', 0.0),
    #                 'records_processed': exc.get('records_processed', 1),
    #                 'sop_summary': exc.get('sop_summary', '')
    #             })
            
    #         db_records = {
    #             'purchase_order_records': purchase_order_records,
    #             'invoice_records': invoice_records,
    #             'goods_receipt_records': goods_receipt_records,
    #             'exception_records': exception_records
    #         }
        
    #     logger.info(f"Database records extracted - PO: {len(db_records.get('purchase_order_records', []))}, "
    #                f"Invoice: {len(db_records.get('invoice_records', []))}, "
    #                f"GR: {len(db_records.get('goods_receipt_records', []))}, "
    #                f"Exceptions: {len(db_records.get('exception_records', []))}")
        
    #     # Print database insert values in JSON format
    #     print("\n" + "="*80)
    #     print("DATABASE INSERT VALUES (JSON FORMAT)")
    #     print("="*80)
    #     print(json.dumps(db_records, indent=2, default=str))
    #     print("="*80 + "\n")
        
    #     # Initialize database connection
    #     logger.info(f"Attempting to connect to database: {DB_HOST}:{DB_PORT}/{DB_DATABASE}")
    #     conn = psycopg2.connect(
    #         host=DB_HOST,
    #         port=DB_PORT,
    #         user=DB_USER,
    #         password=DB_PASSWORD,
    #         dbname=DB_DATABASE
    #     )
    #     logger.info("Database connection established successfully")
    #     cursor = conn.cursor()
        
    #     insertion_results = {
    #         "purchase_order_insertions": 0,
    #         "invoice_insertions": 0,
    #         "goods_receipt_insertions": 0,
    #         "exception_insertions": 0,
    #         "errors": []
    #     }
        
    #     try:
    #         # Insert Purchase Order records
    #         if 'purchase_order_records' in db_records:
    #             logger.info(f"Starting insertion of {len(db_records['purchase_order_records'])} purchase order records")
    #             print(f"\n--- INSERTING {len(db_records['purchase_order_records'])} PURCHASE ORDER RECORDS ---")
    #             for i, po_record in enumerate(db_records['purchase_order_records']):
    #                 logger.info(f"Processing PO record {i+1}: {po_record.get('po_number')}")
    #                 print(f"\nPO Record {i+1}:")
    #                 print(json.dumps(po_record, indent=2, default=str))
    #                 try:
    #                     cursor.execute("""
    #                         INSERT INTO purchase_order 
    #                         (po_number, vendor, order_quantity, unit_price, 
    #                          uom, total_value, created_at, updated_at)
    #                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #                         ON CONFLICT (po_number) DO UPDATE SET
    #                             order_quantity = EXCLUDED.order_quantity,
    #                             unit_price = EXCLUDED.unit_price,
    #                             total_value = EXCLUDED.total_value,
    #                             updated_at = EXCLUDED.updated_at
    #                     """, (
    #                         po_record.get('po_number'),
    #                         po_record.get('vendor'),
    #                         po_record.get('order_quantity'),
    #                         po_record.get('unit_price'),
    #                         po_record.get('uom'),
    #                         po_record.get('total_value'),
    #                         po_record.get('created_at', current_timestamp),
    #                         po_record.get('updated_at', current_timestamp)
    #                     ))
    #                     logger.info(f"Successfully inserted/updated purchase_order: {po_record.get('po_number')}")
    #                     insertion_results["purchase_order_insertions"] += 1
    #                 except Exception as e:
    #                     logger.error(f"Failed to insert/update purchase_order {po_record.get('po_number')}: {str(e)}")
    #                     logger.error(f"SQL Query: INSERT INTO purchase_order ... VALUES {po_record}")
    #                     insertion_results["errors"].append(f"PO insertion error: {str(e)}")
            
    #         # Insert Invoice records
    #         if 'invoice_records' in db_records:
    #             logger.info(f"Starting insertion of {len(db_records['invoice_records'])} invoice records")
    #             print(f"\n--- INSERTING {len(db_records['invoice_records'])} INVOICE RECORDS ---")
    #             for i, invoice_record in enumerate(db_records['invoice_records']):
    #                 logger.info(f"Processing invoice record {i+1}: {invoice_record.get('invoice_number')}")
    #                 print(f"\nInvoice Record {i+1}:")
    #                 print(json.dumps(invoice_record, indent=2, default=str))
    #                 try:
    #                     cursor.execute("""
    #                         INSERT INTO invoice 
    #                         (invoice_number, po_number, invoiced_quantity, 
    #                          unit_price, invoice_date, currency, created_at, updated_at)
    #                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #                         ON CONFLICT (invoice_number) DO UPDATE SET
    #                             invoiced_quantity = EXCLUDED.invoiced_quantity,
    #                             unit_price = EXCLUDED.unit_price,
    #                             invoice_date = EXCLUDED.invoice_date,
    #                             updated_at = EXCLUDED.updated_at
    #                     """, (
    #                         invoice_record.get('invoice_number'),
    #                         invoice_record.get('po_number'),
    #                         invoice_record.get('invoiced_quantity'),
    #                         invoice_record.get('unit_price'),
    #                         invoice_record.get('invoice_date'),
    #                         invoice_record.get('currency'),
    #                         invoice_record.get('created_at', current_timestamp),
    #                         invoice_record.get('updated_at', current_timestamp)
    #                     ))
    #                     logger.info(f"Successfully inserted/updated invoice: {invoice_record.get('invoice_number')}")
    #                     insertion_results["invoice_insertions"] += 1
    #                 except Exception as e:
    #                     logger.error(f"Failed to insert/update invoice {invoice_record.get('invoice_number')}: {str(e)}")
    #                     logger.error(f"SQL Query: INSERT INTO invoice ... VALUES {invoice_record}")
    #                     insertion_results["errors"].append(f"Invoice insertion error: {str(e)}")
            
    #         # Insert Goods Receipt records
    #         if 'goods_receipt_records' in db_records:
    #             logger.info(f"Starting insertion of {len(db_records['goods_receipt_records'])} goods receipt records")
    #             print(f"\n--- INSERTING {len(db_records['goods_receipt_records'])} GOODS RECEIPT RECORDS ---")
    #             for i, gr_record in enumerate(db_records['goods_receipt_records']):
    #                 logger.info(f"Processing GR record {i+1}: {gr_record.get('gr_number')}")
    #                 print(f"\nGR Record {i+1}:")
    #                 print(json.dumps(gr_record, indent=2, default=str))
    #                 try:
    #                     cursor.execute("""
    #                         INSERT INTO goods_receipt 
    #                         (gr_number, po_number, received_quantity, 
    #                          receipt_date, variance, created_at, updated_at)
    #                         VALUES (%s, %s, %s, %s, %s, %s, %s)
    #                         ON CONFLICT (gr_number) DO UPDATE SET
    #                             received_quantity = EXCLUDED.received_quantity,
    #                             receipt_date = EXCLUDED.receipt_date,
    #                             variance = EXCLUDED.variance,
    #                             updated_at = EXCLUDED.updated_at
    #                     """, (
    #                         gr_record.get('gr_number'),
    #                         gr_record.get('po_number'),
    #                         gr_record.get('received_quantity'),
    #                         gr_record.get('receipt_date'),
    #                         gr_record.get('variance'),
    #                         gr_record.get('created_at', current_timestamp),
    #                         gr_record.get('updated_at', current_timestamp)
    #                     ))
    #                     logger.info(f"Successfully inserted/updated goods_receipt: {gr_record.get('gr_number')}")
    #                     insertion_results["goods_receipt_insertions"] += 1
    #                 except Exception as e:
    #                     logger.error(f"Failed to insert/update goods_receipt {gr_record.get('gr_number')}: {str(e)}")
    #                     logger.error(f"SQL Query: INSERT INTO goods_receipt ... VALUES {gr_record}")
    #                     insertion_results["errors"].append(f"GR insertion error: {str(e)}")
            
    #         # Insert Exception records
    #         if 'exception_records' in db_records:
    #             logger.info(f"Starting insertion of {len(db_records['exception_records'])} exception records")
    #             print(f"\n--- INSERTING {len(db_records['exception_records'])} EXCEPTION RECORDS ---")
    #             for i, exception_record in enumerate(db_records['exception_records']):
    #                 logger.info(f"Processing exception record {i+1}: {exception_record.get('exception_id')}")
    #                 print(f"\nException Record {i+1}:")
    #                 print(json.dumps(exception_record, indent=2, default=str))
    #                 try:
    #                     cursor.execute("""
    #                         INSERT INTO exceptions 
    #                         (exception_id, exception_type, severity, status, po_number, 
    #                          created_at, updated_at, sop_connected, 
    #                          exception_summary, exception_value, records_processed, sop_summary)
    #                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #                         ON CONFLICT (exception_id) DO UPDATE SET
    #                             severity = EXCLUDED.severity,
    #                             status = EXCLUDED.status,
    #                             updated_at = EXCLUDED.updated_at,
    #                             sop_connected = EXCLUDED.sop_connected,
    #                             exception_summary = EXCLUDED.exception_summary,
    #                             exception_value = EXCLUDED.exception_value,
    #                             records_processed = EXCLUDED.records_processed,
    #                             sop_summary = EXCLUDED.sop_summary
    #                     """, (
    #                         exception_record.get('exception_id'),
    #                         exception_record.get('exception_type'),
    #                         exception_record.get('severity'),
    #                         exception_record.get('status'),
    #                         exception_record.get('po_number'),
    #                         exception_record.get('created_at', current_timestamp),
    #                         exception_record.get('updated_at', current_timestamp),
    #                         exception_record.get('sop_connected'),
    #                         exception_record.get('exception_summary') or exception_record.get('agent_reasoning_summary'),
    #                         exception_record.get('exception_value'),
    #                         exception_record.get('records_processed'),
    #                         exception_record.get('sop_summary')
    #                     ))
    #                     logger.info(f"Successfully inserted/updated exception: {exception_record.get('exception_id')}")
    #                     insertion_results["exception_insertions"] += 1
    #                 except Exception as e:
    #                     logger.error(f"Failed to insert/update exception {exception_record.get('exception_id')}: {str(e)}")
    #                     logger.error(f"SQL Query: INSERT INTO exceptions ... VALUES {exception_record}")
    #                     insertion_results["errors"].append(f"Exception insertion error: {str(e)}")
            
    #         # Commit all transactions
    #         logger.info("Committing all database transactions...")
    #         conn.commit()
    #         logger.info("All database transactions committed successfully")
            
    #         # Generate summary text
    #         total_insertions = (insertion_results["purchase_order_insertions"] + 
    #                           insertion_results["invoice_insertions"] + 
    #                           insertion_results["goods_receipt_insertions"] + 
    #                           insertion_results["exception_insertions"])
            
    #         logger.info(f"Database insertion completed - Total records: {total_insertions}")
            
    #         summary_text = f"Database insertion completed successfully!\n\n"
    #         summary_text += f"Total Records Inserted: {total_insertions}\n"
    #         summary_text += f"- Purchase Orders: {insertion_results['purchase_order_insertions']}\n"
    #         summary_text += f"- Invoices: {insertion_results['invoice_insertions']}\n"
    #         summary_text += f"- Goods Receipts: {insertion_results['goods_receipt_insertions']}\n"
    #         summary_text += f"- Exceptions: {insertion_results['exception_insertions']}\n"
            
    #         if insertion_results["errors"]:
    #             logger.warning(f"Database insertion completed with {len(insertion_results['errors'])} errors")
    #             summary_text += f"\nErrors encountered: {len(insertion_results['errors'])}\n"
    #             for error in insertion_results["errors"]:
    #                 logger.error(f"Database insertion error: {error}")
    #                 summary_text += f"- {error}\n"
    #         else:
    #             logger.info("Database insertion completed without errors")
            
            # return {
            #     "status": "success",
            #     "content": [{
            #         "text": summary_text,
            #         "data": {
            #             "insertion_results": insertion_results,
            #             "total_insertions": total_insertions,
            #             "database_records_processed": db_records
            #         }
            #     }]
            # }
            
    #     except Exception as e:
    #         # Rollback on error
    #         logger.error(f"Database transaction error: {str(e)}")
    #         logger.error("Rolling back database transaction...")
    #         conn.rollback()
    #         logger.error("Database transaction rolled back")
    #         raise e
            
    #     finally:
    #         logger.info("Closing database connection...")
    #         cursor.close()
    #         conn.close()
    #         logger.info("Database connection closed")
            
    # except psycopg2.Error as e:
    #     logger.error(f"PostgreSQL database error: {str(e)}")
    #     return {
    #         "status": "error",
    #         "content": [{"text": f"Database error: {str(e)}"}]
    #     }
    # except Exception as e:
    #     logger.error(f"Unexpected error during database insertion: {str(e)}")
    #     return {
    #         "status": "error",
    #         "content": [{"text": f"Error inserting to database: {str(e)}"}]
        # }


def _generate_database_records(po_number: str, po_record: dict, analysis_results: list, exceptions_found: list, match_status: str) -> dict:
    """
    Generate database-compatible JSON records for all tables based on analysis results
    
    Args:
        po_number: Purchase Order number
        po_record: PO metadata record
        analysis_results: 3-way match analysis results
        exceptions_found: List of exceptions detected
        match_status: Overall match status
        
    Returns:
        Dictionary with database records for all tables
    """
    logger.info(f"Generating database records for PO {po_number}")
    logger.info(f"Analysis results count: {len(analysis_results)}")
    logger.info(f"Exceptions found count: {len(exceptions_found)}")
    
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    # Generate Purchase Order records
    purchase_order_records = []
    invoice_records = []
    goods_receipt_records = []
    exception_records = []
    
    # Process each analysis result to generate database records
    for i, item in enumerate(analysis_results):
        item_number = item.get('item_number', '')
        logger.info(f"Processing analysis result {i+1}: item {item_number}")
        
        # Purchase Order Record
        po_record_data = {
            "po_number": po_number,
            "vendor": "Unknown Vendor",  # Could be enhanced to extract from PO header
            "order_quantity": item.get('ordered_qty', 0),
            "unit_price": item.get('ordered_unit_price', 0),
            "uom": item.get('po_uom', 'EA'),
            "total_value": item.get('ordered_amount', 0),
            "created_at": current_timestamp,
            "updated_at": current_timestamp,
            "agent_reasoning_summary": f"PO {po_number} item {item_number} analysis completed with {match_status} status",
            "sop_summary": f"3-way match analysis for PO {po_number} item {item_number}"
        }
        purchase_order_records.append(po_record_data)
        logger.info(f"Generated PO record for PO {po_number} item {item_number}")
        
        # Invoice Record (if invoice exists)
        if item.get('has_invoice', False):
            invoice_record_data = {
                "invoice_number": item.get('invoice_number', f"INV-{po_number}-{item_number}"),
                "po_number": po_number,
                "invoiced_quantity": item.get('invoiced_qty', 0),
                "unit_price": item.get('invoiced_unit_price', 0),
                "invoice_date": datetime.now().strftime('%Y-%m-%d'),
                "currency": "INR",
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
                "agent_reasoning_summary": f"Invoice analysis for PO {po_number} item {item_number} - Unit price: {item.get('invoiced_unit_price', 0)}",
                "sop_summary": f"Invoice processing for PO {po_number} item {item_number}"
            }
            invoice_records.append(invoice_record_data)
            logger.info(f"Generated invoice record for PO {po_number} item {item_number}")
        
        # Goods Receipt Record (if GR exists)
        if item.get('has_gr', False):
            gr_record_data = {
                "gr_number": item.get('grn_number', f"GR-{po_number}-{item_number}"),
                "po_number": po_number,
                "received_quantity": item.get('received_qty', 0),
                "receipt_date": datetime.now().strftime('%Y-%m-%d'),
                "variance": item.get('qty_variance', 0),
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
                "agent_reasoning_summary": f"GRN analysis for PO {po_number} item {item_number} - Received: {item.get('received_qty', 0)}, Variance: {item.get('qty_variance', 0)}",
                "sop_summary": f"Goods receipt processing for PO {po_number} item {item_number}"
            }
            goods_receipt_records.append(gr_record_data)
            logger.info(f"Generated GR record for PO {po_number} item {item_number}")
    
    # Generate Exception Records
    for i, exception in enumerate(exceptions_found):
        logger.info(f"Processing exception {i+1}: {exception.get('exception_type')}")
        
        # Extract exception type code from exception code (e.g., 'exp-003' -> '003')
        exception_code = exception.get('exception_code', 'exp-000')
        if exception_code.startswith('exp-'):
            type_code = exception_code.replace('exp-', '')
        else:
            type_code = EXCEPTION_TYPE_CODES.get(exception.get('exception_type', 'Unknown'), '000')
        
        # Generate random 4-digit suffix for uniqueness
        random_suffix = random.randint(1000, 9999)
        
        exception_record_data = {
            "exception_id": f"EXC-{type_code}-{random_suffix:04d}",
            "exception_type": exception.get('exception_type', 'Unknown'),
            "severity": exception.get('severity', 'Medium'),
            "status": "Pending",
            "po_number": po_number,
            "created_at": current_timestamp,
            "updated_at": current_timestamp,
            "sop_connected": f"SOP_{exception.get('exception_type', 'UNKNOWN').upper().replace(' ', '_')}_V1",
            "exception_summary": exception.get('exception_summary') or exception.get('variance_data', {}).get('agent_reasoning_summary', f"Exception detected: {exception.get('exception_type', 'Unknown')} for PO {po_number}"),
            "exception_value": exception.get('exception_value') or exception.get('variance_data', {}).get('price_variance', 0.0),
            "records_processed": exception.get('records_processed', 1),
            "sop_summary": f"SOP-based resolution for {exception.get('exception_type', 'Unknown')} exception in PO {po_number}"
        }
        exception_records.append(exception_record_data)
        logger.info(f"Generated exception record: {exception_record_data['exception_id']}")
    
    result = {
        "purchase_order_records": purchase_order_records,
        "invoice_records": invoice_records,
        "goods_receipt_records": goods_receipt_records,
        "exception_records": exception_records
    }
    
    logger.info(f"Database records generation completed:")
    logger.info(f"  - Purchase Orders: {len(purchase_order_records)}")
    logger.info(f"  - Invoices: {len(invoice_records)}")
    logger.info(f"  - Goods Receipts: {len(goods_receipt_records)}")
    logger.info(f"  - Exceptions: {len(exception_records)}")
    
    return result


@tool
def perform_three_way_match(po_number: str) -> dict:
    """
    Perform 3-way match analysis between PO, GR, and Invoice
    
    Args:
        po_number: Purchase Order number to analyze
        
    Returns:
        Dictionary with 3-way match analysis results
    """
    try:
        logger.info(f"Starting 3-way match analysis for PO {po_number}")
        
        # First check metadata to understand relationships
        logger.info("Fetching metadata to understand document relationships...")
        metadata_result = fetch_metadata()
        if metadata_result["status"] != "success":
            logger.error(f"Failed to fetch metadata: {metadata_result}")
            return metadata_result
        
        metadata = metadata_result["content"][0]["data"]["metadata"]
        po_record = next((record for record in metadata if record.get('po') == po_number), None)
        if not po_record:
            logger.error(f"Purchase Order {po_number} not found in metadata")
            return {
                "status": "error",
                "content": [{"text": f"Purchase Order {po_number} not found in metadata"}]
            }
        
        logger.info(f"Found PO record in metadata: {po_record}")
        
        # Fetch all three data sources
        logger.info("Fetching Purchase Order data...")
        po_data = fetch_purchase_order_data(po_number)
        if po_data["status"] != "success":
            logger.error(f"Failed to fetch PO data: {po_data}")
            return po_data
        
        logger.info("Fetching Goods Receipt data...")
        gr_data = fetch_goods_receipt_data(po_number)
        
        logger.info("Fetching Supplier Invoice data...")
        invoice_data = fetch_supplier_invoice_data(po_number)
        
        # Extract data for analysis
        po_items = po_data["content"][0]["data"]["item_records"]
        logger.info(f"Extracted {len(po_items)} PO items for analysis")
        
        # Check if GR and Invoice data exist
        gr_items = []
        invoice_items = []
        
        if gr_data["status"] == "success" and gr_data["content"][0]["data"]["item_count"] > 0:
            gr_items = gr_data["content"][0]["data"]["item_records"]
            logger.info(f"Extracted {len(gr_items)} GR items")
        else:
            logger.warning("No GR data available or fetch failed")
        
        if invoice_data["status"] == "success" and invoice_data["content"][0]["data"]["item_count"] > 0:
            invoice_items = invoice_data["content"][0]["data"]["item_records"]
            logger.info(f"Extracted {len(invoice_items)} invoice items")
        else:
            logger.warning("No invoice data available or fetch failed")
        
        # Perform 3-way match analysis with enhanced exception detection
        logger.info("Starting 3-way match analysis...")
        analysis_results = []
        match_status = "COMPLETE"
        exceptions_found = []
        
        # Check for duplicate invoices
        invoice_amounts = {}
        for invoice_item in invoice_items:
            invoice_doc = invoice_item.get('BillingDocument', '')
            amount = float(invoice_item.get('NetValueAmount', 0))
            if invoice_doc in invoice_amounts:
                invoice_amounts[invoice_doc].append(amount)
            else:
                invoice_amounts[invoice_doc] = [amount]
        
        duplicate_invoices = []
        for doc, amounts in invoice_amounts.items():
            if len(amounts) > 1 and len(set(amounts)) == 1:  # Same amounts
                duplicate_invoices.append(doc)
        
        for po_item in po_items:
            item_number = po_item.get('PurchaseOrderItem', '')
            ordered_qty = float(po_item.get('OrderQuantity', 0))
            ordered_amount = float(po_item.get('NetPriceAmount', 0))
            ordered_unit_price = ordered_amount / ordered_qty if ordered_qty > 0 else 0
            
            # Find corresponding GR item with corrected validation logic
            gr_item = None
            po_material = po_item.get('Material', '')
            
            for item in gr_items:
                gr_item_number = item.get('PurchaseOrderItem', '')
                gr_material = item.get('Material', '')
                
                # Check: Item number matching with SAP formats (zero-padded)
                item_number_match = False
                try:
                    po_item_num = int(item_number)
                    gr_item_num = int(gr_item_number)
                    # Check if GR item number matches PO item (with zero-padding)
                    if (po_item_num == gr_item_num or 
                        gr_item_number == f"{po_item_num:05d}" or
                        gr_item_number == f"{po_item_num:04d}"):
                        item_number_match = True
                except (ValueError, TypeError):
                    # If conversion fails, try direct string comparison
                    if gr_item_number == item_number:
                        item_number_match = True
                
                # CORRECTED LOGIC: Use GR item if item number matches, regardless of material
                # Material differences may be due to master data issues, but GRN is valid if item number matches
                if item_number_match:
                    gr_item = item
                    # Log material differences for investigation but don't reject the GRN
                    if po_material and gr_material and po_material != gr_material:
                        print(f"INFO: Item {item_number} has material difference - PO: {po_material}, GR: {gr_material} (investigate master data)")
                    break
            
            # CORRECTED: Use single GR item if it matches any PO item number
            if not gr_item and len(gr_items) == 1:
                single_gr_item = gr_items[0]
                single_gr_item_number = single_gr_item.get('PurchaseOrderItem', '')
                try:
                    po_item_num = int(item_number)
                    gr_item_num = int(single_gr_item_number)
                    if (po_item_num == gr_item_num or 
                        single_gr_item_number == f"{po_item_num:05d}" or
                        single_gr_item_number == f"{po_item_num:04d}"):
                        gr_item = single_gr_item
                        print(f"INFO: Using single GR item {single_gr_item_number} for PO item {item_number}")
                except (ValueError, TypeError):
                    if single_gr_item_number == item_number:
                        gr_item = single_gr_item
            
            # CORRECTED: Use GRN quantity if item number matches (material differences are separate issue)
            if gr_item:
                received_qty = float(gr_item.get('QuantityInEntryUnit', 0))
            else:
                received_qty = 0  # No GRN item found
            
            # Find corresponding Invoice item with material validation
            invoice_item = None
            for item in invoice_items:
                if item.get('ReferencePurchaseOrderItem') == item_number:
                    # Additional validation: Check if materials match
                    invoice_material = item.get('Material', '')
                    if po_material == invoice_material or not invoice_material:
                        invoice_item = item
                        break
                    else:
                        print(f"WARNING: Invoice item {item_number} has material mismatch - PO: {po_material}, Invoice: {invoice_material}")
            
            # If no exact match found, try without material validation as fallback
            if not invoice_item:
                invoice_item = next((item for item in invoice_items 
                                   if item.get('ReferencePurchaseOrderItem') == item_number), None)
            invoiced_qty = float(invoice_item.get('BillingQuantity', 0)) if invoice_item else 0
            invoiced_amount = float(invoice_item.get('NetValueAmount', 0)) if invoice_item else 0
            invoiced_unit_price = invoiced_amount / invoiced_qty if invoiced_qty > 0 else 0
            
            # Calculate variances with validation
            qty_variance = received_qty - ordered_qty
            amount_variance = invoiced_amount - ordered_amount
            price_variance = invoiced_unit_price - ordered_unit_price
            
            # Validation: Check for data integrity issues (informational only)
            data_quality_issues = []
            if gr_item and not po_material:
                data_quality_issues.append("PO material missing")
            if invoice_item and not po_material:
                data_quality_issues.append("PO material missing for invoice comparison")
            if gr_item and po_material and gr_item.get('Material') != po_material:
                data_quality_issues.append(f"Material difference: PO({po_material}) vs GR({gr_item.get('Material')}) - investigate master data")
            if invoice_item and po_material and invoice_item.get('Material') != po_material:
                data_quality_issues.append(f"Material difference: PO({po_material}) vs Invoice({invoice_item.get('Material')}) - investigate master data")
            
            # Check for UoM issues with improved logic
            po_uom = po_item.get('OrderUnit', '')
            gr_uom = gr_item.get('EntryUnit', '') if gr_item else ''
            invoice_uom = invoice_item.get('BillingQuantityUnit', '') if invoice_item else ''
            
            # UoM issue exists if different units are used without proper conversion
            uom_issue = False
            if po_uom and gr_uom and po_uom != gr_uom:
                uom_issue = True
            if po_uom and invoice_uom and po_uom != invoice_uom:
                uom_issue = True
            
            # Determine match status
            item_status = "MATCH"
            if abs(qty_variance) > 0.01 or abs(amount_variance) > 0.01:
                item_status = "VARIANCE"
                match_status = "VARIANCE"
            
            # Check for missing documents
            if not gr_item:
                item_status = "MISSING_GR"
                match_status = "INCOMPLETE"
            if not invoice_item:
                item_status = "MISSING_INVOICE"
                match_status = "INCOMPLETE"
            
            # Create exception data for categorization with enhanced validation
            exception_data = {
                'has_po': True,
                'has_grn': gr_item is not None,
                'has_invoice': invoice_item is not None,
                'price_variance': price_variance,
                'qty_variance': qty_variance,
                'uom_issue': uom_issue,
                'duplicate_invoice': invoice_item.get('BillingDocument', '') in duplicate_invoices if invoice_item else False,
                'po_number': po_number,
                'item_number': item_number,
                'data_quality_issues': data_quality_issues,
                'po_material': po_material,
                'gr_material': gr_item.get('Material', '') if gr_item else '',
                'invoice_material': invoice_item.get('Material', '') if invoice_item else '',
                'ordered_qty': ordered_qty,
                'received_qty': received_qty,
                'invoiced_qty': invoiced_qty,
                'ordered_amount': ordered_amount,
                'invoiced_amount': invoiced_amount,
                'po_uom': po_uom,
                'gr_uom': gr_uom,
                'invoice_uom': invoice_uom,
                'invoice_number': invoice_item.get('BillingDocument', '') if invoice_item else '',
                'grn_number': po_record.get('grn', '') if po_record else '',
                'duplicate_invoices': duplicate_invoices
            }
            
            # Categorize exception
            categorization_result = categorize_exception(exception_data)
            if categorization_result["status"] == "success":
                exception_info = categorization_result["content"][0]["data"]
                if exception_info["exception_type"] != "No Exception":
                    exceptions_found.append({
                        "item_number": item_number,
                        "exception_type": exception_info["exception_type"],
                        "exception_code": exception_info["exception_code"],
                        "severity": exception_info["severity"],
                        "confidence_score": exception_info["confidence_score"],
                        "variance_data": exception_data
                    })
            
            analysis_results.append({
                "item_number": item_number,
                "ordered_qty": ordered_qty,
                "received_qty": received_qty,
                "invoiced_qty": invoiced_qty,
                "ordered_amount": ordered_amount,
                "invoiced_amount": invoiced_amount,
                "ordered_unit_price": ordered_unit_price,
                "invoiced_unit_price": invoiced_unit_price,
                "qty_variance": qty_variance,
                "amount_variance": amount_variance,
                "price_variance": price_variance,
                "status": item_status,
                "has_gr": gr_item is not None,
                "has_invoice": invoice_item is not None,
                "uom_issue": uom_issue,
                "po_uom": po_uom,
                "gr_uom": gr_uom,
                "invoice_uom": invoice_uom,
                "po_material": po_material,
                "gr_material": gr_item.get('Material', '') if gr_item else '',
                "invoice_material": invoice_item.get('Material', '') if invoice_item else '',
                "data_quality_issues": data_quality_issues
            })
        
        # Create detailed summary for agent response with SOP-based analysis
        summary_text = f"3-way match analysis completed for PO {po_number}\n\n"
        summary_text += f"Overall Status: {match_status}\n"
        summary_text += f"Metadata Status: {po_record.get('status', 'unknown')}\n"
        summary_text += f"Total Items Analyzed: {len(analysis_results)}\n"
        summary_text += f"Exceptions Found: {len(exceptions_found)}\n\n"
        
        # Add exception summary with data quality warnings
        if exceptions_found:
            summary_text += "EXCEPTIONS DETECTED:\n"
            for exception in exceptions_found:
                summary_text += f"\nItem {exception['item_number']}:\n"
                summary_text += f"  Exception Type: {exception['exception_type']}\n"
                summary_text += f"  Exception Code: {exception['exception_code']}\n"
                summary_text += f"  Severity: {exception['severity']}\n"
                summary_text += f"  Confidence: {exception['confidence_score']:.2f}\n"
                
                # Add data quality warnings if present
                if 'data_quality_issues' in exception and exception['data_quality_issues']:
                    summary_text += f"  Data Quality Issues: {', '.join(exception['data_quality_issues'])}\n"
            summary_text += "\n"
        
        # Add critical data quality summary
        data_quality_items = [item for item in analysis_results if item.get('data_quality_issues')]
        if data_quality_items:
            summary_text += "CRITICAL DATA QUALITY ISSUES:\n"
            for item in data_quality_items:
                summary_text += f"\nItem {item['item_number']} ({item['po_material']}):\n"
                for issue in item['data_quality_issues']:
                    summary_text += f"  - {issue}\n"
            summary_text += "\n"
        
        summary_text += "ITEM-BY-ITEM ANALYSIS:\n"
        for item in analysis_results:
            summary_text += f"\nItem {item['item_number']}:\n"
            summary_text += f"  Material: {item['po_material']}\n"
            summary_text += f"  Ordered Qty: {item['ordered_qty']}\n"
            summary_text += f"  Received Qty: {item['received_qty']}\n"
            summary_text += f"  Invoiced Qty: {item['invoiced_qty']}\n"
            summary_text += f"  Ordered Amount: {item['ordered_amount']}\n"
            summary_text += f"  Invoiced Amount: {item['invoiced_amount']}\n"
            summary_text += f"  Ordered Unit Price: {item['ordered_unit_price']:.4f}\n"
            summary_text += f"  Invoiced Unit Price: {item['invoiced_unit_price']:.4f}\n"
            summary_text += f"  Qty Variance: {item['qty_variance']}\n"
            summary_text += f"  Amount Variance: {item['amount_variance']}\n"
            summary_text += f"  Price Variance: {item['price_variance']:.4f}\n"
            summary_text += f"  Status: {item['status']}\n"
            summary_text += f"  Has GR: {item['has_gr']}\n"
            summary_text += f"  Has Invoice: {item['has_invoice']}\n"
            summary_text += f"  UoM Issue: {item['uom_issue']}\n"
            
            # Add material comparison
            if item['gr_material'] and item['gr_material'] != item['po_material']:
                summary_text += f"  WARNING: GR Material Mismatch - PO: {item['po_material']}, GR: {item['gr_material']}\n"
            if item['invoice_material'] and item['invoice_material'] != item['po_material']:
                summary_text += f"  WARNING: Invoice Material Mismatch - PO: {item['po_material']}, Invoice: {item['invoice_material']}\n"
            
            # Add data quality issues
            if item['data_quality_issues']:
                summary_text += f"  Data Quality Issues: {', '.join(item['data_quality_issues'])}\n"
            
            if item['uom_issue']:
                summary_text += f"  PO UoM: {item['po_uom']}\n"
                summary_text += f"  GR UoM: {item['gr_uom']}\n"
                summary_text += f"  Invoice UoM: {item['invoice_uom']}\n"
        
        # Generate database records
        logger.info("Generating database records for analysis results...")
        database_records = _generate_database_records(po_number, po_record, analysis_results, exceptions_found, match_status)
        
        result = {
            "status": "success",
            "content": [{
                "text": summary_text,
                "data": {
                    "po_number": po_number,
                    "overall_status": match_status,
                    "metadata_status": po_record.get('status', 'unknown'),
                    "item_analysis": analysis_results,
                    "exceptions_found": exceptions_found,
                    "total_items": len(analysis_results),
                    "matched_items": len([r for r in analysis_results if r["status"] == "MATCH"]),
                    "variance_items": len([r for r in analysis_results if r["status"] == "VARIANCE"]),
                    "missing_gr_items": len([r for r in analysis_results if r["status"] == "MISSING_GR"]),
                    "missing_invoice_items": len([r for r in analysis_results if r["status"] == "MISSING_INVOICE"]),
                    "exception_count": len(exceptions_found),
                    "exception_types": list(set([e["exception_type"] for e in exceptions_found])),
                    "high_severity_exceptions": len([e for e in exceptions_found if e["severity"] == "High"]),
                    "medium_severity_exceptions": len([e for e in exceptions_found if e["severity"] == "Medium"]),
                    "low_severity_exceptions": len([e for e in exceptions_found if e["severity"] == "Low"]),
                    "database_records": database_records
                }
            }]
        }
        
        logger.info(f"3-way match analysis completed for PO {po_number}")
        logger.info(f"Returning result with database_records containing:")
        logger.info(f"  - Purchase Orders: {len(database_records.get('purchase_order_records', []))}")
        logger.info(f"  - Invoices: {len(database_records.get('invoice_records', []))}")
        logger.info(f"  - Goods Receipts: {len(database_records.get('goods_receipt_records', []))}")
        logger.info(f"  - Exceptions: {len(database_records.get('exception_records', []))}")
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error performing 3-way match: {str(e)}"}]
        }












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
        get_sop_recommendations,
        get_multiple_sop_recommendations,
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

Your Core Responsibilities:

3-Way Match Analysis with Exception Categorization
- Perform comprehensive 3-way matching between PO, GRN, and Invoice data
- Automatically categorize exceptions into 6 SOP types:
  * Price Change (PO vs Invoice unit price variance) - exp-001
  * Quantity Change (Ordered vs Received vs Invoiced mismatch) - exp-002  
  * Unit of Measure Issue (inconsistent UoM without valid conversion) - exp-003
  * Missing Goods Receipt (Invoice posted without GRN) - exp-004
  * Missing Purchase Order (Invoice without valid PO reference) - exp-005
  * Duplicate Invoice (same vendor, PO, amount) - exp-006
- Assign severity levels (Low/Medium/High) based on variance percentages
- Provide confidence scores for each exception detection

SOP-Based Analysis and Recommendations
- Fetch relevant SOP documents from S3 bucket (sop/ folder)
- Apply SOP procedures to analyze exceptions
- Generate recommendations based on SOP guidelines
- Provide reasoning summaries following SOP format
- Include specific action items from SOP documents

Enhanced Data Analysis
- Calculate unit price variances (ordered vs invoiced)
- Detect UoM inconsistencies across documents
- Identify duplicate invoices with same vendor/PO/amount
- Analyze quantity mismatches between ordered/received/invoiced
- Check for missing documents and their business impact

Response Requirements:
- ALWAYS categorize exceptions using the 6 SOP types
- ALWAYS assign severity levels and confidence scores
- ALWAYS fetch relevant SOP documents for recommendations
- ALWAYS provide SOP-based reasoning summaries
- ALWAYS include specific variance calculations and percentages
- ALWAYS show exception codes (exp-001 through exp-006)
- ALWAYS report data availability status before analysis
- ALWAYS state when analysis cannot be completed due to missing data

Critical Analysis Steps:
1. Check metadata for PO relationships and document availability
2. Fetch PO, GRN, and Invoice data - VERIFY ALL FETCHES SUCCEEDED
3. If any critical data fetch fails, STOP and report the failure
4. Perform 3-way match analysis with exception detection ONLY on successfully fetched data
5. Categorize each exception into appropriate SOP type
6. Fetch relevant SOP documents from S3 - VERIFY FETCH SUCCEEDED
7. Generate SOP-based recommendations ONLY if SOP was successfully fetched
8. Provide detailed reasoning summaries
9. Include confidence scores and severity levels
10. ALWAYS report any data limitations or fetch failures

Tool Usage Guidelines:
- Use fetch_metadata to understand document relationships - CHECK FOR SUCCESS
- Use fetch_purchase_order_data, fetch_goods_receipt_data, fetch_supplier_invoice_data for data - VERIFY SUCCESS
- Use categorize_exception to classify exceptions into SOP types
- Use fetch_sop_document to get SOP procedures - VERIFY SUCCESS
- Use get_sop_recommendations for single exception SOP-based guidance
- Use get_multiple_sop_recommendations for multiple exceptions requiring multiple SOPs - PREFERRED FOR MULTIPLE EXCEPTIONS
- Use perform_three_way_match for comprehensive analysis with exception detection
- Use insert_to_database to store analysis results in PostgreSQL database - CALL AFTER ANALYSIS COMPLETION
- ALWAYS check tool return status before proceeding with analysis

Database Integration:
- After completing 3-way match analysis, ALWAYS call insert_to_database to store results
- insert_to_database requires analysis_data with database_records section
- Database records are automatically generated by perform_three_way_match
- Database insertion includes: Purchase Orders, Invoices, Goods Receipts, and Exceptions
- All records include agent_reasoning_summary and sop_summary for audit trail
- Database credentials must be configured via environment variables (db_host, db_user, db_password, db_database)
- Database operations use UPSERT (INSERT ON CONFLICT) to handle duplicate records gracefully

Multiple Exception Handling:
- When multiple exceptions are detected, ALWAYS use get_multiple_sop_recommendations instead of multiple get_sop_recommendations calls
- get_multiple_sop_recommendations efficiently fetches all required SOPs in one operation
- It provides consolidated analysis across all exception types
- It handles SOP fetch failures gracefully and reports which SOPs failed
- It deduplicates recommendations and provides comprehensive analysis

Exception Categorization Rules:
- Missing Purchase Order: Invoice without PO reference (exp-001) - HIGHEST PRIORITY
- Missing Goods Receipt: Invoice exists but no GRN (exp-002) - HIGH PRIORITY
- Quantity Change: Quantity variance > 0.01 (exp-003) - MEDIUM PRIORITY
- Price Change: Unit price variance > 1 cent (exp-004) - MEDIUM PRIORITY
- UoM Issue: Different units without valid conversion (exp-005) - HIGH PRIORITY
- Duplicate Invoice: Same vendor/PO/amount (exp-006) - HIGH PRIORITY
- Material differences are informational only - investigate master data but don't block processing

Critical Validation Rules:
- ALWAYS check item number matching between PO, GRN, and Invoice (primary matching criteria)
- ALWAYS validate data integrity before calculating variances
- ALWAYS report material differences for investigation but don't block processing
- ALWAYS use GRN quantities when item numbers match (material differences are separate issue)
- ALWAYS show material information in analysis for investigation
- ALWAYS verify all data sources were successfully fetched before analysis
- NEVER proceed with calculations if source data is missing or invalid
- ALWAYS recognize partial deliveries as valid GRN (not rejections)

Severity Assignment:
- Low: ≤5% variance or informational only
- Medium: 5-10% variance or requires review
- High: >10% variance or critical issues

Data Availability Reporting:
- ALWAYS start responses with data availability status
- Report each data source fetch status (SUCCESS/FAILED/NOT_FOUND)
- If any critical data is missing, state analysis limitations
- Never provide analysis on incomplete data without explicit warnings

Response Format:
- Start with DATA AVAILABILITY STATUS
- Report any fetch failures or missing data
- If data is incomplete, state analysis limitations
- Only proceed with analysis if all required data is available
- Start with overall exception summary (if data is complete)
- List each exception with type, code, severity, and confidence
- Provide SOP-based recommendations for each exception
- Include JSON output format for each exception as specified in SOP documents
- Include detailed item-by-item analysis
- Show variance calculations with percentages
- ALWAYS call insert_to_database after analysis completion to store results
- Report database insertion status and results
- End with actionable next steps based on SOP procedures
- ALWAYS include data quality and availability disclaimers
- ALWAYS provide JSON output matching SOP format for dashboard integration
    """
    
    # Create agent
    agent = Agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt
    )
    
    return agent


def main():
    """
    Main function to run the SAP S3 3-way match analysis agent
    """
    print("Initializing SAP S3 3-Way Match Analysis Agent...")
    
    try:
        # Create the SAP agent
        sap_agent = create_sap_agent()
        
        print("SAP 3-Way Match Agent initialized successfully!")
        print("This agent specializes in 3-way matching between PO, GRN, and Invoice data.")
        print()
        print("Available commands:")
        print("- Ask about specific PO numbers")
        print("- Request 3-way match analysis")
        print("- List available data files")
        print("- Type 'exit' to quit")
        print()
        
        # Interactive loop
        while True:
            try:
                user_input = input("Enter your question (type 'exit' to quit): ").strip()
                
                if user_input.lower() in ['exit', 'quit', 'bye']:
                    print("Goodbye!")
                    break
                
                if not user_input:
                    continue
                
                print("\nProcessing your request...")
                response = sap_agent(user_input)
                
                print("\n[3-Way Match Agent Response]:")
                print(response.message["content"][0]["text"])
                print()
                
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error processing request: {e}")
                
    except Exception as e:
        print(f"Failed to initialize agent: {e}")




if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("SAP S3 3-Way Match Analysis Agent")
            print("Usage:")
            print("  python strands_sap_s3.py           # Run the 3-way match agent")
            print("  python strands_sap_s3.py --help    # Show this help")
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Use --help for usage information")
    else:
        main()
