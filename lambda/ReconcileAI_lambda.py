import os
import psycopg2
import json
import random
import logging
import secrets
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import secrets
import string
import boto3
from botocore.exceptions import ClientError
from datetime import date
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

def format_datetime_to_ist(dt):
    """
    Convert datetime to IST timezone and format as string.
    
    Args:
        dt: datetime object (timezone-aware or naive)
        - If timezone-aware: normalizes to UTC first, then converts to IST
        - If naive: assumes UTC (PostgreSQL stores timestamps in UTC) and converts to IST
        
    Returns:
        Formatted string in IST timezone (YYYY-MM-DD HH:MM:SS) or None if dt is None
    """
    if dt is None:
        return None
    
    # Normalize to UTC first to avoid any timezone confusion
    if dt.tzinfo is not None:
        # Convert to UTC first, then to IST
        dt_utc = dt.astimezone(timezone.utc)
    else:
        # If naive, assume it's UTC (PostgreSQL stores timestamps in UTC)
        dt_utc = dt.replace(tzinfo=timezone.utc)
    
    # Now convert from UTC to IST
    dt_ist = dt_utc.astimezone(IST)
    
    # Format as string
    return dt_ist.strftime('%Y-%m-%d %H:%M:%S')


import json
from pprint import pprint
from datetime import datetime
import re
import psycopg2
from psycopg2.extras import Json, execute_values

normalize_date = globals().get("normalize_date", lambda d: d)
normalize_exception_id = globals().get("normalize_exception_id", lambda eid: eid)
get_exception_name_from_id = globals().get("get_exception_name_from_id", lambda eid: None)

def _first_value_from_dict(d):
    if not d:
        return None
    return d[next(iter(d))]


def normalize_exception_id(exception_id):
    """
    Normalize exception ID format from EXC-0043087 to EXC-004-3087
    Handles:
    - EXC-0043087 -> EXC-004-3087
    - EXC-004-3087 -> EXC-004-3087 (already correct)
    - EXC-12345678 -> EXC-123-45678
    """
    if not exception_id or exception_id is None:
        return None
    
    exception_id = str(exception_id).strip()
    
    # Check if already in correct format (has two dashes)
    if exception_id.count('-') == 2:
        return exception_id
    
    # Pattern: EXC-XXXXXXX (one dash followed by 7+ digits)
    match = re.match(r'^(EXC)-(\d+)$', exception_id, re.IGNORECASE)
    
    if match:
        prefix = match.group(1).upper()
        digits = match.group(2)
        
        # Split the digits: first 3 digits, then the rest
        if len(digits) >= 4:
            part1 = digits[:3]
            part2 = digits[3:]
            return f"{prefix}-{part1}-{part2}"
        else:
            # If less than 4 digits, return as is
            return exception_id
    
    # If pattern doesn't match, return as is
    return exception_id


def normalize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD
    Handles:
    - YYYYMMDD (20241215) -> 2024-12-15
    - YYYYMMD (2024125) -> 2024-12-05
    - YYYYMDD (202495) -> 2024-09-05
    - YYYYMD (20249) -> 2024-00-09 (invalid, will try to parse)
    """
    if not date_str or date_str is None:
        return None
    
    date_str = str(date_str).strip()
    
    # If already in correct format with dashes or has time component, return as is
    if '-' in date_str or 'T' in date_str:
        return date_str.split('T')[0] if 'T' in date_str else date_str
    
    # If not all digits, return as is
    if not date_str.isdigit():
        return date_str
    
    length = len(date_str)
    
    try:
        # Standard format: YYYYMMDD (8 digits)
        if length == 8:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            # Validate the date
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        
        # Format: YYYYMDD (7 digits) - single digit month
        elif length == 7:
            year = date_str[:4]
            month = date_str[4:5].zfill(2)  # Pad month with zero
            day = date_str[5:7]
            # Validate the date
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        
        # Format: YYYYMMD (7 digits) - single digit day
        # Need to distinguish from YYYYMDD
        elif length == 7:
            # Try both interpretations
            # First try: YYYYMMD (month is 2 digits, day is 1 digit)
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:7].zfill(2)  # Pad day with zero
            try:
                datetime(int(year), int(month), int(day))
                return f"{year}-{month}-{day}"
            except ValueError:
                # If that fails, try YYYYMDD (month is 1 digit, day is 2 digits)
                year = date_str[:4]
                month = date_str[4:5].zfill(2)
                day = date_str[5:7]
                datetime(int(year), int(month), int(day))
                return f"{year}-{month}-{day}"
        
        # Format: YYYYMD (6 digits) - single digit month and day
        elif length == 6:
            year = date_str[:4]
            month = date_str[4:5].zfill(2)  # Pad month with zero
            day = date_str[5:6].zfill(2)    # Pad day with zero
            # Validate the date
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        
        else:
            # Unsupported length, return as is
            print(f"Warning: Unexpected date format length {length}: {date_str}")
            return date_str
            
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not parse date '{date_str}': {e}")
        return date_str




def prepare_exception_table_data(analysis_data, session_id, created_by="system", userid=None):
    """
    Return list of dicts to insert into exception_table (one dict per exception).
    
    Args:
        analysis_data: Dictionary containing database_records with exception_records
        session_id: Session identifier for grouping exceptions
        created_by: User/system identifier who created the record (default: "system")
        userid: User ID from the request body to associate with all exceptions
    """
    exception_records_dict = analysis_data.get("database_records", {}).get("exception_records", {}) or {}
    exception_data_list = []
    now = datetime.now(timezone.utc)

    for exc_key, exception in exception_records_dict.items():
        unique_exception_id = exception.get("exception_id") or exception.get("exceptionId") or exc_key
        exception_name = exception.get("exception_name") or exception.get("exception_type")
        if not exception_name and unique_exception_id:
            exception_name = get_exception_name_from_id(unique_exception_id)

        exc_val = exception.get("exception_value")
        # Keep structured values as JSON where possible
        exception_value = Json(exc_val) if exc_val is not None else None

        po_number = exception.get("po_number")

        item_no = exception.get("item_number") or exception.get("item_no") or exception.get("PurchaseOrderItem")
        exception_summary = exception.get("exception_summary")  

        row = {
            "session_id": session_id,
            "unique_exception_id": unique_exception_id,
            "exception_status": exception.get("status"),
            "exception_value": exception_value,
            "exception_summary": exception_summary,     
            "created_at": now,
            "created_by": created_by,
            "po_number" : po_number,
            "updated_at": now,
            "updated_by": created_by,
            "exception_name": exception_name,
            "item_no": item_no,
            "userid": userid  # Use userid from request body, not from exception record
        }
        exception_data_list.append(row)

    return exception_data_list



def prepare_session_table_rows(analysis_data, session_id, bundle_id, created_by="system", userid=None):
    """
    Return a list of session rows (one per exception). Each session row contains full JSONB
    payloads (purchase_order, goods_receipt, invoice) and exception-specific summary fields.
    If there are no exceptions, return a single session row with unique_exception_id=None.
    
    Args:
        analysis_data: Dictionary containing database_records with all record types
        session_id: Session identifier for grouping records
        bundle_id: Bundle identifier
        created_by: User/system identifier who created the record (default: "system")
        userid: User ID from the request body to associate with all session records
    """
    now = datetime.now(timezone.utc)
    db_records = analysis_data.get("database_records", {}) or {}
    purchase_order_records = db_records.get("purchase_order_records") or {}
    invoice_records = db_records.get("invoice_records") or {}
    goods_receipt_records = db_records.get("goods_receipt_records") or {}
    exception_records = db_records.get("exception_records") or {}

    # normalize a few inner fields (best-effort)
    def _normalize_record_dates(record):
        if not isinstance(record, dict):
            return record
        for k in list(record.keys()):
            if k.lower().endswith("date") or k in ("CreatedOn", "PostingDate", "receipt_date", "created_at", "updated_at"):
                try:
                    record[k] = normalize_date(record[k])
                except Exception:
                    record[k] = record[k]
            if k in ("exception_id", "exceptionId"):
                record[k] = normalize_exception_id(record.get(k))
        return record

    for k, v in list(purchase_order_records.items()):
        purchase_order_records[k] = _normalize_record_dates(v)
    for k, v in list(invoice_records.items()):
        invoice_records[k] = _normalize_record_dates(v)
    for k, v in list(goods_receipt_records.items()):
        goods_receipt_records[k] = _normalize_record_dates(v)
    for k, v in list(exception_records.items()):
        if isinstance(v, dict) and "exception_id" in v:
            v["exception_id"] = normalize_exception_id(v["exception_id"])
            exception_records[k] = v

    # get po_number from first PO record (value or dict key)
    # For Missing PO exception, we need to get the PO number from exception records
    # since the PO doesn't exist in purchase_order_records
    po_number = None
    first_po = _first_value_from_dict(purchase_order_records)
    if first_po and isinstance(first_po, dict):
        po_number = first_po.get("PurchaseOrder") or first_po.get("po_number") or first_po.get("poNumber")
    else:
        if purchase_order_records:
            po_number = next(iter(purchase_order_records), None)
    
    # If still no po_number, check exception records for Missing PO exception
    # This handles the case where user provided a wrong PO number
    if not po_number and exception_records:
        for exc_key, exc in exception_records.items():
            if isinstance(exc, dict):
                exc_name = exc.get('exception_name') or exc.get('exception_type')
                if exc_name == 'Missing PO':
                    po_number = exc.get('po_number')
                    if po_number:
                        break

    # If exceptions exist -> create one session row per exception
    session_rows = []

    if exception_records:
        for exc_key, exc in exception_records.items():
            if not isinstance(exc, dict):
                # skip malformed entries
                continue
            unique_exception_id = exc.get("exception_id") or exc.get("exceptionId") or exc_key
            exception_status = exc.get("status")
            severity = exc.get("severity")

            # Build session row
            row = {
                "session_id": session_id,
                "unique_exception_id": unique_exception_id,
                "bundle_id": bundle_id,
                "overall_status": None,  # we'll compute overall_status per session-row or keep None; we'll compute below as session-level summary
                "purchase_order": purchase_order_records,
                "goods_receipt": goods_receipt_records,
                "invoice": invoice_records,
                "exception_status": exception_status,
                "severity": severity,
                "created_at": now,
                "created_by": created_by,
                "updated_at": now,
                "updated_by": created_by,
                "po_number": po_number,
                "userid": userid  # Include userid from request body
            }
            session_rows.append(row)

        # Compute overall_status across all exceptions (session-level) and set into each session row
        statuses = [exc.get("status") for exc in exception_records.values() if isinstance(exc, dict)]
        overall_status = "Pending"
        if statuses:
            if all(s == "Resolved" for s in statuses if s is not None):
                overall_status = "Resolved"
            elif any(s == "In Progress" for s in statuses):
                overall_status = "In Progress"
            else:
                overall_status = statuses[0] or "Pending"
        for r in session_rows:
            r["overall_status"] = overall_status

    else:
        # No exceptions -> create single session row with unique_exception_id None
        row = {
            "session_id": session_id,
            "unique_exception_id": None,
            "bundle_id": bundle_id,
            "overall_status": "Pending",
            "purchase_order": purchase_order_records,
            "goods_receipt": goods_receipt_records,
            "invoice": invoice_records,
            "exception_status": None,
            "severity": None,
            "created_at": now,
            "created_by": created_by,
            "updated_at": now,
            "updated_by": created_by,
            "po_number": po_number,
            "userid": userid  # Include userid from request body
        }
        session_rows.append(row)

    return session_rows

# Material code to description mapping for Missing PO creation
MATERIAL_DESCRIPTIONS = {
    'MZ-RM-R300-01': 'Steel Fastener Assembly',
    'MZ-RM-R310-07': 'Motor Housing Casting',
    'MZ-RM-R322-12': 'High Torque Spindle Assembly',
    'MZ-EL-CTRL-88': 'Motor Control PCB Module',
    'MZ-ASM-GEAR-44': 'Precision Gearbox Assembly'
}




def get_trigger_email_for_exception_type(exception_type: str) -> str:
    """
    Map exception type to trigger email based on business rules.
    Returns the appropriate email address for each exception type.
    """
    # Normalize exception_type to handle case variations
    exc_type = (exception_type or '').strip()
    
    # Exception type to email mapping based on the SOP Control Center configuration
    email_mapping = {
        'Quantity Variance': 'analysis@company.com',
        'Price Mismatch': 'procurement@company.com',
        'Invoice Errors': 'accounts.payable@company.com',
        'Missing GR': 'logistics@company.com',
        'UoM Issues': 'quality.control@company.com',
        'Missing PO': 'purchasing@company.com'
    }
    
    return email_mapping.get(exc_type, None)

def normalize_exception_id(exception_id):
    """
    Normalize exception_id to ensure it doesn't have duplicate EXC- prefix.
    Examples:
        "EXC-001-0001" -> "EXC-001-0001" (no change)
        "EXC-EXC-001-0001" -> "EXC-001-0001" (removes duplicate)
        "001-0001" -> "EXC-001-0001" (adds prefix if missing)
    """
    if not exception_id:
        return exception_id
    
    # Remove all "EXC-" prefixes
    normalized = exception_id
    while normalized.startswith('EXC-'):
        normalized = normalized[4:]  # Remove "EXC-" (4 characters)
    
    # Add back single "EXC-" prefix
    return f"EXC-{normalized}"

def get_exception_name_from_id(exception_id: str) -> str:
    """
    Map exception ID prefix to exception name.
    Extracts the type code (e.g., EXC-001) from the full ID and returns the exception name.
    
    Examples:
        "EXC-001-1234" -> "Quantity Variance"
        "EXC-002-5678" -> "Price Mismatch"
        "EXC-003-9012" -> "Invoice Errors"
    
    Returns None if no mapping found.
    """
    if not exception_id:
        return None
    
    # Exception ID to name mapping
    exception_mapping = {
        'EXC-001': 'Quantity Variance',
        'EXC-002': 'Price Mismatch',
        'EXC-003': 'Invoice Errors',
        'EXC-004': 'Missing GR',
        'EXC-005': 'UoM Issues',
        'EXC-006': 'Missing PO'
    }
    
    # Extract prefix (first 7 characters: EXC-XXX)
    exception_id = str(exception_id).strip()
    if len(exception_id) >= 7:
        prefix = exception_id[:7].upper()
        return exception_mapping.get(prefix, None)
    
    return None

def randomize_exception_id_suffix(exception_id: str) -> str:
    """
    Return the given exception id with a new random 4-digit suffix, preserving the prefix.

    The first part (e.g., "EXC-001") will NOT be modified; only the unique ID part changes.
    Examples:
      - "EXC-001-1234" -> "EXC-001-<rand>"
      - "EXC-002" -> "EXC-002-<rand>"
      - "001-9999" -> "EXC-001-<rand>" (normalized to add missing EXC- prefix)

    Uses the `secrets` module for unbiased randomness and zero-pads to 4 digits.
    """
    if not exception_id:
        raise ValueError("exception_id is required")

    s = str(exception_id).strip()
    parts = s.split('-')

    # Determine prefix as EXC-<three_digit_code>
    prefix = None
    if len(parts) >= 2 and parts[0] == 'EXC' and parts[1].isdigit() and len(parts[1]) == 3:
        prefix = f"{parts[0]}-{parts[1]}"
    else:
        # Normalize then re-parse to extract prefix
        normalized = normalize_exception_id(s)
        nparts = normalized.split('-')
        if len(nparts) >= 2 and nparts[0] == 'EXC' and nparts[1].isdigit() and len(nparts[1]) == 3:
            prefix = f"{nparts[0]}-{nparts[1]}"
        else:
            raise ValueError("Invalid exception_id format; expected 'EXC-xxx-####' or 'EXC-xxx'")

    # Generate a fully random 4-digit suffix [0000..9999], zero-padded
    suffix = f"{secrets.randbelow(10_000):04d}"
    return f"{prefix}-{suffix}"

def fetch_json_from_s3(bucket_name, file_key):
    """
    Fetch JSON file from S3 bucket and return parsed JSON data.
    
    Args:
        bucket_name: S3 bucket name
        file_key: S3 object key (path to file)
        
    Returns:
        Parsed JSON data as dictionary, or None if file not found or error occurs
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.warning(f"File not found in S3: {file_key}")
            return None
        else:
            logger.error(f"S3 error fetching {file_key}: {str(e)}")
            return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON from {file_key}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching {file_key}: {str(e)}")
        return None

def lambda_handler(event, context):
    # CORS headers to include in all responses
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST'
    }
    print("RAW EVENT:", event)
    
    host = os.environ['db_host']
    port = int(os.environ['db_port'])
    user = os.environ['db_user']
    password = os.environ['db_password']
    database = os.environ['db_database']
    COGNITO_CLIENT_ID=os.environ['COGNITO_CLIENT_ID']
    COGNITO_USER_POOL_ID=os.environ['COGNITO_USER_POOL_ID']
    
    # Store as module-level constants for insert_data event
    DB_HOST = host
    DB_PORT = port
    DB_USER = user
    DB_PASSWORD = password
    DB_DATABASE = database

    # Extract event_type from the request body
    try:
        # Ensure `body` is always defined as a dict for downstream handlers
        body = {}
        # Handle API Gateway format (body is a JSON string or dict)
        if isinstance(event, dict) and 'body' in event and event['body']:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        elif isinstance(event, dict):
            # Event might already be the body with event_type directly present
            body = event

        # Safely get event_type from the parsed body
        event_type = body.get('event_type') if isinstance(body, dict) else None
    except Exception as e:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({
                "error": f"Invalid request body: {str(e)}"
            })
        }

    if not event_type:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({
                "error": "event_type is required"
            })
        }

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
        cursor = conn.cursor()

        # Check if PO has been queried and fixed by this user
        if event_type == "check_po_queried":
            print("=" * 80)
            print("LAMBDA: Starting check_po_queried event")
            print(f"Timestamp: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            
            po_number = body.get("po_number")
            userid = body.get("userid")
            
            print(f"LAMBDA: Checking PO={po_number}, UserID={userid}")
            
            if not po_number or not userid:
                cursor.close()
                conn.close()
                print("LAMBDA: ✗ Missing po_number or userid")
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "status": "error",
                        "message": "po_number and userid are required"
                    })
                }
            
            try:
                # Query: Check if this PO+userid exists in session_table
                # AND if all records have exception_status = 'Fixed'
                query = """
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN exception_status = 'Fixed' THEN 1 ELSE 0 END) as fixed_records
                    FROM erp.session_table
                    WHERE po_number = %s AND userid = %s
                """
                
                print(f"LAMBDA: Executing query: {query}")
                print(f"LAMBDA: Parameters: po_number={po_number}, userid={userid}")
                
                cursor.execute(query, (po_number, userid))
                result = cursor.fetchone()
                
                print(f"LAMBDA: Query result: {result}")
                
                total_records = result[0] if result else 0
                fixed_records = result[1] if result else 0
                
                print(f"LAMBDA: Total records: {total_records}, Fixed records: {fixed_records}")
                
                cursor.close()
                conn.close()
                print("LAMBDA: Database connection closed")
                
                # Determine status
                if total_records == 0:
                    status = "not_queried"
                    message = f"PO {po_number} has not been queried before"
                    print(f"LAMBDA: ✓ Status: {status} - {message}")
                elif fixed_records >= 1:
                    status = "queried"
                    message = f"PO {po_number} has already been processed and all exceptions are fixed"
                    print(f"LAMBDA: ✓ Status: {status} - {message}")
                else:
                    status = "not_queried"
                    message = f"PO {po_number} has been queried but not all exceptions are fixed"
                    print(f"LAMBDA: ✓ Status: {status} - {message}")
                
                # Prepare payload to send to EC2
                response_data = {
                    "status": status,
                    "message": message,
                    "total_records": int(total_records),
                    "fixed_records": int(fixed_records),
                    "po_number": po_number,
                    "userid": userid,
                    "timestamp": datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                    "event_type": "check_po_queried"
                }
                
                print(f"LAMBDA: Prepared response payload:")
                print(json.dumps(response_data, indent=2))
                
                # Return response immediately (don't wait for EC2 POST)
                # EC2 will get the data from this API response
                print("LAMBDA: Returning response to caller immediately")
                print("=" * 80)
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "lambda_status": "success",
                        "data": response_data
                    })
                }
                
                
            except Exception as e:
                print(f"LAMBDA: ✗ Database error: {str(e)}")
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "status": "error",
                        "message": f"Database error: {str(e)}"
                    })
                }

        elif event_type == "login_api":
            print(event_type)
            email = body.get("email")
            password = body.get("password")
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
                print(COGNITO_USER_POOL_ID,"COGNITO_USER_POOL_ID")
                print(COGNITO_CLIENT_ID,"COGNITO_CLIENT_ID")
                # Authenticate user with Cognito
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
                refresh_token = tokens["RefreshToken"]

                # Get user attributes (e.g., name, email)
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

                # Query userid from database using email_id field
                # This is critical for user identification in all subsequent API calls
                userid = None
                try:
                    cursor.execute("SELECT userid FROM erp.users WHERE email_id = %s", (emailid,))
                    result = cursor.fetchone()
                    if result:
                        userid = result[0]
                        logger.info(f"Successfully fetched userid {userid} for email {emailid}")
                    else:
                        logger.warning(f"No userid found in database for email {emailid}")
                except Exception as db_error:
                    logger.error(f"Error fetching userid from database: {str(db_error)}")
                    # Continue without userid if query fails, but log the error

                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "message": "Login successful",
                        "username": username,
                        "email": emailid,
                        "userid": userid,
                        "tokens": {
                            "access_token": access_token,
                            "id_token": id_token,
                            "refresh_token": refresh_token
                        },
                        "user_attributes": user_attributes
                    })
                }

            except ClientError as e:
                # Handle AWS Cognito specific errors
                error_code = e.response.get('Error', {}).get('Code', '')
                error_message = e.response.get('Error', {}).get('Message', str(e))
                
                logger.error(f"Cognito error in login_api: {error_code} - {error_message}")
                
                # Handle specific Cognito error codes
                if error_code == 'NotAuthorizedException':
                    return {
                        "statusCode": 401,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "Invalid email or password."})
                    }
                elif error_code == 'UserNotConfirmedException':
                    return {
                        "statusCode": 403,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "User account is not confirmed. Please confirm your account."})
                    }
                elif error_code == 'UserNotFoundException':
                    return {
                        "statusCode": 404,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "User not found."})
                    }
                elif error_code == 'TooManyRequestsException':
                    return {
                        "statusCode": 429,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "Too many requests. Please try again later."})
                    }
                else:
                    # Generic Cognito error
                    return {
                        "statusCode": 400,
                        "headers": cors_headers,
                        "body": json.dumps({"error": f"Authentication failed: {error_message}"})
                    }

            except Exception as e:
                # Handle any other unexpected errors
                logger.error(f"Unexpected error in login_api: {str(e)}")
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "Internal server error. Please try again later."})
                }

        elif event_type == "export_csv":
            # Return all bundles (no pagination) for CSV export
            # Export the same columns shown in bundle_exceptions table
            try:
                query = """
                    SELECT 
                        b.po_number,
                        b.status,
                        b.created_at,
                        b.updated_at,
                        COALESCE(COUNT(e.exception_id), 0) as exception_count
                    FROM bundle b
                    LEFT JOIN exceptions e ON b.po_number = e.po_number
                    GROUP BY b.po_number, b.status, b.created_at, b.updated_at
                    ORDER BY b.updated_at DESC;
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                data = []
                for r in rows:
                    data.append({
                        "poNumber": r[0],
                        "status": r[1] if r[1] else "Pending",
                        "createdAt": r[2].strftime('%d %b %Y, %I:%M %p') if r[2] else None,
                        "updatedAt": r[3].strftime('%d %b %Y, %I:%M %p') if r[3] else None,
                        "exceptionCount": int(r[4]) if r[4] else 0
                    })

                cursor.close()
                conn.close()

                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "export_exceptions": data
                    })
                }
            except Exception as e:
                logger.error(f"Failed to export bundles: {e}")
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({"error": str(e)})
                }



        elif event_type == "agent_summary":
            # Return aggregated exception data grouped by exception type identifier with pagination.
            # Extract the exception type prefix from exception_id (e.g., "EXC-001" from "EXC-001-0001")
            # and group by it to get: exception_type_code, exception_name, sop_connected,
            # last_run (MAX updated_at), and total_exceptions (COUNT).
            
            # Extract pagination parameters from request body
            page = body.get('page', 1)
            page_size = body.get('page_size', 10)
            offset = (page - 1) * page_size

            # First, get total count of distinct exception type groups
            count_query = """
            SELECT COUNT(*) FROM (
                SELECT SUBSTRING(exception_id FROM 1 FOR 7) AS exception_type_code
                FROM exceptions
                GROUP BY SUBSTRING(exception_id FROM 1 FOR 7)
            ) subquery;
            """
            cursor.execute(count_query)
            total_result = cursor.fetchone()
            total_count = total_result[0] if total_result else 0

            # Get paginated agent summary
            query = """
            SELECT 
                SUBSTRING(exception_id FROM 1 FOR 7) AS exception_type_code,
                MIN(exception_type) AS exception_name,
                MIN(sop_connected) AS sop_connected,
                MAX(updated_at) AS last_run,
                COUNT(*) AS total_exceptions,
                MIN(trigger_email) AS trigger_email
            FROM exceptions
            GROUP BY SUBSTRING(exception_id FROM 1 FOR 7)
            ORDER BY exception_type_code
            LIMIT %s OFFSET %s;
            """

            cursor.execute(query, [page_size, offset])
            rows = cursor.fetchall()
            data = []
            for r in rows:
                # r[0]=exception_type_code, r[1]=exception_name, r[2]=sop_connected,
                # r[3]=last_run, r[4]=total_exceptions, r[5]=trigger_email
                data.append({
                    "exceptionId": r[0],
                    "exceptionName": r[1],
                    "sopId": r[2],
                    "lastRun": r[3].strftime('%d %b %Y, %I:%M %p') if r[3] else None,
                    "records": int(r[4]),
                    "triggerEmail": r[5]
                })

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "agent_summary": data,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": (total_count + page_size - 1) // page_size  # Ceiling division
                    }
                })
            }

        elif event_type == "update_trigger_email":
            # Update trigger_email for all exceptions with a given exception type prefix
            exception_type_prefix = body.get('exception_type_prefix')
            new_email = body.get('trigger_email')
            
            if not exception_type_prefix:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "exception_type_prefix is required"
                    })
                }
            
            if new_email is None:  # Allow empty string to clear email
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "trigger_email is required"
                    })
                }
            
            try:
                # Get current IST time for updated_at
                updated_at = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                # Update all exceptions that match the prefix pattern
                # e.g., if prefix is "EXC-001", update all "EXC-001-XXXX"
                update_query = """
                    UPDATE exceptions 
                    SET trigger_email = %s, updated_at = %s 
                    WHERE exception_id LIKE %s
                """
                like_pattern = f"{exception_type_prefix}%"
                cursor.execute(update_query, (new_email, updated_at, like_pattern))
                rows_affected = cursor.rowcount
                conn.commit()
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "status": "success",
                        "message": f"Updated trigger_email for {rows_affected} exceptions with prefix {exception_type_prefix}",
                        "rows_affected": rows_affected,
                        "exception_type_prefix": exception_type_prefix,
                        "new_email": new_email
                    })
                }
            except Exception as e:
                conn.rollback()
                cursor.close()
                conn.close()
                logger.error(f"Failed to update trigger_email: {str(e)}")
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Database error: {str(e)}"
                    })
                }


        elif event_type == "sop_details":
            # Get exception type prefix (e.g., "EXC-001") from request body
            exception_type_code = body.get('exception_type_code')
            
            if not exception_type_code:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "exception_type_code is required"
                    })
                }
            
            # Query to fetch SOP summary for the given exception type prefix
            # Use SUBSTRING to match the exception type code (first 7 characters of exception_id)
            # Get the first matching row's sop_summary
            query = """
                SELECT 
                    sop_summary,
                    sop_connected,
                    exception_type
                FROM exceptions
                WHERE SUBSTRING(exception_id FROM 1 FOR 7) = %s
                LIMIT 1;
            """
            
            cursor.execute(query, (exception_type_code,))
            row = cursor.fetchone()
            
            if not row:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 404,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "SOP details not found for this exception type"
                    })
                }
            
            result = {
                "sopSummary": row[0],
                "sopConnected": row[1],
                "exceptionType": row[2]
            }
            
            cursor.close()
            conn.close()
            
            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps(result)
            }

        

        elif event_type == "new_po_exception_details":
            # Get all exceptions for a given bundle and session
            bundle_id = body.get('bundle_id')
            session_id = body.get('session_id')
            userid = body.get("userid", 0)

            if not bundle_id or not session_id:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "bundle_id and session_id are required"})
                }

            # ---------------- ANCHOR LOGIC (SAME AS session_exceptions) ----------------
            earliest_user_created_at = None
            if userid != 0:
                try:
                    cursor.execute(
                        """
                        SELECT MIN(created_at)
                        FROM erp.exception_table
                        WHERE userid = %s
                          AND created_at >= CURRENT_DATE - INTERVAL '6 days'
                        """,
                        (userid,)
                    )
                    res = cursor.fetchone()
                    if res and res[0]:
                        earliest_user_created_at = res[0]
                except Exception as e:
                    logger.error(f"Error fetching anchor for details API: {e}")
                    earliest_user_created_at = None

            base_offsets = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            anchor_dt = earliest_user_created_at if earliest_user_created_at else datetime.now(IST)

            # --------------------------------------------------------------------------

            query = """
                SELECT
                    s.session_id,
                    s.unique_exception_id,
                    s.bundle_id,
                    s.overall_status,
                    s.purchase_order,
                    s.goods_receipt,
                    s.invoice,
                    s.exception_status,
                    s.severity,
                    s.created_at,
                    s.updated_at,
                    s.po_number,
                    e.session_id,
                    e.unique_exception_id,
                    e.exception_status,
                    e.exception_value,
                    e.exception_summary,
                    e.created_at,
                    e.created_by,
                    e.updated_at,
                    e.updated_by,
                    e.exception_name,
                    e.item_no,
                    e.userid,
                    e.po_number
                FROM erp.session_table s
                LEFT JOIN erp.exception_table e
                    ON s.session_id = e.session_id
                    AND s.unique_exception_id = e.unique_exception_id
                WHERE s.bundle_id = %s
                AND s.session_id = %s
                ORDER BY s.created_at DESC, e.created_at DESC
            """

            cursor.execute(query, (bundle_id, session_id))
            rows = cursor.fetchall()
            print("actual fetched data from db", rows)

            if not rows:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                return {
                    "statusCode": 404,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "No exceptions found for this bundle and session"})
                }

            first_row = rows[0]
            bundle_status = first_row[3] or "Pending"
            po_number = first_row[11] or None 

                        # Collect exception names for sop lookup (use e_exception_name or mapping)
            exception_names = set()
            for r in rows:
                exc_name = r[21]  # e_exception_name
                if exc_name:
                    exception_names.add(exc_name)
                else:
                    # fallback: try mapping using e_unique_exception_id or s_unique_exception_id
                    uniq_id = r[13] or r[1]  # e_unique_exception_id or s_unique_exception_id
                    mapped = get_exception_name_from_id(uniq_id)
                    if mapped:
                        exception_names.add(mapped)
            exception_names = list(exception_names)

            # Fetch trigger emails from erp.sop for these exception_names
            exception_email_map = {}
            if exception_names:
                print("exception_names",exception_names)
                placeholders = ",".join(["%s"] * len(exception_names))
                email_query = f"""
                    SELECT DISTINCT exception_name, mail
                    FROM erp.sop
                    WHERE exception_name IN ({placeholders})
                """
                cursor.execute(email_query, exception_names)
                email_rows = cursor.fetchall()
                print("email_rows",email_rows)
                for exc_name, mail_data in email_rows:
                    if not mail_data:
                        continue
                    if isinstance(mail_data, list):
                        exception_email_map[exc_name] = mail_data
                    elif isinstance(mail_data, str):
                        try:
                            parsed = json.loads(mail_data)
                            if isinstance(parsed, list):
                                exception_email_map[exc_name] = parsed
                            else:
                                exception_email_map[exc_name] = [mail_data]
                        except Exception:
                            exception_email_map[exc_name] = [mail_data]

            # ---------------- BUILD DUMMY DATE MAP (for userid=0) ----------------
            from datetime import timedelta

            dummy_date_map = {}
            global_dummy_index = 0

            for r in rows:
                e_userid = r[23]
                if e_userid == 0:
                    if global_dummy_index < len(base_offsets):
                        offset_days = base_offsets[global_dummy_index]
                    else:
                        offset_days = base_offsets[-1] + (global_dummy_index - len(base_offsets) + 1)

                    base_created = anchor_dt - timedelta(days=offset_days)
                    row_status = r[14]  # e_exception_status

                    if row_status == 'Pending':
                        base_updated = base_created
                    else:
                        base_updated = base_created + timedelta(days=1)

                    dummy_date_map[r[13]] = (base_created, base_updated)  # key = unique_exception_id
                    global_dummy_index += 1

            # ----------------------------------------------------------------------

            exceptions_list = []

            for r in rows:
                (
                    s_session_id, s_unique_exception_id, s_bundle_id, s_overall_status,
                    s_purchase_order, s_goods_receipt, s_invoice, s_exception_status,
                    s_severity, s_created_at, s_updated_at, s_po_number,
                    e_session_id, e_unique_exception_id, e_exception_status,
                    e_exception_value, e_exception_summary, e_created_at,
                    e_created_by, e_updated_at, e_updated_by, e_exception_name,
                    e_item_no, e_userid, e_po_number
                ) = r

                # If exception name missing, you can still map with your helper if needed
                # exception_name = e_exception_name or get_exception_name_from_id(e_unique_exception_id)

                # ---------------- APPLY SAME DISPLAY LOGIC ----------------
                if e_userid == 0 and e_unique_exception_id in dummy_date_map:
                    disp_created_at, disp_updated_at = dummy_date_map[e_unique_exception_id]
                else:
                    disp_created_at = e_created_at
                    disp_updated_at = e_updated_at

                # ----------------------------------------------------------
                # If exception name missing, try mapping
                exception_name = e_exception_name or get_exception_name_from_id(e_unique_exception_id or r[1])
                print(exception_name)

                # Trigger emails for this exception type
                trigger_email_list = exception_email_map.get(exception_name, [])
                print(trigger_email_list)

                exception_obj = {
                    "exceptionId": e_unique_exception_id,
                    "exceptionName": e_exception_name,
                    "exceptionStatus": e_exception_status,
                    "exceptionValue": e_exception_value,
                    "exceptionSummary": e_exception_summary,

                    # 🔴 FIX: use the same IST formatter as session_exceptions
                    "createdAt": format_datetime_to_ist(disp_created_at) if disp_created_at else None,
                    "createdBy": e_created_by,
                    "severity": s_severity,
                    "updatedAt": format_datetime_to_ist(disp_updated_at) if disp_updated_at else None,
                    "updatedBy": e_updated_by,
                    "itemNo": e_item_no,
                    "userid": e_userid,
                    "poNumber": e_po_number,
                    "triggerEmail": trigger_email_list,
                }

                exception_detail = {
                    "exception": exception_obj,
                    "purchaseOrder": s_purchase_order if s_purchase_order else None,
                    "goodsReceipt": s_goods_receipt if s_goods_receipt else None,
                    "invoice": s_invoice if s_invoice else None
                }
                exceptions_list.append(exception_detail)

            # Aggregate trigger emails across all exceptions
            all_trigger_emails = set()
            for ex in exceptions_list:
                tlist = ex['exception'].get('triggerEmail', [])
                if isinstance(tlist, list):
                    all_trigger_emails.update(tlist)

            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "bundle_id": bundle_id,
                    "session_id": session_id,
                    "po_number": po_number,
                    "bundle_status": bundle_status,
                    "trigger_emails": list(all_trigger_emails),
                    "exception_count": len(exceptions_list),
                    "exceptions": exceptions_list
                }, default=str)
            }


        elif event_type == "insert_new_data":
            # close any previous cursor/connection objects if they exist
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            # Get analysis_data and userid from request body
            analysis_data = body.get('analysis_data', {})
            userid = body.get('userid')
            
            # Validate userid is present
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "userid is required in request body for insert_new_data event"
                    })
                }

            # Generate session_id
            chars = string.ascii_letters + string.digits
            session_id = ''.join(secrets.choice(chars) for _ in range(16))

            # Prepare data using your helper functions (which you updated)
            # Pass userid to both preparation functions
            try:
                exception_data_list = prepare_exception_table_data(analysis_data, session_id, userid=userid)
                session_rows = prepare_session_table_rows(analysis_data, session_id, "BUN_3", userid=userid)
            except Exception as e:
                logger.exception("Failed to prepare data: %s", e)
                return {"statusCode": 500, "body": json.dumps({"error": f"prepare failed: {str(e)}"})}

            # Connect to DB and insert in a single transaction
            try:
                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_DATABASE
                )
                logger.info("Database connection established successfully for insert_new_data")

                with conn:
                    with conn.cursor() as cur:
                        # Bulk insert exceptions (if any)
                        if exception_data_list:
                            insert_query_exception = """
                                INSERT INTO erp.exception_table
                                (session_id, unique_exception_id, exception_status, exception_value, exception_summary,
                                    created_at, created_by, updated_at, updated_by, exception_name, item_no, userid,po_number)
                                VALUES %s
                            """
                            exc_tuples = []
                            for ex in exception_data_list:
                                exc_tuples.append((
                                    ex.get("session_id"),
                                    ex.get("unique_exception_id"),
                                    ex.get("exception_status"),
                                    ex.get("exception_value"), 
                                    ex.get("exception_summary"),
                                    ex.get("created_at"),
                                    ex.get("created_by"),
                                    ex.get("updated_at"),
                                    ex.get("updated_by"),
                                    ex.get("exception_name"),
                                    ex.get("item_no"),
                                    ex.get("userid"),
                                    ex.get("po_number")
                                ))
                            # execute_values will perform the bulk insert and will raise on failure
                            execute_values(cur, insert_query_exception, exc_tuples, page_size=100)

                        # Bulk insert session rows (one row per exception, or single row if no exceptions)
                        if session_rows:
                            insert_query_session = """
                                INSERT INTO erp.session_table
                                (session_id, unique_exception_id, bundle_id, overall_status,
                                    purchase_order, goods_receipt, invoice, exception_status,
                                    severity, created_at, created_by, updated_at, updated_by, po_number, userid)
                                VALUES %s
                            """
                            sess_tuples = []
                            for s in session_rows:
                                sess_tuples.append((
                                    s.get("session_id"),
                                    s.get("unique_exception_id"),
                                    s.get("bundle_id"),
                                    s.get("overall_status"),
                                    Json(s.get("purchase_order")),   # ensure JSONB storage
                                    Json(s.get("goods_receipt")),
                                    Json(s.get("invoice")),
                                    s.get("exception_status"),
                                    s.get("severity"),
                                    s.get("created_at"),
                                    s.get("created_by"),
                                    s.get("updated_at"),
                                    s.get("updated_by"),
                                    s.get("po_number"),
                                    s.get("userid")  # Include userid in session table insert
                                ))
                            execute_values(cur, insert_query_session, sess_tuples, page_size=100)

                # If we get here, transaction committed successfully
                logger.info("Inserted session_id=%s: %d session rows, %d exception rows",
                            session_id, len(session_rows), len(exception_data_list))

                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "status": 200,
                        "session_id": session_id,
                        "session_rows_inserted": len(session_rows),
                        "exception_rows_inserted": len(exception_data_list)
                    })
                }

            except Exception as e:
                # with conn: context will rollback automatically, but log and return useful error
                logger.exception("Failed to insert data for session_id=%s: %s", session_id, e)
                return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

            finally:
                try:
                    conn.close()
                except Exception:
                    pass


        elif event_type == "new_schema_dashboard_cards":
            # Dashboard cards endpoint for the new schema (erp.session_table and erp.exception_table)
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            # Build userid filter condition
            # If userid = 0: only show userid 0 data
            # If userid != 0: show userid 0 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                filter_params = ()
            else:
                userid_filter = "userid IN (0, %s)"
                filter_params = (userid,)
            
            # Query 1: total_records_processed - count of unique session IDs
            try:
                query = f"SELECT COUNT(DISTINCT session_id) FROM erp.session_table WHERE {userid_filter};"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                total_records_processed = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"Error fetching total unique sessions: {e}")
                total_records_processed = 0

            # Query 2: total_exceptions - total number of entries in session_table
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE {userid_filter};"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                total_exceptions = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"Error fetching total exceptions: {e}")
                total_exceptions = 0

            # Query 3: resolved_exceptions - count entries with overall_status = 'Resolved'
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE overall_status = 'Resolved' AND {userid_filter};"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                resolved_exceptions = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"Error fetching fixed exceptions: {e}")
                resolved_exceptions = 0

            # Query 4: pending_exceptions - count entries with exception_status = 'Pending'
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE exception_status = 'Pending' AND {userid_filter};"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                pending_exceptions = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"Error fetching pending exceptions: {e}")
                pending_exceptions = 0

            # Query 5: critical_alerts - count entries with severity = 'High'
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE severity = 'High' AND {userid_filter};"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                critical_alerts = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"Error fetching critical alerts: {e}")
                critical_alerts = 0

            
            #Query 6: Pending Approval - count entries with overall_status = 'Pending Approval'
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE overall_status = 'Pending Approval' AND {userid_filter}"
                cursor.execute(query, filter_params)
                r = cursor.fetchone()
                pending_approval = int(r[0]) if r and r[0] is not None else 0
            except Exception as e:
                logger.error(f"error fetching pending approval:{e}")
                pending_approval = 0



            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "total_records_processed": total_records_processed,
                    "total_exceptions": total_exceptions,
                    "resolved_exceptions": resolved_exceptions,
                    "pending_exceptions": pending_exceptions,
                    "critical_alerts": critical_alerts,
                    "pending_approval": pending_approval
                })
            }
        
        elif event_type == "severity_analysis_new_schema":
            # Return counts grouped by severity level
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            # Build userid filter condition
            # If userid = 1: only show userid 1 data
            # If userid != 1: show userid 1 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                filter_params = ()
            else:
                userid_filter = "userid IN (0, %s)"
                filter_params = (userid,)
            
            # Reference the table with schema qualification to avoid "relation does not exist" errors
            query = f"SELECT severity, COUNT(*) as cnt FROM erp.session_table WHERE {userid_filter} GROUP BY severity ORDER BY cnt DESC;"
            cursor.execute(query, filter_params)
            rows = cursor.fetchall()
            data = []
            for r in rows:
                # r[0]=severity, r[1]=count
                data.append({
                    "severity": r[0],
                    "count": int(r[1])
                })

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "severity_analysis_new_schema": data
                })
            }
        
        elif event_type == "exceptions_by_type_new_schema":
            # Return counts grouped by exception_type
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            # Build userid filter condition
            # If userid = 1: only show userid 1 data
            # If userid != 1: show userid 1 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                filter_params = ()
            else:
                userid_filter = "userid IN (0, %s)"
                filter_params = (userid,)
            
            query = f"SELECT exception_name, COUNT(*) as cnt FROM erp.exception_table WHERE {userid_filter} GROUP BY exception_name ORDER BY cnt DESC;"
            cursor.execute(query, filter_params)
            rows = cursor.fetchall()
            data = []
            for r in rows:
                # r[0]=exception_type, r[1]=count
                data.append({
                    "name": r[0],
                    "count": int(r[1])
                })

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "exceptions_by_type_new_schema": data
                })
            }

        elif event_type == "exception_trend_7days_new_schema":
            # Return last 7 days trend: detected and resolved counts per day.
            # New logic:
            # 1) Fetch ALL real exceptions for this userid in the last 7 days.
            # 2) Use them as-is to build the trend.
            # 3) If there is at least one real exception:
            #       - Anchor date for dummy = OLDEST real exception date (in last 7 days).
            #       - Use base_offsets = [2,3,4,5,6,7,8,9,10,11].
            #       - For each dummy i, dummy_day = anchor_day - base_offsets[i].
            #       - Only add dummy counts for dummy_day that:
            #            * fall within the last 7 days window, and
            #            * do NOT already have real data.
            # 4) If there are NO real exceptions in last 7 days:
            #       - Anchor on today (IST) and use only dummy with the same offsets.

            userid = body.get("userid")
            if not userid:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }

            try:
                userid = int(userid)
            except (ValueError, TypeError):
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }

            from datetime import timedelta  # safe even if already imported

            # Last 7 calendar days window in IST
            now_ist = datetime.now(IST)
            today_ist = now_ist.date()
            start_day = today_ist - timedelta(days=6)

            detected_counts = {}
            resolved_counts = {}

            # --- STEP 1: Fetch ALL real exceptions for this userid in the last 7 days ---
            try:
                cursor.execute(
                    """
                    SELECT created_at, exception_status
                    FROM erp.exception_table
                    WHERE userid = %s
                    AND created_at >= CURRENT_DATE - INTERVAL '6 days'
                    ORDER BY created_at ASC
                    """,
                    (userid,)
                )
                real_rows = cursor.fetchall()
            except Exception as e:
                logger.error(f"Error fetching real exceptions for trend_7days: {e}")
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "Database error fetching real exceptions for trend"})
                }

            oldest_real_ist = None

            # --- STEP 2: Aggregate real rows into the 7-day buckets ---
            for created_at, status in real_rows:
                if created_at is None:
                    continue

                # Normalize to IST
                if created_at.tzinfo is None:
                    created_at_utc = created_at.replace(tzinfo=timezone.utc)
                else:
                    created_at_utc = created_at.astimezone(timezone.utc)
                created_at_ist = created_at_utc.astimezone(IST)
                day_ist = created_at_ist.date()

                # Keep only those within [start_day, today_ist]
                if day_ist < start_day or day_ist > today_ist:
                    continue

                detected_counts[day_ist] = detected_counts.get(day_ist, 0) + 1
                if status == 'Resolved':
                    resolved_counts[day_ist] = resolved_counts.get(day_ist, 0) + 1

                # Track oldest real exception (in IST)
                if oldest_real_ist is None or created_at_ist < oldest_real_ist:
                    oldest_real_ist = created_at_ist

            # --- STEP 3: If we have real data, anchor dummy on OLDEST real date ---
            if oldest_real_ist is not None:
                anchor_day = oldest_real_ist.date()
                base_offsets = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

                # Fetch dummy statuses
                try:
                    cursor.execute(
                        """
                        SELECT exception_status
                        FROM erp.exception_table
                        WHERE userid = 0
                        ORDER BY created_at ASC
                        LIMIT %s
                        """,
                        (len(base_offsets),)
                    )
                    dummy_rows = cursor.fetchall()
                except Exception as e:
                    logger.error(f"Error fetching dummy exceptions for trend_7days: {e}")
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 500,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "Database error fetching dummy exceptions for trend"})
                    }

                days_with_real = set(detected_counts.keys())

                for i, (status,) in enumerate(dummy_rows):
                    if i >= len(base_offsets):
                        break
                    dummy_day = anchor_day - timedelta(days=base_offsets[i])

                    # Only within last 7 days window
                    if dummy_day < start_day or dummy_day > today_ist:
                        continue

                    # Only if this day has NO real data
                    if dummy_day in days_with_real:
                        continue

                    detected_counts[dummy_day] = detected_counts.get(dummy_day, 0) + 1
                    if status == 'Resolved':
                        resolved_counts[dummy_day] = resolved_counts.get(dummy_day, 0) + 1

            else:
                # --- STEP 4: No real data in last 7 days -> use dummy anchored on today ---
                base_offsets = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

                try:
                    cursor.execute(
                        """
                        SELECT exception_status
                        FROM erp.exception_table
                        WHERE userid = 0
                        ORDER BY created_at ASC
                        LIMIT %s
                        """,
                        (len(base_offsets),)
                    )
                    dummy_rows = cursor.fetchall()
                except Exception as e:
                    logger.error(f"Error fetching dummy exceptions for trend_7days (no real data): {e}")
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 500,
                        "headers": cors_headers,
                        "body": json.dumps({"error": "Database error fetching dummy exceptions for trend"})
                    }

                for i, (status,) in enumerate(dummy_rows):
                    if i >= len(base_offsets):
                        break
                    dummy_day = today_ist - timedelta(days=base_offsets[i])

                    if dummy_day < start_day or dummy_day > today_ist:
                        continue

                    detected_counts[dummy_day] = detected_counts.get(dummy_day, 0) + 1
                    if status == 'Resolved':
                        resolved_counts[dummy_day] = resolved_counts.get(dummy_day, 0) + 1

            # --- STEP 5: Build final 7-day series [start_day .. today_ist] ---
            data = []
            for i in range(7):
                day = start_day + timedelta(days=i)
                data.append({
                    "day": day.strftime('%b %d'),  # e.g. "Nov 28"
                    "detected": int(detected_counts.get(day, 0)),
                    "resolved": int(resolved_counts.get(day, 0))
                })

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "exception_trend_7days_new_schema": data
                })
            }
        
        if event_type == "exception_page_cards_new_schema":
            # Consolidated exception page cards endpoint to return multiple KPIs for Exceptions page
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            # Build userid filter condition
            # If userid = 1: only show userid 1 data
            # If userid != 1: show userid 1 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                filter_params = ()
            else:
                userid_filter = "userid IN (0, %s)"
                filter_params = (userid,)
            
            # Query 1: total_exceptions (count all rows)
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE {userid_filter};"
                if filter_params:
                    cursor.execute(query, filter_params)
                else:
                    cursor.execute(query)
                r = cursor.fetchone()
                total_exceptions = int(r[0]) if r and r[0] is not None else 0
            except Exception:
                total_exceptions = 0

            # Query 2: high_severity_exceptions (severity IN ('High', 'Critical'))
            try:
                query = f"SELECT COUNT(*) FROM erp.session_table WHERE severity IN ('High', 'Critical') AND {userid_filter};"
                if filter_params:
                    cursor.execute(query, filter_params)
                else:
                    cursor.execute(query)
                r = cursor.fetchone()
                high_severity_exceptions = int(r[0]) if r and r[0] is not None else 0
            except Exception:
                high_severity_exceptions = 0

            # Query 3: resolved_today (status = 'Resolved' AND DATE(updated_at) = CURRENT_DATE)
            try:
                query = f"""
                    SELECT COUNT(*) 
                    FROM erp.session_table 
                    WHERE exception_status = 'Fixed' 
                    AND DATE(updated_at) = CURRENT_DATE
                    AND {userid_filter};
                """
                if filter_params:
                    cursor.execute(query, filter_params)
                else:
                    cursor.execute(query)
                r = cursor.fetchone()
                resolved_today = int(r[0]) if r and r[0] is not None else 0
            except Exception:
                resolved_today = 0

            # Query 4: total_value_at_risk (sum of exception_value)
            try:
                query = f"""
                SELECT SUM((exception_value)::numeric)
                FROM erp.exception_table
                WHERE exception_value ~ '^-?[0-9]+(\\.[0-9]+)?$'
                AND {userid_filter};
                """
                if filter_params:
                    cursor.execute(query, filter_params)
                else:
                    cursor.execute(query)
                r = cursor.fetchone()
                total_value_at_risk = float(r[0]) if r and r[0] is not None else 0.0
            except Exception as e:
                print("SUM numeric error:", str(e))
                total_value_at_risk = 0.0

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "total_exceptions": total_exceptions,
                    "high_severity_exceptions": high_severity_exceptions,
                    "resolved_today": resolved_today,
                    "total_value_at_risk": total_value_at_risk
                })
            }
        
        elif event_type == 'session_exceptions':
            # New event type for session-based exceptions table
            # Groups by session_id and returns po_number, created_at, updated_at, overall_status, exception_count
            # Supports pagination and filtering by date and status
            
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            page = body.get('page', 1)
            page_size = body.get('page_size', 10)
            status_filter = body.get('status_filter', 'all')  # all, pending, resolved, escalated
            date_filter = body.get('date_filter', 'all')  # all, last7days, last30days
            
            offset = (page - 1) * page_size
            
            # Build userid filter condition
            # If userid = 1: only show userid 1 data
            # If userid != 1: show userid 1 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                query_params = []
            else:
                userid_filter = "userid IN (0, %s)"
                query_params = [userid]
            
            # Build WHERE clause for filters
            where_conditions = []
            
            # Add userid filter first
            where_conditions.append(userid_filter)
            
            if status_filter != 'all':
                # Map frontend status values to database values
                status_map = {
                    'pending': 'Pending',
                    'resolved': 'Resolved',
                    'escalated': 'Escalated'
                }
                db_status = status_map.get(status_filter.lower(), status_filter)
                where_conditions.append(f"overall_status = '{db_status}'")
            
            if date_filter == 'last7days':
                where_conditions.append("created_at >= NOW() - INTERVAL '7 days'")
            elif date_filter == 'last30days':
                where_conditions.append("created_at >= NOW() - INTERVAL '30 days'")
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            # --- STEP 1: Find user's oldest real exception (non-dummy) in LAST 7 DAYS ---
            # We align this with exception_trend_7days_new_schema:
            # anchor = oldest real exception for this userid in last 7 days from erp.exception_table
            earliest_user_created_at = None
            if userid != 0:
                try:
                    cursor.execute(
                        """
                        SELECT MIN(created_at)
                        FROM erp.exception_table
                        WHERE userid = %s
                          AND created_at >= CURRENT_DATE - INTERVAL '6 days'
                        """,
                        (userid,)
                    )
                    res = cursor.fetchone()
                    if res and res[0]:
                        earliest_user_created_at = res[0]
                except Exception as e:
                    logger.error(f"Error fetching earliest created_at for user {userid} from exception_table: {e}")
                    earliest_user_created_at = None

            # --- STEP 1A: count how many dummy rows are BEFORE this page (for global offset) ---  # NEW
            dummy_before = 0  # NEW
            if offset > 0:  # NEW
                count_dummy_before_query = f"""  -- NEW
                    WITH ordered AS (
                        SELECT userid
                        FROM erp.session_table
                        {where_clause}
                        ORDER BY created_at DESC
                        LIMIT {offset}
                    )
                    SELECT COUNT(*) FROM ordered WHERE userid = 0;
                """  # NEW
                try:  # NEW
                    if query_params:  # NEW
                        cursor.execute(count_dummy_before_query, tuple(query_params))  # NEW
                    else:  # NEW
                        cursor.execute(count_dummy_before_query)  # NEW
                    res = cursor.fetchone()  # NEW
                    dummy_before = res[0] if res and res[0] is not None else 0  # NEW
                except Exception as e:  # NEW
                    logger.error(f"Error counting dummy_before for user {userid}: {e}")  # NEW
                    dummy_before = 0  # NEW
            
            # Get total count for pagination
            count_query = f"""
                SELECT COUNT(DISTINCT session_id) 
                FROM erp.session_table
                {where_clause}
            """ 
            if query_params:
                # Count query has WHERE clause with 1 parameter placeholder
                cursor.execute(count_query, tuple(query_params))
            else:
                cursor.execute(count_query)
            total_count = cursor.fetchone()[0]
            
            # Main query: group by session_id and aggregate data
            # Note: WHERE clause appears three times (in all three CTEs), so we need 3 parameters if userid != 0
            query = f"""
                WITH latest_per_session AS (
                    SELECT DISTINCT ON (session_id)
                        session_id,
                        bundle_id,
                        po_number,
                        created_at,
                        updated_at,
                        overall_status,
                        userid
                    FROM erp.session_table
                    {where_clause}
                    ORDER BY session_id, created_at DESC
                ),
                session_counts AS (
                    SELECT session_id, COUNT(*) AS exception_count
                    FROM erp.session_table
                    {where_clause}
                    GROUP BY session_id
                ),
                session_severity AS (
                    SELECT session_id,
                        CASE 
                            WHEN MAX(CASE WHEN severity = 'High' THEN 3 WHEN severity = 'Medium' THEN 2 WHEN severity = 'Low' THEN 1 ELSE 0 END) = 3 THEN 'High'
                            WHEN MAX(CASE WHEN severity = 'High' THEN 3 WHEN severity = 'Medium' THEN 2 WHEN severity = 'Low' THEN 1 ELSE 0 END) = 2 THEN 'Medium'
                            WHEN MAX(CASE WHEN severity = 'High' THEN 3 WHEN severity = 'Medium' THEN 2 WHEN severity = 'Low' THEN 1 ELSE 0 END) = 1 THEN 'Low'
                            ELSE NULL
                        END AS max_severity
                    FROM erp.session_table
                    {where_clause}
                    GROUP BY session_id
                )
                SELECT
                    l.session_id,
                    l.bundle_id,
                    l.po_number,
                    l.created_at,
                    l.updated_at,
                    l.overall_status,
                    COALESCE(s.max_severity, 'N/A') AS severity,
                    COALESCE(c.exception_count, 0) AS exception_count,
                    l.userid
                FROM latest_per_session l
                LEFT JOIN session_counts c USING (session_id)
                LEFT JOIN session_severity s USING (session_id)
                ORDER BY l.created_at DESC
                LIMIT {page_size} OFFSET {offset}
            """
            
            if query_params:
                # Main query has WHERE clause three times (in all three CTEs), so we need 3 parameters
                cursor.execute(query, tuple(query_params * 3))
            else:
                cursor.execute(query)
            rows = cursor.fetchall()

            from datetime import timedelta  # already imported at top, but harmless

            dummy_indices = []
            for idx, row in enumerate(rows):
                row_userid = row[8]  # l.userid from SELECT
                if row_userid == 0:
                    dummy_indices.append(idx)

            dynamic_created_map = {}
            dynamic_updated_map = {}

            if dummy_indices:
                base_offsets = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
                sorted_indices = sorted(dummy_indices)

                if earliest_user_created_at:
                    # Case A: user has real exceptions in last 7 days
                    # Anchor on earliest real exception (same as trend API)
                    anchor_dt = earliest_user_created_at
                else:
                    # Case B: user has NO real exceptions in last 7 days
                    # Anchor on "now" in IST (same fallback as trend API)
                    anchor_dt = datetime.now(IST)

                for local_i, idx in enumerate(sorted_indices):
                    # GLOBAL dummy index across all pages
                    global_i = dummy_before + local_i

                    # Choose offset from base_offsets; extend if more dummies than offsets
                    if global_i < len(base_offsets):
                        offset_days = base_offsets[global_i]
                    else:
                        offset_days = base_offsets[-1] + (global_i - len(base_offsets) + 1)

                    # Dummy created_at
                    base_created = anchor_dt - timedelta(days=offset_days)

                    # Status for this dummy row (from SELECT)
                    row_status = rows[idx][5]

                    # Dummy updated_at rule:
                    # - If Pending -> same as created_at
                    # - Else      -> created_at + 1 day
                    if row_status == 'Pending':
                        base_updated = base_created
                    else:
                        base_updated = base_created + timedelta(days=1)

                    dynamic_created_map[idx] = base_created
                    dynamic_updated_map[idx] = base_updated
            
            print("dynamic_created_map:",dynamic_created_map)
            print("dynamic_updated_map:",dynamic_updated_map)
            
            # --- STEP 3: Build response, using dynamic timestamps for dummy rows ---
            session_exceptions = []
            for idx, row in enumerate(rows):
                # row mapping after SELECT:
                # 0: session_id
                # 1: bundle_id
                # 2: po_number
                # 3: created_at (DB)
                # 4: updated_at (DB)
                # 5: overall_status
                # 6: severity
                # 7: exception_count
                # 8: userid (for internal use only)

                created_at_raw = dynamic_created_map.get(idx, row[3])
                updated_at_raw = dynamic_updated_map.get(idx, row[4])

                session_exceptions.append({
                    "sessionId": row[0],
                    "bundleId": row[1],
                    "poNumber": row[2],
                    "createdAt": format_datetime_to_ist(created_at_raw),
                    "updatedAt": format_datetime_to_ist(updated_at_raw),
                    "status": row[5],
                    "severity": row[6],
                    "exceptionCount": row[7]
                })

            cursor.close()
            conn.close()

            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "session_exceptions": session_exceptions,
                    "pagination": {
                        "total_count": total_count,
                        "page": page,
                        "page_size": page_size,
                        "total_pages": (total_count + page_size - 1) // page_size
                    }
                })
            }
            
        
        elif event_type == 'sop_agents_new_schema':
            # New event type for SOP Agents page using new schema (erp.sop, erp.exception_table, erp.session_table)
            # Returns: exception_type (from sop.exception_id), exception_name (from sop.exception_name),
            # sop_connected (hardcoded mapping), last_run (from session_table or dummy logic), 
            # total_exceptions (count from exception_table), trigger_email (from sop.mail)
            
            # Extract userid from request body
            userid = body.get("userid")
            if not userid:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid is required"})
                }
            
            try:
                userid = int(userid)
            except (ValueError, TypeError):
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": "userid must be a valid integer"})
                }
            
            # Build userid filter condition
            # If userid = 0: only show userid 0 data
            # If userid != 0: show userid 0 data + their own data
            if userid == 0:
                userid_filter = "userid = 0"
                filter_params = ()
            else:
                userid_filter = "userid IN (0, %s)"
                filter_params = (userid,)
            
            page = body.get('page', 1)
            page_size = body.get('page_size', 10)
            offset = (page - 1) * page_size

            from datetime import timedelta  # safe even if already imported

            # --- Anchor logic: same as exception_trend_7days_new_schema & session_exceptions ---
            earliest_user_created_at = None
            if userid != 0:
                try:
                    cursor.execute(
                        """
                        SELECT MIN(created_at)
                        FROM erp.exception_table
                        WHERE userid = %s
                          AND created_at >= CURRENT_DATE - INTERVAL '6 days'
                        """,
                        (userid,)
                    )
                    res = cursor.fetchone()
                    if res and res[0]:
                        earliest_user_created_at = res[0]
                except Exception as e:
                    logger.error(f"Error fetching earliest created_at for SOP anchor, user {userid}: {e}")
                    earliest_user_created_at = None

            # Base offsets for dummy timeline (same as other APIs)
            base_offsets = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

            # ------------------------------------------------------------------
            # Build synthetic updated_at for ALL dummy sessions (userid = 0)
            # using the SAME logic as session_exceptions:
            #   - anchor_dt (earliest_user_created_at or now IST)
            #   - global ordering by created_at DESC
            #   - status rule: Pending -> created_at, else created_at + 1 day
            # ------------------------------------------------------------------
            dummy_session_updated_map = {}

            try:
                cursor.execute(
                    """
                    WITH latest_per_session AS (
                        SELECT DISTINCT ON (session_id)
                            session_id,
                            created_at,
                            overall_status
                        FROM erp.session_table
                        WHERE userid = 0
                        ORDER BY session_id, created_at DESC
                    )
                    SELECT session_id, created_at, overall_status
                    FROM latest_per_session
                    ORDER BY created_at DESC
                    """
                )
                dummy_rows = cursor.fetchall()

                if userid != 0 and earliest_user_created_at:
                    anchor_dt = earliest_user_created_at
                else:
                    anchor_dt = datetime.now(IST)

                for global_i, (sess_id, created_at_db, overall_status) in enumerate(dummy_rows):
                    if global_i < len(base_offsets):
                        offset_days = base_offsets[global_i]
                    else:
                        offset_days = base_offsets[-1] + (global_i - len(base_offsets) + 1)

                    # Synthetic created_at for this dummy session
                    base_created = anchor_dt - timedelta(days=offset_days)

                    # Status rule (same as session_exceptions dummy updated_at):
                    # - Pending -> updated_at = created_at
                    # - Else    -> updated_at = created_at + 1 day
                    if overall_status == 'Pending':
                        base_updated = base_created
                    else:
                        base_updated = base_created + timedelta(days=1)

                    dummy_session_updated_map[sess_id] = base_updated

            except Exception as e:
                logger.error(f"Error building dummy_session_updated_map for SOP: {e}")
                dummy_session_updated_map = {}

            # Hardcoded SOP mapping based on exception names
            sop_mapping = {
                'Quantity Variance': 'SOP_QUANTITY_CHANGE_V1',
                'Price Mismatch': 'SOP_PRICE_CHANGE_V1',
                'Invoice Errors': 'SOP_DUPLICATE_INVOICE_V1',
                'Missing GR': 'SOP_MISSING_GRN_V1',
                'UoM Issues': 'SOP_UOM_ISSUE_V1',
                'Missing PO': 'SOP_MISSING_PO_V1'
            }
            
            # Get total count of SOP entries
            count_query = "SELECT COUNT(*) FROM erp.sop"
            cursor.execute(count_query)
            total_count = cursor.fetchone()[0]
            
            # Main query to get SOP data
            query = """
                SELECT 
                    s.exception_id,
                    s.exception_name,
                    s.mail,
                    s.updated_at
                FROM erp.sop s
                ORDER BY s.exception_id
                LIMIT %s OFFSET %s
            """
            
            cursor.execute(query, (page_size, offset))
            rows = cursor.fetchall()
            
            sop_agents = []
            for row in rows:
                exception_id = row[0]
                exception_name = row[1]
                mail = row[2]  # JSONB
                sop_updated_at = row[3]
                
                # Get hardcoded SOP connection
                sop_connected = sop_mapping.get(exception_name, 'SOP_UNKNOWN_V1')

                # ---------------- LAST RUN LOGIC (REAL + DUMMY) ----------------

                # 1) Real exception count for this user & exception_name
                real_exception_count = 0
                if userid != 0:
                    try:
                        cursor.execute(
                            """
                            SELECT COUNT(*)
                            FROM erp.exception_table
                            WHERE exception_name = %s
                              AND userid = %s
                            """,
                            (exception_name, userid)
                        )
                        real_exception_count = cursor.fetchone()[0]
                    except Exception as e:
                        logger.error(f"Error counting real exceptions for SOP {exception_name}, user {userid}: {e}")
                        real_exception_count = 0

                # 2) Total exceptions (user + dummy) for records column (same as before)
                exception_count_query = f"""
                    SELECT COUNT(*)
                    FROM erp.exception_table
                    WHERE exception_name = %s
                      AND {userid_filter}
                """
                if filter_params:
                    cursor.execute(exception_count_query, (exception_name,) + filter_params)
                else:
                    cursor.execute(exception_count_query, (exception_name,))
                exception_count = cursor.fetchone()[0]

                # 3) Compute last_run
                last_run = None

                if real_exception_count > 0:
                    # Real data exists for this exception type -> last_run = latest real session updated_at
                    try:
                        cursor.execute(
                            """
                            SELECT MAX(st.updated_at)
                            FROM erp.session_table st
                            WHERE st.userid = %s
                              AND EXISTS (
                                  SELECT 1 FROM erp.exception_table et
                                  WHERE et.session_id = st.session_id
                                    AND et.exception_name = %s
                                    AND et.userid = %s
                              )
                            """,
                            (userid, exception_name, userid)
                        )
                        res = cursor.fetchone()
                        if res and res[0]:
                            last_run = res[0]
                    except Exception as e:
                        logger.error(f"Error fetching real last_run for SOP {exception_name}, user {userid}: {e}")
                        last_run = None

                else:
                    # No real data for this SOP type -> derive from dummy sessions (userid = 0), if any
                    dummy_session_ids = []
                    try:
                        cursor.execute(
                            """
                            SELECT DISTINCT session_id
                            FROM erp.exception_table
                            WHERE exception_name = %s
                              AND userid = 0
                            """,
                            (exception_name,)
                        )
                        dummy_session_ids = [r[0] for r in cursor.fetchall() if r and r[0]]
                    except Exception as e:
                        logger.error(f"Error fetching dummy session_ids for SOP {exception_name}: {e}")
                        dummy_session_ids = []

                    # Look up synthetic updated_at for those dummy sessions
                    candidate_dates = [
                        dummy_session_updated_map[sid]
                        for sid in dummy_session_ids
                        if sid in dummy_session_updated_map
                    ]

                    if candidate_dates:
                        # last_run = max synthetic updated_at across those dummy sessions
                        last_run = max(candidate_dates)
                    else:
                        # No real, no mapped dummy -> fall back to SOP updated_at
                        last_run = sop_updated_at

                # -------------------------------------------------------------------------

                # Extract email array from JSONB mail column
                trigger_emails = []
                if mail:
                    if isinstance(mail, list):
                        trigger_emails = mail
                    elif isinstance(mail, str):
                        try:
                            mail_data = json.loads(mail)
                            if isinstance(mail_data, list):
                                trigger_emails = mail_data
                            else:
                                trigger_emails = [mail_data] if mail_data else []
                        except:
                            trigger_emails = []
                
                sop_agents.append({
                    "exceptionId": exception_id,
                    "exceptionName": exception_name,
                    "sopId": sop_connected,
                    "lastRun": last_run.strftime('%d %b %Y, %I:%M %p') if last_run else None,
                    "records": exception_count,
                    "triggerEmail": trigger_emails,
                    "userid": userid
                })
            
            cursor.close()
            conn.close()
            
            return {
                "statusCode": 200,
                "headers": cors_headers,
                "body": json.dumps({
                    "agent_summary": sop_agents,
                    "userid": userid,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": (total_count + page_size - 1) // page_size
                    }
                })
            }
        
        elif event_type == 'update_sop_trigger_emails':
            # Update trigger emails for a specific SOP agent
            exception_id = body.get('exception_id')
            trigger_emails = body.get('trigger_emails', [])
            
            if not exception_id:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "exception_id is required"
                    })
                }
            
            # Validate that trigger_emails is a list
            if not isinstance(trigger_emails, list):
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "trigger_emails must be an array"
                    })
                }
            
            # Update the mail column in erp.sop table
            # Convert the list to JSONB format
            update_query = """
                UPDATE erp.sop
                SET mail = %s::jsonb,
                    updated_at = NOW()
                WHERE exception_id = %s
            """
            
            try:
                # Convert list to JSON string for JSONB column
                mail_json = json.dumps(trigger_emails)
                cursor.execute(update_query, (mail_json, exception_id))
                conn.commit()
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "success": True,
                        "message": "Trigger emails updated successfully"
                    })
                }
            except Exception as update_error:
                conn.rollback()
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to update emails: {str(update_error)}"
                    })
                }
        
        elif event_type == 'get_sop_presigned_url':
            # Generate presigned URL for SOP document from S3
            exception_name = body.get('exception_name')
            
            if not exception_name:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "exception_name is required"
                    })
                }
            
            # Query erp.sop to get the s3_uri for this exception_name
            query = """
                SELECT s3_uri, exception_id
                FROM erp.sop
                WHERE exception_name = %s
                LIMIT 1
            """
            
            cursor.execute(query, (exception_name,))
            result = cursor.fetchone()
            
            if not result or not result[0]:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 404,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"No SOP document found for exception: {exception_name}"
                    })
                }
            
            s3_uri = result[0]
            exception_id = result[1]
            
            # Parse S3 URI (format: s3://bucket-name/path/to/file.pdf)
            if not s3_uri.startswith('s3://'):
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Invalid S3 URI format"
                    })
                }
            
            # Extract bucket and key from s3://bucket/key
            s3_path = s3_uri[5:]  # Remove 's3://'
            bucket_name = s3_path.split('/')[0]
            object_key = '/'.join(s3_path.split('/')[1:])
            
            # Generate presigned URL
            try:
                s3_client = boto3.client('s3')
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': bucket_name,
                        'Key': object_key
                    },
                    ExpiresIn=3600  # URL valid for 1 hour
                )
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "presigned_url": presigned_url,
                        "exception_id": exception_id,
                        "exception_name": exception_name,
                        "expires_in": 3600
                    })
                }
            except ClientError as e:
                logger.error(f"Error generating presigned URL: {e}")
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to generate presigned URL: {str(e)}"
                    })
                }
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Unexpected error: {str(e)}"
                    })
                }
            
        elif event_type == "update_bundle_status_new_schema":

            # Close the initial connection since we need a fresh one for this operation
            cursor.close()
            conn.close()
            
            # Update status in erp.session_table for a given bundle_id and session_id
            bundle_id = body.get('bundle_id')
            session_id = body.get('session_id')
            new_status = body.get('status')
            
            if not bundle_id or not session_id or not new_status:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Missing required parameters: bundle_id, session_id, and status"
                    })
                }
            
            # Validate status value
            valid_statuses = ['Pending', 'Escalated', 'Pending Approval', 'Resolved']
            if new_status not in valid_statuses:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
                    })
                }
            
            try:
                # Create new database connection for this operation
                conn = psycopg2.connect(
                    host=DB_HOST,
                    database=DB_DATABASE,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                cursor = conn.cursor()
                
                # Get current IST time for updated_at
                updated_at = format_datetime_to_ist(datetime.now(IST))
                
                # Update the overall_status for all records in this bundle and session
                update_query = """
                    UPDATE erp.session_table 
                    SET overall_status = %s, updated_at = %s 
                    WHERE bundle_id = %s AND session_id = %s
                """
                cursor.execute(update_query, (new_status, updated_at, bundle_id, session_id))
                conn.commit()
                
                if cursor.rowcount == 0:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 404,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": f"No records found with bundle_id: {bundle_id} and session_id: {session_id}"
                        })
                    }
                
                # Data correction logic when status is "Pending Approval"
                # This performs ITEM-LEVEL corrections based on the exception type
                if new_status == "Pending Approval":
                    # Get all records for this bundle and session with their exception types
                    cursor.execute("""
                        SELECT 
                            s.unique_exception_id,
                            s.purchase_order,
                            s.goods_receipt,
                            s.invoice,
                            e.exception_name,
                            s.po_number
                        FROM erp.session_table s
                        LEFT JOIN erp.exception_table e 
                            ON s.unique_exception_id = e.unique_exception_id 
                            AND s.session_id = e.session_id
                        WHERE s.session_id = %s
                        ORDER BY s.unique_exception_id
                    """, (session_id,))
                    
                    records = cursor.fetchall()
                    print("DB records structure: ",records)
                    
                    if not records:
                        cursor.close()
                        conn.close()
                        return {
                            "statusCode": 404,
                            "headers": cors_headers,
                            "body": json.dumps({
                                "error": f"No records found for session_id: {session_id}"
                            })
                        }
                    
                    # Use the FIRST record's JSONB as the shared base (all records have same PO/GR/Invoice)
                    # We'll accumulate ALL corrections to this shared object
                    shared_purchase_order = records[0][1] if records[0][1] else {}
                    shared_goods_receipt = records[0][2] if records[0][2] else {}
                    shared_invoice = records[0][3] if records[0][3] else {}
                    po_number = records[0][5]
                    
                    # Helper function to get first value from dict
                    def get_first_dict_value(d):
                        if d and isinstance(d, dict):
                            return next(iter(d.values())) if d else None
                        return None
                    
                    # Extract document structures from shared JSONB
                    po_document = get_first_dict_value(shared_purchase_order)
                    gr_document = get_first_dict_value(shared_goods_receipt)
                    inv_document = get_first_dict_value(shared_invoice)
                    
                    # For Missing PO exception, PO document won't exist - we'll create it from Invoice/GR
                    # So we skip the PO validation in that case
                    has_missing_po_exception = any(
                        record[4] == 'Missing PO' for record in records if record[4]
                    )
                    
                    # Validate PO document has Items (unless it's Missing PO exception)
                    if not has_missing_po_exception:
                        if not po_document or not isinstance(po_document, dict) or 'Items' not in po_document:
                            print(f"WARNING: No valid PO document found")
                            cursor.close()
                            conn.close()
                            return {
                                "statusCode": 400,
                                "headers": cors_headers,
                                "body": json.dumps({
                                    "error": "Invalid PO document structure"
                                })
                            }
                    
                    # Get PO items array (will be empty for Missing PO, which is okay)
                    po_items = po_document.get('Items', []) if po_document else []
                    
                    # Flags to track what needs updating across ALL exceptions
                    any_po_update = False
                    any_gr_update = False
                    any_inv_update = False
                    exceptions_to_mark_fixed = []
                    
                    # Process EACH exception and accumulate changes to the SAME shared JSONB
                    for record in records:
                        unique_exception_id = record[0]
                        exception_name = record[4]
                        
                        print(f"Processing exception: {unique_exception_id}, type: {exception_name}")
                        
                        # If exception_name is null, derive it from unique_exception_id
                        if not exception_name:
                            exception_name = get_exception_name_from_id(unique_exception_id)
                            print(f"Mapped exception_name from ID: {exception_name}")
                        
                        # Skip if still no exception name
                        if not exception_name:
                            print(f"WARNING: Could not determine exception_name for {unique_exception_id}")
                            continue
                        
                        # Handle Missing PO exception - create new PO from Invoice and GR data
                        if exception_name == 'Missing PO':
                            # For Missing PO, we need to create a completely new PO from scratch
                            # Use data from Invoice and GR documents
                            
                            # Get Invoice and GR documents
                            inv_items = inv_document.get('Items', []) if inv_document and isinstance(inv_document, dict) else []
                            gr_items = gr_document.get('Items', []) if gr_document and isinstance(gr_document, dict) else []
                            
                            if not inv_items:
                                print(f"ERROR: Cannot create PO without Invoice items")
                                continue
                            
                            # PO number comes from the wrong PO number user provided (stored in po_number field)
                            new_po_number = po_number
                            
                            # Get Vendor and Currency from Invoice
                            vendor = inv_document.get('Supplier', 'VEND-1001')
                            currency = inv_document.get('DocumentCurrency', 'USD')
                            company_code = inv_document.get('CompanyCode', '1000')
                            
                            # Calculate total amount from Invoice items
                            total_amount = sum(item.get('NetValueAmount', 0) for item in inv_items)
                            
                            # Get dates from GR and Invoice - use around the same time
                            
                            
                            gr_posting_date = None
                            inv_posting_date = None
                            
                            if gr_document:
                                gr_posting_date = gr_document.get('PostingDate')
                            if inv_document:
                                inv_posting_date = inv_document.get('PostingDate')
                            
                            # Use GR date as base, or Invoice date, or current date
                            base_date_str = gr_posting_date or inv_posting_date
                            if base_date_str:
                                try:
                                    base_date = datetime.strptime(base_date_str, '%Y-%m-%d')
                                    # PO date should be a few days before GR/Invoice
                                    po_date = (base_date - timedelta(days=random.randint(2, 5))).strftime('%Y-%m-%d')
                                except:
                                    po_date = datetime.now().strftime('%Y-%m-%d')
                            else:
                                po_date = datetime.now().strftime('%Y-%m-%d')
                            
                            # Create PO items from Invoice items
                            po_items_new = []
                            for idx, inv_item in enumerate(inv_items, start=1):
                                material = inv_item.get('Material', '')
                                po_item_number = inv_item.get('ReferencePurchaseOrderItem') or f"{idx*10:05d}"
                                
                                # Get material description from mapping or use generic
                                material_desc = MATERIAL_DESCRIPTIONS.get(material, f"Material {material}")
                                
                                # Calculate delivery date (few days after PO date)
                                try:
                                    po_date_obj = datetime.strptime(po_date, '%Y-%m-%d')
                                    delivery_date = (po_date_obj + timedelta(days=random.randint(2, 4))).strftime('%Y-%m-%d')
                                except:
                                    delivery_date = po_date
                                
                                po_items_new.append({
                                    "PurchaseOrderItem": po_item_number,
                                    "Material": material,
                                    "MaterialDescription": material_desc,
                                    "Plant": "PL01",
                                    "OrderQuantity": inv_item.get('BillingQuantity', 0),
                                    "OrderUnit": inv_item.get('BillingQuantityUnit', 'PC'),
                                    "OrderPrice": inv_item.get('InvoiceUnitPrice', 0),
                                    "NetAmount": inv_item.get('NetValueAmount', 0),
                                    "DeliveryDate": delivery_date
                                })
                            
                            # Create new PO document structure
                            shared_purchase_order = {
                                new_po_number: {
                                    "PurchaseOrder": new_po_number,
                                    "Vendor": vendor,
                                    "CompanyCode": company_code,
                                    "DocumentCurrency": currency,
                                    "POHeaderNetValue": total_amount,
                                    "CreatedOn": po_date,
                                    "Items": po_items_new
                                }
                            }
                            
                            # Update po_document reference for subsequent exceptions
                            po_document = get_first_dict_value(shared_purchase_order)
                            po_items = po_document.get('Items', [])
                            
                            # Mark this as needing PO update
                            any_po_update = True
                            exceptions_to_mark_fixed.append(unique_exception_id)
                            print(f"Created new PO {new_po_number} from Invoice data with {len(po_items_new)} items")
                        
                        # Handle Missing GR exception - update existing GR structure with correct data from PO
                        elif exception_name == 'Missing GR':
                            # Don't generate new GR number or date - preserve existing ones
                            # Extract existing GR number and date if they exist
                            existing_gr_number = None
                            existing_posting_date = None
                            
                            if gr_document:
                                existing_gr_number = gr_document.get('MaterialDocument')
                                existing_posting_date = gr_document.get('PostingDate')
                            
                            # If no existing GR document, only then generate new number and date
                            if not existing_gr_number:
                                import random
                                existing_gr_number = f"5{random.randint(100000000, 999999999)}"
                            
                            if not existing_posting_date:
                                existing_posting_date = date.today().strftime('%Y-%m-%d')
                            
                            # Update GR items to match PO data
                            gr_items = []
                            for idx, po_item in enumerate(po_items, start=1):
                                gr_items.append({
                                    "MaterialDocumentItem": f"{idx:04d}",
                                    "PurchaseOrder": po_number,
                                    "PurchaseOrderItem": po_item.get('PurchaseOrderItem'),
                                    "Material": po_item.get('Material'),
                                    "EntryUnit": po_item.get('OrderUnit'),
                                    "QuantityInEntryUnit": po_item.get('OrderQuantity'),
                                    "AmountInDocumentCurrency": po_item.get('NetAmount'),
                                    "GRStatus": "Received"
                                })
                            
                            # Update shared GR document (preserve existing GR number and posting date)
                            shared_goods_receipt = {
                                existing_gr_number: {
                                    "MaterialDocument": existing_gr_number,
                                    "PostingDate": existing_posting_date,
                                    "CompanyCode": po_document.get('CompanyCode', '1000'),
                                    "Items": gr_items
                                }
                            }
                            # Update gr_document reference for subsequent exceptions
                            gr_document = get_first_dict_value(shared_goods_receipt)
                            any_gr_update = True
                            exceptions_to_mark_fixed.append(unique_exception_id)
                        
                        # For other exception types, identify the specific item with the exception
                        # and update only that item in GR and/or Invoice
                        else:
                            # Get GR and Invoice items arrays if they exist (from shared JSONB)
                            gr_items = gr_document.get('Items', []) if gr_document and isinstance(gr_document, dict) else []
                            inv_items = inv_document.get('Items', []) if inv_document and isinstance(inv_document, dict) else []
                            
                            # Process each PO item and find matching GR/Invoice items
                            for po_item in po_items:
                                po_item_number = po_item.get('PurchaseOrderItem')
                                
                                # Handle Quantity Variance - update specific item quantities
                                if exception_name == 'Quantity Variance':
                                    source_quantity = po_item.get('OrderQuantity')
                                    
                                    # Update matching GR item
                                    for gr_item in gr_items:
                                        if gr_item.get('PurchaseOrderItem') == po_item_number:
                                            gr_item['QuantityInEntryUnit'] = source_quantity
                                            # Ensure GR status reflects receipt after quantity correction
                                            gr_item['GRStatus'] = 'Received'
                                            any_gr_update = True
                                            print(f"Updated GR item {po_item_number} quantity to {source_quantity} and set GRStatus to 'Received'")
                                    
                                    # Update matching Invoice item (Invoice uses ReferencePurchaseOrderItem and BillingQuantity)
                                    for inv_item in inv_items:
                                        if inv_item.get('ReferencePurchaseOrderItem') == po_item_number:
                                            inv_item['BillingQuantity'] = source_quantity
                                            any_inv_update = True
                                            print(f"Updated Invoice item {po_item_number} quantity to {source_quantity}")
                                
                                # Handle UoM Issues - update specific item UoMs
                                elif exception_name == 'UoM Issues':
                                    source_uom = po_item.get('OrderUnit')
                                    
                                    # Update matching GR item
                                    for gr_item in gr_items:
                                        if gr_item.get('PurchaseOrderItem') == po_item_number:
                                            gr_item['EntryUnit'] = source_uom
                                            any_gr_update = True
                                            print(f"Updated GR item {po_item_number} UoM to {source_uom}")
                                    
                                    # Update matching Invoice item (Invoice uses ReferencePurchaseOrderItem and BillingQuantityUnit)
                                    for inv_item in inv_items:
                                        if inv_item.get('ReferencePurchaseOrderItem') == po_item_number:
                                            inv_item['BillingQuantityUnit'] = source_uom
                                            any_inv_update = True
                                            print(f"Updated Invoice item {po_item_number} UoM to {source_uom}")
                                
                                # Handle Price Mismatch - update specific item prices
                                elif exception_name == 'Price Mismatch':
                                    source_amount = po_item.get('NetAmount')
                                    source_unit_price = po_item.get('OrderPrice')
                                    
                                    # Update matching GR item (GR uses AmountInDocumentCurrency)
                                    for gr_item in gr_items:
                                        if gr_item.get('PurchaseOrderItem') == po_item_number:
                                            gr_item['AmountInDocumentCurrency'] = source_amount
                                            any_gr_update = True
                                            print(f"Updated GR item {po_item_number} amount to {source_amount}")
                                    
                                    # Update matching Invoice item (Invoice uses ReferencePurchaseOrderItem, NetValueAmount, and InvoiceUnitPrice)
                                    for inv_item in inv_items:
                                        if inv_item.get('ReferencePurchaseOrderItem') == po_item_number:
                                            # Update both amount and unit price to match PO
                                            inv_item['NetValueAmount'] = source_amount
                                            inv_item['InvoiceUnitPrice'] = source_unit_price
                                            any_inv_update = True
                                            print(f"Updated Invoice item {po_item_number} amount to {source_amount} and unit price to {source_unit_price}")
                            
                            # Handle Invoice Errors (just mark as Fixed, no data change)
                            if exception_name == 'Invoice Errors':
                                exceptions_to_mark_fixed.append(unique_exception_id)
                            elif exception_name in ['Quantity Variance', 'UoM Issues', 'Price Mismatch']:
                                exceptions_to_mark_fixed.append(unique_exception_id)
                    
                    # After processing ALL exceptions, update the database ONCE with the accumulated changes
                    # Update ALL session records with the same session_id with the corrected shared JSONB
                    if any_po_update or any_gr_update or any_inv_update:
                        # Build dynamic update query based on what changed
                        update_fields = []
                        update_values = []
                        
                        if any_po_update:
                            update_fields.append("purchase_order = %s::jsonb")
                            update_values.append(json.dumps(shared_purchase_order))
                        if any_gr_update:
                            update_fields.append("goods_receipt = %s::jsonb")
                            update_values.append(json.dumps(shared_goods_receipt))
                        if any_inv_update:
                            update_fields.append("invoice = %s::jsonb")
                            update_values.append(json.dumps(shared_invoice))
                        
                        update_fields.append("updated_at = %s")
                        update_values.append(updated_at)
                        update_values.append(session_id)
                        
                        update_query = f"""
                            UPDATE erp.session_table
                            SET {', '.join(update_fields)}
                            WHERE session_id = %s
                        """
                        
                        cursor.execute(update_query, update_values)
                        
                        update_desc = []
                        if any_po_update:
                            update_desc.append("PO")
                        if any_gr_update:
                            update_desc.append("GR")
                        if any_inv_update:
                            update_desc.append("Invoice")
                        
                        print(f"Updated {cursor.rowcount} session records with corrected {' and '.join(update_desc)} data")
                    
                    # Mark exceptions as Fixed (both in session_table and exception_table)
                    for exc_id in exceptions_to_mark_fixed:
                        cursor.execute("""
                            UPDATE erp.session_table
                            SET exception_status = %s, updated_at = %s
                            WHERE unique_exception_id = %s AND session_id = %s
                        """, ('Fixed', updated_at, exc_id, session_id))
                        
                        cursor.execute("""
                            UPDATE erp.exception_table
                            SET exception_status = %s, updated_at = %s
                            WHERE unique_exception_id = %s AND session_id = %s
                        """, ('Fixed', updated_at, exc_id, session_id))
                    
                    conn.commit()

                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "status": "success",
                        "message": f"Bundle status updated to {new_status}",
                        "bundle_id": bundle_id,
                        "session_id": session_id,
                        "new_status": new_status,
                        "updated_at": updated_at
                    })
                }
            except Exception as e:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Database error: {str(e)}"
                    })
                }

        elif event_type == "list_users":
            # List all users from database with pagination
            # Extract userid from request body to verify admin access
            requesting_userid = body.get("userid")
            
            # Convert to int if it's a string, handle None case
            try:
                requesting_userid_int = int(requesting_userid) if requesting_userid is not None else None
            except (ValueError, TypeError):
                requesting_userid_int = None
            
            if not requesting_userid_int or requesting_userid_int != 1:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 403,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Only admin users can access user management"
                    })
                }
            
            page = body.get('page', 1)
            page_size = body.get('page_size', 10)
            offset = (page - 1) * page_size
            
            try:
                # Get total count
                cursor.execute("SELECT COUNT(*) FROM erp.users;")
                total_result = cursor.fetchone()
                total_count = total_result[0] if total_result else 0
                
                # Get paginated users - using actual schema: userid, email_id, created_date, is_active
                query = """
                    SELECT 
                        userid,
                        email_id,
                        created_date,
                        is_active
                    FROM erp.users
                    ORDER BY created_date DESC NULLS LAST
                    LIMIT %s OFFSET %s;
                """
                cursor.execute(query, (page_size, offset))
                rows = cursor.fetchall()
                
                users = []
                for row in rows:
                    users.append({
                        "userid": row[0],
                        "email": row[1],
                        "createdAt": format_datetime_to_ist(row[2]) if len(row) > 2 and row[2] else None,
                        "isActive": row[3] if len(row) > 3 and row[3] is not None else True
                    })
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "users": users,
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total_count": total_count,
                            "total_pages": (total_count + page_size - 1) // page_size
                        }
                    })
                }
            except Exception as e:
                logger.error(f"Error listing users: {str(e)}")
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to list users: {str(e)}"
                    })
                }
        
        elif event_type == "create_user":
            # Create new user in Cognito and database
            # Extract userid from request body to verify admin access
            requesting_userid = body.get("userid")
            
            # Convert to int if it's a string, handle None case
            try:
                requesting_userid_int = int(requesting_userid) if requesting_userid is not None else None
            except (ValueError, TypeError):
                requesting_userid_int = None
            
            if not requesting_userid_int or requesting_userid_int != 1:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 403,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Only admin users can create users"
                    })
                }
            
            email = body.get("email")
            password = body.get("password")
            
            if not email or not password:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Email and password are required"
                    })
                }
            
            try:
                cognito_client = boto3.client(
                    'cognito-idp',
                    region_name='us-west-2'
                )
                
                # Create user in Cognito
                try:
                    cognito_response = cognito_client.admin_create_user(
                        UserPoolId=COGNITO_USER_POOL_ID,
                        Username=email,
                        UserAttributes=[
                            {'Name': 'email', 'Value': email},
                            {'Name': 'email_verified', 'Value': 'true'}
                        ],
                        MessageAction='SUPPRESS',  # Don't send welcome email
                        TemporaryPassword=password,
                        ForceAliasCreation=False
                    )
                    
                    # Set permanent password
                    cognito_client.admin_set_user_password(
                        UserPoolId=COGNITO_USER_POOL_ID,
                        Username=email,
                        Password=password,
                        Permanent=True
                    )
                    
                except cognito_client.exceptions.UsernameExistsException:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 409,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": "User with this email already exists"
                        })
                    }
                
                # Insert user into database - using actual schema: email_id, created_date, is_active
                now = datetime.now(timezone.utc)
                cursor.execute("""
                    INSERT INTO erp.users (email_id, created_date, is_active)
                    VALUES (%s, %s, %s)
                    RETURNING userid;
                """, (email, now, True))
                
                result = cursor.fetchone()
                new_userid = result[0] if result else None
                
                conn.commit()
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "message": "User created successfully",
                        "userid": new_userid,
                        "email": email
                    })
                }
                
            except Exception as e:
                logger.error(f"Error creating user: {str(e)}")
                conn.rollback()
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to create user: {str(e)}"
                    })
                }
        
        elif event_type == "toggle_user_status":
            # Enable or disable user in Cognito and database
            # Extract userid from request body to verify admin access
            requesting_userid = body.get("userid")
            
            # Convert to int if it's a string, handle None case
            try:
                requesting_userid_int = int(requesting_userid) if requesting_userid is not None else None
            except (ValueError, TypeError):
                requesting_userid_int = None
            
            if not requesting_userid_int or requesting_userid_int != 1:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 403,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Only admin users can modify user status"
                    })
                }
            
            target_email = body.get("email")
            is_active = body.get("is_active", True)
            
            if not target_email:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Email is required"
                    })
                }
            
            try:
                cognito_client = boto3.client(
                    'cognito-idp',
                    region_name='us-west-2'
                )
                
                # Update Cognito user status
                if is_active:
                    cognito_client.admin_enable_user(
                        UserPoolId=COGNITO_USER_POOL_ID,
                        Username=target_email
                    )
                else:
                    cognito_client.admin_disable_user(
                        UserPoolId=COGNITO_USER_POOL_ID,
                        Username=target_email
                    )
                
                # Update database - using actual schema (no updated_at column)
                cursor.execute("""
                    UPDATE erp.users
                    SET is_active = %s
                    WHERE email_id = %s
                    RETURNING userid;
                """, (is_active, target_email))
                
                result = cursor.fetchone()
                if not result:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 404,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": "User not found in database"
                        })
                    }
                
                conn.commit()
                
                cursor.close()
                conn.close()
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "message": f"User {'enabled' if is_active else 'disabled'} successfully",
                        "email": target_email,
                        "is_active": is_active
                    })
                }
                
            except Exception as e:
                logger.error(f"Error toggling user status: {str(e)}")
                conn.rollback()
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to update user status: {str(e)}"
                    })
                }

        elif event_type == "delete_user":
            # Delete user from Cognito and database (permanent deletion)
            # Extract userid from request body to verify admin access
            requesting_userid = body.get("userid")
            
            # Convert to int if it's a string, handle None case
            try:
                requesting_userid_int = int(requesting_userid) if requesting_userid is not None else None
            except (ValueError, TypeError):
                requesting_userid_int = None
            
            if not requesting_userid_int or requesting_userid_int != 1:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 403,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Only admin users can delete users"
                    })
                }
            
            target_email = body.get("email")
            
            if not target_email:
                cursor.close()
                conn.close()
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": "Email is required"
                    })
                }
            
            try:
                # First, get the userid from the database to delete related records
                cursor.execute("""
                    SELECT userid FROM erp.users WHERE email_id = %s
                """, (target_email,))
                
                user_result = cursor.fetchone()
                if not user_result:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 404,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": "User not found in database"
                        })
                    }
                
                target_userid = user_result[0]
                
                # Prevent admin from deleting themselves
                if target_userid == 1:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 403,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": "Cannot delete the admin user"
                        })
                    }
                
                # Delete user's data from related tables first (exception_table, session_table, chat_history)
                # This prevents foreign key constraint violations
                
                # Delete from chat_history
                cursor.execute("""
                    DELETE FROM erp.chat_history WHERE userid = %s
                """, (target_userid,))
                chat_deleted = cursor.rowcount
                
                # Delete from exception_table
                cursor.execute("""
                    DELETE FROM erp.exception_table WHERE userid = %s
                """, (target_userid,))
                exceptions_deleted = cursor.rowcount
                
                # Delete from session_table
                cursor.execute("""
                    DELETE FROM erp.session_table WHERE userid = %s
                """, (target_userid,))
                sessions_deleted = cursor.rowcount
                
                # Delete user from database
                cursor.execute("""
                    DELETE FROM erp.users WHERE userid = %s
                """, (target_userid,))
                
                # Commit database changes
                conn.commit()
                
                # Delete user from Cognito (after successful database deletion)
                cognito_client = boto3.client(
                    'cognito-idp',
                    region_name='us-west-2'
                )
                
                try:
                    cognito_client.admin_delete_user(
                        UserPoolId=COGNITO_USER_POOL_ID,
                        Username=target_email
                    )
                    logger.info(f"Successfully deleted user from Cognito: {target_email}")
                except cognito_client.exceptions.UserNotFoundException:
                    # User not found in Cognito, but database deletion succeeded
                    logger.warning(f"User not found in Cognito: {target_email}")
                except Exception as cognito_error:
                    # Log Cognito error but don't rollback database changes
                    # Database deletion succeeded, Cognito deletion failed
                    logger.error(f"Error deleting user from Cognito: {str(cognito_error)}")
                
                cursor.close()
                conn.close()
                
                logger.info(f"Successfully deleted user: {target_email} (userid: {target_userid})")
                logger.info(f"Deleted data: {sessions_deleted} sessions, {exceptions_deleted} exceptions, {chat_deleted} chat messages")
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "message": "User deleted successfully",
                        "email": target_email,
                        "deleted_records": {
                            "sessions": sessions_deleted,
                            "exceptions": exceptions_deleted,
                            "chat_messages": chat_deleted
                        }
                    })
                }
                
            except Exception as e:
                logger.error(f"Error deleting user: {str(e)}")
                conn.rollback()
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to delete user: {str(e)}"
                    })
                }

        elif event_type == "reset_user_data":
            """
            Reset User Data - Deletes all records from session_table and exception_table 
            for the specified user EXCEPT those with session_id in the protected list
            (S001-S010)
            """
            try:
                userid = body.get("userid")
                
                if not userid:
                    cursor.close()
                    conn.close()
                    return {
                        "statusCode": 400,
                        "headers": cors_headers,
                        "body": json.dumps({
                            "error": "userid is required"
                        })
                    }
                
                # Protected session IDs that should NOT be deleted
                protected_sessions = ['S001', 'S002', 'S003', 'S004', 'S005', 
                                     'S006', 'S007', 'S008', 'S009', 'S010']
                
                # Delete from exception_table first (to avoid foreign key issues if any)
                exception_delete_query = """
                    DELETE FROM erp.exception_table 
                    WHERE userid = %s 
                    AND session_id NOT IN %s
                """
                cursor.execute(exception_delete_query, (userid, tuple(protected_sessions)))
                exceptions_deleted = cursor.rowcount
                
                # Delete from session_table
                session_delete_query = """
                    DELETE FROM erp.session_table 
                    WHERE userid = %s 
                    AND session_id NOT IN %s
                """
                cursor.execute(session_delete_query, (userid, tuple(protected_sessions)))
                sessions_deleted = cursor.rowcount
                
                # Commit the transaction
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"Reset completed for userid {userid}: {sessions_deleted} sessions deleted, {exceptions_deleted} exceptions deleted")
                
                return {
                    "statusCode": 200,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "message": "User data reset successfully",
                        "sessions_deleted": sessions_deleted,
                        "exceptions_deleted": exceptions_deleted
                    })
                }
                
            except Exception as e:
                logger.error(f"Error resetting user data: {str(e)}")
                conn.rollback()
                cursor.close()
                conn.close()
                return {
                    "statusCode": 500,
                    "headers": cors_headers,
                    "body": json.dumps({
                        "error": f"Failed to reset user data: {str(e)}"
                    })
                }

    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        
        return {
            "statusCode": 500,
            "headers": cors_headers,
            "body": json.dumps({
                "error": str(e)
            })
        }
