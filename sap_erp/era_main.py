"""
ERA Real Estate Property Search Agent - Backend Service

This service provides a Strands agent backend for property search functionality
with WebSocket streaming support for real-time responses.

Features:
- Property search tool with intelligent filtering
- FastAPI REST endpoints
- WebSocket streaming for real-time responses
- Error handling and logging
"""

import json
import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from dotenv import load_dotenv

from strands import Agent, tool
from strands.models import BedrockModel

# Load environment variables
load_dotenv()

# ------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------

# Create logs directory if it doesn't exist
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Create log file path with timestamp
LOG_FILE = os.path.join(LOG_DIR, f"era_main_{datetime.now().strftime('%Y%m%d')}.log")

# Configure root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Remove existing handlers to avoid duplicates
logger.handlers.clear()

# Create formatters
detailed_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# File handler with rotation (max 10MB per file, keep 5 backup files)
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(detailed_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Log initialization
logger.info("=" * 80)
logger.info("ERA Real Estate Property Search Agent - Starting Application")
logger.info(f"Log file: {LOG_FILE}")
logger.info("=" * 80)

# Global variable to store final response text
final_text_ff = ""

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

# Environment variables with defaults
LOCAL_PROPERTIES_FOLDER = os.getenv("LOCAL_PROPERTIES_FOLDER", "era_docs")
LOCAL_PROPERTIES_FILE = os.path.join(LOCAL_PROPERTIES_FOLDER, "properties.json")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "sap-mcp-data-server")
S3_PROPERTIES_KEY = os.getenv("S3_PROPERTIES_KEY", "era_demo/properties.json")
WEBSOCKET_ENDPOINT = os.getenv(
    "WEBSOCKET_ENDPOINT",
    "https://0if9awq559.execute-api.us-west-2.amazonaws.com/production"
)

# In-memory cache for properties data
_properties_cache: Optional[List[Dict[str, Any]]] = None

# In-memory cache for FAQ knowledge base
_faq_kb_cache: Optional[str] = None
FAQ_KB_FILE = os.path.join(LOCAL_PROPERTIES_FOLDER, "ERA KB.txt")


# ------------------------------------------------------------
# WebSocket and Callback Handler Functions
# ------------------------------------------------------------

def send_ws_message(connection_id: str, data: Dict[str, Any]) -> None:
    """
    Send WebSocket message to the connected client.
    
    Args:
        connection_id: WebSocket connection ID
        data: Message data (dict or string)
    """
    logger.debug(f"send_ws_message() called - Connection ID: {connection_id}, Data type: {type(data)}")
    
    try:
        logger.debug("Creating boto3 API Gateway Management API client")
        client = boto3.client(
            'apigatewaymanagementapi',
            endpoint_url="https://0if9awq559.execute-api.us-west-2.amazonaws.com/production"
        )
        logger.debug("API Gateway client created successfully")

        if isinstance(data, dict):
            logger.debug("Converting dict data to JSON string")
            data = json.dumps(data)
            logger.debug(f"JSON data length: {len(data)} characters")

        logger.info(f"Sending WebSocket message to connection {connection_id}")
        logger.debug(f"Message content preview: {str(data)[:200]}...")
        
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=data
        )
        
        logger.info(f"Successfully sent WebSocket message to connection {connection_id}")
        
    except client.exceptions.GoneException:
        logger.warning(f"Connection {connection_id} is gone (client disconnected)")
    except Exception as e:
        logger.error(f"Error sending WebSocket message to {connection_id}: {str(e)}", exc_info=True)


def make_callback_handler(connection_id: str, message_id: str):
    """
    Create a callback handler for streaming agent responses via WebSocket.
   
    Args:
        connection_id: WebSocket connection ID
        message_id: Message ID for tracking
       
    Returns:
        Callback handler function
    """
    logger.debug(f"make_callback_handler() called - Connection ID: {connection_id}, Message ID: {message_id}")
    
    global final_text_ff

    state = {"buffer": []}
    logger.debug("Initialized callback handler state buffer")
   
    def custom_callback_handler(**kwargs):
        """
        Custom callback handler to process streaming data from the agent.
        Streams intermediate tokens and final response via WebSocket.
        """
        logger.debug(f"custom_callback_handler() invoked with keys: {list(kwargs.keys())}")
        
        # Handle streaming data (intermediate tokens)
        if "data" in kwargs:
            token = kwargs["data"]
            logger.debug(f"Received streaming token (length: {len(token) if token else 0})")
            state["buffer"].append(token)
            logger.debug(f"Buffer now contains {len(state['buffer'])} tokens")
            
            logger.info(f"Sending streaming token to connection {connection_id}")
            send_ws_message(connection_id, {
                "type": "thinking",
                "message": token,
                "message_id": message_id
            })
       
        # Handle final assistant message
        if "message" in kwargs and kwargs["message"].get("role") == "assistant":
            logger.info("Received final assistant message")
            final_text = kwargs["message"].get("content", "")
            logger.debug(f"Final text length: {len(final_text)} characters")
            
            final_text_ff = final_text
            logger.debug("Stored final text in global variable")
            
            logger.info(f"Sending final answer to connection {connection_id}")
            send_ws_message(connection_id, {
                "type": "answer",
                "message": final_text,
                "message_id": message_id
            })
            
            state["buffer"].clear()
            logger.debug("Cleared callback handler buffer")
            logger.info("Callback handler processing completed")
   
    logger.debug("Callback handler created successfully")
    return custom_callback_handler


# ------------------------------------------------------------
# S3 Data Fetching Tool
# ------------------------------------------------------------

@tool
def fetch_properties_from_s3(
    bucket_name: str = S3_BUCKET_NAME,
    object_key: str = S3_PROPERTIES_KEY,
    local_folder: str = LOCAL_PROPERTIES_FOLDER,
    local_filename: str = "properties.json"
) -> Dict[str, Any]:
    """
    Fetch properties.json from S3 and store it locally in era_docs folder.
    
    This tool checks if the file exists locally first. If not found, it fetches
    from S3 and saves it to the local folder for future use.
    
    Args:
        bucket_name: S3 bucket name (default: from env or "sap-mcp-data-server")
        object_key: S3 object key/path (default: "properties.json")
        local_folder: Local folder to store the file (default: "era_docs")
        local_filename: Local filename to save as (default: "properties.json")
    
    Returns:
        Dictionary with status, message, and file path information
    
    Raises:
        RuntimeError: If S3 fetch fails
        ValueError: If downloaded JSON is invalid
    """
    logger.info(f"fetch_properties_from_s3() called - Bucket: {bucket_name}, Key: {object_key}")
    logger.debug(f"Parameters - local_folder: {local_folder}, local_filename: {local_filename}")
    
    # Ensure local folder exists
    logger.debug(f"Ensuring local folder exists: {local_folder}")
    os.makedirs(local_folder, exist_ok=True)
    logger.debug(f"Local folder ready: {local_folder}")
    
    local_file_path = os.path.join(local_folder, local_filename)
    logger.debug(f"Local file path: {local_file_path}")
    
    # Check if file already exists locally
    if os.path.exists(local_file_path):
        logger.info(f"Properties file already exists locally at {local_file_path}")
        logger.debug("Attempting to read and validate local file")
        try:
            with open(local_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            property_count = len(data) if isinstance(data, list) else 0
            logger.info(f"Local file validated - contains {property_count} properties")
            logger.debug("Returning local file data")
            
            return {
                "status": "success",
                "message": f"Properties file already exists locally",
                "file_path": local_file_path,
                "source": "local",
                "property_count": property_count
            }
        except json.JSONDecodeError as e:
            logger.warning(f"Local file exists but is invalid JSON: {e}. Fetching from S3...")
            logger.debug("Continuing to fetch from S3 due to corrupted local file")
            # Continue to fetch from S3 if local file is corrupted
    
    # Fetch from S3
    logger.info("Initializing S3 client")
    s3_client = boto3.client("s3")
    logger.debug("S3 client initialized")
    
    try:
        logger.info(f"Fetching properties.json from S3: s3://{bucket_name}/{object_key}")
        logger.debug("Calling S3 get_object API")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        logger.debug("S3 get_object response received")
        
        text = response["Body"].read().decode("utf-8")
        logger.debug(f"Downloaded {len(text)} characters from S3")
        
        # Validate JSON before saving
        logger.debug("Validating JSON structure")
        try:
            properties_data = json.loads(text)
            logger.debug("JSON parsing successful")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in S3 object: {e}")
            raise ValueError(f"Invalid JSON in S3 object {object_key}: {e}")
        
        if not isinstance(properties_data, list):
            logger.error(f"S3 object is not a JSON array, type: {type(properties_data)}")
            raise ValueError(f"S3 object {object_key} must contain a JSON array")
        
        logger.info(f"Validated JSON array with {len(properties_data)} properties")
        
        # Save to local file
        logger.debug(f"Saving properties to local file: {local_file_path}")
        with open(local_file_path, 'w', encoding='utf-8') as f:
            json.dump(properties_data, f, indent=2, ensure_ascii=False)
        logger.debug("File saved successfully")
        
        logger.info(
            f"Successfully fetched and saved {len(properties_data)} properties "
            f"to {local_file_path}"
        )
        
        # Clear cache to force reload
        global _properties_cache
        logger.debug("Clearing properties cache")
        _properties_cache = None
        logger.debug("Cache cleared")
        
        result = {
            "status": "success",
            "message": f"Properties fetched from S3 and saved locally",
            "file_path": local_file_path,
            "source": "s3",
            "property_count": len(properties_data),
            "s3_bucket": bucket_name,
            "s3_key": object_key
        }
        logger.info("fetch_properties_from_s3() completed successfully")
        return result
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logger.error(f"S3 ClientError - Code: {error_code}, Message: {error_msg}", exc_info=True)
        raise RuntimeError(
            f"Failed to fetch {object_key} from S3 bucket {bucket_name}: "
            f"[{error_code}] {error_msg}"
        )
    except Exception as e:
        logger.error(f"Unexpected error in fetch_properties_from_s3: {str(e)}", exc_info=True)
        raise RuntimeError(f"Unexpected error fetching from S3: {str(e)}")


def check_local_properties_file(file_path: str = LOCAL_PROPERTIES_FILE) -> bool:
    """
    Check if local properties file exists and is valid.
    
    Args:
        file_path: Path to the local properties file
    
    Returns:
        True if file exists and is valid JSON, False otherwise
    """
    logger.debug(f"check_local_properties_file() called - File path: {file_path}")
    
    if not os.path.exists(file_path):
        logger.debug(f"File does not exist: {file_path}")
        return False
    
    logger.debug(f"File exists, validating JSON structure")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        is_valid = isinstance(data, list)
        logger.debug(f"File validation result: {is_valid} (is list: {isinstance(data, list)})")
        return is_valid
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"File validation failed: {type(e).__name__} - {str(e)}")
        return False


def load_properties_data(
    local_file_path: Optional[str] = None,
    force_reload: bool = False
) -> List[Dict[str, Any]]:
    """
    Load properties JSON data from local file and cache in memory.
    
    This function checks for the file locally first. If not found, it should
    be called after fetch_properties_from_s3() to ensure the file exists.
    
    Args:
        local_file_path: Optional custom path to properties file
        force_reload: If True, reload from file even if cached
    
    Returns:
        List of property dictionaries
        
    Raises:
        FileNotFoundError: If properties file doesn't exist
        ValueError: If JSON is invalid
    """
    logger.debug(f"load_properties_data() called - force_reload: {force_reload}")
    
    global _properties_cache
    
    # Return cached data if available and not forcing reload
    if _properties_cache is not None and not force_reload:
        logger.info(f"Returning cached properties data ({len(_properties_cache)} properties)")
        return _properties_cache
    
    logger.debug("Cache miss or force_reload=True, loading from file")
    
    # Determine file path
    if local_file_path is None:
        local_file_path = LOCAL_PROPERTIES_FILE
    logger.debug(f"Using file path: {local_file_path}")
    
    # Check if file exists
    if not os.path.exists(local_file_path):
        logger.error(f"Properties file not found at {local_file_path}")
        raise FileNotFoundError(
            f"Properties file not found at {local_file_path}. "
            f"Please use fetch_properties_from_s3() tool first to download from S3."
        )
    
    logger.debug(f"File exists, reading and parsing JSON")
    try:
        with open(local_file_path, 'r', encoding='utf-8') as f:
            _properties_cache = json.load(f)
        logger.debug("JSON loaded successfully")
        
        if not isinstance(_properties_cache, list):
            logger.error(f"Properties file is not a JSON array, type: {type(_properties_cache)}")
            raise ValueError("Properties file must contain a JSON array")
        
        logger.info(f"Loaded {len(_properties_cache)} properties from {local_file_path}")
        logger.debug("Properties data cached in memory")
        return _properties_cache
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in properties file: {e}", exc_info=True)
        raise ValueError(f"Invalid JSON in properties file: {e}")
    except IOError as e:
        logger.error(f"Error reading properties file: {e}", exc_info=True)
        raise FileNotFoundError(f"Error reading properties file: {e}")


# ------------------------------------------------------------
# Property Search Tool
# ------------------------------------------------------------


@tool
def search_properties(
    query: str,
    max_results: int = 10,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Search properties based on natural language query and filters.
    
    This tool performs intelligent filtering on the property dataset based on
    location, price, property type, TOP year, tenure, amenities, and project name.
    
    Args:
        query: Natural language query describing what properties to find
        max_results: Maximum number of results to return (default: 10)
        filters: Optional dictionary of filter parameters:
            - district: int (1-28)
            - region: str ("CCR", "RCR", "OCR")
            - estate: str (e.g., "Punggol", "Eunos")
            - property_type: str ("Non-Landed", "HDB", "Landed")
            - property_subtype: str ("Condo", "Apartment", "EC")
            - min_price: float
            - max_price: float
            - top_after: int (TOP year >= this value)
            - top_before: int (TOP year <= this value)
            - tenure_type: str ("Freehold", "99-year")
            - max_mrt_distance_km: float
            - mrt_station: str (specific MRT station name)
            - project_name: str (exact or partial match)
            - developer: str (developer name)
    
    Returns:
        Dictionary with status, count, properties list, and filters applied
    """
    logger.info(f"search_properties() called - Query: '{query}', Max results: {max_results}")
    logger.debug(f"Filters provided: {filters}")
    
    try:
        # Ensure properties data is available
        # First check if local file exists, if not, fetch from S3
        logger.debug("Checking if local properties file exists")
        if not check_local_properties_file():
            logger.info("Local properties file not found. Fetching from S3...")
            fetch_properties_from_s3()
        else:
            logger.debug("Local properties file exists")
        
        # Load properties data from local file
        logger.debug("Loading properties data")
        properties = load_properties_data()
        logger.info(f"Loaded {len(properties)} total properties for search")
        
        # Initialize filters if not provided
        if filters is None:
            filters = {}
            logger.debug("No filters provided, using empty filter dict")
        else:
            logger.debug(f"Applying {len(filters)} filter(s): {list(filters.keys())}")
        
        # Apply filters
        logger.debug("Starting property filtering")
        filtered_properties = properties.copy()
        initial_count = len(filtered_properties)
        logger.debug(f"Starting with {initial_count} properties")
        
        # Location filters
        if "district" in filters:
            district = filters["district"]
            logger.debug(f"Applying district filter: {district}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("district") == district
            ]
            logger.debug(f"District filter: {before_count} -> {len(filtered_properties)} properties")
        
        if "region" in filters:
            region = filters["region"].upper()
            logger.debug(f"Applying region filter: {region}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("region", "").upper() == region
            ]
            logger.debug(f"Region filter: {before_count} -> {len(filtered_properties)} properties")
        
        if "estate" in filters:
            estate = filters["estate"].lower()
            logger.debug(f"Applying estate filter: {estate}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if estate in p.get("estate", "").lower()
            ]
            logger.debug(f"Estate filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Property type filters
        if "property_type" in filters:
            prop_type = filters["property_type"]
            logger.debug(f"Applying property_type filter: {prop_type}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("property_type") == prop_type
            ]
            logger.debug(f"Property type filter: {before_count} -> {len(filtered_properties)} properties")
        
        if "property_subtype" in filters:
            subtype = filters["property_subtype"]
            logger.debug(f"Applying property_subtype filter: {subtype}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("property_subtype") == subtype
            ]
            logger.debug(f"Property subtype filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Price filters
        if "min_price" in filters:
            min_price = filters["min_price"]
            logger.debug(f"Applying min_price filter: {min_price}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("price_max", 0) >= min_price
            ]
            logger.debug(f"Min price filter: {before_count} -> {len(filtered_properties)} properties")
        
        if "max_price" in filters:
            max_price = filters["max_price"]
            logger.debug(f"Applying max_price filter: {max_price}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("price_min", float('inf')) <= max_price
            ]
            logger.debug(f"Max price filter: {before_count} -> {len(filtered_properties)} properties")
        
        # TOP year filters
        if "top_after" in filters:
            top_after = filters["top_after"]
            logger.debug(f"Applying top_after filter: {top_after}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("top_year", 0) >= top_after
            ]
            logger.debug(f"TOP after filter: {before_count} -> {len(filtered_properties)} properties")
        
        if "top_before" in filters:
            top_before = filters["top_before"]
            logger.debug(f"Applying top_before filter: {top_before}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("top_year", float('inf')) <= top_before
            ]
            logger.debug(f"TOP before filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Tenure filter
        if "tenure_type" in filters:
            tenure = filters["tenure_type"]
            logger.debug(f"Applying tenure_type filter: {tenure}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if p.get("tenure_type") == tenure
            ]
            logger.debug(f"Tenure filter: {before_count} -> {len(filtered_properties)} properties")
        
        # MRT distance filter
        if "max_mrt_distance_km" in filters:
            max_distance = filters["max_mrt_distance_km"]
            logger.debug(f"Applying max_mrt_distance_km filter: {max_distance}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if any(
                    mrt.get("distance_km", float('inf')) <= max_distance
                    for mrt in p.get("amenities_mrt", [])
                )
            ]
            logger.debug(f"MRT distance filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Specific MRT station filter
        if "mrt_station" in filters:
            station_name = filters["mrt_station"].lower()
            logger.debug(f"Applying mrt_station filter: {station_name}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if any(
                    station_name in mrt.get("station", "").lower()
                    for mrt in p.get("amenities_mrt", [])
                )
            ]
            logger.debug(f"MRT station filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Project name filter (case-insensitive partial match)
        if "project_name" in filters:
            project_name = filters["project_name"].lower()
            logger.debug(f"Applying project_name filter: {project_name}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if project_name in p.get("project_name", "").lower()
            ]
            logger.debug(f"Project name filter: {before_count} -> {len(filtered_properties)} properties")
        
        # Developer filter
        if "developer" in filters:
            developer = filters["developer"].lower()
            logger.debug(f"Applying developer filter: {developer}")
            before_count = len(filtered_properties)
            filtered_properties = [
                p for p in filtered_properties
                if developer in p.get("developer", "").lower()
            ]
            logger.debug(f"Developer filter: {before_count} -> {len(filtered_properties)} properties")
        
        logger.info(f"After filtering: {len(filtered_properties)} properties match criteria")
        
        # Sort by price (ascending)
        logger.debug("Sorting properties by price (ascending)")
        filtered_properties.sort(key=lambda p: p.get("price_min", 0))
        
        # Limit results
        result_count = len(filtered_properties)
        logger.debug(f"Limiting results from {result_count} to max {max_results}")
        filtered_properties = filtered_properties[:max_results]
        logger.debug(f"Final result count: {len(filtered_properties)}")
        
        # Build response
        if result_count == 0:
            logger.warning(f"No properties found matching criteria. Query: '{query}', Filters: {filters}")
            return {
                "status": "error",
                "message": "No properties found matching criteria",
                "suggestion": "Try relaxing price or location filters",
                "count": 0,
                "properties": [],
                "filters_applied": filters
            }
        
        # Generate query summary
        query_summary = f"Found {result_count} propert{'y' if result_count == 1 else 'ies'}"
        if filters:
            filter_parts = []
            if "district" in filters:
                filter_parts.append(f"D{filters['district']}")
            if "property_subtype" in filters:
                filter_parts.append(filters["property_subtype"].lower())
            if "max_price" in filters:
                filter_parts.append(f"under ${filters['max_price']:,.0f}")
            if filter_parts:
                query_summary += f" ({', '.join(filter_parts)})"
        
        response = {
            "status": "success" if result_count >= max_results else "partial",
            "count": result_count,
            "query_summary": query_summary,
            "properties": filtered_properties,
            "filters_applied": filters
        }
        
        if result_count < max_results and result_count > 0:
            response["message"] = (
                f"Found {result_count} properties (requested {max_results}). "
                "Consider relaxing filters."
            )
        
        logger.info(f"search_properties() completed - Status: {response['status']}, Count: {result_count}")
        logger.debug(f"Returning {len(filtered_properties)} properties in response")
        return response
        
    except FileNotFoundError as e:
        logger.error(f"Properties file not found: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "suggestion": "Please ensure properties.json file exists in the data directory"
        }
    except ValueError as e:
        logger.error(f"Invalid data format: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Data format error: {e}",
            "suggestion": "Please check the properties.json file format"
        }
    except Exception as e:
        logger.error(f"Error searching properties: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "suggestion": "Please try again or contact support"
        }


# ------------------------------------------------------------
# FAQ Knowledge Base Tool
# ------------------------------------------------------------

def load_faq_knowledge_base(
    kb_file_path: Optional[str] = None,
    force_reload: bool = False
) -> str:
    """
    Load FAQ knowledge base from ERA KB.txt file.
    
    Args:
        kb_file_path: Optional custom path to KB file
        force_reload: If True, reload from file even if cached
    
    Returns:
        Full text content of the knowledge base
        
    Raises:
        FileNotFoundError: If KB file doesn't exist
        IOError: If file cannot be read
    """
    logger.debug(f"load_faq_knowledge_base() called - force_reload: {force_reload}")
    
    global _faq_kb_cache
    
    # Return cached data if available and not forcing reload
    if _faq_kb_cache is not None and not force_reload:
        logger.info(f"Returning cached FAQ knowledge base ({len(_faq_kb_cache)} characters)")
        return _faq_kb_cache
    
    logger.debug("Cache miss or force_reload=True, loading from file")
    
    # Determine file path
    if kb_file_path is None:
        kb_file_path = FAQ_KB_FILE
    logger.debug(f"Using KB file path: {kb_file_path}")
    
    # Check if file exists
    if not os.path.exists(kb_file_path):
        logger.error(f"FAQ knowledge base file not found at {kb_file_path}")
        raise FileNotFoundError(
            f"FAQ knowledge base file not found at {kb_file_path}. "
            f"Please ensure ERA KB.txt exists in the era_docs folder."
        )
    
    logger.debug(f"File exists, reading content")
    try:
        with open(kb_file_path, 'r', encoding='utf-8') as f:
            _faq_kb_cache = f.read()
        
        logger.info(f"Loaded FAQ knowledge base from {kb_file_path} ({len(_faq_kb_cache)} characters)")
        logger.debug("FAQ knowledge base cached in memory")
        return _faq_kb_cache
        
    except IOError as e:
        logger.error(f"Error reading FAQ knowledge base file: {e}", exc_info=True)
        raise IOError(f"Error reading FAQ knowledge base file: {e}")


def parse_faq_sections(kb_text: str) -> List[Dict[str, Any]]:
    """
    Parse FAQ knowledge base text into structured Q&A sections.
    
    Args:
        kb_text: Full text content of the knowledge base
    
    Returns:
        List of dictionaries with category, question, and answer
    """
    sections = []
    current_category = None
    current_question = None
    current_answer = []
    
    lines = kb_text.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Detect category headers
        if line.startswith('CATEGORY'):
            # Save previous section if exists
            if current_question and current_answer:
                sections.append({
                    'category': current_category,
                    'question': current_question,
                    'answer': '\n'.join(current_answer).strip()
                })
            
            current_category = line
            current_question = None
            current_answer = []
            continue
        
        # Detect numbered questions
        if line and (line[0].isdigit() or line.startswith('Answer:')):
            # Save previous section if exists
            if current_question and current_answer:
                sections.append({
                    'category': current_category,
                    'question': current_question,
                    'answer': '\n'.join(current_answer).strip()
                })
            
            if line.startswith('Answer:'):
                current_answer = []
            else:
                current_question = line
                current_answer = []
            continue
        
        # Collect answer text
        if current_question and line:
            current_answer.append(line)
    
    # Save last section
    if current_question and current_answer:
        sections.append({
            'category': current_category,
            'question': current_question,
            'answer': '\n'.join(current_answer).strip()
        })
    
    return sections


def find_relevant_faq_answer(query: str, kb_text: str) -> Dict[str, Any]:
    """
    Find the most relevant FAQ answer for a given query.
    
    Args:
        query: User's question
        kb_text: Full text content of the knowledge base
    
    Returns:
        Dictionary with matched question, answer, category, and relevance score
    """
    # Parse FAQ sections
    sections = parse_faq_sections(kb_text)
    
    if not sections:
        return {
            "status": "error",
            "message": "No FAQ sections found in knowledge base"
        }
    
    # Normalize query for matching
    query_lower = query.lower()
    query_words = set(query_lower.split())
    
    best_match = None
    best_score = 0
    
    # Score each section based on keyword matches
    for section in sections:
        question = section.get('question', '').lower()
        answer = section.get('answer', '').lower()
        category = section.get('category', '').lower()
        
        # Combine question, answer, and category for matching
        combined_text = f"{question} {answer} {category}"
        combined_words = set(combined_text.split())
        
        # Calculate relevance score
        # Count matching words
        matching_words = query_words.intersection(combined_words)
        score = len(matching_words)
        
        # Boost score if question contains query words
        question_words = set(question.split())
        question_matches = query_words.intersection(question_words)
        score += len(question_matches) * 2  # Double weight for question matches
        
        # Boost score for exact phrase matches
        if query_lower in question or query_lower in answer:
            score += 5
        
        if score > best_score:
            best_score = score
            best_match = section.copy()
            best_match['relevance_score'] = score
    
    if best_match and best_score > 0:
        return {
            "status": "success",
            "query": query,
            "category": best_match.get('category', 'Unknown'),
            "question": best_match.get('question', ''),
            "answer": best_match.get('answer', ''),
            "relevance_score": best_score,
            "total_sections_searched": len(sections)
        }
    else:
        # Return top 3 most relevant sections if no strong match
        scored_sections = []
        for section in sections:
            question = section.get('question', '').lower()
            answer = section.get('answer', '').lower()
            combined_text = f"{question} {answer}".lower()
            combined_words = set(combined_text.split())
            matching_words = query_words.intersection(combined_words)
            score = len(matching_words)
            
            if score > 0:
                scored_sections.append((score, section))
        
        scored_sections.sort(reverse=True, key=lambda x: x[0])
        top_sections = scored_sections[:3]
        
        return {
            "status": "partial",
            "query": query,
            "message": "No exact match found. Here are the most relevant FAQ sections:",
            "suggestions": [
                {
                    "category": section.get('category', 'Unknown'),
                    "question": section.get('question', ''),
                    "answer": section.get('answer', '')[:200] + "..." if len(section.get('answer', '')) > 200 else section.get('answer', ''),
                    "relevance_score": score
                }
                for score, section in top_sections
            ] if top_sections else [],
            "total_sections_searched": len(sections)
        }


@tool
def search_faq_knowledge_base(question: str) -> Dict[str, Any]:
    """
    Search the ERA FAQ Knowledge Base to answer questions about:
    - CEA compliance and agent conduct
    - Transaction procedures and ERA SOP
    - Buyer eligibility and government regulations
    - Financial guidelines (BSD, ABSD, TDSR, CPF)
    - Rental and leasing policies
    - Project sales and new launch SOP
    
    This tool reads from the ERA KB.txt file in the era_docs folder and finds
    the most relevant answer based on the question asked.
    
    Args:
        question: The FAQ question to search for (e.g., "What is TDSR?", 
                  "How is BSD calculated?", "What are CEA advertising guidelines?")
    
    Returns:
        Dictionary with status, matched question, answer, category, and relevance information
    """
    logger.info(f"search_faq_knowledge_base() called - Question: '{question}'")
    
    try:
        # Load FAQ knowledge base
        logger.debug("Loading FAQ knowledge base")
        kb_text = load_faq_knowledge_base()
        logger.debug(f"Knowledge base loaded ({len(kb_text)} characters)")
        
        # Find relevant answer
        logger.debug("Searching for relevant FAQ answer")
        result = find_relevant_faq_answer(question, kb_text)
        
        logger.info(
            f"FAQ search for '{question}': "
            f"status={result.get('status')}, "
            f"score={result.get('relevance_score', 0)}"
        )
        logger.debug(f"FAQ search result: {result.get('category', 'N/A')}")
        
        return result
        
    except FileNotFoundError as e:
        logger.error(f"FAQ knowledge base file not found: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "suggestion": "Please ensure ERA KB.txt exists in the era_docs folder"
        }
    except IOError as e:
        logger.error(f"Error reading FAQ knowledge base: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Error reading FAQ knowledge base: {e}",
            "suggestion": "Please check file permissions and try again"
        }
    except Exception as e:
        logger.error(f"Error searching FAQ knowledge base: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "suggestion": "Please try again or contact support"
        }


# ------------------------------------------------------------
# Strands Agent Definition
# ------------------------------------------------------------

ERA_SYSTEM_PROMPT = """
You are an AI assistant for ERA Real Estate sales agents. Your primary role is to help 
agents quickly retrieve property information and answer questions about ERA procedures, 
CEA compliance, and real estate regulations.

## Your Capabilities

You have access to two main types of tools:

### Property Search Tools
Tools that can query a database of property listings with the following information:
- Project name, developer, district (1-28), region (CCR/RCR/OCR), estate
- Property type (Non-Landed/HDB/Landed) and subtype (Condo/Apartment/EC)
- Price range (min/max), PSF trends, TOP year, tenure (Freehold/99-year)
- Nearby amenities: MRT stations with distances, primary schools, supermarkets

### FAQ Knowledge Base Tool
Tool that answers questions about:
- CEA compliance and agent conduct
- Transaction procedures and ERA SOP
- Buyer eligibility and government regulations
- Financial guidelines (BSD, ABSD, TDSR, CPF)
- Rental and leasing policies
- Project sales and new launch SOP

## Available Tools

1. **fetch_properties_from_s3**: Fetches the latest properties.json from S3 and stores it 
   locally in the era_docs folder. This tool automatically checks if the file exists locally 
   first. Use this if you need to refresh the property data or if search_properties indicates 
   the data is missing.

2. **search_properties**: Searches the property database based on natural language queries 
   and filters. This tool uses the locally stored properties.json file from era_docs folder.

3. **search_faq_knowledge_base**: Searches the ERA FAQ Knowledge Base (ERA KB.txt) to answer 
   questions about procedures, regulations, compliance, and policies. Use this for questions 
   about how things work, what rules apply, or what procedures to follow.

## How to Use the Tools - Query Routing

**CRITICAL: You must first determine if the question is about:**
- **Property listings/search** → Use `search_properties`
- **FAQ/Procedures/Regulations** → Use `search_faq_knowledge_base`

### Property Search Queries
These are questions about finding specific properties, such as:
- "Find condos in D14"
- "Show me properties under $1.2M"
- "What's available in Punggol?"
- "Tell me about Parc Esta"

**Process:**
1. Use search_properties tool - it will automatically check for local data and fetch from S3 if needed
2. If search_properties fails due to missing data, use fetch_properties_from_s3 first
3. Identify what they're looking for (location, price, type, etc.)
4. Extract relevant filter parameters from their query
5. Call the search_properties tool with appropriate filters
6. Present the results in a clear, helpful format

### FAQ/Knowledge Base Queries
These are questions about procedures, regulations, compliance, or how things work, such as:
- "What is TDSR?"
- "How is BSD calculated?"
- "What are CEA advertising guidelines?"
- "What steps must be taken before listing a property?"
- "What is the minimum rental period?"
- "How do I process a private resale transaction?"

**Process:**
1. Use search_faq_knowledge_base tool with the question
2. Present the answer clearly, including the category if relevant
3. If the answer isn't found, suggest related topics from the suggestions provided
4. dont include the tool name in the response.

## Query Examples You Should Handle

**Location-based:**
- "Find properties in D14"
- "Show me condos in OCR"
- "Properties in Punggol"

**Price-based:**
- "Condos under $1.2M"
- "Properties between 800K and 1.5M"

**Property type:**
- "Show me ECs"
- "Find condos in D14"

**TOP year:**
- "ECs in Punggol that TOP after 2015"
- "New condos TOP after 2020"

**Amenities:**
- "Condos with MRT < 1km"
- "Properties near Punggol MRT"

**Project-specific:**
- "Tell me about Central Grove"
- "Details of The Interlace"

**Complex queries:**
- "Find condos in D14 under $1.2M with MRT < 1km"
- "Freehold condos in CCR under 2M TOP after 2020"

## Response Guidelines

1. **Be concise but informative**: Provide key details (name, location, price range, TOP year)
2. **Highlight relevant matches**: If query mentions specific criteria, emphasize how results match
3. **Format clearly**: Use bullet points or structured format for multiple properties
4. **Include context**: Mention district, region, and key features
5. **Be helpful**: If no results, suggest relaxing filters or checking spelling

## Important Notes

### Query Type Identification
- **Property Search**: Questions about finding specific properties, listings, projects, locations, prices
  - Examples: "Find condos in D14", "Show me properties under 1.2M", "What's available in Punggol?"
  - Use: `search_properties` tool
  
- **FAQ/Knowledge Base**: Questions about procedures, regulations, compliance, how things work
  - Examples: "What is TDSR?", "How is BSD calculated?", "What are CEA guidelines?", "What steps before listing?"
  - Use: `search_faq_knowledge_base` tool

### Property Search Guidelines
- The search_properties tool automatically handles data loading - it checks for local files 
  and fetches from S3 if needed
- If you encounter data loading errors, you can explicitly call fetch_properties_from_s3 
  to refresh the data
- Extract filters intelligently from natural language
- If a query mentions a project name, use the project_name filter
- Price queries should convert to numbers (e.g., "1.2M" = 1200000)
- District queries can be "D14" or "district 14" or just "14"
- MRT distance queries like "< 1km" should use max_mrt_distance_km filter

### FAQ Search Guidelines
- Use search_faq_knowledge_base for any question about procedures, regulations, or policies
- The tool searches across all categories: CEA compliance, transaction procedures, buyer eligibility, 
  financial guidelines, rental policies, and project sales SOP
- If an exact match isn't found, the tool provides suggestions - use those to help the agent
- Present answers clearly with the category context when relevant

Remember: Your goal is to help sales agents quickly find both property information and procedural 
guidance to assist their clients effectively. Always identify the query type first!

IMPORTANT: dont give "based on the knowledge base" in the response.
"""


def create_agent() -> Agent:
    """
    Create and configure the ERA Property Intelligence Assistant agent.
    
    Returns:
        Configured Strands Agent instance
    """
    logger.info("create_agent() called - Initializing ERA Property Intelligence Assistant")
    
    # Initialize Bedrock model
    logger.debug("Initializing Bedrock model")
    model = BedrockModel(
        model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    )
    logger.debug("Bedrock model initialized")
    
    # Create agent with tools
    logger.debug("Creating agent with tools and system prompt")
    agent = Agent(
        model=model,
        tools=[fetch_properties_from_s3, search_properties, search_faq_knowledge_base],
        system_prompt=ERA_SYSTEM_PROMPT,
    )
    logger.info("Agent created successfully with 3 tools")
    
    return agent


# Initialize agent
logger.info("Initializing global property agent")
property_agent = create_agent()
logger.info("Global property agent initialized successfully")


# ------------------------------------------------------------
# FastAPI Application Setup
# ------------------------------------------------------------

app = FastAPI(
    title="ERA Property Search Agent",
    description="Strands agent backend for property search with WebSocket streaming",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health_check")
async def check_health():
    """
    Health check endpoint to verify service is running.
    
    Returns:
        Dictionary with status information
    """
    logger.debug("health_check endpoint called")
    properties_loaded = _properties_cache is not None
    logger.info(f"Health check - Properties loaded: {properties_loaded}")
    
    return {
        "status": "ok",
        "service": "ERA Property Search Agent",
        "properties_loaded": properties_loaded
    }


@app.post("/process")
async def process_property_query(request: Request):
    print("the request that i got",request)
    """
    Process property search requests with streaming support.
   
    Receives user question, connection_id, and message_id.
    Streams intermediate results and final response via WebSocket.
    
    Request body:
        - user_question: str - The property search query
        - connection_id: str - WebSocket connection ID
        - message_id: str - Message ID for tracking
    
    Returns:
        Dictionary with processing status
    """
    global final_text_ff
    
    logger.info("=" * 80)
    logger.info("POST /process endpoint called")
    
    try:
        logger.debug("Parsing request JSON")

        raw_body = await request.body()
        logger.info(f"RAW request body bytes: {raw_body!r}")
        print("the raw body that i got",raw_body)
        try:
            body_str = raw_body.decode('utf-8')
            print("the body string that i got",body_str)
        except Exception:
            body_str =str(raw_body)
        logger.info(f"Body string: {body_str!r}")

        event = await request.json()

        try:
            event = await request.json()
            logger.info("Parsed JSON object:\n" + json.dumps(event, indent=2))
            print(json.dumps(event, indent=2))
        except Exception:
            logger.error("Failed to parse JSON from request body")
            return {"status": "error", "message": "Invalid JSON in request body"}
        
        if isinstance(event, dict) and "body" in event:
            try:
                event = json.loads(event["body"])
                print("the event response that i got",event)
            except (TypeError, json.JSONDecodeError):
                pass  # fall back if body isn't JSON
        print("the event response that i got",event)
        logger.debug("the event response that i got",event) 
        logger.debug(f"Request JSON parsed successfully. Keys: {list(event.keys())}")
        
        user_input = event.get("user_question")
        connection_id = event.get("connection_id")
        message_id = event.get("message_id")
        
        logger.debug(f"Extracted - user_question: {'present' if user_input else 'missing'}, "
                    f"connection_id: {'present' if connection_id else 'missing'}, "
                    f"message_id: {'present' if message_id else 'missing'}")
       
        # Validate required fields
        if not user_input:
            logger.error("Validation failed: user_question is required")
            return {"status": "error", "message": "user_question is required"}
        
        if not connection_id:
            logger.error("Validation failed: connection_id is required")
            return {"status": "error", "message": "connection_id is required"}
        
        if not message_id:
            logger.error("Validation failed: message_id is required")
            return {"status": "error", "message": "message_id is required"}
       
        logger.info("Request validation passed")
        # Log request details
        logger.info(
            f"Processing request - Connection: {connection_id}, "
            f"Message: {message_id}"
        )
        logger.info(f"User question: {user_input}")

        # Call agent with streaming callback handler
        logger.debug("Creating callback handler for streaming")
        callback_handler = make_callback_handler(connection_id, message_id)
        logger.debug("Callback handler created")
        
        logger.info("Invoking property agent with user query")
        logger.debug("Agent processing started")
        response = property_agent(
            user_input,
            callback_handler=callback_handler
        )
        logger.debug("Agent processing completed")

        # Extract response text
        logger.debug("Extracting response text from agent response")
        response_text = response.message["content"][0]["text"]
        logger.info(f"Agent processing completed. Response length: {len(response_text)} characters")
        logger.debug(f"Response preview: {response_text[:200]}...")

        # Send final response as backup (in case streaming didn't capture everything)
        if response_text and response_text != final_text_ff:
            logger.debug("Sending final response as backup via WebSocket")
            send_ws_message(
                connection_id,
                {
                    "type": "answer",
                    "message": response_text,
                    "message_id": message_id
                }
            )
            logger.debug("Backup response sent")
        else:
            logger.debug("Skipping backup response (already sent or empty)")
       
        logger.info("Request processing completed successfully")
        logger.info("=" * 80)
        return {"status": "success", "message": "Processing completed"}

    except KeyError as e:
        error_msg = f"Missing required field: {e}"
        logger.error(f"KeyError in /process endpoint: {error_msg}", exc_info=True)
        return {"status": "error", "message": error_msg}
    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        logger.error(f"Exception in /process endpoint: {error_msg}", exc_info=True)
        
        # Send error message to frontend if connection_id is available
        connection_id = event.get("connection_id") if 'event' in locals() else None
        message_id = event.get("message_id") if 'event' in locals() else None
        
        if connection_id and message_id:
            logger.debug("Sending error message to frontend via WebSocket")
            send_ws_message(
                connection_id,
                {
                    "type": "error",
                    "message": error_msg,
                    "message_id": message_id
                }
            )
        else:
            logger.warning("Cannot send error to frontend - missing connection_id or message_id")
        
        logger.info("=" * 80)
        return {"status": "error", "message": error_msg}


# ------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting ERA Real Estate Property Search Agent Application")
    logger.info("=" * 80)
    
    # Ensure properties data is available on startup
    logger.info("Step 1: Loading properties data on startup")
    try:
        # Check if local file exists
        logger.debug("Checking for local properties file")
        if not check_local_properties_file():
            logger.info("Local properties file not found. Fetching from S3...")
            fetch_properties_from_s3()
        else:
            logger.debug("Local properties file found")
        
        # Load properties data
        logger.debug("Loading properties data into memory")
        load_properties_data()
        logger.info("Properties data loaded successfully on startup")
    except Exception as e:
        logger.warning(f"Could not load properties data on startup: {e}", exc_info=True)
        logger.info("Properties will be fetched and loaded on first query")
    
    # Load FAQ knowledge base on startup if available
    logger.info("Step 2: Loading FAQ knowledge base on startup")
    try:
        if os.path.exists(FAQ_KB_FILE):
            logger.debug(f"FAQ KB file found at {FAQ_KB_FILE}")
            load_faq_knowledge_base()
            logger.info("FAQ knowledge base loaded successfully on startup")
        else:
            logger.info(f"FAQ knowledge base not found at {FAQ_KB_FILE}. Will load on first query.")
    except Exception as e:
        logger.warning(f"Could not load FAQ knowledge base on startup: {e}", exc_info=True)
        logger.info("FAQ knowledge base will be loaded on first query")
    
    # Run FastAPI server
    port = int(os.getenv("PORT", 8084))
    reload = os.getenv("RELOAD", "false").lower() == "true"
    
    logger.info("=" * 80)
    logger.info(f"Step 3: Starting FastAPI server")
    logger.info(f"Host: 0.0.0.0, Port: {port}, Reload: {reload}")
    logger.info("=" * 80)
    
    uvicorn.run(
        "era_main:app",
        host="0.0.0.0",
        port=port,
        reload=reload
    )

