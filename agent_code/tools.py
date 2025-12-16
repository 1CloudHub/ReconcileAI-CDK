"""
SAP OData Tools for Strands Agent.
Defines tools using the @tool decorator that the agent can use to interact with SAP OData services.
"""

import json
import logging
from typing import Optional

from strands import tool

try:
    from .config import get_sap_config, get_odata_config, get_request_config
    from .odata_client import ODataClient, ODataClientError
except ImportError:
    from config import get_sap_config, get_odata_config, get_request_config
    from odata_client import ODataClient, ODataClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OData client (lazy loading)
_odata_client: Optional[ODataClient] = None


def get_odata_client() -> ODataClient:
    """
    Get or create OData client instance (singleton pattern).
    Uses lazy loading to avoid initialization errors when tools are imported.
    """
    global _odata_client
    if _odata_client is None:
        sap_config = get_sap_config()
        odata_config = get_odata_config()
        request_config = get_request_config()
        _odata_client = ODataClient(sap_config, odata_config, request_config)
    return _odata_client


# ============================================================================
# Query Tools
# ============================================================================

@tool(
    name="query_sap_entity_set",
    description="Query an SAP OData entity set to retrieve data with optional filtering, sorting, and pagination"
)
def query_sap_entity_set(
    service: str,
    entity: str,
    filter_expr: Optional[str] = None,
    select: Optional[str] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
    expand: Optional[str] = None
) -> str:
    """
    Query an SAP OData entity set with optional filters and pagination.
    
    Args:
        service: The OData service name (e.g., 'API_BUSINESS_PARTNER', 'API_SALES_ORDER')
        entity: The entity set name (e.g., 'A_BusinessPartner', 'A_SalesOrder')
        filter_expr: OData $filter expression (e.g., "BusinessPartnerCategory eq '1'")
        select: Comma-separated list of fields to select (e.g., "BusinessPartner,FirstName,LastName")
        top: Maximum number of records to return
        skip: Number of records to skip (for pagination)
        orderby: OData $orderby expression (e.g., "LastName asc")
        expand: Navigation properties to expand (e.g., "to_BusinessPartnerAddress")
        
    Returns:
        JSON string containing the query results or error message
    """
    try:
        client = get_odata_client()
        results = client.query_entity_set(
            service=service,
            entity=entity,
            filter_expr=filter_expr,
            select=select,
            top=top,
            skip=skip,
            orderby=orderby,
            expand=expand
        )
        
        result_count = len(results) if isinstance(results, list) else 1
        logger.info(f"Query returned {result_count} records")
        
        return json.dumps({
            "success": True,
            "count": result_count,
            "data": results
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData query error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code,
            "details": e.details
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error in query: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


@tool(
    name="get_sap_entity",
    description="Get a single SAP entity by its key value"
)
def get_sap_entity(
    service: str,
    entity: str,
    key: str,
    key_fields: Optional[str] = None
) -> str:
    """
    Get a single entity by its key from an SAP OData service.
    
    Args:
        service: The OData service name
        entity: The entity set name
        key: The entity key value (for simple keys) or JSON string for composite keys
        key_fields: Optional comma-separated field names for composite keys
        
    Returns:
        JSON string containing the entity data or error message
    """
    try:
        client = get_odata_client()
        
        # Handle composite keys
        if key_fields:
            fields = [f.strip() for f in key_fields.split(",")]
            values = [v.strip() for v in key.split(",")]
            if len(fields) != len(values):
                return json.dumps({
                    "success": False,
                    "error": "Number of key fields must match number of key values"
                }, indent=2)
            key_dict = dict(zip(fields, values))
            result = client.get_entity_by_key(service, entity, key_dict)
        else:
            result = client.get_entity_by_key(service, entity, key)
        
        return json.dumps({
            "success": True,
            "data": result
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData get entity error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error getting entity: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


# ============================================================================
# Metadata Tools
# ============================================================================

@tool(
    name="get_sap_service_metadata",
    description="Get the metadata schema definition for an SAP OData service"
)
def get_sap_service_metadata(service: str) -> str:
    """
    Get the OData service metadata (schema definition).
    Returns XML format metadata describing entity types, properties, and relationships.
    
    Args:
        service: The OData service name
        
    Returns:
        Service metadata in XML format or error message
    """
    try:
        client = get_odata_client()
        metadata = client.get_service_metadata(service)
        
        return json.dumps({
            "success": True,
            "service": service,
            "metadata": metadata
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData metadata error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error getting metadata: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


@tool(
    name="list_sap_entity_sets",
    description="List all available entity sets in an SAP OData service"
)
def list_sap_entity_sets(service: str) -> str:
    """
    List all entity sets available in an OData service.
    This helps discover what data is available to query.
    
    Args:
        service: The OData service name
        
    Returns:
        JSON string containing list of entity sets or error message
    """
    try:
        client = get_odata_client()
        service_doc = client.get_service_document(service)
        
        # Extract entity set names from service document
        entity_sets = []
        
        # OData V2 format
        if "d" in service_doc:
            collections = service_doc["d"].get("EntitySets", [])
            entity_sets = collections if isinstance(collections, list) else []
        
        # OData V4 format
        elif "value" in service_doc:
            for item in service_doc["value"]:
                if "name" in item:
                    entity_sets.append(item["name"])
                elif "url" in item:
                    entity_sets.append(item["url"])
        
        return json.dumps({
            "success": True,
            "service": service,
            "entity_sets": entity_sets,
            "count": len(entity_sets)
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData service document error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error listing entity sets: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


# ============================================================================
# Create/Update/Delete Tools
# ============================================================================

@tool(
    name="create_sap_entity",
    description="Create a new entity in an SAP OData service"
)
def create_sap_entity(
    service: str,
    entity: str,
    data: str
) -> str:
    """
    Create a new entity in an SAP OData service.
    
    Args:
        service: The OData service name
        entity: The entity set name
        data: JSON string containing the entity data to create
        
    Returns:
        JSON string containing the created entity or error message
    """
    try:
        client = get_odata_client()
        
        # Parse JSON data
        try:
            entity_data = json.loads(data)
        except json.JSONDecodeError as e:
            return json.dumps({
                "success": False,
                "error": f"Invalid JSON data: {e}"
            }, indent=2)
        
        result = client.create_entity(service, entity, entity_data)
        
        return json.dumps({
            "success": True,
            "message": f"Entity created in {entity}",
            "data": result
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData create error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code,
            "details": e.details
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error creating entity: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


@tool(
    name="update_sap_entity",
    description="Update an existing entity in an SAP OData service"
)
def update_sap_entity(
    service: str,
    entity: str,
    key: str,
    data: str,
    key_fields: Optional[str] = None
) -> str:
    """
    Update an existing entity in an SAP OData service.
    
    Args:
        service: The OData service name
        entity: The entity set name
        key: The entity key value
        data: JSON string containing the fields to update
        key_fields: Optional comma-separated field names for composite keys
        
    Returns:
        JSON string containing the update result or error message
    """
    try:
        client = get_odata_client()
        
        # Parse JSON data
        try:
            update_data = json.loads(data)
        except json.JSONDecodeError as e:
            return json.dumps({
                "success": False,
                "error": f"Invalid JSON data: {e}"
            }, indent=2)
        
        # Handle composite keys
        if key_fields:
            fields = [f.strip() for f in key_fields.split(",")]
            values = [v.strip() for v in key.split(",")]
            key_dict = dict(zip(fields, values))
            result = client.update_entity(service, entity, key_dict, update_data)
        else:
            result = client.update_entity(service, entity, key, update_data)
        
        return json.dumps({
            "success": True,
            "message": f"Entity updated in {entity}",
            "data": result
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData update error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code,
            "details": e.details
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error updating entity: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


@tool(
    name="delete_sap_entity",
    description="Delete an entity from an SAP OData service"
)
def delete_sap_entity(
    service: str,
    entity: str,
    key: str,
    key_fields: Optional[str] = None
) -> str:
    """
    Delete an entity from an SAP OData service.
    
    Args:
        service: The OData service name
        entity: The entity set name
        key: The entity key value
        key_fields: Optional comma-separated field names for composite keys
        
    Returns:
        JSON string containing the deletion result or error message
    """
    try:
        client = get_odata_client()
        
        # Handle composite keys
        if key_fields:
            fields = [f.strip() for f in key_fields.split(",")]
            values = [v.strip() for v in key.split(",")]
            key_dict = dict(zip(fields, values))
            result = client.delete_entity(service, entity, key_dict)
        else:
            result = client.delete_entity(service, entity, key)
        
        return json.dumps({
            "success": True,
            "message": f"Entity deleted from {entity}",
            "data": result
        }, indent=2)
        
    except ODataClientError as e:
        logger.error(f"OData delete error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e),
            "status_code": e.status_code,
            "details": e.details
        }, indent=2)
    except Exception as e:
        logger.error(f"Unexpected error deleting entity: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


# ============================================================================
# Connection Tools
# ============================================================================

@tool(
    name="test_sap_connection",
    description="Test the connection to the SAP system"
)
def test_sap_connection() -> str:
    """
    Test the connection to the configured SAP system.
    Verifies authentication and network connectivity.
    
    Returns:
        JSON string indicating connection status
    """
    try:
        client = get_odata_client()
        success = client.test_connection()
        
        if success:
            return json.dumps({
                "success": True,
                "message": "Successfully connected to SAP system",
                "host": client.sap_config.host,
                "client": client.sap_config.client
            }, indent=2)
        else:
            return json.dumps({
                "success": False,
                "error": "Connection test failed",
                "host": client.sap_config.host
            }, indent=2)
            
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)


# ============================================================================
# Export all tools for easy import
# ============================================================================

SAP_ODATA_TOOLS = [
    query_sap_entity_set,
    get_sap_entity,
    get_sap_service_metadata,
    list_sap_entity_sets,
    create_sap_entity,
    update_sap_entity,
    delete_sap_entity,
    test_sap_connection
]



