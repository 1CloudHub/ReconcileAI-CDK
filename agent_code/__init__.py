"""
SAP OData Agent - Strands-based agent for SAP OData service interactions.

This package provides an intelligent agent that can:
- Query SAP OData entity sets
- Create, update, and delete entities
- Discover available services and entity sets
- Handle authentication and error recovery

Usage:
    from sap_odata_agent import create_sap_odata_agent
    
    agent = create_sap_odata_agent()
    response = agent("Get the first 10 business partners")
"""

from .agent import create_sap_odata_agent, SAP_ODATA_AGENT_SYSTEM_PROMPT
from .tools import (
    SAP_ODATA_TOOLS,
    query_sap_entity_set,
    get_sap_entity,
    get_sap_service_metadata,
    list_sap_entity_sets,
    create_sap_entity,
    update_sap_entity,
    delete_sap_entity,
    test_sap_connection
)
from .config import (
    ConfigLoader,
    SAPConfig,
    ODataConfig,
    RequestConfig,
    get_sap_config,
    get_odata_config,
    get_request_config
)
from .odata_client import ODataClient, ODataClientError

__version__ = "1.0.0"

__all__ = [
    # Agent
    "create_sap_odata_agent",
    "SAP_ODATA_AGENT_SYSTEM_PROMPT",
    
    # Tools
    "SAP_ODATA_TOOLS",
    "query_sap_entity_set",
    "get_sap_entity",
    "get_sap_service_metadata",
    "list_sap_entity_sets",
    "create_sap_entity",
    "update_sap_entity",
    "delete_sap_entity",
    "test_sap_connection",
    
    # Configuration
    "ConfigLoader",
    "SAPConfig",
    "ODataConfig",
    "RequestConfig",
    "get_sap_config",
    "get_odata_config",
    "get_request_config",
    
    # Client
    "ODataClient",
    "ODataClientError"
]



