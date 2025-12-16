"""
OData Client module for SAP OData Agent.
Handles HTTP communication with SAP OData services.
"""

import json
import time
import logging
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.auth import HTTPBasicAuth

from config import SAPConfig, ODataConfig, RequestConfig

# Configure logging
logger = logging.getLogger(__name__)


class ODataClientError(Exception):
    """Custom exception for OData client errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, details: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


class ODataClient:
    """
    SAP OData HTTP client with retry logic and response parsing.
    Supports both OData V2 and V4 response formats.
    """
    
    def __init__(
        self,
        sap_config: SAPConfig,
        odata_config: ODataConfig,
        request_config: RequestConfig
    ):
        """
        Initialize OData client with configuration.
        
        Args:
            sap_config: SAP system connection settings
            odata_config: OData service settings
            request_config: HTTP request settings
        """
        self.sap_config = sap_config
        self.odata_config = odata_config
        self.request_config = request_config
        
        # Create session for connection pooling
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(sap_config.user, sap_config.password)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
    
    def _build_service_url(self, service: str) -> str:
        """Build base URL for a service."""
        base_path = self.odata_config.base_path
        if base_path:
            return f"{self.sap_config.base_url}{base_path}/{service}"
        return f"{self.sap_config.base_url}/{service}"
    
    def _build_query_params(
        self,
        filter_expr: Optional[str] = None,
        select: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None,
        expand: Optional[str] = None
    ) -> dict[str, str]:
        """Build OData query parameters."""
        params = {
            "sap-client": self.sap_config.client,
            "sap-language": self.sap_config.language
        }
        
        if filter_expr:
            params["$filter"] = filter_expr
        if select:
            params["$select"] = select
        if top is not None:
            params["$top"] = str(top)
        if skip is not None:
            params["$skip"] = str(skip)
        if orderby:
            params["$orderby"] = orderby
        if expand:
            params["$expand"] = expand
            
        return params
    
    def _parse_response(self, data: Any) -> Any:
        """
        Parse OData response (supports V2 and V4 formats).
        
        Args:
            data: Raw response data from API
            
        Returns:
            Parsed data (list of entities or single entity)
        """
        if not data:
            return []
        
        # OData V2 format: { d: { results: [...] } }
        if isinstance(data, dict) and "d" in data:
            d_value = data["d"]
            if isinstance(d_value, dict) and "results" in d_value:
                return d_value["results"]
            return d_value
        
        # OData V4 format: { value: [...] }
        if isinstance(data, dict) and "value" in data:
            return data["value"]
        
        return data
    
    def _execute_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Execute HTTP request with retry logic.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: Request URL
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            ODataClientError: If all retries fail
        """
        last_error = None
        
        for attempt in range(self.request_config.max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}: {method} {url}")
                
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.request_config.timeout_seconds,
                    **kwargs
                )
                
                # Check for HTTP errors
                response.raise_for_status()
                return response
                
            except requests.exceptions.HTTPError as e:
                last_error = e
                status_code = e.response.status_code if e.response else None
                
                # Don't retry client errors (4xx)
                if status_code and 400 <= status_code < 500:
                    error_detail = self._extract_error_detail(e.response)
                    raise ODataClientError(
                        f"HTTP {status_code} error: {error_detail}",
                        status_code=status_code,
                        details=error_detail
                    )
                
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
            
            # Wait before retry with exponential backoff
            if attempt < self.request_config.max_retries - 1:
                delay = self.request_config.retry_delay_ms * (2 ** attempt) / 1000
                logger.debug(f"Waiting {delay}s before retry...")
                time.sleep(delay)
        
        raise ODataClientError(
            f"Request failed after {self.request_config.max_retries} attempts: {last_error}",
            details=str(last_error)
        )
    
    def _extract_error_detail(self, response: requests.Response) -> str:
        """Extract error message from response."""
        try:
            error_data = response.json()
            # OData error format
            if "error" in error_data:
                error = error_data["error"]
                if isinstance(error, dict) and "message" in error:
                    message = error["message"]
                    if isinstance(message, dict):
                        return message.get("value", str(error))
                    return str(message)
                return str(error)
            return response.text
        except (json.JSONDecodeError, ValueError):
            return response.text
    
    def query_entity_set(
        self,
        service: str,
        entity: str,
        filter_expr: Optional[str] = None,
        select: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None,
        expand: Optional[str] = None
    ) -> list[dict]:
        """
        Query an OData entity set with optional filters.
        
        Args:
            service: OData service name
            entity: Entity set name
            filter_expr: OData $filter expression
            select: Comma-separated list of fields to select
            top: Maximum number of records to return
            skip: Number of records to skip
            orderby: OData $orderby expression
            expand: Navigation properties to expand
            
        Returns:
            List of entity records
        """
        service_url = self._build_service_url(service)
        params = self._build_query_params(filter_expr, select, top, skip, orderby, expand)
        url = f"{service_url}/{entity}?{urlencode(params)}"
        
        logger.info(f"Querying entity set: {service}/{entity}")
        
        response = self._execute_with_retry("GET", url)
        data = response.json()
        
        return self._parse_response(data)
    
    def get_entity_by_key(
        self,
        service: str,
        entity: str,
        key: str | dict[str, Any]
    ) -> dict:
        """
        Get a single entity by its key.
        
        Args:
            service: OData service name
            entity: Entity set name
            key: Entity key (string for simple key, dict for composite key)
            
        Returns:
            Entity record
        """
        service_url = self._build_service_url(service)
        
        # Build key string
        if isinstance(key, dict):
            # Composite key: { ID: '1', Type: 'A' } -> ID='1',Type='A'
            key_string = ",".join(f"{k}='{v}'" for k, v in key.items())
        else:
            # Simple key
            key_string = f"'{key}'"
        
        params = {
            "sap-client": self.sap_config.client,
            "sap-language": self.sap_config.language
        }
        
        url = f"{service_url}/{entity}({key_string})?{urlencode(params)}"
        
        logger.info(f"Getting entity: {service}/{entity}({key_string})")
        
        response = self._execute_with_retry("GET", url)
        data = response.json()
        
        return self._parse_response(data)
    
    def get_service_metadata(self, service: str) -> str:
        """
        Get service metadata (XML format).
        
        Args:
            service: OData service name
            
        Returns:
            Metadata XML string
        """
        service_url = self._build_service_url(service)
        params = {"sap-client": self.sap_config.client}
        url = f"{service_url}/$metadata?{urlencode(params)}"
        
        logger.info(f"Getting metadata for service: {service}")
        
        # Override accept header for XML
        headers = {"Accept": "application/xml"}
        response = self._execute_with_retry("GET", url, headers=headers)
        
        return response.text
    
    def get_service_document(self, service: str) -> dict:
        """
        Get service document (list of entity sets).
        
        Args:
            service: OData service name
            
        Returns:
            Service document data
        """
        service_url = self._build_service_url(service)
        params = {
            "sap-client": self.sap_config.client,
            "sap-language": self.sap_config.language
        }
        url = f"{service_url}/?{urlencode(params)}"
        
        logger.info(f"Getting service document for: {service}")
        
        response = self._execute_with_retry("GET", url)
        return response.json()
    
    def create_entity(
        self,
        service: str,
        entity: str,
        data: dict[str, Any]
    ) -> dict:
        """
        Create a new entity.
        
        Args:
            service: OData service name
            entity: Entity set name
            data: Entity data to create
            
        Returns:
            Created entity record
        """
        service_url = self._build_service_url(service)
        params = {
            "sap-client": self.sap_config.client,
            "sap-language": self.sap_config.language
        }
        url = f"{service_url}/{entity}?{urlencode(params)}"
        
        logger.info(f"Creating entity in: {service}/{entity}")
        
        response = self._execute_with_retry("POST", url, json=data)
        
        # Handle 201 Created with or without response body
        if response.status_code == 201:
            try:
                return self._parse_response(response.json())
            except (json.JSONDecodeError, ValueError):
                return {"status": "created"}
        
        return self._parse_response(response.json())
    
    def update_entity(
        self,
        service: str,
        entity: str,
        key: str | dict[str, Any],
        data: dict[str, Any]
    ) -> dict:
        """
        Update an existing entity.
        
        Args:
            service: OData service name
            entity: Entity set name
            key: Entity key
            data: Updated entity data
            
        Returns:
            Updated entity record
        """
        service_url = self._build_service_url(service)
        
        # Build key string
        if isinstance(key, dict):
            key_string = ",".join(f"{k}='{v}'" for k, v in key.items())
        else:
            key_string = f"'{key}'"
        
        params = {
            "sap-client": self.sap_config.client,
            "sap-language": self.sap_config.language
        }
        
        url = f"{service_url}/{entity}({key_string})?{urlencode(params)}"
        
        logger.info(f"Updating entity: {service}/{entity}({key_string})")
        
        # Use PATCH for partial updates
        response = self._execute_with_retry("PATCH", url, json=data)
        
        # Handle 204 No Content
        if response.status_code == 204:
            return {"status": "updated", "key": key}
        
        return self._parse_response(response.json())
    
    def delete_entity(
        self,
        service: str,
        entity: str,
        key: str | dict[str, Any]
    ) -> dict:
        """
        Delete an entity.
        
        Args:
            service: OData service name
            entity: Entity set name
            key: Entity key
            
        Returns:
            Deletion confirmation
        """
        service_url = self._build_service_url(service)
        
        # Build key string
        if isinstance(key, dict):
            key_string = ",".join(f"{k}='{v}'" for k, v in key.items())
        else:
            key_string = f"'{key}'"
        
        params = {"sap-client": self.sap_config.client}
        url = f"{service_url}/{entity}({key_string})?{urlencode(params)}"
        
        logger.info(f"Deleting entity: {service}/{entity}({key_string})")
        
        self._execute_with_retry("DELETE", url)
        
        return {"status": "deleted", "key": key}
    
    def test_connection(self) -> bool:
        """
        Test connection to SAP system.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            url = f"{self.sap_config.base_url}/?sap-client={self.sap_config.client}"
            self._execute_with_retry("GET", url)
            logger.info("SAP connection test successful")
            return True
        except ODataClientError as e:
            logger.error(f"SAP connection test failed: {e}")
            return False
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()



