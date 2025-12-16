"""
Configuration module for SAP OData Agent.
Handles environment variable loading and validation.
"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class SAPConfig:
    """SAP system connection configuration."""
    host: str
    user: str
    password: str
    client: str = "001"
    language: str = "EN"
    use_ssl: bool = True
    
    @property
    def base_url(self) -> str:
        """Generate base URL for SAP system."""
        protocol = "https" if self.use_ssl else "http"
        return f"{protocol}://{self.host}"


@dataclass
class ODataConfig:
    """OData service configuration."""
    base_path: str = "/sap/opu/odata/sap"
    default_service: Optional[str] = None
    default_entity: Optional[str] = None


@dataclass
class RequestConfig:
    """HTTP request configuration."""
    timeout_ms: int = 30000
    max_retries: int = 3
    retry_delay_ms: int = 1000
    
    @property
    def timeout_seconds(self) -> float:
        """Convert timeout to seconds for requests library."""
        return self.timeout_ms / 1000


class ConfigLoader:
    """
    Centralized configuration loader.
    Loads all settings from environment variables.
    """
    
    @staticmethod
    def get_sap_config() -> SAPConfig:
        """Load SAP configuration from environment variables."""
        return SAPConfig(
            host=os.getenv("SAP_HOST", ""),
            user=os.getenv("SAP_USER", ""),
            password=os.getenv("SAP_PASSWORD", ""),
            client=os.getenv("SAP_CLIENT", "001"),
            language=os.getenv("SAP_LANGUAGE", "EN"),
            use_ssl=os.getenv("SAP_USE_SSL", "true").lower() == "true"
        )
    
    @staticmethod
    def get_odata_config() -> ODataConfig:
        """Load OData configuration from environment variables."""
        return ODataConfig(
            base_path=os.getenv("ODATA_BASE_PATH", "/sap/opu/odata/sap"),
            default_service=os.getenv("ODATA_DEFAULT_SERVICE"),
            default_entity=os.getenv("ODATA_DEFAULT_ENTITY")
        )
    
    @staticmethod
    def get_request_config() -> RequestConfig:
        """Load request configuration from environment variables."""
        return RequestConfig(
            timeout_ms=int(os.getenv("REQUEST_TIMEOUT_MS", "30000")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay_ms=int(os.getenv("RETRY_DELAY_MS", "1000"))
        )
    
    @staticmethod
    def validate() -> list[str]:
        """
        Validate required configuration.
        Returns list of missing required fields.
        """
        sap_config = ConfigLoader.get_sap_config()
        missing = []
        
        if not sap_config.host:
            missing.append("SAP_HOST")
        if not sap_config.user:
            missing.append("SAP_USER")
        if not sap_config.password:
            missing.append("SAP_PASSWORD")
            
        return missing
    
    @staticmethod
    def validate_or_raise() -> None:
        """Validate configuration and raise error if invalid."""
        missing = ConfigLoader.validate()
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Please set them in your .env file or environment."
            )


# Convenience functions for direct access
def get_sap_config() -> SAPConfig:
    """Get SAP configuration."""
    return ConfigLoader.get_sap_config()


def get_odata_config() -> ODataConfig:
    """Get OData configuration."""
    return ConfigLoader.get_odata_config()


def get_request_config() -> RequestConfig:
    """Get request configuration."""
    return ConfigLoader.get_request_config()



