"""
SAP OData Agent - Main entry point.
Uses Strands Agents framework to create an intelligent agent for SAP OData interactions.
"""

import logging
import sys
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

import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import random
import requests
import threading
import time

# -------------------------------------------------------------------
# Global state
# -------------------------------------------------------------------

# Global variable to store userid for the current request context
current_userid: Optional[str] = None
# Global variable to store final response text from streaming
final_text_ff: str = ""

# -------------------------------------------------------------------
# WebSocket and Callback Handler Functions
# -------------------------------------------------------------------

def send_ws_message(connection_id: str, data: Any):
    """
    Send WebSocket message to the connected client

    Args:
        connection_id: WebSocket connection ID
        data: Message data (dict or string)
    """
    # NOTE: Keep this endpoint as in your existing code.
    client = boto3.client(
        "apigatewaymanagementapi",
        endpoint_url="https://0if9awq559.execute-api.us-west-2.amazonaws.com/production",
    )

    if isinstance(data, dict):
        data = json.dumps(data)

    try:
        response = client.post_to_connection(
            ConnectionId=connection_id,
            Data=data,
        )
        return response
    except client.exceptions.GoneException:
        print(f"Connection {connection_id} is gone.")
    except Exception as e:
        print(f"Error sending message: {e}")


def make_callback_handler(connection_id: str, message_id: str):
    """
    Create a callback handler for streaming agent responses via WebSocket.

    Args:
        connection_id: WebSocket connection ID
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
            send_ws_message(connection_id, {
                "type": "thinking",
                "message": token,
                "message_id": message_id,
            })

        # Handle final assistant message
        if "message" in kwargs and kwargs["message"].get("role") == "assistant":
            final_text = kwargs["message"].get("content", "")
            final_text_ff = final_text
            send_ws_message(connection_id, {
                "type": "answer",
                "message": final_text,
                "message_id": message_id,
            })
            state["buffer"].clear()

    return custom_callback_handler


# -------------------------------------------------------------------
# Config / Tools import
# -------------------------------------------------------------------

try:
    from .config import ConfigLoader
    from .tools import SAP_ODATA_TOOLS
except ImportError:
    from config import ConfigLoader
    from tools import SAP_ODATA_TOOLS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# System prompt that defines the agent's behavior and capabilities
# -------------------------------------------------------------------

SAP_ODATA_AGENT_SYSTEM_PROMPT = """
You are an intelligent SAP OData Agent that helps users interact with SAP systems through OData services.

## Your Capabilities:
1. **Query Data**: Retrieve data from SAP entity sets with filtering, sorting, and pagination
2. **Get Single Entities**: Fetch specific records by their key values
3. **Create Entities**: Add new records to SAP systems
4. **Update Entities**: Modify existing records
5. **Delete Entities**: Remove records from SAP systems
6. **Service Discovery**: List available entity sets and view service metadata

## Important Guidelines:

### When querying data:
- Always ask for the service and entity name if not provided
- Use $filter for filtering (e.g., "BusinessPartnerCategory eq '1'")
- Use $select to limit returned fields for better performance
- Use $top and $skip for pagination
- Use $orderby for sorting results
- Use $expand to include related entities

### Common SAP OData Services:
- API_BUSINESS_PARTNER: Business partner data (A_BusinessPartner, A_BusinessPartnerAddress)
- API_SALES_ORDER_SRV: Sales order data (A_SalesOrder, A_SalesOrderItem)
- API_PURCHASEORDER_PROCESS_SRV: Purchase order data (A_PurchaseOrder, A_PurchaseOrderItem)
- API_SUPPLIERINVOICE_PROCESS_SRV: Supplier invoice data
  * A_SupplierInvoice: Main supplier invoice entity
  * A_SupplierInvoiceItemGLAcct: Invoice item G/L account assignments
  * A_SupplierInvoiceTax: Tax information for invoices
  * A_SuplrInvcHeaderWhldgTax: Withholding tax at header level
  * A_SuplrInvcItemAcctAssgmt: Item account assignments
  * A_SuplrInvcItemPurOrdRef: Item purchase order references
- API_MATERIAL_DOCUMENT_SRV: Material document (goods movement) data
  * A_MaterialDocumentHeader: Material document headers
  * A_MaterialDocumentItem: Material document line items
  * A_SerialNumberMaterialDocument: Serial numbers in material documents
- API_PRODUCT_SRV: Product master data (A_Product, A_ProductDescription)
- API_MATERIAL_STOCK_SRV: Material and inventory data

### For "last N" queries:
- Use $orderby with desc to sort by creation/modification date
- Combine with $top to get the most recent records
- Example: $orderby=CreationDate desc&$top=5

### Error Handling:
- If a connection test fails, suggest checking SAP credentials and network connectivity
- If an entity is not found, suggest listing available entity sets first
- Provide clear error messages and suggestions for resolution

### Best Practices:
- Start by testing the connection if unsure about system availability
- Use list_sap_entity_sets to discover available data before querying
- Always use $top to limit results for large entity sets
- Validate entity keys before delete or update operations

## Response Format:
- Provide clear, formatted responses
- For query results, summarize the data and highlight key fields
- For errors, explain what went wrong and suggest next steps
- When showing data, format it in a readable way
"""

# -------------------------------------------------------------------
# Agent factory (existing logic kept as-is)
# -------------------------------------------------------------------

def create_sap_odata_agent(
    system_prompt: Optional[str] = None,
    callback_handler=None,
) -> Agent:
    """
    Create and configure the SAP OData Agent.

    Args:
        system_prompt: Optional custom system prompt (uses default if not provided)
        callback_handler: Optional callback handler for streaming responses

    Returns:
        Configured Strands Agent instance
    """
    logger.info("Creating SAP OData Agent...")
    model = BedrockModel(
    model_id="us.anthropic.claude-sonnet-4-20250514-v1:0"
    )

    agent = Agent(
        system_prompt=system_prompt or SAP_ODATA_AGENT_SYSTEM_PROMPT,
        model=model,
        tools=SAP_ODATA_TOOLS,
        callback_handler=callback_handler,
        name="SAP OData Agent",
    )

    logger.info("SAP OData Agent created successfully")
    return agent

# -------------------------------------------------------------------
# FastAPI application (added, does not change CLI logic)
# -------------------------------------------------------------------
# Create global agent ONCE
GLOBAL_AGENT = create_sap_odata_agent()

app = FastAPI(title="SAP OData Agent API")

# CORS for frontend usage
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health_check")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "sap_odata_agent"}


@app.post("/process")
async def process(request: Request):
    """
    Process SAP OData agent requests with streaming support.

    Expected JSON body:
    {
        "user_question": "<string>",
        "connection_id": "<websocket-connection-id>",
        "message_id": "<unique-message-id>",
        "userid": "<user-id>"
    }
    """


    event = await request.json()

    user_input = event.get("user_question")
    connection_id = event.get("connection_id")
    message_id = event.get("message_id")
    userid = event.get("userid")

    current_userid = userid

    logger.info(
        f"[API] Processing request - connection={connection_id}, "
        f"message={message_id}, userid={userid}"
    )
    logger.info(f"[API] User question: {user_input}")

    if not user_input:
        return {"status": "error", "message": "Missing 'user_question' in request body"}

    if not connection_id or not message_id:
        # We still process, but cannot stream via WS
        logger.warning("[API] Missing connection_id or message_id; WS streaming disabled")

    try:
        # Reuse global agent
        agent = GLOBAL_AGENT
        # Attach callback handler dynamically (keeps streaming working)
        if connection_id and message_id:
            agent.callback_handler = make_callback_handler(connection_id, message_id)

        # Call agent (synchronous call inside async endpoint)
        response = agent(user_input)

        # --- Fallback: extract final text in case streaming missed something ---
        response_text = None

        # Strands response might have `message` or `content`
        if hasattr(response, "message"):
            msg = getattr(response, "message")
            # Support structures like {"content": [{"text": "..."}]} or plain text
            if isinstance(msg, dict):
                content = msg.get("content", "")
                if (
                    isinstance(content, list)
                    and len(content) > 0
                    and isinstance(content[0], dict)
                    and "text" in content[0]
                ):
                    response_text = content[0]["text"]
                else:
                    response_text = str(content)

        if response_text is None:
            if hasattr(response, "content"):
                response_text = response.content
            elif isinstance(response, str):
                response_text = response
            else:
                response_text = str(response)

        logger.info(
            f"[API] Agent processing completed. Response length: {len(response_text)}"
        )

        # If streaming didn't send the final text or WS ids were missing, send backup
        if response_text and response_text != final_text_ff and connection_id:
            send_ws_message(
                connection_id,
                {
                    "type": "answer",
                    "message": response_text,
                    "message_id": message_id,
                },
            )

        return {"status": "success", "message": "Processing completed"}

    except Exception as e:
        logger.error(f"[API] Error processing request: {str(e)}", exc_info=True)

        # Inform frontend via WebSocket if possible
        if connection_id:
            send_ws_message(
                connection_id,
                {
                    "type": "error",
                    "message": f"Error processing your request: {str(e)}",
                    "message_id": message_id,
                },
            )

        return {"status": "error", "message": str(e)}

# -------------------------------------------------------------------
# Existing interactive CLI mode (unchanged)
# -------------------------------------------------------------------

def run_interactive_mode(agent: Agent) -> None:
    """
    Run the agent in interactive mode with user input.

    Args:
        agent: Configured Strands Agent instance
    """
    print("\n" + "=" * 60)
    print("  SAP OData Agent - Interactive Mode")
    print("=" * 60)
    print("\nType your questions or commands to interact with SAP data.")
    print("Type 'exit' or 'quit' to end the session.")
    print("Type 'help' to see example commands.")
    print("-" * 60 + "\n")

    while True:
        try:
            user_input = input("You: ").strip()

            if not user_input:
                continue

            if user_input.lower() in ["exit", "quit", "q"]:
                print("\nGoodbye! Thank you for using SAP OData Agent.")
                break

            # Send message to agent
            print("\nAgent: ")
            response = agent(user_input)

            # Handle different response types
            if hasattr(response, "content"):
                # Strands response object
                print(response.content)
            elif isinstance(response, str):
                print(response)
            else:
                print(str(response))
            print()

        except KeyboardInterrupt:
            print("\n\nSession interrupted. Goodbye!")
            break
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            print(f"\nError: {e}\n")


def main():
    """Main entry point for the SAP OData Agent (CLI / single-command mode)."""
    # Validate configuration
    missing = ConfigLoader.validate()
    if missing:
        print(f"Error: Missing required configuration: {', '.join(missing)}")
        print("Please set these in your .env file or environment variables.")
        print("\nSee env.example for configuration template.")
        sys.exit(1)

    # Create the agent
    agent = create_sap_odata_agent()

    # Check for command line arguments
    if len(sys.argv) > 1:
        # If the first extra arg is --server we do NOT treat it as a question
        # (server startup is handled in __main__ below)
        if "--server" in sys.argv:
            # Do nothing here: server mode will start in __main__
            return

        # Single command mode
        command = " ".join(sys.argv[1:])
        print(f"\nExecuting: {command}\n")
        response = agent(command)

        # Handle different response types
        if hasattr(response, "content"):
            print(response.content)
        elif isinstance(response, str):
            print(response)
        else:
            print(str(response))
    else:
        # Interactive mode
        run_interactive_mode(agent)


# -------------------------------------------------------------------
# Hybrid launcher: CLI or FastAPI server
# -------------------------------------------------------------------

if __name__ == "__main__":
    # If you want API server mode:
    #   python agent.py --server
    #
    # If you want CLI mode (existing behavior):
    #   python agent.py
    #   python agent.py "Your question here"
    if "--server" in sys.argv:
        # Remove the flag so uvicorn doesn't see it
        sys.argv = [arg for arg in sys.argv if arg != "--server"]
        print("Starting SAP OData Agent FastAPI server on port 8085...")
        uvicorn.run("agent:app", host="0.0.0.0", port=8085, reload=True)
    else:
        main()
