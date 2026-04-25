#!/usr/bin/env python3
"""
ReconcileAI No-SAP CDK Application

Reads two deployment-time selections from environment variables
(set by deploy.py before calling `cdk deploy`):

  CDK_MODEL_SELECTION  – "amazon" | "anthropic"
  CDK_MODEL_ID         – full Bedrock model ID string
  CDK_TOKEN_LIMIT      – "20000" | "50000"

What this app does with those values:
  1. Passes MODEL_ID and TOKEN_LIMIT as Lambda environment variables.
  2. Runs a one-shot DB-init Lambda (custom resource) that:
       - Creates the DB schema / tables if they don't exist.
       - Inserts (or upserts) the token_limit into a `config_table`
         row so the application can read it at runtime from the DB.
"""

import os
import aws_cdk as cdk
from reconcileai_no_sap_cdk.reconcileai_no_sap_cdk_stack import ReconcileaiNoSapCdkStack

# -----------------------------------------------------------------------
# Read deployment selections from environment (injected by deploy.py)
# -----------------------------------------------------------------------

MODEL_SELECTION = os.getenv("CDK_MODEL_SELECTION", "amazon")
MODEL_ID = os.getenv(
    "CDK_MODEL_ID",
    "us.amazon.nova-pro-v1:0",          # default: Amazon Nova Pro
)
TOKEN_LIMIT = int(os.getenv("CDK_TOKEN_LIMIT", "20000"))   # default: 20 000

# Validate
_VALID_MODELS = {"amazon", "anthropic"}
if MODEL_SELECTION not in _VALID_MODELS:
    raise ValueError(
        f"CDK_MODEL_SELECTION must be one of {_VALID_MODELS}; got '{MODEL_SELECTION}'"
    )

_VALID_TOKENS = {20000, 50000}
if TOKEN_LIMIT not in _VALID_TOKENS:
    raise ValueError(
        f"CDK_TOKEN_LIMIT must be one of {_VALID_TOKENS}; got '{TOKEN_LIMIT}'"
    )
# -----------------------------------------------------------------------
# CDK App
# -----------------------------------------------------------------------

app = cdk.App()

ReconcileaiNoSapCdkStack(
    app,
    "ReconcileaiNoSapCdkStack",
    # ── Pass the two user selections into the stack ──────────────────
    # Your stack's __init__ must accept these kwargs and forward them
    # to the Lambda environment and the DB-init custom resource.
    # See the docstring in the stack class for wiring instructions.
    bedrock_model_id=MODEL_ID,
    token_limit=TOKEN_LIMIT,
    # ── Standard CDK environment ─────────────────────────────────────
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()