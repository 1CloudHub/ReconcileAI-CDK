#!/usr/bin/env python3
"""
ReconcileAI CDK CLI Deployment Tool
Interactive CLI for deploying ReconcileAI No-SAP CDK stack
with model and token limit selection.
"""

import subprocess
import sys
from typing import Dict

# --- Auto-install questionary if not found ---
try:
    import questionary
except ImportError:
    print("📦 Installing required library: questionary...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "questionary"])
    import site
    site.addsitedir(site.getusersitepackages())
    import questionary

# -----------------------------------------------------------------------
# Stack configuration
# -----------------------------------------------------------------------
STACK = {
    "stack_name": "ReconcileaiNoSapCdkStack",
    "description": "ReconcileAI No-SAP reconciliation stack",
    "display_name": "🔁 ReconcileAI No-SAP Stack",
}

# -----------------------------------------------------------------------
# Model options
# -----------------------------------------------------------------------
MODELS: Dict[str, Dict[str, str]] = {
    "amazon": {
        "model_id": "us.amazon.nova-pro-v1:0",
        "display_name": "Amazon Nova Pro",
    },
    "anthropic": {
        "model_id": "global.anthropic.claude-sonnet-4-6",
        "display_name": "Anthropic Claude Sonnet 4",
    },
}

# -----------------------------------------------------------------------
# Token limit options
# -----------------------------------------------------------------------
TOKEN_LIMITS: Dict[str, int] = {
    "20,000 tokens": 20000,
    "50,000 tokens": 50000,
    "Unlimited": -1,
}


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _format_token_limit_for_display(token_limit: int) -> str:
    """Human-readable token limit for CLI output only (never show internal sentinel)."""
    if token_limit == -1:
        return "Unlimited"
    return f"{token_limit:,}"


def get_deployment_confirmation(model_info: Dict[str, str], token_limit: int) -> bool:
    """Print a deployment summary and ask for confirmation."""
    print("\n📝 Deployment Summary:")
    print(f"   Stack       : {STACK['display_name']}")
    print(f"   Stack Name  : {STACK['stack_name']}")
    print(f"   Model       : {model_info['display_name']}")
    print(f"   Model ID    : {model_info['model_id']}")
    print(f"   Token Limit : {_format_token_limit_for_display(token_limit)}")
    print("\n⚠️  This will create / update AWS resources that may incur costs.")

    return questionary.confirm(
        "Do you want to proceed with deployment?",
        default=False,
    ).ask()


def deploy_stack(model_selection: str, model_id: str, token_limit: int) -> None:
    """Set env vars and run `cdk deploy` for the single ReconcileAI stack."""
    import os

    stack_name = STACK["stack_name"]

    print(f"\n🚀 Deploying stack : {stack_name}")
    print(f"🤖 Model           : {model_id}")
    print(f"📏 Token Limit     : {_format_token_limit_for_display(token_limit)}")
    print("⏳ This may take several minutes...\n")

    # Pass selections to app.py via environment variables
    os.environ["CDK_MODEL_SELECTION"] = model_selection
    os.environ["CDK_MODEL_ID"] = model_id
    os.environ["CDK_TOKEN_LIMIT"] = str(token_limit)

    print(f"🔧 CDK_MODEL_SELECTION = {model_selection}")
    print(f"🔧 CDK_MODEL_ID        = {model_id}")
    print(f"🔧 CDK_TOKEN_LIMIT     = {_format_token_limit_for_display(token_limit)}")
    print()

    try:
        subprocess.run(
            f"cdk deploy {stack_name} --require-approval never",
            shell=True,
            check=True,
        )
        print(f"\n✅ Stack '{stack_name}' deployed successfully!")

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Deployment failed: {e}")
        sys.exit(1)

    finally:
        # Clean up environment variables
        for key in ("CDK_MODEL_SELECTION", "CDK_MODEL_ID", "CDK_TOKEN_LIMIT"):
            os.environ.pop(key, None)


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main() -> None:
    print("🌟 Welcome to ReconcileAI CDK CLI 🌟")
    print("=" * 50)

    # ── Q1: Model selection ────────────────────────────────────────────
    model_choice = questionary.select(
        "Which Bedrock model do you want to use?",
        choices=[
            f"🟡 Amazon Nova Pro  (us.amazon.nova-pro-v1:0)",
            f"🟣 Anthropic Claude (global.anthropic.claude-sonnet-4-6)",
            "❌ Exit",
        ],
    ).ask()

    if model_choice == "❌ Exit":
        print("👋 Exiting CLI. Bye!")
        sys.exit(0)

    model_key = "amazon" if "Amazon" in model_choice else "anthropic"
    model_info = MODELS[model_key]

    # ── Q2: Token limit selection ──────────────────────────────────────
    token_choice = questionary.select(
        "Select the maximum token limit for Bedrock inference:",
        choices=list(TOKEN_LIMITS.keys()) + ["❌ Exit"],
    ).ask()

    if token_choice == "❌ Exit":
        print("👋 Exiting CLI. Bye!")
        sys.exit(0)

    token_limit = TOKEN_LIMITS[token_choice]

    # ── Confirmation ───────────────────────────────────────────────────
    if not get_deployment_confirmation(model_info, token_limit):
        print("❌ Deployment cancelled by user.")
        sys.exit(0)

    # ── Deploy ─────────────────────────────────────────────────────────
    deploy_stack(model_key, model_info["model_id"], token_limit)


if __name__ == "__main__":
    main()