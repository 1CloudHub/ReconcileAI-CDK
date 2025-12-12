"""
Simple Strands Agent Script for EC2 Testing
===========================================

A minimal standalone Python script using the Strands Agents SDK.
Demonstrates basic agent functionality with simple tools.

Author: Converted for EC2 Execution
Date: 2025-10-27
"""

import json
import logging
from strands import Agent, tool
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Define Tools
# ----------------------------------------------------------------------

@tool
def get_current_time() -> dict:
    """
    Get the current time in a readable format
    """
    current_time = datetime.now()
    return {
        "status": "success",
        "content": [{
            "text": f"Current time is: {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
            "data": {
                "timestamp": current_time.isoformat(),
                "formatted_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                "timezone": "UTC"
            }
        }]
    }

@tool
def calculate_sum(numbers: list) -> dict:
    """
    Calculate the sum of a list of numbers
    """
    try:
        total = sum(numbers)
        return {
            "status": "success",
            "content": [{
                "text": f"Sum of {numbers} is {total}",
                "data": {
                    "numbers": numbers,
                    "sum": total,
                    "count": len(numbers)
                }
            }]
        }
    except Exception as e:
        return {
            "status": "error",
            "content": [{"text": f"Error calculating sum: {str(e)}"}]
        }

# ----------------------------------------------------------------------
# Agent Creation
# ----------------------------------------------------------------------

def create_test_agent() -> Agent:
    """
    Create a simple test agent with basic tools
    """
    try:
        agent = Agent(
            tools=[get_current_time, calculate_sum],
            system_prompt=(
                "You are a helpful assistant that can tell time and perform basic calculations. "
                "Always use the available tools when appropriate."
            )
        )
        return agent
    except Exception as e:
        logger.error(f"Error creating agent: {str(e)}")
        raise

# ----------------------------------------------------------------------
# Main CLI Interaction
# ----------------------------------------------------------------------

def main():
    logger.info("Initializing Strands Agent for EC2 testing...")
    agent = create_test_agent()
    logger.info("Agent initialized successfully ✅")

    print("\n💬 Type your questions below (type 'exit' to quit):\n")

    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["exit", "quit"]:
            print("👋 Exiting the agent. Goodbye!")
            break

        try:
            response = agent(user_input)
            response_text = response.message["content"][0]["text"]
            print(f"🤖 Agent: {response_text}\n")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            print(f"⚠️ Error: {e}\n")

# ----------------------------------------------------------------------

if __name__ == "__main__":
    main()
