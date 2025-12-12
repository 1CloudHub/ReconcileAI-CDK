"""
Simple Strands Agent for Testing
================================

A minimal Strands Agents SDK example for testing purposes.
This script demonstrates basic agent functionality with simple tools.

Author: Generated for Strands Testing
Date: 2025-01-17
"""

import logging
from strands import Agent, tool

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@tool
def get_current_time() -> dict:
    """
    Get the current time in a readable format

    Returns:
        Dictionary with current time information
    """
    from datetime import datetime

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

    Args:
        numbers: List of numbers to sum

    Returns:
        Dictionary with calculation result
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

def create_test_agent() -> Agent:
    """
    Create a simple test agent with basic tools using default model

    Returns:
        Configured Strands Agent instance
    """
    try:
        agent = Agent(
            tools=[get_current_time, calculate_sum],
            system_prompt="You are a helpful assistant that can tell time and perform basic calculations. Always use the available tools when appropriate."
        )
        return agent
    except Exception as e:
        logger.error(f"Error creating agent: {str(e)}")
        raise e

# For local testing
if __name__ == "__main__":
    print("Testing Strands Agent locally...")

    try:
        agent = create_test_agent()

        test_messages = [
            "What time is it?",
            "Calculate the sum of [1, 2, 3, 4, 5]",
            "Hello, how are you?",
            "What can you help me with?"
        ]

        for message in test_messages:
            print(f"\nUser: {message}")
            response = agent(message)
            print(f"Agent: {response.message['content'][0]['text']}")

    except Exception as e:
        print(f"Error",e)
 
