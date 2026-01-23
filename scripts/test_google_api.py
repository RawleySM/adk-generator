#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "python-dotenv",
#   "google-genai",
# ]
# ///

import os
from dotenv import load_dotenv
from google import genai

def main():
    # Load environment variables from .env
    load_dotenv()
    
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY not found in .env file.")
        return

    # Initialize the client
    # Note: By default, the SDK uses Vertex AI if configured, 
    # but we'll specify the API key to ensure it uses the Gemini API directly.
    client = genai.Client(api_key=api_key)

    try:
        print("Pinging Gemini API...")
        response = client.models.generate_content(
            model="gemini-3-pro-preview",
            contents="Say 'API connection successful!'"
        )
        print(f"Response: {response.text}")
        
        if response.usage_metadata:
            print("\nUsage Metadata:")
            print(f"  Prompt Tokens: {response.usage_metadata.prompt_token_count}")
            print(f"  Candidates Tokens: {response.usage_metadata.candidates_token_count}")
            print(f"  Total Tokens: {response.usage_metadata.total_token_count}")

        print("\nFetching Model Configuration...")
        model_info = client.models.get(model="gemini-3-pro-preview")
        print(f"  Display Name: {model_info.display_name}")
        print(f"  Input Token Limit: {model_info.input_token_limit}")
        print(f"  Output Token Limit: {model_info.output_token_limit}")
        
    except Exception as e:
        print(f"Error connecting to API: {e}")

if __name__ == "__main__":
    main()
