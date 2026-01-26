#!/bin/bash

ENV_FILE="$HOME/dev/.env"
SCOPE="adk-secrets"
PROFILE="rstanhope"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

echo "Reading secrets from $ENV_FILE and syncing ONLY specific keys to Databricks scope '$SCOPE'..."

# List of keys to allow
ALLOWED_KEYS="OPENAI_API_KEY ANTHROPIC_API_KEY PERPLEXITY_API_KEY FIRECRAWL_API_KEY TAVILY_API_KEY GEMINI_API_KEY"

while IFS= read -r line || [ -n "$line" ]; do
    # Trim leading/trailing whitespace
    line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    # Skip empty lines and comments
    if [ -z "$line" ] || [[ "$line" == \#* ]]; then
        continue
    fi

    # Check if line contains '='
    if [[ "$line" == *"="* ]]; then
        # Extract Key and Value
        KEY=$(echo "$line" | cut -d '=' -f 1)
        VALUE=$(echo "$line" | cut -d '=' -f 2-)

        # Remove potential surrounding quotes from Value
        VALUE=$(echo "$VALUE" | sed 's/^"//;s/"$//')
        VALUE=$(echo "$VALUE" | sed "s/^'//;s/'$//")

        # Check if KEY is in ALLOWED_KEYS
        if [[ " $ALLOWED_KEYS " =~ " $KEY " ]]; then
            echo "Putting secret: $KEY"
            
            # Execute Databricks command
            databricks secrets put-secret "$SCOPE" "$KEY" --string-value "$VALUE" --profile "$PROFILE"

            if [ $? -eq 0 ]; then
                echo "Successfully added $KEY"
            else
                echo "Failed to add $KEY"
            fi

            # 2-second delay
            sleep 2
        else
            echo "Skipping key (not in allowlist): $KEY"
        fi
    else
        echo "Skipping malformed line: $line"
    fi
done < "$ENV_FILE"

echo "Sync complete."
