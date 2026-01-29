#!/bin/bash
# Deploy and optionally run the populate_jira_pr_urls job
#
# Usage:
#   ./scripts/deploy_populate_pr_urls.sh           # Deploy only
#   ./scripts/deploy_populate_pr_urls.sh --run     # Deploy and run
#
# Prerequisites:
#   - Databricks CLI configured with profile 'rstanhope'
#   - JIRA_API_KEY and USER_NAME in adk-secrets scope

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROFILE="rstanhope"

NOTEBOOK_SRC="$SCRIPT_DIR/databricks_populate_pr_urls.py"
NOTEBOOK_DEST="/Workspace/Users/rstanhope@spendmend.com/databricks_populate_pr_urls"
JOB_JSON="$SCRIPT_DIR/populate_pr_urls_job.json"
JOB_NAME="populate_jira_pr_urls"

# Parse arguments
RUN_JOB=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --run|-r)
            RUN_JOB=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "============================================================"
echo "Deploying populate_jira_pr_urls"
echo "============================================================"
echo "Profile: $PROFILE"
echo "Notebook: $NOTEBOOK_DEST"
echo "Run after deploy: $RUN_JOB"
echo "============================================================"

# Step 1: Verify secrets exist
echo ""
echo "[Step 1] Verifying secrets in adk-secrets scope..."
if databricks secrets list-secrets --profile "$PROFILE" adk-secrets 2>/dev/null | grep -q "JIRA_API_KEY"; then
    echo "  ✓ JIRA_API_KEY found"
else
    echo "  ✗ JIRA_API_KEY not found in adk-secrets scope"
    echo "  Run: databricks secrets put-secret --profile $PROFILE adk-secrets JIRA_API_KEY"
    exit 1
fi

if databricks secrets list-secrets --profile "$PROFILE" adk-secrets 2>/dev/null | grep -q "USER_NAME"; then
    echo "  ✓ USER_NAME found"
else
    echo "  ✗ USER_NAME not found in adk-secrets scope"
    echo "  Run: databricks secrets put-secret --profile $PROFILE adk-secrets USER_NAME"
    exit 1
fi

# Step 2: Upload notebook
echo ""
echo "[Step 2] Uploading notebook..."
databricks workspace import \
    --profile "$PROFILE" \
    --file "$NOTEBOOK_SRC" \
    --format SOURCE \
    --language PYTHON \
    --overwrite \
    "$NOTEBOOK_DEST"
echo "  ✓ Notebook uploaded to $NOTEBOOK_DEST"

# Step 3: Create or update job
echo ""
echo "[Step 3] Creating/updating job..."

# Check if job exists
EXISTING_JOB_ID=$(databricks jobs list --profile "$PROFILE" --output json 2>/dev/null | \
    jq -r ".[] | select(.settings.name == \"$JOB_NAME\") | .job_id" || echo "")

if [[ -n "$EXISTING_JOB_ID" && "$EXISTING_JOB_ID" != "null" ]]; then
    echo "  Found existing job ID: $EXISTING_JOB_ID"
    # Update existing job - need to include job_id in the JSON
    JOB_JSON_WITH_ID=$(jq --arg job_id "$EXISTING_JOB_ID" '. + {job_id: ($job_id | tonumber)}' "$JOB_JSON")
    echo "$JOB_JSON_WITH_ID" | databricks jobs reset --profile "$PROFILE" --json @-
    JOB_ID="$EXISTING_JOB_ID"
    echo "  ✓ Job updated"
else
    # Create new job
    JOB_ID=$(databricks jobs create --profile "$PROFILE" --json "@$JOB_JSON" | jq -r '.job_id')
    echo "  ✓ Job created with ID: $JOB_ID"
fi

# Step 4: Run job if requested
if [[ "$RUN_JOB" == "true" ]]; then
    echo ""
    echo "[Step 4] Starting job run..."
    RUN_RESPONSE=$(databricks jobs run-now --profile "$PROFILE" "$JOB_ID")
    RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
    echo "  ✓ Job run started with ID: $RUN_ID"
    
    # Get run URL
    DATABRICKS_HOST=$(databricks auth describe --profile "$PROFILE" --output json | jq -r '.host')
    echo ""
    echo "============================================================"
    echo "Job Run Details"
    echo "============================================================"
    echo "  Job ID: $JOB_ID"
    echo "  Run ID: $RUN_ID"
    echo "  URL: ${DATABRICKS_HOST}#job/$JOB_ID/run/$RUN_ID"
    echo "============================================================"
    
    # Wait for completion
    echo ""
    echo "Waiting for job to complete (Ctrl+C to stop waiting)..."
    while true; do
        RUN_STATUS=$(databricks runs get --profile "$PROFILE" "$RUN_ID" | jq -r '.state.life_cycle_state')
        RESULT_STATE=$(databricks runs get --profile "$PROFILE" "$RUN_ID" | jq -r '.state.result_state // empty')
        
        echo "  Status: $RUN_STATUS${RESULT_STATE:+ ($RESULT_STATE)}"
        
        if [[ "$RUN_STATUS" == "TERMINATED" ]] || [[ "$RUN_STATUS" == "SKIPPED" ]] || [[ "$RUN_STATUS" == "INTERNAL_ERROR" ]]; then
            break
        fi
        
        sleep 30
    done
    
    echo ""
    if [[ "$RESULT_STATE" == "SUCCESS" ]]; then
        echo "✓ Job completed successfully!"
    else
        echo "✗ Job finished with status: $RESULT_STATE"
        exit 1
    fi
else
    echo ""
    echo "============================================================"
    echo "Deployment Complete"
    echo "============================================================"
    echo "  Job ID: $JOB_ID"
    echo "  To run: databricks jobs run-now --profile $PROFILE $JOB_ID"
    echo "============================================================"
fi
