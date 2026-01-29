#!/bin/bash
# Deploy and run the get_repo_file test job
#
# This script:
#   1. Uploads add_filepath_columns.py to DBFS
#   2. Uploads test_weburl_get_repo_file.py to DBFS
#   3. Creates or updates the test job
#   4. Optionally runs the job
#
# Usage:
#   ./scripts/deploy_test_get_repo_file.sh [--run]
#
# Prerequisites:
#   - Databricks CLI installed and configured
#   - jq installed for JSON parsing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-rstanhope}"
CLUSTER_ID="${CLUSTER_ID:-1115-120035-jyzgoasz}"
DBFS_TARGET_DIR="dbfs:/tmp/spendmend"
JOB_NAME="test_get_repo_file_full_filepath"

# Files to upload
ETL_SCRIPT="$PROJECT_ROOT/scripts/add_filepath_columns.py"
TEST_SCRIPT="$PROJECT_ROOT/databricks_rlm_agent/tests/test_weburl_get_repo_file.py"
JOB_JSON="$PROJECT_ROOT/scripts/test_get_repo_file_job.json"

# Flags
RUN_AFTER_DEPLOY=false

log_info() {
    echo "[INFO] $*"
}

log_success() {
    echo "[OK] $*"
}

log_error() {
    echo "[ERROR] $*" >&2
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --run)
            RUN_AFTER_DEPLOY=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check dependencies
if ! command -v databricks &> /dev/null; then
    log_error "databricks CLI not found"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    log_error "jq not found"
    exit 1
fi

# Upload files to DBFS
log_info "Uploading scripts to DBFS..."

databricks fs mkdirs "$DBFS_TARGET_DIR" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true

log_info "Uploading add_filepath_columns.py..."
databricks fs cp "$ETL_SCRIPT" "$DBFS_TARGET_DIR/add_filepath_columns.py" --overwrite --profile "$DATABRICKS_PROFILE"
log_success "Uploaded add_filepath_columns.py"

log_info "Uploading test_weburl_get_repo_file.py..."
databricks fs cp "$TEST_SCRIPT" "$DBFS_TARGET_DIR/test_weburl_get_repo_file.py" --overwrite --profile "$DATABRICKS_PROFILE"
log_success "Uploaded test_weburl_get_repo_file.py"

# Check if job exists
log_info "Checking if job '$JOB_NAME' exists..."
EXISTING_JOB_ID=$(databricks jobs list --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq -r ".jobs[]? | select(.settings.name == \"$JOB_NAME\") | .job_id" || echo "")

if [[ -n "$EXISTING_JOB_ID" && "$EXISTING_JOB_ID" != "null" ]]; then
    log_info "Found existing job with ID: $EXISTING_JOB_ID"
    JOB_ID="$EXISTING_JOB_ID"
    
    # Update the job
    log_info "Updating job..."
    
    # Create update payload from job JSON (need to wrap settings in new_settings)
    UPDATE_PAYLOAD=$(jq --arg job_id "$JOB_ID" '{
        "job_id": ($job_id | tonumber),
        "new_settings": {
            "name": .name,
            "tasks": .tasks,
            "max_concurrent_runs": .max_concurrent_runs,
            "format": .format
        }
    }' "$JOB_JSON")
    
    databricks jobs update --profile "$DATABRICKS_PROFILE" --json "$UPDATE_PAYLOAD"
    log_success "Job updated"
else
    log_info "Creating new job..."
    
    # Create the job
    CREATE_RESPONSE=$(databricks jobs create --profile "$DATABRICKS_PROFILE" --json "$(cat "$JOB_JSON")")
    JOB_ID=$(echo "$CREATE_RESPONSE" | jq -r '.job_id')
    log_success "Created job with ID: $JOB_ID"
fi

echo ""
echo "=============================================="
echo "Deployment Complete!"
echo "=============================================="
echo "Job Name: $JOB_NAME"
echo "Job ID:   $JOB_ID"
echo "=============================================="
echo ""
echo "To run the job manually:"
echo "  databricks jobs run-now --job-id $JOB_ID --profile $DATABRICKS_PROFILE"
echo ""

# Optionally run the job
if [[ "$RUN_AFTER_DEPLOY" == "true" ]]; then
    log_info "Triggering job run..."
    
    RUN_RESPONSE=$(databricks jobs run-now "$JOB_ID" --profile "$DATABRICKS_PROFILE" --output json)
    RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
    
    log_success "Job run started with run_id: $RUN_ID"
    
    echo ""
    echo "Monitor the run at:"
    echo "  databricks runs get $RUN_ID --profile $DATABRICKS_PROFILE"
    echo ""
    echo "Or use run_and_wait.py:"
    echo "  uv run scripts/run_and_wait.py --job-id $JOB_ID --profile $DATABRICKS_PROFILE"
    echo ""
    
    # Wait for the run to complete
    log_info "Waiting for job to complete..."
    
    while true; do
        RUN_STATUS=$(databricks runs get "$RUN_ID" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null)
        LIFE_CYCLE_STATE=$(echo "$RUN_STATUS" | jq -r '.state.life_cycle_state')
        RESULT_STATE=$(echo "$RUN_STATUS" | jq -r '.state.result_state // "RUNNING"')
        
        log_info "Status: $LIFE_CYCLE_STATE / $RESULT_STATE"
        
        if [[ "$LIFE_CYCLE_STATE" == "TERMINATED" ]]; then
            if [[ "$RESULT_STATE" == "SUCCESS" ]]; then
                log_success "Job completed successfully!"
                
                # Get task outputs
                echo ""
                echo "Task Results:"
                echo "$RUN_STATUS" | jq '.tasks[] | {task_key, state: .state}'
            else
                log_error "Job failed with result: $RESULT_STATE"
                echo "$RUN_STATUS" | jq '.state'
                exit 1
            fi
            break
        elif [[ "$LIFE_CYCLE_STATE" == "INTERNAL_ERROR" || "$LIFE_CYCLE_STATE" == "SKIPPED" ]]; then
            log_error "Job failed with state: $LIFE_CYCLE_STATE"
            exit 1
        fi
        
        sleep 30
    done
fi
