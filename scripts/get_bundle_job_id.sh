#!/bin/bash
# Get the job ID for a specific job resource from a deployed Databricks Asset Bundle
#
# Usage:
#   ./scripts/get_bundle_job_id.sh [job_resource_key] [target] [profile]
#
# Arguments:
#   job_resource_key  - The job resource key in databricks.yml (default: rlm_orchestrator_job)
#   target            - The bundle target (default: dev)
#   profile           - Databricks CLI profile (default: rstanhope)
#
# Examples:
#   ./scripts/get_bundle_job_id.sh                                    # Get orchestrator job ID
#   ./scripts/get_bundle_job_id.sh rlm_executor_job dev rstanhope     # Get executor job ID
#   ./scripts/get_bundle_job_id.sh rlm_orchestrator_job prod          # Get prod orchestrator job ID
#
# Output:
#   Prints the job ID on the last line (for capture via: JOB_ID=$(./scripts/get_bundle_job_id.sh | tail -n 1))

set -e

# Default values
JOB_RESOURCE_KEY="${1:-rlm_orchestrator_job}"
TARGET="${2:-dev}"
PROFILE="${3:-rstanhope}"

# Get the job name pattern based on the target
# Bundle jobs are named with pattern: "[dev username] [${target}] Job Name"
# We need to match more loosely to handle the prefix
case "$JOB_RESOURCE_KEY" in
    rlm_orchestrator_job)
        JOB_NAME_PATTERN="RLM Orchestrator"
        ;;
    rlm_executor_job)
        JOB_NAME_PATTERN="RLM Executor"
        ;;
    rlm_ingestor_job)
        JOB_NAME_PATTERN="RLM Ingestor"
        ;;
    *)
        # For other jobs, try to match the resource key directly
        JOB_NAME_PATTERN="$JOB_RESOURCE_KEY"
        ;;
esac

echo "Looking up job: $JOB_NAME_PATTERN" >&2
echo "Profile: $PROFILE" >&2
echo "Target: $TARGET" >&2

# Method 1: Try to get job ID from bundle summary (if available)
# This requires the bundle to have been deployed
BUNDLE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if command -v jq &> /dev/null; then
    # Use jq for reliable JSON parsing
    # Use --limit to get more jobs (default is 25)
    # Note: CLI returns array directly, not {"jobs": [...]}
    JOB_ID=$(databricks jobs list --profile "$PROFILE" --limit 100 --output json 2>/dev/null | \
        jq -r --arg pattern "$JOB_NAME_PATTERN" \
        '.[] | select(.settings.name | test($pattern)) | .job_id' 2>/dev/null | \
        head -n 1)
else
    # Fallback: grep-based parsing (less reliable)
    echo "Warning: jq not found, using grep-based parsing" >&2
    JOB_ID=$(databricks jobs list --profile "$PROFILE" --limit 100 2>/dev/null | \
        grep -F "$JOB_NAME_PATTERN" | \
        head -n 1 | \
        awk '{print $1}')
fi

if [[ -z "$JOB_ID" || "$JOB_ID" == "null" ]]; then
    echo "Error: Could not find job matching pattern: $JOB_NAME_PATTERN" >&2
    echo "Make sure you have deployed the bundle first: databricks bundle deploy --profile $PROFILE" >&2
    exit 1
fi

echo "Found job ID: $JOB_ID" >&2

# Output just the job ID on the last line (for scripting)
echo "$JOB_ID"

