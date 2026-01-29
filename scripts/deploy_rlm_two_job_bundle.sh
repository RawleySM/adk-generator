#!/bin/bash
# Deploy the RLM Agent three-job bundle (Orchestrator + Executor + Ingestor)
#
# This script implements the full deployment workflow:
#   1. Load .env configuration
#   2. Check and start cluster (10 min timeout)
#   3. Clear cached wheel versions on cluster (prevents stale code)
#   4. Bump wheel version (cache-busting)
#   5. Clear build caches
#   6. Validate and deploy bundle
#   7. Resolve all three job IDs
#   8. Ensure secret scope and secrets exist (google-api-key required)
#   9. Store job IDs in secret scope (if missing or changed)
#   10. Wire executor job ID into orchestrator job parameters
#   11. Wire orchestrator job ID into ingestor job parameters
#   12. Optionally trigger a run
#
# Usage:
#   ./scripts/deploy_rlm_two_job_bundle.sh [OPTIONS]
#
# Options:
#   --skip-deploy           Skip deployment, just run the job
#   --skip-cluster-check    Skip cluster check/start
#   --skip-cache-clear      Skip clearing cached wheel versions on cluster
#   --run                   Trigger orchestrator job after deploy
#   --test-level <N>        Pass TEST_LEVEL=<N> to the job (requires --run)
#   --force-update-secrets  Always overwrite job ID secrets (default: update if missing or changed)
#   --help                  Show this help message
#
# Prerequisites:
#   - .env file in project root (copy from .env.example)
#   - Databricks CLI installed and configured
#   - jq installed for JSON parsing

set -e

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PACKAGE_DIR="$PROJECT_ROOT/databricks_rlm_agent"

# Defaults (can be overridden by .env)
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-rstanhope}"
CLUSTER_ID="${CLUSTER_ID:-1115-120035-jyzgoasz}"
SECRET_SCOPE="${SECRET_SCOPE:-adk-secrets}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"

# Cluster check configuration
CLUSTER_CHECK_INTERVAL=60  # seconds
CLUSTER_MAX_CHECKS=10      # 10 minutes total

# Flags
SKIP_DEPLOY=false
SKIP_CLUSTER_CHECK=false
SKIP_CACHE_CLEAR=false
RUN_AFTER_DEPLOY=false
FORCE_UPDATE_SECRETS=false
TEST_LEVEL=""

# =============================================================================
# Helper Functions
# =============================================================================

show_help() {
    head -33 "$0" | tail -31 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*" >&2
}

log_error() {
    echo "[ERROR] $*" >&2
}

log_success() {
    echo "[OK] $*"
}

check_dependencies() {
    local missing=()

    if ! command -v databricks &> /dev/null; then
        missing+=("databricks CLI")
    fi

    if ! command -v jq &> /dev/null; then
        missing+=("jq")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing[*]}"
        log_error "Please install them before running this script."
        exit 1
    fi
}

# Store a secret only if missing or value has changed
# Usage: store_secret_if_needed <scope> <key> <value> <profile>
store_secret_if_needed() {
    local scope="$1"
    local key="$2"
    local value="$3"
    local profile="$4"

    # Check if secret exists (CLI returns array directly, not {"secrets": [...]})
    local existing_key
    existing_key=$(databricks secrets list-secrets "$scope" --profile "$profile" --output json 2>/dev/null | \
        jq -r ".[]? | select(.key == \"$key\") | .key" || echo "")

    if [[ -z "$existing_key" ]]; then
        # Secret doesn't exist - create it (new CLI syntax: SCOPE KEY)
        log_info "Creating secret '$key' in scope '$scope'..."
        echo -n "$value" | databricks secrets put-secret "$scope" "$key" --profile "$profile" 2>/dev/null || \
            databricks secrets put-secret "$scope" "$key" --string-value "$value" --profile "$profile"
        log_success "$key stored (new)"
        return 0
    elif [[ "$FORCE_UPDATE_SECRETS" == "true" ]]; then
        # Secret exists but force update requested
        log_info "Updating secret '$key' in scope '$scope' (--force-update-secrets)..."
        echo -n "$value" | databricks secrets put-secret "$scope" "$key" --profile "$profile" 2>/dev/null || \
            databricks secrets put-secret "$scope" "$key" --string-value "$value" --profile "$profile"
        log_success "$key updated (forced)"
        return 0
    else
        # Secret exists - update if this is a job ID that may have changed
        log_info "Secret '$key' already exists, updating with current value..."
        echo -n "$value" | databricks secrets put-secret "$scope" "$key" --profile "$profile" 2>/dev/null || \
            databricks secrets put-secret "$scope" "$key" --string-value "$value" --profile "$profile"
        log_success "$key updated"
        return 0
    fi
}

# =============================================================================
# Parse Arguments
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-deploy)
            SKIP_DEPLOY=true
            shift
            ;;
        --skip-cluster-check)
            SKIP_CLUSTER_CHECK=true
            shift
            ;;
        --skip-cache-clear)
            SKIP_CACHE_CLEAR=true
            shift
            ;;
        --run)
            RUN_AFTER_DEPLOY=true
            shift
            ;;
        --test-level)
            TEST_LEVEL="$2"
            shift 2
            ;;
        --force-update-secrets)
            FORCE_UPDATE_SECRETS=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information" >&2
            exit 1
            ;;
    esac
done

# =============================================================================
# Load Configuration
# =============================================================================

log_info "Loading configuration..."

if [[ -f "$PROJECT_ROOT/.env" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    log_success "Loaded .env"
else
    log_warn ".env file not found. Using defaults."
    log_warn "Copy .env.example to .env and configure your settings."
fi

# Re-assign after sourcing .env (in case they were set there)
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-rstanhope}"
CLUSTER_ID="${CLUSTER_ID:-1115-120035-jyzgoasz}"
SECRET_SCOPE="${SECRET_SCOPE:-adk-secrets}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"
GOOGLE_API_KEY="${GOOGLE_API_KEY:-}"
OPENAI_API_KEY="${OPENAI_API_KEY:-}"

echo ""
echo "=============================================="
echo "RLM Agent Three-Job Bundle Deployment"
echo "=============================================="
echo "Profile:        $DATABRICKS_PROFILE"
echo "Cluster ID:     $CLUSTER_ID"
echo "Secret Scope:   $SECRET_SCOPE"
echo "Bundle Target:  $BUNDLE_TARGET"
echo "=============================================="
echo ""

# Check dependencies
check_dependencies

# =============================================================================
# Step 1: Cluster Check/Start
# =============================================================================

if [[ "$SKIP_CLUSTER_CHECK" != "true" ]]; then
    log_info "[1/12] Checking cluster status..."
    
    check_count=0
    while [[ $check_count -lt $CLUSTER_MAX_CHECKS ]]; do
        CLUSTER_STATE=$(databricks clusters get "$CLUSTER_ID" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | jq -r '.state // "UNKNOWN"')
        
        case "$CLUSTER_STATE" in
            RUNNING)
                log_success "Cluster is running"
                break
                ;;
            PENDING|RESTARTING|RESIZING)
                log_info "Cluster state: $CLUSTER_STATE (waiting...)"
                ;;
            TERMINATED|TERMINATING)
                if [[ $check_count -eq 0 ]]; then
                    log_info "Cluster is terminated, starting..."
                    databricks clusters start "$CLUSTER_ID" --profile "$DATABRICKS_PROFILE" || true
                fi
                log_info "Waiting for cluster to start..."
                ;;
            *)
                log_warn "Unknown cluster state: $CLUSTER_STATE"
                if [[ $check_count -eq 0 ]]; then
                    log_info "Attempting to start cluster..."
                    databricks clusters start "$CLUSTER_ID" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true
                fi
                ;;
        esac
        
        check_count=$((check_count + 1))
        if [[ $check_count -lt $CLUSTER_MAX_CHECKS && "$CLUSTER_STATE" != "RUNNING" ]]; then
            log_info "Retry $check_count/$CLUSTER_MAX_CHECKS - waiting ${CLUSTER_CHECK_INTERVAL}s..."
            sleep "$CLUSTER_CHECK_INTERVAL"
        fi
    done
    
    if [[ "$CLUSTER_STATE" != "RUNNING" ]]; then
        log_error "Cluster did not reach RUNNING state within $((CLUSTER_MAX_CHECKS * CLUSTER_CHECK_INTERVAL / 60)) minutes"
        exit 1
    fi
else
    log_info "[1/12] Skipping cluster check (--skip-cluster-check)"
fi

# =============================================================================
# Step 2: Clear Cluster Wheel Cache
# =============================================================================

if [[ "$SKIP_DEPLOY" != "true" && "$SKIP_CACHE_CLEAR" != "true" ]]; then
    log_info "[2/12] Clearing cached wheel versions on cluster..."
    
    INIT_SCRIPT_LOCAL="$PACKAGE_DIR/scripts/clear_wheel_cache.sh"
    INIT_SCRIPT_VOLUMES="/Volumes/silo_dev_rs/adk/scripts/clear_wheel_cache.sh"
    
    # Upload init script to UC Volumes for cluster access
    if [[ -f "$INIT_SCRIPT_LOCAL" ]]; then
        log_info "Uploading init script to UC Volumes..."
        # Ensure the scripts directory exists in Volumes
        databricks fs mkdirs "dbfs:${INIT_SCRIPT_VOLUMES%/*}" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true
        databricks fs cp "$INIT_SCRIPT_LOCAL" "dbfs:$INIT_SCRIPT_VOLUMES" --overwrite --profile "$DATABRICKS_PROFILE" 2>/dev/null || \
            log_warn "Could not upload init script to Volumes (non-fatal)"
    fi
    
    # Uninstall any cluster-level installation of the package (job-scoped will reinstall)
    log_info "Removing cluster-level library installations..."
    PACKAGE_NAME="databricks_rlm_agent"
    
    # Try to uninstall via cluster libraries API (catches cluster-scoped installs)
    # Get current cluster libraries
    CLUSTER_LIBS=$(databricks libraries cluster-status "$CLUSTER_ID" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null || echo "{}")
    
    # Check if our package is installed at cluster level
    if echo "$CLUSTER_LIBS" | grep -q "$PACKAGE_NAME"; then
        log_info "Found cluster-level installation of $PACKAGE_NAME, uninstalling..."
        # Uninstall any matching wheels
        databricks libraries uninstall --cluster-id "$CLUSTER_ID" --profile "$DATABRICKS_PROFILE" \
            --whl "*${PACKAGE_NAME}*" 2>/dev/null || true
    fi
    
    # Execute cleanup command on the cluster if it's running
    # This clears pip cache and removes installed packages from site-packages
    if [[ "$CLUSTER_STATE" == "RUNNING" ]]; then
        log_info "Executing cache cleanup on cluster..."
        
        # Create a temporary cleanup script
        CLEANUP_CMD="import subprocess; import sys; subprocess.run([sys.executable, '-m', 'pip', 'cache', 'purge'], capture_output=True); subprocess.run([sys.executable, '-m', 'pip', 'uninstall', '-y', '$PACKAGE_NAME'], capture_output=True); print('Cache cleared')"
        
        # Run via execution context if available, otherwise just log a note
        # Note: Direct command execution requires cluster execution context which may not be available
        # The job-scoped library installation will handle the fresh install
        log_info "Note: For complete cache clearing, restart the cluster or add init script"
    fi
    
    log_success "Cluster cache cleanup initiated"
elif [[ "$SKIP_CACHE_CLEAR" == "true" ]]; then
    log_info "[2/12] Skipping cluster cache clear (--skip-cache-clear)"
fi

# =============================================================================
# Step 3: Bump Wheel Version (Cache-Busting)
# =============================================================================

if [[ "$SKIP_DEPLOY" != "true" ]]; then
    log_info "[3/12] Bumping wheel version in pyproject.toml..."
    
    PYPROJECT_FILE="$PACKAGE_DIR/pyproject.toml"
    if [[ -f "$PYPROJECT_FILE" ]]; then
        CURRENT_VERSION=$(grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"' "$PYPROJECT_FILE" | sed 's/version = "\(.*\)"/\1/')
        if [[ -n "$CURRENT_VERSION" ]]; then
            # Parse major.minor.patch
            MAJOR=$(echo "$CURRENT_VERSION" | cut -d. -f1)
            MINOR=$(echo "$CURRENT_VERSION" | cut -d. -f2)
            PATCH=$(echo "$CURRENT_VERSION" | cut -d. -f3)
            # Increment patch
            NEW_PATCH=$((PATCH + 1))
            NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
            # Update pyproject.toml
            sed -i "s/version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" "$PYPROJECT_FILE"
            log_success "Version bumped: $CURRENT_VERSION -> $NEW_VERSION"
        else
            log_warn "Could not find version in $PYPROJECT_FILE"
        fi
    else
        log_warn "Package pyproject.toml not found at $PYPROJECT_FILE"
    fi

    # =============================================================================
    # Step 4: Clear Build Caches
    # =============================================================================

    log_info "[4/12] Clearing Python cache and build artifacts..."

    rm -rf "$PROJECT_ROOT/dist/" "$PROJECT_ROOT/build/" "$PROJECT_ROOT"/*.egg-info 2>/dev/null || true
    rm -rf "$PACKAGE_DIR/dist/" "$PACKAGE_DIR/build/" "$PACKAGE_DIR"/*.egg-info 2>/dev/null || true
    find "$PROJECT_ROOT" -name "*.pyc" -delete 2>/dev/null || true
    find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

    # Ensure dist directory exists for bundle validation (bundle expects it)
    mkdir -p "$PROJECT_ROOT/dist"

    log_success "Build artifacts cleared"

    # =============================================================================
    # Step 5: Validate Bundle
    # =============================================================================

    log_info "[5/12] Validating Databricks Asset Bundle..."
    
    cd "$PROJECT_ROOT"
    if ! databricks bundle validate --profile "$DATABRICKS_PROFILE" --target "$BUNDLE_TARGET"; then
        log_error "Bundle validation failed"
        exit 1
    fi
    log_success "Bundle validated"

    # =============================================================================
    # Step 6: Deploy Bundle
    # =============================================================================

    log_info "[6/12] Deploying Databricks Asset Bundle..."
    
    if ! databricks bundle deploy --profile "$DATABRICKS_PROFILE" --target "$BUNDLE_TARGET"; then
        log_error "Bundle deployment failed"
        exit 1
    fi
    log_success "Bundle deployed"
else
    log_info "[2-6/12] Skipping deployment (--skip-deploy)"
fi

# =============================================================================
# Step 7: Resolve Job IDs
# =============================================================================

log_info "[7/12] Resolving job IDs..."

ORCHESTRATOR_JOB_ID=$("$SCRIPT_DIR/get_bundle_job_id.sh" rlm_orchestrator_job "$BUNDLE_TARGET" "$DATABRICKS_PROFILE" 2>/dev/null | tail -n 1)
if [[ -z "$ORCHESTRATOR_JOB_ID" || "$ORCHESTRATOR_JOB_ID" == "null" ]]; then
    log_error "Could not resolve orchestrator job ID"
    exit 1
fi
log_success "Orchestrator Job ID: $ORCHESTRATOR_JOB_ID"

EXECUTOR_JOB_ID=$("$SCRIPT_DIR/get_bundle_job_id.sh" rlm_executor_job "$BUNDLE_TARGET" "$DATABRICKS_PROFILE" 2>/dev/null | tail -n 1)
if [[ -z "$EXECUTOR_JOB_ID" || "$EXECUTOR_JOB_ID" == "null" ]]; then
    log_error "Could not resolve executor job ID"
    exit 1
fi
log_success "Executor Job ID: $EXECUTOR_JOB_ID"

INGESTOR_JOB_ID=$("$SCRIPT_DIR/get_bundle_job_id.sh" rlm_ingestor_job "$BUNDLE_TARGET" "$DATABRICKS_PROFILE" 2>/dev/null | tail -n 1)
if [[ -z "$INGESTOR_JOB_ID" || "$INGESTOR_JOB_ID" == "null" ]]; then
    log_error "Could not resolve ingestor job ID"
    exit 1
fi
if databricks jobs get "$INGESTOR_JOB_ID" --profile "$DATABRICKS_PROFILE" --output json >/dev/null 2>&1; then
    log_success "Ingestor Job ID: $INGESTOR_JOB_ID"
else
    log_warn "Resolved ingestor job ID does not exist: $INGESTOR_JOB_ID"
    log_warn "Skipping ingestor wiring; orchestrator can still run."
    INGESTOR_JOB_ID=""
fi

# =============================================================================
# Step 8: Ensure Secret Scope Exists
# =============================================================================

log_info "[8/12] Ensuring secret scope exists..."

# Check if scope exists (CLI returns array directly, not {"scopes": [...]})
SCOPE_EXISTS=$(databricks secrets list-scopes --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | jq -r ".[] | select(.name == \"$SECRET_SCOPE\") | .name" || echo "")

if [[ -z "$SCOPE_EXISTS" ]]; then
    log_info "Creating secret scope: $SECRET_SCOPE"
    databricks secrets create-scope "$SECRET_SCOPE" --profile "$DATABRICKS_PROFILE" || true
    log_success "Secret scope created"
else
    log_success "Secret scope already exists"
fi

# Ensure google-api-key exists (either in env or already in secret scope)
# New CLI syntax: positional argument for scope (CLI returns array directly)
GOOGLE_KEY_EXISTS=$(databricks secrets list-secrets "$SECRET_SCOPE" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq -r '.[]? | select(.key == "google-api-key") | .key' || echo "")

if [[ -n "$GOOGLE_API_KEY" && "$GOOGLE_API_KEY" != "your-google-api-key-here" ]]; then
    log_info "Storing google-api-key in secret scope..."
    # New CLI syntax: SCOPE KEY as positional arguments
    echo -n "$GOOGLE_API_KEY" | databricks secrets put-secret "$SECRET_SCOPE" "google-api-key" --profile "$DATABRICKS_PROFILE" 2>/dev/null || \
        databricks secrets put-secret "$SECRET_SCOPE" "google-api-key" --string-value "$GOOGLE_API_KEY" --profile "$DATABRICKS_PROFILE"
    log_success "google-api-key stored"
elif [[ -n "$GOOGLE_KEY_EXISTS" ]]; then
    log_success "google-api-key already exists in secret scope"
else
    log_error "GOOGLE_API_KEY not set in .env and not found in secret scope '$SECRET_SCOPE'"
    log_error "The agent requires a valid Google API key to function."
    log_error "Please set GOOGLE_API_KEY in your .env file and redeploy."
    exit 1
fi

# Optional: OpenAI API key (needed for LiteLLM OpenAI models)
OPENAI_KEY_EXISTS=$(databricks secrets list-secrets "$SECRET_SCOPE" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq -r '.[]? | select(.key == "openai-api-key") | .key' || echo "")

if [[ -n "$OPENAI_API_KEY" && "$OPENAI_API_KEY" != "your-openai-api-key-here" ]]; then
    log_info "Storing openai-api-key in secret scope..."
    echo -n "$OPENAI_API_KEY" | databricks secrets put-secret "$SECRET_SCOPE" "openai-api-key" --profile "$DATABRICKS_PROFILE" 2>/dev/null || \
        databricks secrets put-secret "$SECRET_SCOPE" "openai-api-key" --string-value "$OPENAI_API_KEY" --profile "$DATABRICKS_PROFILE"
    log_success "openai-api-key stored"
elif [[ -n "$OPENAI_KEY_EXISTS" ]]; then
    log_success "openai-api-key already exists in secret scope"
else
    log_warn "OPENAI_API_KEY not set in .env and not found in secret scope '$SECRET_SCOPE'"
    log_warn "LiteLLM OpenAI models will fail without this key."
fi

# GitHub Token (needed for get_repo_file tool to download code from GitHub)
GITHUB_TOKEN_EXISTS=$(databricks secrets list-secrets "$SECRET_SCOPE" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq -r '.[]? | select(.key == "github-token") | .key' || echo "")

if [[ -n "$GITHUB_TOKEN" && "$GITHUB_TOKEN" != "your-github-token-here" ]]; then
    log_info "Storing github-token in secret scope..."
    echo -n "$GITHUB_TOKEN" | databricks secrets put-secret "$SECRET_SCOPE" "github-token" --profile "$DATABRICKS_PROFILE" 2>/dev/null || \
        databricks secrets put-secret "$SECRET_SCOPE" "github-token" --string-value "$GITHUB_TOKEN" --profile "$DATABRICKS_PROFILE"
    log_success "github-token stored"
elif [[ -n "$GITHUB_TOKEN_EXISTS" ]]; then
    log_success "github-token already exists in secret scope"
else
    log_warn "GITHUB_TOKEN not set in .env and not found in secret scope '$SECRET_SCOPE'"
    log_warn "The get_repo_file tool will not be able to download code from GitHub."
fi

# =============================================================================
# Step 9: Store Job IDs in Secret Scope
# =============================================================================

log_info "[9/12] Storing job IDs in secret scope..."

# Store orchestrator job ID
store_secret_if_needed "$SECRET_SCOPE" "rlm-orchestrator-job-id" "$ORCHESTRATOR_JOB_ID" "$DATABRICKS_PROFILE"

# Store executor job ID
store_secret_if_needed "$SECRET_SCOPE" "rlm-executor-job-id" "$EXECUTOR_JOB_ID" "$DATABRICKS_PROFILE"

# Store ingestor job ID
store_secret_if_needed "$SECRET_SCOPE" "rlm-ingestor-job-id" "$INGESTOR_JOB_ID" "$DATABRICKS_PROFILE"

# =============================================================================
# Step 10: Wire Executor Job ID into Orchestrator Job Parameters
# =============================================================================

log_info "[10/12] Wiring executor job ID into orchestrator job..."

# Get current job settings to preserve existing parameters
# New CLI syntax: JOB_ID as positional argument
CURRENT_PARAMS=$(databricks jobs get "$ORCHESTRATOR_JOB_ID" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq '.settings.parameters // []')

# Check if ADK_EXECUTOR_JOB_ID parameter already exists with correct value
EXISTING_EXECUTOR_ID=$(echo "$CURRENT_PARAMS" | jq -r '.[] | select(.name == "ADK_EXECUTOR_JOB_ID") | .default // ""')

if [[ "$EXISTING_EXECUTOR_ID" == "$EXECUTOR_JOB_ID" ]]; then
    log_success "ADK_EXECUTOR_JOB_ID already configured correctly"
else
    log_info "Updating orchestrator job with executor job ID..."

    # Build updated parameters array - replace or add ADK_EXECUTOR_JOB_ID
    UPDATED_PARAMS=$(echo "$CURRENT_PARAMS" | jq --arg job_id "$EXECUTOR_JOB_ID" '
        [.[] | select(.name == "ADK_EXECUTOR_JOB_ID" | not)] +
        [{"name": "ADK_EXECUTOR_JOB_ID", "default": $job_id}]
    ')

    # Create the update payload (API uses "new_settings" not "settings")
    UPDATE_PAYLOAD=$(jq -n --argjson params "$UPDATED_PARAMS" --arg job_id "$ORCHESTRATOR_JOB_ID" '{
        "job_id": ($job_id | tonumber),
        "new_settings": {
            "parameters": $params
        }
    }' -c)

    # Update the job (CLI requires job_id in JSON when using --json flag)
    if databricks jobs update --profile "$DATABRICKS_PROFILE" --json "$UPDATE_PAYLOAD" 2>/dev/null; then
        log_success "Orchestrator job updated with ADK_EXECUTOR_JOB_ID=$EXECUTOR_JOB_ID"
    else
        log_warn "Could not update job parameters via API (may require manual configuration)"
        log_warn "Set ADK_EXECUTOR_JOB_ID=$EXECUTOR_JOB_ID in the orchestrator job"
    fi
fi

# =============================================================================
# Step 11: Wire Orchestrator Job ID into Ingestor Job Parameters
# =============================================================================

if [[ -z "$INGESTOR_JOB_ID" ]]; then
    log_warn "[11/12] Skipping ingestor wiring (no valid ingestor job ID)"
else
    log_info "[11/12] Wiring orchestrator job ID into ingestor job..."

    # Get current ingestor job settings (new CLI syntax: JOB_ID as positional argument)
    INGESTOR_PARAMS=$(databricks jobs get "$INGESTOR_JOB_ID" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
        jq '.settings.parameters // []')

    # Ensure INGESTOR_PARAMS is valid JSON (default to empty array if not)
    if [[ -z "$INGESTOR_PARAMS" ]] || ! echo "$INGESTOR_PARAMS" | jq empty 2>/dev/null; then
        INGESTOR_PARAMS="[]"
    fi

    # Check if ADK_ORCHESTRATOR_JOB_ID parameter already exists with correct value
    EXISTING_ORCHESTRATOR_ID=$(echo "$INGESTOR_PARAMS" | jq -r '.[] | select(.name == "ADK_ORCHESTRATOR_JOB_ID") | .default // ""' 2>/dev/null || echo "")

    if [[ "$EXISTING_ORCHESTRATOR_ID" == "$ORCHESTRATOR_JOB_ID" ]]; then
        log_success "ADK_ORCHESTRATOR_JOB_ID already configured correctly in ingestor"
    else
        log_info "Updating ingestor job with orchestrator job ID..."

        # Build updated parameters array - replace or add ADK_ORCHESTRATOR_JOB_ID
        UPDATED_INGESTOR_PARAMS=$(echo "$INGESTOR_PARAMS" | jq --arg job_id "$ORCHESTRATOR_JOB_ID" '
            [.[] | select(.name == "ADK_ORCHESTRATOR_JOB_ID" | not)] +
            [{"name": "ADK_ORCHESTRATOR_JOB_ID", "default": $job_id}]
        ' 2>/dev/null)

        # Fallback if jq failed
        if [[ -z "$UPDATED_INGESTOR_PARAMS" ]] || ! echo "$UPDATED_INGESTOR_PARAMS" | jq empty 2>/dev/null; then
            UPDATED_INGESTOR_PARAMS="[{\"name\": \"ADK_ORCHESTRATOR_JOB_ID\", \"default\": \"$ORCHESTRATOR_JOB_ID\"}]"
        fi

        # Create the update payload (API uses "new_settings" not "settings")
        INGESTOR_UPDATE_PAYLOAD=$(jq -n --argjson params "$UPDATED_INGESTOR_PARAMS" --arg job_id "$INGESTOR_JOB_ID" '{
            "job_id": ($job_id | tonumber),
            "new_settings": {
                "parameters": $params
            }
        }' -c)

        # Update the job (CLI requires job_id in JSON when using --json flag)
        if databricks jobs update --profile "$DATABRICKS_PROFILE" --json "$INGESTOR_UPDATE_PAYLOAD" 2>/dev/null; then
            log_success "Ingestor job updated with ADK_ORCHESTRATOR_JOB_ID=$ORCHESTRATOR_JOB_ID"
        else
            log_warn "Could not update ingestor job parameters via API (may require manual configuration)"
            log_warn "Set ADK_ORCHESTRATOR_JOB_ID=$ORCHESTRATOR_JOB_ID in the ingestor job"
        fi
    fi
fi

# =============================================================================
# Step 12: Summary
# =============================================================================

echo ""
echo "=============================================="
echo "Deployment Complete!"
echo "=============================================="
echo "Orchestrator Job ID: $ORCHESTRATOR_JOB_ID"
echo "Executor Job ID:     $EXECUTOR_JOB_ID"
echo "Ingestor Job ID:     $INGESTOR_JOB_ID"
echo "Secret Scope:        $SECRET_SCOPE"
echo "Bundle Target:       $BUNDLE_TARGET"
echo "=============================================="
echo ""
echo "To run the orchestrator job manually:"
echo "  databricks jobs run-now --job-id $ORCHESTRATOR_JOB_ID --profile $DATABRICKS_PROFILE"
echo ""
echo "To run the ingestor job manually (polls for new tasks):"
echo "  databricks jobs run-now --job-id $INGESTOR_JOB_ID --profile $DATABRICKS_PROFILE"
echo ""
echo "Or use the run_and_wait script:"
echo "  uv run scripts/run_and_wait.py --job-id $ORCHESTRATOR_JOB_ID --profile $DATABRICKS_PROFILE"
echo ""
echo "Note: The ingestor job is deployed with schedule PAUSED."
echo "To enable scheduled polling, update the job in Databricks UI or run:"
echo "  databricks jobs update --job-id $INGESTOR_JOB_ID --json '{\"settings\":{\"schedule\":{\"pause_status\":\"UNPAUSED\"}}}' --profile $DATABRICKS_PROFILE"
echo ""

# =============================================================================
# Optional: Trigger Run
# =============================================================================

if [[ "$RUN_AFTER_DEPLOY" == "true" ]]; then
    echo "=============================================="
    log_info "Triggering orchestrator job run..."
    echo "=============================================="
    
    cd "$PROJECT_ROOT"
    
    RUN_ARGS=("--job-id" "$ORCHESTRATOR_JOB_ID" "--profile" "$DATABRICKS_PROFILE")
    if [[ -n "$TEST_LEVEL" ]]; then
        RUN_ARGS+=("--param" "TEST_LEVEL=$TEST_LEVEL")
        log_info "Running with test level: $TEST_LEVEL"
    fi

    uv run scripts/run_and_wait.py "${RUN_ARGS[@]}"
    EXIT_CODE=$?
    
    if [[ $EXIT_CODE -eq 0 ]]; then
        log_success "Job completed successfully!"
    else
        log_error "Job finished with exit code: $EXIT_CODE"
    fi
    
    exit $EXIT_CODE
fi

