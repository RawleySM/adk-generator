#!/bin/bash
# =============================================================================
# Local Runtime for Databricks RLM Agent
# =============================================================================
# This script provides a local entrypoint for running the RLM Agent without
# deploying to Databricks. It uses local DuckDB for session persistence and
# connects to UC via SQL Warehouse API for data access.
#
# Usage Examples:
#   ./scripts/run_local_rlm.sh --test-level 7
#   ./scripts/run_local_rlm.sh --prompt-file ./my_task.txt
#   ./scripts/run_local_rlm.sh --profile rstanhope --warehouse-id abc123 --test-level 3
#   ./scripts/run_local_rlm.sh --prompt "Hello, describe your capabilities"
#   ./scripts/run_local_rlm.sh --checks-only
#   ./scripts/run_local_rlm.sh --trace --test-level 1
#
# Prerequisites:
#   - Python 3.10+ or uv installed
#   - GOOGLE_API_KEY set (required for Gemini models)
#   - Databricks authentication configured (profile or HOST+TOKEN)
#
# Environment Variables:
#   ADK_RUN_MODE=local (set automatically)
#   ADK_LOCAL_DB_PATH=.adk_local/adk.duckdb (default)
#   ADK_LOCAL_ARTIFACTS_PATH=.adk_local/artifacts (default)
#   ADK_SQL_WAREHOUSE_ID (optional, auto-discovers if not set)
#   DATABRICKS_PROFILE (default: rstanhope)
#   GOOGLE_API_KEY (required)
#   OPENAI_API_KEY (optional, for LiteLLM OpenAI models)
#   GITHUB_TOKEN (optional, for get_repo_file tool)
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration Defaults
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PACKAGE_DIR="$PROJECT_ROOT/databricks_rlm_agent"

# Defaults (can be overridden by .env or CLI args)
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-rstanhope}"
ADK_LOCAL_DB_PATH="${ADK_LOCAL_DB_PATH:-.adk_local/adk.duckdb}"
ADK_LOCAL_ARTIFACTS_PATH="${ADK_LOCAL_ARTIFACTS_PATH:-.adk_local/artifacts}"
ADK_SQL_WAREHOUSE_ID="${ADK_SQL_WAREHOUSE_ID:-}"
ADK_MAX_ITERATIONS="${ADK_MAX_ITERATIONS:-}"

# CLI argument defaults
TEST_LEVEL=""
PROMPT_FILE=""
PROMPT_STRING=""
SESSION_ID=""
DRY_RUN_CHECKS=false
LOG_LEVEL="INFO"
TRACE_MODE=false

# =============================================================================
# Logging Helpers
# =============================================================================

_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
    echo "[$(_timestamp)] [INFO] $*"
}

log_warn() {
    echo "[$(_timestamp)] [WARN] $*" >&2
}

log_error() {
    echo "[$(_timestamp)] [ERROR] $*" >&2
}

log_success() {
    echo "[$(_timestamp)] [OK] $*"
}

log_debug() {
    if [[ "$LOG_LEVEL" == "DEBUG" ]]; then
        echo "[$(_timestamp)] [DEBUG] $*"
    fi
}

log_banner() {
    echo ""
    echo "=============================================="
    echo "$*"
    echo "=============================================="
}

die() {
    log_error "$*"
    exit 1
}

# =============================================================================
# Help
# =============================================================================

show_help() {
    cat << 'EOF'
Usage: ./scripts/run_local_rlm.sh [OPTIONS]

Local runtime entrypoint for the Databricks RLM Agent. Runs the agent locally
with DuckDB session persistence and SQL Warehouse API for UC data access.

Options:
  --test-level <N>        Load prompt from test_tasks.py level N (1-17)
  --prompt-file <path>    Read prompt from a local file
  --prompt <string>       Use literal prompt string
  --session-id <id>       Session ID (default: auto-generated with timestamp)
  --max-iterations <N>    Maximum loop iterations (sets ADK_MAX_ITERATIONS)
  --profile <name>        Databricks CLI profile (default: rstanhope)
  --warehouse-id <id>     SQL Warehouse ID (default: auto-discover)
  --local-db <path>       Local DuckDB path (default: .adk_local/adk.duckdb)
  --local-artifacts <path> Local artifacts path (default: .adk_local/artifacts)
  --dry-run-checks        Run checks only, don't start the agent
  --checks-only           Alias for --dry-run-checks
  --log-level <LEVEL>     Log level: INFO or DEBUG (default: INFO)
  --trace                 Enable bash tracing (set -x with timestamps)
  --help, -h              Show this help message

Examples:
  # Run test task level 7
  ./scripts/run_local_rlm.sh --test-level 7

  # Run with custom prompt from file
  ./scripts/run_local_rlm.sh --prompt-file ./my_task.txt

  # Run with explicit profile and warehouse
  ./scripts/run_local_rlm.sh --profile rstanhope --warehouse-id abc123 --test-level 3

  # Run with literal prompt
  ./scripts/run_local_rlm.sh --prompt "Hello, describe your capabilities"

  # Check environment without running agent
  ./scripts/run_local_rlm.sh --checks-only

  # Debug mode with tracing
  ./scripts/run_local_rlm.sh --trace --log-level DEBUG --test-level 1

Prerequisites:
  - GOOGLE_API_KEY must be set (required for Gemini models)
  - Databricks authentication: either DATABRICKS_PROFILE or HOST+TOKEN
  - Python 3.10+ with databricks-sdk, google-genai dependencies

EOF
    exit 0
}

# =============================================================================
# Parse Arguments
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --test-level)
            TEST_LEVEL="$2"
            shift 2
            ;;
        --prompt-file)
            PROMPT_FILE="$2"
            shift 2
            ;;
        --prompt)
            PROMPT_STRING="$2"
            shift 2
            ;;
        --session-id)
            SESSION_ID="$2"
            shift 2
            ;;
        --max-iterations)
            ADK_MAX_ITERATIONS="$2"
            shift 2
            ;;
        --profile)
            DATABRICKS_PROFILE="$2"
            shift 2
            ;;
        --warehouse-id)
            ADK_SQL_WAREHOUSE_ID="$2"
            shift 2
            ;;
        --local-db)
            ADK_LOCAL_DB_PATH="$2"
            shift 2
            ;;
        --local-artifacts)
            ADK_LOCAL_ARTIFACTS_PATH="$2"
            shift 2
            ;;
        --dry-run-checks|--checks-only)
            DRY_RUN_CHECKS=true
            shift
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --trace)
            TRACE_MODE=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            die "Unknown option: $1. Use --help for usage information."
            ;;
    esac
done

# Enable tracing if requested
if [[ "$TRACE_MODE" == "true" ]]; then
    export PS4='+ [$(_timestamp)] ${BASH_SOURCE[0]##*/}:${LINENO}: '
    set -x
fi

# =============================================================================
# Phase 1: Load Configuration
# =============================================================================

log_banner "Phase 1: Loading Configuration"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
    log_info "Loading .env from $PROJECT_ROOT/.env"
    # shellcheck source=/dev/null
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    log_success ".env loaded"
else
    log_warn ".env file not found at $PROJECT_ROOT/.env"
    log_warn "Using environment variables and defaults only"
fi

# Re-assign after sourcing .env (CLI args take precedence)
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-rstanhope}"

# =============================================================================
# Phase 2: Dependency Checks
# =============================================================================

log_banner "Phase 2: Dependency Checks"

check_command() {
    local cmd="$1"
    local required="${2:-true}"
    if command -v "$cmd" &> /dev/null; then
        log_success "$cmd: found ($(command -v "$cmd"))"
        return 0
    else
        if [[ "$required" == "true" ]]; then
            die "$cmd: NOT FOUND (required)"
        else
            log_warn "$cmd: not found (optional)"
            return 1
        fi
    fi
}

# Check required commands
log_info "Checking required commands..."

# Check for Python (prefer uv if available)
if command -v uv &> /dev/null; then
    PYTHON_CMD="uv run python"
    log_success "uv: found (will use 'uv run python')"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
    log_success "python3: found"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
    log_success "python: found"
else
    die "No Python interpreter found. Install Python 3.10+ or uv."
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
log_info "Python version: $PYTHON_VERSION"

# Optional commands
check_command "databricks" false || true
check_command "jq" false || true

# =============================================================================
# Phase 3: Environment Variable Checks
# =============================================================================

log_banner "Phase 3: Environment Variable Checks"

# Function to check env var (shows YES/NO, never the value for secrets)
check_env_secret() {
    local var_name="$1"
    local required="${2:-false}"
    local value="${!var_name:-}"
    
    if [[ -n "$value" ]]; then
        log_success "$var_name: YES (set)"
        return 0
    else
        if [[ "$required" == "true" ]]; then
            log_error "$var_name: NOT SET (required)"
            return 1
        else
            log_warn "$var_name: not set (optional)"
            return 0
        fi
    fi
}

check_env_value() {
    local var_name="$1"
    local value="${!var_name:-}"
    local default="${2:-}"
    
    if [[ -n "$value" ]]; then
        log_info "$var_name: $value"
    elif [[ -n "$default" ]]; then
        log_info "$var_name: <not set, default: $default>"
    else
        log_info "$var_name: <not set>"
    fi
}

log_info "Checking API keys..."

# GOOGLE_API_KEY is required
if ! check_env_secret "GOOGLE_API_KEY" true; then
    die "GOOGLE_API_KEY is required for Gemini models. Set it in .env or environment."
fi

# Optional API keys
check_env_secret "OPENAI_API_KEY" false
check_env_secret "GITHUB_TOKEN" false

log_info ""
log_info "Checking Databricks configuration..."
check_env_value "DATABRICKS_PROFILE" "$DATABRICKS_PROFILE"

# Check if DATABRICKS_HOST and DATABRICKS_TOKEN are set (alternative to profile)
DATABRICKS_HOST_SET="NO"
DATABRICKS_TOKEN_SET="NO"
if [[ -n "${DATABRICKS_HOST:-}" ]]; then
    DATABRICKS_HOST_SET="YES"
fi
if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
    DATABRICKS_TOKEN_SET="YES"
fi
log_info "DATABRICKS_HOST: $DATABRICKS_HOST_SET"
log_info "DATABRICKS_TOKEN: $DATABRICKS_TOKEN_SET"

# =============================================================================
# Phase 4: Databricks Authentication Check
# =============================================================================

log_banner "Phase 4: Databricks Authentication Check"

log_info "Verifying Databricks authentication..."

# Create a Python script for auth check
AUTH_CHECK_RESULT=$($PYTHON_CMD << 'PYTHON_AUTH_CHECK'
import os
import sys

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print("ERROR: databricks-sdk not installed. Run: uv pip install databricks-sdk")
    sys.exit(1)

profile = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
host = os.environ.get("DATABRICKS_HOST", "")
token = os.environ.get("DATABRICKS_TOKEN", "")

auth_method = "unknown"
user_info = "unknown"

# Try profile auth first
try:
    client = WorkspaceClient(profile=profile)
    me = client.current_user.me()
    auth_method = f"profile:{profile}"
    user_info = me.user_name or me.display_name or str(me.id)
    print(f"OK|{auth_method}|{user_info}")
    sys.exit(0)
except Exception as e:
    pass

# Try env var auth
if host and token:
    try:
        client = WorkspaceClient(host=host, token=token)
        me = client.current_user.me()
        auth_method = "env:HOST+TOKEN"
        user_info = me.user_name or me.display_name or str(me.id)
        print(f"OK|{auth_method}|{user_info}")
        sys.exit(0)
    except Exception as e:
        pass

# Try default auth (no profile)
try:
    client = WorkspaceClient()
    me = client.current_user.me()
    auth_method = "default"
    user_info = me.user_name or me.display_name or str(me.id)
    print(f"OK|{auth_method}|{user_info}")
    sys.exit(0)
except Exception as e:
    pass

print(f"ERROR|none|Authentication failed")
print("Remediation steps:", file=sys.stderr)
print(f"  1. Run: databricks auth login --profile {profile}", file=sys.stderr)
print("  2. Or set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables", file=sys.stderr)
sys.exit(1)
PYTHON_AUTH_CHECK
) || true

# Parse auth check result
AUTH_STATUS=$(echo "$AUTH_CHECK_RESULT" | head -1 | cut -d'|' -f1)
AUTH_METHOD=$(echo "$AUTH_CHECK_RESULT" | head -1 | cut -d'|' -f2)
AUTH_USER=$(echo "$AUTH_CHECK_RESULT" | head -1 | cut -d'|' -f3)

if [[ "$AUTH_STATUS" == "OK" ]]; then
    log_success "Databricks authentication successful"
    log_info "  Auth method: $AUTH_METHOD"
    log_info "  User: $AUTH_USER"
else
    log_error "Databricks authentication failed"
    echo "$AUTH_CHECK_RESULT" | tail -n +2 >&2
    die "Cannot proceed without Databricks authentication"
fi

# =============================================================================
# Phase 5: SQL Warehouse Health Check
# =============================================================================

log_banner "Phase 5: SQL Warehouse Health Check"

log_info "Checking SQL Warehouse connectivity..."

# Export env vars for the Python check
export DATABRICKS_PROFILE
export ADK_SQL_WAREHOUSE_ID

WAREHOUSE_CHECK_RESULT=$($PYTHON_CMD << 'PYTHON_WAREHOUSE_CHECK'
import os
import sys
import time

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print("[CHECK] ERROR: databricks-sdk not installed")
    sys.exit(1)

profile = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
warehouse_id_env = os.environ.get("ADK_SQL_WAREHOUSE_ID", "")

# Get client
try:
    client = WorkspaceClient(profile=profile)
except Exception:
    try:
        client = WorkspaceClient()
    except Exception as e:
        print(f"[CHECK] ERROR: Could not create WorkspaceClient: {e}")
        sys.exit(1)

# Get warehouse ID
warehouse_id = warehouse_id_env
warehouse_name = "unknown"
warehouse_state = "unknown"

if not warehouse_id:
    print("[CHECK] ADK_SQL_WAREHOUSE_ID not set, auto-discovering...")
    warehouses = list(client.warehouses.list())
    
    # Prefer running warehouses
    running_warehouses = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    if running_warehouses:
        wh = running_warehouses[0]
        warehouse_id = wh.id
        warehouse_name = wh.name
        warehouse_state = wh.state.value
        print(f"[CHECK] Found RUNNING warehouse: {warehouse_name} ({warehouse_id})")
    else:
        # Try to start a stopped warehouse
        stopped_warehouses = [w for w in warehouses if w.state and w.state.value == "STOPPED"]
        if stopped_warehouses:
            wh = stopped_warehouses[0]
            warehouse_id = wh.id
            warehouse_name = wh.name
            print(f"[CHECK] Starting STOPPED warehouse: {warehouse_name} ({warehouse_id})...")
            client.warehouses.start(warehouse_id)
            
            # Wait for startup (up to 5 minutes)
            for i in range(30):
                status = client.warehouses.get(warehouse_id)
                if status.state and status.state.value == "RUNNING":
                    warehouse_state = "RUNNING"
                    print(f"[CHECK] Warehouse started successfully")
                    break
                print(f"[CHECK] Waiting for warehouse startup... ({i+1}/30)")
                time.sleep(10)
            else:
                print(f"[CHECK] ERROR: Warehouse did not start in time")
                sys.exit(1)
        else:
            print("[CHECK] ERROR: No SQL warehouses available")
            print("[CHECK] Remediation: Create a SQL warehouse in Databricks or set ADK_SQL_WAREHOUSE_ID")
            sys.exit(1)
else:
    # Get warehouse info for the provided ID
    try:
        wh = client.warehouses.get(warehouse_id)
        warehouse_name = wh.name
        warehouse_state = wh.state.value if wh.state else "unknown"
        print(f"[CHECK] Using configured warehouse: {warehouse_name} ({warehouse_id}) [{warehouse_state}]")
        
        if warehouse_state != "RUNNING":
            print(f"[CHECK] Warehouse not running, attempting to start...")
            client.warehouses.start(warehouse_id)
            for i in range(30):
                status = client.warehouses.get(warehouse_id)
                if status.state and status.state.value == "RUNNING":
                    warehouse_state = "RUNNING"
                    print(f"[CHECK] Warehouse started successfully")
                    break
                print(f"[CHECK] Waiting for warehouse startup... ({i+1}/30)")
                time.sleep(10)
            else:
                print(f"[CHECK] ERROR: Warehouse did not start in time")
                sys.exit(1)
    except Exception as e:
        print(f"[CHECK] ERROR: Could not get warehouse {warehouse_id}: {e}")
        sys.exit(1)

# Execute health check query
print(f"[CHECK] Executing health check: SELECT 1")
start_time = time.time()

try:
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT 1 AS health_check",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    
    elapsed_ms = int((time.time() - start_time) * 1000)
    
    if response.status and response.status.state == StatementState.SUCCEEDED:
        statement_id = response.statement_id
        print(f"[CHECK] OK: SELECT 1 succeeded")
        print(f"[CHECK]   Statement ID: {statement_id}")
        print(f"[CHECK]   Elapsed: {elapsed_ms}ms")
        print(f"[CHECK]   Warehouse: {warehouse_name} ({warehouse_id})")
        print(f"[CHECK]   State: {warehouse_state}")
        
        # Output warehouse ID for capture
        print(f"WAREHOUSE_ID={warehouse_id}")
        sys.exit(0)
    else:
        error_msg = "Unknown error"
        if response.status and response.status.error:
            error_msg = response.status.error.message
        print(f"[CHECK] ERROR: Health check failed: {error_msg}")
        sys.exit(1)
        
except Exception as e:
    print(f"[CHECK] ERROR: Health check failed: {e}")
    print("[CHECK] Remediation steps:")
    print("  1. Verify the SQL warehouse is running")
    print("  2. Check warehouse permissions for your user")
    print("  3. Try: databricks sql warehouses list --profile <profile>")
    sys.exit(1)
PYTHON_WAREHOUSE_CHECK
) || {
    log_error "SQL Warehouse health check failed"
    echo "$WAREHOUSE_CHECK_RESULT" | grep "^\[CHECK\]" | sed 's/^\[CHECK\] /  /'
    die "Cannot proceed without SQL Warehouse connectivity"
}

# Print check output with [CHECK] prefix
echo "$WAREHOUSE_CHECK_RESULT" | grep "^\[CHECK\]" | sed 's/^\[CHECK\] /  /'

# Extract warehouse ID if auto-discovered
DISCOVERED_WAREHOUSE_ID=$(echo "$WAREHOUSE_CHECK_RESULT" | grep "^WAREHOUSE_ID=" | cut -d'=' -f2)
if [[ -n "$DISCOVERED_WAREHOUSE_ID" && -z "$ADK_SQL_WAREHOUSE_ID" ]]; then
    ADK_SQL_WAREHOUSE_ID="$DISCOVERED_WAREHOUSE_ID"
    log_info "Using discovered warehouse ID: $ADK_SQL_WAREHOUSE_ID"
fi

log_success "SQL Warehouse health check passed"

# =============================================================================
# Phase 6: Prompt Resolution
# =============================================================================

log_banner "Phase 6: Prompt Resolution"

PROMPT=""
PROMPT_SOURCE=""
PROMPT_SIZE=0

# Priority: --prompt > --prompt-file > --test-level > default
if [[ -n "$PROMPT_STRING" ]]; then
    PROMPT="$PROMPT_STRING"
    PROMPT_SOURCE="--prompt argument"
    log_info "Using prompt from CLI argument"
elif [[ -n "$PROMPT_FILE" ]]; then
    if [[ ! -f "$PROMPT_FILE" ]]; then
        die "Prompt file not found: $PROMPT_FILE"
    fi
    PROMPT=$(cat "$PROMPT_FILE")
    PROMPT_SOURCE="file: $PROMPT_FILE"
    log_info "Using prompt from file: $PROMPT_FILE"
elif [[ -n "$TEST_LEVEL" ]]; then
    log_info "Loading test task level $TEST_LEVEL from test_tasks.py..."
    
    PROMPT=$($PYTHON_CMD << PYTHON_GET_PROMPT
import sys
sys.path.insert(0, "$PACKAGE_DIR")
try:
    from test_tasks import get_task_prompt
    prompt = get_task_prompt($TEST_LEVEL)
    if prompt:
        print(prompt)
    else:
        print(f"ERROR: No task found for level $TEST_LEVEL", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to load test task: {e}", file=sys.stderr)
    sys.exit(1)
PYTHON_GET_PROMPT
    ) || die "Failed to load test task level $TEST_LEVEL"
    
    PROMPT_SOURCE="test_tasks.py level $TEST_LEVEL"
    log_success "Loaded test task level $TEST_LEVEL"
else
    # Default prompt if none specified
    PROMPT="Hello! Please describe your capabilities and available tools."
    PROMPT_SOURCE="default (no prompt specified)"
    log_warn "No prompt specified, using default greeting"
fi

PROMPT_SIZE=${#PROMPT}
log_info "Prompt source: $PROMPT_SOURCE"
log_info "Prompt size: $PROMPT_SIZE characters"

# =============================================================================
# Phase 7: Session ID Resolution
# =============================================================================

log_banner "Phase 7: Session ID Resolution"

if [[ -z "$SESSION_ID" ]]; then
    # Generate session ID with timestamp and test level
    TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
    if [[ -n "$TEST_LEVEL" ]]; then
        SESSION_ID="local_test_${TEST_LEVEL}_${TIMESTAMP}"
    else
        SESSION_ID="local_${TIMESTAMP}"
    fi
    log_info "Generated session ID: $SESSION_ID"
else
    log_info "Using provided session ID: $SESSION_ID"
fi

# =============================================================================
# Phase 8: Git Status (Informational)
# =============================================================================

log_banner "Phase 8: Git Status"

if command -v git &> /dev/null && [[ -d "$PROJECT_ROOT/.git" ]]; then
    cd "$PROJECT_ROOT"
    GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    GIT_DIRTY=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
    
    log_info "Git commit: $GIT_COMMIT"
    log_info "Git branch: $GIT_BRANCH"
    if [[ "$GIT_DIRTY" -gt 0 ]]; then
        log_warn "Working directory has $GIT_DIRTY uncommitted change(s)"
    else
        log_info "Working directory: clean"
    fi
else
    log_warn "Git not available or not a git repository"
    GIT_COMMIT="unknown"
    GIT_BRANCH="unknown"
fi

# =============================================================================
# Phase 9: Resolved Run Plan
# =============================================================================

log_banner "Phase 9: Resolved Run Plan"

echo ""
echo "  Prompt Source:          $PROMPT_SOURCE"
echo "  Prompt Size:            $PROMPT_SIZE characters"
echo "  Session ID:             $SESSION_ID"
echo ""
echo "  DATABRICKS_PROFILE:     $DATABRICKS_PROFILE"
echo "  DATABRICKS_HOST set:    $DATABRICKS_HOST_SET"
echo "  DATABRICKS_TOKEN set:   $DATABRICKS_TOKEN_SET"
echo ""
echo "  ADK_SQL_WAREHOUSE_ID:   ${ADK_SQL_WAREHOUSE_ID:-auto-discover}"
echo "  ADK_LOCAL_DB_PATH:      $ADK_LOCAL_DB_PATH"
echo "  ADK_LOCAL_ARTIFACTS_PATH: $ADK_LOCAL_ARTIFACTS_PATH"
echo "  ADK_MAX_ITERATIONS:     ${ADK_MAX_ITERATIONS:-<not set>}"
echo ""
echo "  Git Commit:             $GIT_COMMIT"
echo "  Git Branch:             $GIT_BRANCH"
echo ""

# =============================================================================
# Phase 10: Execute Agent (or Dry Run)
# =============================================================================

if [[ "$DRY_RUN_CHECKS" == "true" ]]; then
    log_banner "Dry Run Complete"
    log_success "All checks passed. Agent would run with the configuration above."
    log_info "Remove --dry-run-checks or --checks-only to run the agent."
    exit 0
fi

log_banner "Phase 10: Running Agent"

# Ensure local directories exist
mkdir -p "$(dirname "$ADK_LOCAL_DB_PATH")"
mkdir -p "$ADK_LOCAL_ARTIFACTS_PATH"

# Export environment variables for the agent
export ADK_RUN_MODE="local"
export ADK_LOCAL_DB_PATH
export ADK_LOCAL_ARTIFACTS_PATH
export DATABRICKS_PROFILE

if [[ -n "$ADK_SQL_WAREHOUSE_ID" ]]; then
    export ADK_SQL_WAREHOUSE_ID
fi

if [[ -n "$ADK_MAX_ITERATIONS" ]]; then
    export ADK_MAX_ITERATIONS
fi

log_info "Starting agent with ADK_RUN_MODE=local"
log_info "Session ID: $SESSION_ID"
log_info ""

# Create a temporary Python script to run the agent
# This avoids shell quoting issues with the prompt
TEMP_SCRIPT=$(mktemp /tmp/run_local_rlm_XXXXXX.py)
trap "rm -f $TEMP_SCRIPT" EXIT

cat > "$TEMP_SCRIPT" << 'PYTHON_RUN_AGENT'
import asyncio
import os
import sys

# Add package to path
package_dir = os.environ.get("PACKAGE_DIR", "")
if package_dir:
    sys.path.insert(0, os.path.dirname(package_dir))

from databricks_rlm_agent.run import main

# Get prompt from environment (avoids shell quoting issues)
prompt = os.environ.get("AGENT_PROMPT", "")
session_id = os.environ.get("AGENT_SESSION_ID", "session_001")

if not prompt:
    print("[AGENT] ERROR: No prompt provided", file=sys.stderr)
    sys.exit(1)

print(f"[AGENT] Starting conversation...")
print(f"[AGENT] Session: {session_id}")
print("-" * 60)

try:
    result = asyncio.run(main(
        prompt=prompt,
        session_id=session_id,
        run_mode="local",
    ))
    
    print("-" * 60)
    print(f"[AGENT] Conversation completed")
    print(f"[AGENT] Status: {result.status}")
    if result.delegation_count > 0:
        print(f"[AGENT] Delegations: {result.delegation_count}")
    if result.fatal_error_msg:
        print(f"[AGENT] Error: {result.fatal_error_msg}")
    
    # Exit with appropriate code
    if result.status == "fatal_error":
        sys.exit(1)
    sys.exit(0)
    
except KeyboardInterrupt:
    print("\n[AGENT] Interrupted by user")
    sys.exit(130)
except Exception as e:
    print(f"[AGENT] ERROR: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON_RUN_AGENT

# Export prompt and session ID for the Python script
export AGENT_PROMPT="$PROMPT"
export AGENT_SESSION_ID="$SESSION_ID"
export PACKAGE_DIR="$PACKAGE_DIR"

# Run the agent
cd "$PROJECT_ROOT"
$PYTHON_CMD "$TEMP_SCRIPT"
EXIT_CODE=$?

# =============================================================================
# Phase 11: Summary
# =============================================================================

log_banner "Run Complete"

if [[ $EXIT_CODE -eq 0 ]]; then
    log_success "Agent completed successfully"
else
    log_error "Agent exited with code: $EXIT_CODE"
fi

log_info "Session ID: $SESSION_ID"
log_info "Local DB: $ADK_LOCAL_DB_PATH"
log_info "Artifacts: $ADK_LOCAL_ARTIFACTS_PATH"

exit $EXIT_CODE
