#!/bin/bash
# Databricks cluster init script: Clear cached Python wheel versions
#
# This script runs on cluster startup to ensure fresh wheel installations.
# It clears pip cache and removes any previously installed versions of
# databricks_rlm_agent to prevent stale code from being loaded.
#
# To use: Configure this as a cluster-scoped init script in Databricks.

set -e

PACKAGE_NAME="databricks_rlm_agent"

echo "[init-script] Clearing wheel cache for $PACKAGE_NAME..."

# 1. Clear pip cache directories
echo "[init-script] Clearing pip cache directories..."
rm -rf /root/.cache/pip 2>/dev/null || true
rm -rf /tmp/pip_cache 2>/dev/null || true
rm -rf /databricks/.cache/pip 2>/dev/null || true
rm -rf ~/.cache/pip 2>/dev/null || true

# 2. Remove any installed version of the package from site-packages
# This ensures the job-scoped wheel installation is fresh
echo "[init-script] Removing any pre-installed $PACKAGE_NAME versions..."

# Find and remove from all Python site-packages locations
for site_packages in /databricks/python*/lib/python*/site-packages \
                     /usr/local/lib/python*/dist-packages \
                     /local_disk0/.ephemeral_nfs/*/lib/python*/site-packages; do
    if [ -d "$site_packages" ]; then
        # Remove the package directory
        rm -rf "$site_packages/${PACKAGE_NAME}"* 2>/dev/null || true
        # Remove egg-info/dist-info
        rm -rf "$site_packages/${PACKAGE_NAME//_/-}"*.dist-info 2>/dev/null || true
        rm -rf "$site_packages/${PACKAGE_NAME//_/-}"*.egg-info 2>/dev/null || true
    fi
done

# 3. Clear any .pyc files related to the package
echo "[init-script] Clearing compiled Python files..."
find /databricks -name "${PACKAGE_NAME}*.pyc" -delete 2>/dev/null || true
find /databricks -path "*/${PACKAGE_NAME}/*" -name "*.pyc" -delete 2>/dev/null || true

# 4. Clear __pycache__ directories for the package
find /databricks -type d -name "__pycache__" -path "*/${PACKAGE_NAME}/*" -exec rm -rf {} + 2>/dev/null || true

echo "[init-script] Wheel cache cleared for $PACKAGE_NAME"
