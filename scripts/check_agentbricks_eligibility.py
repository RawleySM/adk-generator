#!/usr/bin/env python3
"""
Best-effort eligibility probe for "Agent Bricks Knowledge assistant" prerequisites.

This script uses the Databricks Python SDK with a workspace profile (no account APIs),
so account-level requirements (budgets / budget-policy) may be "UNKNOWN" unless the
profile is configured for account auth (account host + account_id).
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient


@dataclass
class CheckResult:
    name: str
    status: str  # YES | NO | UNKNOWN
    evidence: str

    def as_dict(self) -> Dict[str, str]:
        return {"name": self.name, "status": self.status, "evidence": self.evidence}


def _yes(name: str, evidence: str) -> CheckResult:
    return CheckResult(name=name, status="YES", evidence=evidence)


def _no(name: str, evidence: str) -> CheckResult:
    return CheckResult(name=name, status="NO", evidence=evidence)


def _unknown(name: str, evidence: str) -> CheckResult:
    return CheckResult(name=name, status="UNKNOWN", evidence=evidence)


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, default=str)


def _get_profile() -> str:
    # Default to the profile the user asked about.
    return os.environ.get("DATABRICKS_CONFIG_PROFILE", "rstanhope")


def _try_list_catalogs(w: WorkspaceClient) -> CheckResult:
    name = "Unity Catalog enabled"
    try:
        catalogs = list(w.catalogs.list(max_results=5))
        # If the UC APIs work at all, UC is enabled in the workspace.
        sample = [c.full_name for c in catalogs if getattr(c, "full_name", None)]
        return _yes(name, f"UC catalogs list succeeded. Sample: {sample[:5]}")
    except Exception as e:  # noqa: BLE001
        return _unknown(name, f"Could not list UC catalogs via SDK: {e}")


def _try_system_ai_schema(w: WorkspaceClient) -> CheckResult:
    name = "Access to foundation models in Unity Catalog via system.ai schema"
    try:
        schemas = list(w.schemas.list(catalog_name="system"))
        full_names = sorted([s.full_name for s in schemas if getattr(s, "full_name", None)])
        if "system.ai" in full_names:
            return _yes(name, "Schema `system.ai` exists and is listable by current principal.")
        return _no(name, f"Listed system schemas but `system.ai` not present. Schemas: {full_names}")
    except Exception as e:  # noqa: BLE001
        return _unknown(name, f"Could not list schemas in catalog `system`: {e}")


def _try_model_serving(w: WorkspaceClient) -> CheckResult:
    name = "Access to Mosaic AI Model Serving"
    try:
        eps = list(w.serving_endpoints.list())
        # Just evidence that the API is accessible; endpoint count is safe.
        return _yes(name, f"Serving endpoints list succeeded. Count: {len(eps)}")
    except Exception as e:  # noqa: BLE001
        return _unknown(name, f"Could not list serving endpoints: {e}")


def _try_system_ai_foundation_models_visible(w: WorkspaceClient) -> CheckResult:
    name = "Foundation models visible (system.ai.*)"
    try:
        eps = list(w.serving_endpoints.list())
        system_ai_entities: List[str] = []
        for ep in eps:
            cfg = getattr(ep, "config", None)
            served = getattr(cfg, "served_entities", None) if cfg else None
            if not served:
                continue
            for ent in served:
                entity_name = getattr(ent, "entity_name", None)
                if isinstance(entity_name, str) and entity_name.startswith("system.ai."):
                    system_ai_entities.append(entity_name)
        system_ai_entities = sorted(set(system_ai_entities))
        if system_ai_entities:
            return _yes(name, f"Found `system.ai.*` entities in serving endpoints: {system_ai_entities[:5]}")
        return _unknown(
            name,
            "Serving endpoints list succeeded, but none referenced `system.ai.*`. "
            "You may still have access via Catalog Explorer / Model Registry.",
        )
    except Exception as e:  # noqa: BLE001
        return _unknown(name, f"Could not inspect serving endpoints for `system.ai.*`: {e}")


def _try_serverless_sql(w: WorkspaceClient) -> CheckResult:
    name = "Serverless compute enabled (SQL warehouse signal)"
    try:
        warehouses = list(w.warehouses.list())
        # Serverless for SQL is the only programmatically visible serverless signal from a workspace-only profile.
        any_serverless_flag = any(getattr(wh, "enable_serverless_compute", False) for wh in warehouses)
        any_serverless_type = any(str(getattr(wh, "warehouse_type", "")).upper() == "SERVERLESS" for wh in warehouses)
        if any_serverless_flag or any_serverless_type:
            return _yes(
                name,
                "At least one warehouse indicates serverless compute "
                f"(flag={any_serverless_flag}, type={any_serverless_type}).",
            )
        return _unknown(
            name,
            "No listed SQL warehouses indicate serverless compute. "
            "This does NOT prove serverless compute for workflows/notebooks is disabled; "
            "that is typically an account-level setting.",
        )
    except Exception as e:  # noqa: BLE001
        return _unknown(name, f"Could not list SQL warehouses: {e}")


def _try_mlflow_production_monitoring(w: WorkspaceClient) -> CheckResult:
    name = "MLflow Production Monitoring (Beta) enabled (Previews)"
    # Docs indicate this is controlled via the workspace Previews UI (workspace admin).
    # We do not have a stable public workspace API to enumerate preview toggles.
    try:
        # Attempt a very lightweight REST call that often exists regardless of features.
        # If it returns {}, that's still not evidence either way.
        resp = w.api_client.do("GET", "/api/2.0/workspace-conf")
        return _unknown(
            name,
            "No reliable workspace API found to read Previews toggles. "
            f"`GET /api/2.0/workspace-conf` returned: {_safe_json(resp)}",
        )
    except Exception as e:  # noqa: BLE001
        return _unknown(
            name,
            "No reliable workspace API found to read Previews toggles. "
            f"Attempted `GET /api/2.0/workspace-conf` and got: {e}",
        )


def _account_budget_policy_note() -> CheckResult:
    return _unknown(
        "Access to a serverless budget policy with a nonzero budget",
        "This is an account-level requirement (budgets / budget-policy APIs). "
        "Your current CLI auth is workspace-scoped; account host/account_id are not configured, "
        "so this cannot be verified from a workspace-only profile.",
    )


def main() -> int:
    profile = _get_profile()
    w = WorkspaceClient(profile=profile)

    me = w.current_user.me()
    print(_safe_json({"workspace_host": w.config.host, "profile": profile, "current_user": me.user_name}))

    results: List[CheckResult] = []
    results.append(_try_mlflow_production_monitoring(w))
    results.append(_try_serverless_sql(w))
    results.append(_try_list_catalogs(w))
    results.append(_try_model_serving(w))
    results.append(_try_system_ai_schema(w))
    results.append(_try_system_ai_foundation_models_visible(w))
    results.append(_account_budget_policy_note())

    print(_safe_json({"checks": [r.as_dict() for r in results]}))

    # Non-zero exit if any explicit NO (keep UNKNOWN as non-fatal).
    any_no = any(r.status == "NO" for r in results)
    return 2 if any_no else 0


if __name__ == "__main__":
    raise SystemExit(main())

