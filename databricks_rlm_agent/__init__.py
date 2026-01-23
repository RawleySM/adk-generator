"""Databricks RLM Agent - Two-job orchestrator/executor pattern.

This package provides:
- Job_A (Orchestrator): Control plane for RLM orchestration
- Job_B (Executor): Execution plane for running generated artifacts

Entry points:
- rlm-orchestrator: CLI for Job_A
- rlm-executor: CLI for Job_B
"""

__version__ = "0.1.0"

