"""CI safety net for the Airflow DAGs.

These tests parse the DAG files the same way the scheduler does, without running
any tasks. They catch what linting can't see: a bad import, a typo in a decorator
argument, a duplicate dag_id, a cycle -- anything that blows up at parse time.
Task logic and network calls are deliberately out of scope.
"""

from __future__ import annotations

from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def _load_dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dags_import_without_errors() -> None:
    """Every DAG file under dags/ parses into a DAG object with no import errors."""
    dagbag = _load_dagbag()
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_dagbag_is_not_empty() -> None:
    """Guard against a wrong path quietly testing nothing and reporting green."""
    dagbag = _load_dagbag()
    assert dagbag.dag_ids, f"no DAGs discovered under {DAGS_DIR}"
