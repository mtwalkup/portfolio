"""
Bronze landing for the CMS synthetic outpatient claims file.

Pulls the published ZIP, unpacks the single CSV inside, and loads it into Neon
(Postgres) as a raw, all-text table. Nothing is reshaped here: bronze takes the
file exactly as CMS ships it.

Run by hand. The source is a static, point-in-time release, so there's nothing
to schedule against. Re-run it when CMS republishes, or when you point it at a
batch you generated yourself with Synthea.
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import urllib.error
import urllib.request
import zipfile
from datetime import timedelta
from typing import Any, Final

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import (
    Param,
    dag,  # pyright: ignore[reportUnknownVariableType]
    get_current_context,
    task,
)

# Static URL for now.
SOURCE_URL: Final[str] = (
    "https://data.cms.gov/sites/default/files/2023-04/"
    "c3d8a962-c6b8-4a59-adb5-f0495cc81fda/Outpatient.zip"
)

# /tmp is local to whichever worker runs the task. Download and unzip are kept
# in one task so they share it. If you ever split the load into its own task it
# can land on a different worker that can't see these files: keep the load
# in-task, or stage to shared storage (S3/GCS/volume) at that point.
WORK_DIR: Final[str] = "/tmp/cms_outpatient"
ZIP_PATH: Final[str] = os.path.join(WORK_DIR, "outpatient.zip")

# Plenty of CDN and .gov front ends quietly block the default "Python-urllib"
# user-agent. Cheap insurance to send something that looks like a real client.
USER_AGENT: Final[str] = "mtwalkup-bronze-loader/1.0"

# Bound the fetch so a half-open socket can't pin a worker slot indefinitely.
DOWNLOAD_TIMEOUT_SECONDS: Final[int] = 300

# Database/environment targets
ENVIRONMENTS: Final[list[str]] = ["development", "production"]
DEFAULT_ENV: Final[str] = os.environ.get("PIPELINE_ENV", "development")

# CMS ships these BENE files pipe-delimited, not comma. Used for both reading the
# header and the COPY, so a future comma/tab release is a one-line change here.
DELIMITER: Final[str] = "|"

# The table is dropped and rebuilt from the CSV header on every run, so the
# schema follows the file and re-running never duplicates rows.
TARGET_SCHEMA: Final[str] = "bronze"
FQ_TABLE: Final[str] = f"{TARGET_SCHEMA}.cms_outpatient"

log = logging.getLogger(__name__)

# Applied to every task in the DAG.
default_args: Final[dict[str, Any]] = {
    "owner": "data-eng",
    # The network fetch is the only thing here that realistically flakes;
    # everything after it is local and deterministic. Retry the fetch, that's it.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _safe_remove(path: str) -> None:
    """Delete a file if it exists, swallowing any OS error.

    Never raises by design: it runs inside except blocks during cleanup, and an
    exception thrown here would mask the original failure in the traceback.

    Args:
        path: Absolute path to the file to remove. A missing file is a no-op.
    """
    try:
        if os.path.exists(path):
            os.remove(path)
            log.info("removed partial artifact %s", path)
    except OSError as exc:
        log.warning("could not remove %s: %s", path, exc)


def _quote_ident(name: str) -> str:
    """Quote a string for use as a Postgres identifier.

    Lets us build column names straight from the CSV header without worrying
    about case, spaces, or reserved words. Embedded double quotes are doubled,
    per Postgres' quoting rules.

    Args:
        name: Raw column name from the CSV header.
    """
    return '"' + name.replace('"', '""') + '"'


@dag(
    dag_id="cms_outpatient_bronze_load",
    description="Land the CMS synthetic outpatient claims file (bronze).",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two concurrent runs would race on the same WORK_DIR
    default_args=default_args,
    doc_md=__doc__,
    tags=["cms", "bronze", "outpatient"],
    params={
        "env": Param(
            DEFAULT_ENV,
            type="string",
            enum=ENVIRONMENTS,
            title="Target environment",
            description="Which Neon connection to load into (healthcare_<env>).",
        ),
    },
)
def cms_outpatient_bronze_load() -> None:
    """Define the bronze-landing DAG for the CMS synthetic outpatient file."""

    @task(execution_timeout=timedelta(minutes=15))
    def download_and_unzip() -> str:
        """Download the source ZIP and extract the single CSV it contains.

        Returns:
            Absolute path to the extracted CSV, which the next task reads off
            XCom.

        Raises:
            ConnectionError: The file could not be fetched (an HTTP status error,
                or the host was unreachable, which on a server usually means
                egress is blocked).
            ValueError: The download came back empty, or the archive did not hold
                exactly one CSV.
            zipfile.BadZipFile: The downloaded bytes were not a valid ZIP.
        """
        os.makedirs(WORK_DIR, exist_ok=True)

        log.info("fetching %s", SOURCE_URL)
        request = urllib.request.Request(SOURCE_URL, headers={"User-Agent": USER_AGENT})
        try:
            with (
                urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as resp,
                open(ZIP_PATH, "wb") as fh,
            ):
                shutil.copyfileobj(resp, fh)
        except urllib.error.HTTPError as exc:
            # A 404 here almost always means CMS moved the file (see SOURCE_URL).
            _safe_remove(ZIP_PATH)
            raise ConnectionError(f"HTTP {exc.code} from {SOURCE_URL}") from exc
        except urllib.error.URLError as exc:
            # DNS, refused, timeout, blocked egress. On a server, the firewall
            # far more often than the code.
            _safe_remove(ZIP_PATH)
            raise ConnectionError(f"can't reach {SOURCE_URL}: {exc.reason}") from exc
        except Exception:
            _safe_remove(ZIP_PATH)
            raise

        # Treat empty as a failed download so we don't unzip nothing and call it a success.
        size = os.path.getsize(ZIP_PATH)
        if not size:
            _safe_remove(ZIP_PATH)
            raise ValueError("downloaded zip is empty")
        log.info("got %s bytes", f"{size:,}")

        # Trusted source, so we extract straight to disk without guarding against
        # zip-slip. That assumption breaks if it's ever not CMS.
        try:
            with zipfile.ZipFile(ZIP_PATH) as archive:
                log.info("archive members: %s", archive.namelist())
                archive.extractall(WORK_DIR)
        except zipfile.BadZipFile as exc:
            raise zipfile.BadZipFile(f"{ZIP_PATH} isn't a valid zip file") from exc

        # Rely on exactly one CSV. Assert it rather than taking [0], so a future
        # multi-file release fails here instead of silently loading the wrong one.
        csvs = [f for f in os.listdir(WORK_DIR) if f.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"expected one csv, found {len(csvs)}: {csvs}")

        csv_path = os.path.join(WORK_DIR, csvs[0])
        log.info("staged %s (%s bytes)", csv_path, f"{os.path.getsize(csv_path):,}")
        return csv_path  # next task picks this up off XCom

    @task(execution_timeout=timedelta(minutes=30))
    def load_to_neon(csv_path: str) -> int:
        """Replace the Neon bronze table with the CSV's contents.

        Columns are taken from the CSV header in file order, all typed as text --
        bronze keeps the data raw. Each run drops and rebuilds the table, then
        COPYs the file in, so it's idempotent: the table always ends up equal to
        the file, never with duplicate rows. (The rebuild and the COPY are
        separate transactions; a mid-load failure leaves an empty table that the
        next run refills -- still no duplicates, since a run replaces, never
        appends.)

        The target environment comes from the run's `env` param (set on the
        trigger form), so the connection id is resolved here at runtime rather
        than baked in at parse time.

        Args:
            csv_path: Absolute path to the extracted CSV, read off XCom. Assumed
                reachable from this worker (see WORK_DIR note on task locality).

        Returns:
            Number of rows loaded.

        Raises:
            ValueError: The header has duplicate column names, which would make
                the COPY target ambiguous.
        """
        env = get_current_context()["params"]["env"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        conn_id = f"healthcare_{env}"

        with open(csv_path, newline="", encoding="utf-8") as fh:
            header = [col.strip() for col in next(csv.reader(fh, delimiter=DELIMITER))]
        if len(set(header)) != len(header):
            raise ValueError(f"duplicate column names in header: {header}")

        columns = [_quote_ident(col) for col in header]
        cols_ddl = ",\n    ".join(f"{col} text" for col in columns)
        col_list = ", ".join(columns)

        hook = PostgresHook(postgres_conn_id=conn_id)
        hook.run(  # pyright: ignore[reportUnknownMemberType]
            [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                f"DROP TABLE IF EXISTS {FQ_TABLE};",
                f"CREATE TABLE {FQ_TABLE} (\n    {cols_ddl}\n);",
            ]
        )
        hook.copy_expert(
            f"COPY {FQ_TABLE} ({col_list}) FROM STDIN "
            f"WITH (FORMAT csv, HEADER true, DELIMITER '{DELIMITER}')",
            csv_path,
        )

        rows = int(hook.get_first(f"SELECT count(*) FROM {FQ_TABLE};")[0])  # pyright: ignore[reportUnknownMemberType]
        log.info("loaded %s rows into %s via %s", f"{rows:,}", FQ_TABLE, conn_id)
        return rows

    load_to_neon(download_and_unzip())  # pyright: ignore[reportArgumentType]


bronze_load_dag = cms_outpatient_bronze_load()

if __name__ == "__main__":
    bronze_load_dag.test()  # pyright: ignore[reportUnknownMemberType]
