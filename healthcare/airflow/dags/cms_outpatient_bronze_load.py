"""
Bronze landing for the CMS synthetic outpatient claims file.

Pulls the published ZIP, unpacks the single CSV inside, and loads it into Neon
(Postgres). Rows land exactly as CMS ships them -- no rows are added, dropped, or
reordered -- but each column is cast to a Postgres type and carries a column
COMMENT. Both the types and the descriptions are fetched at load time from the
authoritative CCW/NCH variable metadata (CMS BlueButton codesets); nothing about
the schema is hard-coded in this repo.

Run by hand. The source is a static, point-in-time release, so there's nothing
to schedule against. Re-run it when CMS republishes, or when you point it at a
batch you generated yourself with Synthea.
"""

from __future__ import annotations

import csv
import io
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
    Asset,
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

# Column types and descriptions come from the authoritative CCW/NCH variable
# metadata -- nothing about the schema is hand-maintained here. The CMS BlueButton
# codesets publish one CSV keyed by variable name with its storage class and
# description: https://github.com/CMSgov/bluebutton-csv-codesets
DICTIONARY_URL: Final[str] = (
    "https://raw.githubusercontent.com/CMSgov/bluebutton-csv-codesets/master/csv/all_meta.csv"
)

# That metadata is landed as its own bronze table in the same run and read back
# to type + comment the claims table -- so the dictionary the load applies is
# always the one just landed (no drift), and it's a reusable, SQL-queryable asset
# for the other claim files and for dbt.
DICTIONARY_TABLE: Final[str] = "bronze.ccw_variable_metadata"
CCW_DICTIONARY_ASSET: Final[Asset] = Asset(f"neon://{DICTIONARY_TABLE}")

# Map the CCW storage class straight onto a Postgres type: dates -> date,
# numerics -> numeric, everything else (codes, ids, flags) stays text.
CCW_TYPE_TO_PG: Final[dict[str, str]] = {"DATE": "date", "NUM": "numeric", "CHAR": "text"}

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

# Both tables are dropped and rebuilt on every run, so re-running never
# duplicates rows. The file is COPYed into an all-text staging table first, then
# cast into the typed table -- so a malformed value surfaces in the cast, not as
# an aborted bulk load.
TARGET_SCHEMA: Final[str] = "bronze"
FQ_TABLE: Final[str] = f"{TARGET_SCHEMA}.cms_outpatient"
STAGING_TABLE: Final[str] = f"{TARGET_SCHEMA}.cms_outpatient_staging"

# Source dates are 'DD-Mon-YYYY' (e.g. 01-Jun-2015). Parsed with an explicit
# format so the load doesn't depend on the Neon server's DateStyle setting.
DATE_INPUT_FORMAT: Final[str] = "DD-Mon-YYYY"

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


def _quote_literal(value: str) -> str:
    """Quote a string as a Postgres string literal for COMMENT statements.

    Embedded single quotes are doubled, per Postgres' literal-quoting rules. Used
    only for trusted dictionary text, never user input.

    Args:
        value: The literal text to embed in SQL.
    """
    return "'" + value.replace("'", "''") + "'"


def _dictionary_from_records(
    records: list[tuple[str | None, str | None, str | None, str | None]],
) -> dict[str, tuple[str, str]]:
    """Build the column -> (postgres_type, description) map from metadata rows.

    Each record is ``(long_name, name, ccw_type, description)`` read from the
    landed CCW dictionary table. Indexed by both names (upper-cased) so a column
    resolves whichever it's filed under; the CCW storage class maps to a Postgres
    type via CCW_TYPE_TO_PG.

    Args:
        records: Rows of ``(long_name, name, type, description)``.

    Returns:
        Mapping of upper-cased variable name -> (postgres_type, description).
    """
    dictionary: dict[str, tuple[str, str]] = {}
    for long_name, name, ccw_type, description in records:
        pg_type = CCW_TYPE_TO_PG.get((ccw_type or "").strip(), "text")
        desc = (description or "").strip()
        for key in (long_name, name):
            if key:
                dictionary.setdefault(key.strip().upper(), (pg_type, desc))
    return dictionary


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

    @task(execution_timeout=timedelta(minutes=10), outlets=[CCW_DICTIONARY_ASSET])
    def land_ccw_dictionary() -> str:
        """Land the CCW variable metadata as its own bronze table.

        Downloads the CMS BlueButton ``all_meta.csv`` and replaces the dictionary
        table with it, every column text -- a plain bronze landing of a source
        file. The claims load reads types and column descriptions back out of this
        table in the same run, so the dictionary it applies is always the one just
        landed (no drift). The table is a reusable, SQL-queryable asset for the
        other claim files and for dbt.

        Returns:
            The fully-qualified dictionary table name.

        Raises:
            ConnectionError: The metadata CSV could not be fetched.
        """
        env = get_current_context()["params"]["env"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        conn_id = f"portfolio_healthcare_{env}"

        # Held in memory and inserted via csv.DictReader rather than COPYed from a
        # file: the source has blank trailing lines COPY would reject, and writing
        # a CSV next to the claims file would break that task's "exactly one CSV"
        # check. all_meta.csv is small (a few hundred rows), so this is cheap.
        log.info("fetching %s", DICTIONARY_URL)
        request = urllib.request.Request(DICTIONARY_URL, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as resp:
                text = resp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise ConnectionError(f"can't reach {DICTIONARY_URL}: {exc.reason}") from exc

        reader = csv.DictReader(io.StringIO(text))
        header = reader.fieldnames
        if not header:
            raise ValueError(f"no header in CCW metadata from {DICTIONARY_URL}")
        rows = [
            [row.get(col) for col in header]
            for row in reader
            if any((value or "").strip() for value in row.values())
        ]

        cols_ddl = ",\n    ".join(f"{_quote_ident(col)} text" for col in header)
        hook = PostgresHook(postgres_conn_id=conn_id)
        hook.run(  # pyright: ignore[reportUnknownMemberType]
            [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                f"DROP TABLE IF EXISTS {DICTIONARY_TABLE};",
                f"CREATE TABLE {DICTIONARY_TABLE} (\n    {cols_ddl}\n);",
            ]
        )
        hook.insert_rows(  # pyright: ignore[reportUnknownMemberType]
            DICTIONARY_TABLE, rows, target_fields=header, commit_every=1000
        )
        log.info("landed %s CCW variables into %s", f"{len(rows):,}", DICTIONARY_TABLE)
        return DICTIONARY_TABLE

    @task(execution_timeout=timedelta(minutes=30))
    def load_to_neon(csv_path: str) -> int:
        """Replace the Neon bronze table with the CSV's contents, typed.

        The file is COPYed verbatim into an all-text staging table, then cast
        column-by-column into the typed table, using types read from the CCW
        dictionary table landed upstream in this same run (blanks normalised to
        NULL). Rows are preserved one-for-one -- only the column types change --
        and each column is annotated with its CCW description as a Postgres
        COMMENT. No types or descriptions are hard-coded in this repo.

        Each run drops and rebuilds both tables, so it's idempotent: the table
        always ends up equal to the file, never with duplicate rows. (The steps
        are separate transactions; a mid-load failure leaves an empty or missing
        table that the next run refills -- still no duplicates, since a run
        replaces, never appends.)

        The target environment comes from the run's `env` param (set on the
        trigger form), so the connection id is resolved here at runtime rather
        than baked in at parse time.

        Args:
            csv_path: Absolute path to the extracted CSV, read off XCom. Assumed
                reachable from this worker (see WORK_DIR note on task locality).

        Returns:
            Number of rows loaded.

        Raises:
            ValueError: The header has duplicate column names (which would make
                the COPY target ambiguous), or a shipped column is absent from
                the CCW metadata (which would leave it un-typed/un-described).
        """
        env = get_current_context()["params"]["env"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        conn_id = f"portfolio_healthcare_{env}"
        hook = PostgresHook(postgres_conn_id=conn_id)

        with open(csv_path, newline="", encoding="utf-8") as fh:
            header = [col.strip() for col in next(csv.reader(fh, delimiter=DELIMITER))]
        if len(set(header)) != len(header):
            raise ValueError(f"duplicate column names in header: {header}")

        # Types + descriptions come from the CCW dictionary table landed upstream
        # in this same run (see land_ccw_dictionary) -- nothing about the schema is
        # hard-coded, and the dictionary applied is always the one just landed, so
        # the two can't drift. Every shipped column must resolve, so a silent
        # layout change fails loudly rather than landing un-typed or un-commented.
        records = hook.get_records(  # pyright: ignore[reportUnknownMemberType]
            f"SELECT long_name, name, type, description FROM {DICTIONARY_TABLE};"
        )
        dictionary = _dictionary_from_records(records)
        unknown = [col for col in header if col.upper() not in dictionary]
        if unknown:
            raise ValueError(f"columns missing from CCW metadata: {unknown}")
        specs = {col: dictionary[col.upper()] for col in header}  # col -> (pg_type, description)

        idents = {col: _quote_ident(col) for col in header}
        col_list = ", ".join(idents[col] for col in header)

        staging_ddl = ",\n    ".join(f"{idents[col]} text" for col in header)
        typed_ddl = ",\n    ".join(f"{idents[col]} {specs[col][0]}" for col in header)

        def _cast(col: str) -> str:
            """Build the staging->typed SELECT expression for one column."""
            ident = idents[col]
            pg_type = specs[col][0]
            if pg_type == "date":
                return f"to_date(NULLIF({ident}, ''), '{DATE_INPUT_FORMAT}') AS {ident}"
            if pg_type == "text":
                return f"NULLIF({ident}, '') AS {ident}"
            return f"NULLIF({ident}, '')::{pg_type} AS {ident}"

        select_list = ",\n    ".join(_cast(col) for col in header)
        insert_typed = (
            f"INSERT INTO {FQ_TABLE} ({col_list})\nSELECT\n    {select_list}\nFROM {STAGING_TABLE};"
        )

        comments = [
            f"COMMENT ON COLUMN {FQ_TABLE}.{idents[col]} IS {_quote_literal(specs[col][1])};"
            for col in header
        ]

        hook.run(  # pyright: ignore[reportUnknownMemberType]
            [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                f"DROP TABLE IF EXISTS {STAGING_TABLE};",
                f"CREATE TABLE {STAGING_TABLE} (\n    {staging_ddl}\n);",
            ]
        )
        hook.copy_expert(
            f"COPY {STAGING_TABLE} ({col_list}) FROM STDIN "
            f"WITH (FORMAT csv, HEADER true, DELIMITER '{DELIMITER}')",
            csv_path,
        )
        hook.run(  # pyright: ignore[reportUnknownMemberType]
            [
                f"DROP TABLE IF EXISTS {FQ_TABLE};",
                f"CREATE TABLE {FQ_TABLE} (\n    {typed_ddl}\n);",
                insert_typed,
                f"DROP TABLE {STAGING_TABLE};",
                *comments,
            ]
        )

        rows = int(hook.get_first(f"SELECT count(*) FROM {FQ_TABLE};")[0])  # pyright: ignore[reportUnknownMemberType]
        log.info("loaded %s rows into %s via %s", f"{rows:,}", FQ_TABLE, conn_id)
        return rows

    # The load reads the dictionary table, so the landing must finish first; the
    # download runs in parallel with it. Both feed the single load.
    dictionary_ready = land_ccw_dictionary()
    loaded = load_to_neon(download_and_unzip())  # pyright: ignore[reportArgumentType]
    dictionary_ready >> loaded  # pyright: ignore[reportUnusedExpression]


bronze_load_dag = cms_outpatient_bronze_load()

if __name__ == "__main__":
    bronze_load_dag.test()  # pyright: ignore[reportUnknownMemberType]
