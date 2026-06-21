"""
Bronze landing for the CMS synthetic beneficiary (patient) summary file.

Pulls the published ZIP, unpacks the eleven yearly CSVs inside (one per
reference year, 2015-2025), and loads them into Neon (Postgres) as a single
table. Rows land exactly as CMS ships them -- no rows are added, dropped, or
reordered, and every yearly file is unioned in -- so the grain here is one row
per beneficiary *per reference year* (a beneficiary recurs across years).
Collapsing to one row per beneficiary is deferred to silver.

Each column is cast to a Postgres type and, where the variable is documented,
carries a column COMMENT. Types and descriptions are fetched at load time from
the same authoritative CCW/NCH variable metadata (CMS BlueButton codesets) that
the outpatient load uses, landed into the *same* ``bronze.ccw_variable_metadata``
table in this run. Unlike the outpatient file, the beneficiary (MBSF) layout is
only partially covered by that codeset: the ~95 columns it doesn't document
(monthly status arrays, Part C contract/plan ids, a few demographics like
``SEX_IDENT_CD`` / ``AGE_AT_END_REF_YR``) land as plain ``text`` with no comment
rather than failing the load. The columns silver needs for ``patients`` -- birth
date, death date, sex, race, age, state/county/zip -- are all present.

Files COPY directly into the typed table (no all-text staging copy), which keeps
peak Neon storage to one copy of the data. In development the load is also capped
to the first N distinct beneficiaries (``dev_beneficiary_limit``, default 1000),
the same cohort across every year, so the whole thing fits a small Neon database.
No separate cohort table is kept: the loaded ``cms_beneficiary`` table *is* the
cohort, and the claim loads subset themselves to the BENE_IDs in it so their dev
rows still join. Production loads everyone.

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
import tempfile
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

# Static URL for now. The published name has spaces; keep them percent-encoded so
# urllib doesn't have to guess. This single ZIP holds all eleven yearly files.
SOURCE_URL: Final[str] = (
    "https://data.cms.gov/sites/default/files/2023-04/"
    "250e6ca0-3515-4767-957c-5528bfcee75c/All%20Beneficiary%20Years.zip"
)

# Column types and descriptions come from the authoritative CCW/NCH variable
# metadata -- the same codeset the outpatient load uses. The CMS BlueButton
# codesets publish one CSV keyed by variable name with its storage class and
# description: https://github.com/CMSgov/bluebutton-csv-codesets
DICTIONARY_URL: Final[str] = (
    "https://raw.githubusercontent.com/CMSgov/bluebutton-csv-codesets/master/csv/all_meta.csv"
)

# Landed as its own bronze table and read back to type + comment the beneficiary
# table -- so the dictionary applied is always the one just landed (no drift),
# and it's a reusable, SQL-queryable asset shared with the other claim loads and
# with dbt. Same table the outpatient load writes; both drop + rebuild it, and
# each lands the identical source file, so re-landing is harmless.
DICTIONARY_TABLE: Final[str] = "bronze.ccw_variable_metadata"
CCW_DICTIONARY_ASSET: Final[Asset] = Asset(f"neon://{DICTIONARY_TABLE}")

# Map the CCW storage class straight onto a Postgres type: dates -> date,
# numerics -> numeric, everything else (codes, ids, flags) stays text. Columns
# the codeset doesn't document also fall back to text (see UNDOCUMENTED_TYPE).
CCW_TYPE_TO_PG: Final[dict[str, str]] = {"DATE": "date", "NUM": "numeric", "CHAR": "text"}
UNDOCUMENTED_TYPE: Final[str] = "text"

# /tmp is local to whichever worker runs the task. Download and unzip are kept
# in one task so they share it. If you ever split the load into its own task it
# can land on a different worker that can't see these files: keep the load
# in-task, or stage to shared storage (S3/GCS/volume) at that point.
WORK_DIR: Final[str] = "/tmp/cms_beneficiary"
ZIP_PATH: Final[str] = os.path.join(WORK_DIR, "beneficiary.zip")

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

# The typed table is dropped and rebuilt on every run, so re-running never
# duplicates rows. Files COPY *directly* into the typed table -- there's no
# all-text staging copy, which would otherwise hold the whole dataset a second
# time on Neon (the staging table is what blows past small Neon tiers). The
# trade-off: a malformed value aborts that file's COPY rather than surfacing in a
# cast. Acceptable for a trusted, consistent synthetic source. Two things make
# direct COPY safe here: CMS dates have a textual month (e.g. 16-Aug-1999) that
# Postgres parses natively into `date` regardless of DateStyle, and CSV-format
# COPY already reads an empty unquoted field as NULL -- so no to_date/NULLIF
# staging pass is needed.
TARGET_SCHEMA: Final[str] = "bronze"
FQ_TABLE: Final[str] = f"{TARGET_SCHEMA}.cms_beneficiary"

# The beneficiary key. Used to pick the dev cohort and to filter every yearly
# file down to it. The MBSF synthetic layout names it BENE_ID. In development the
# load keeps just the first N distinct of these (see dev_beneficiary_limit) so the
# whole thing fits a small Neon database; the resulting cms_beneficiary table *is*
# the cohort, which the claim loads read directly so their dev subset still joins.
BENE_ID_COLUMN: Final[str] = "BENE_ID"

# Cap applied only when env == development; production ignores it and loads all.
DEV_DEFAULT_LIMIT: Final[int] = 1000

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


def _read_header(csv_path: str) -> list[str]:
    """Return the stripped column names from a pipe-delimited CSV's first line.

    Args:
        csv_path: Absolute path to a CSV whose first row is the header.
    """
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return [col.strip() for col in next(csv.reader(fh, delimiter=DELIMITER))]


def _collect_cohort(csv_paths: list[str], bene_idx: int, limit: int) -> list[str]:
    """Return the first ``limit`` distinct beneficiary ids across the yearly files.

    Files are read in the order given and ids collected in first-seen order until
    the cap is hit, so the result is deterministic. The same ids are then used to
    filter *every* file (see _filter_to_cohort), giving a cohort that's consistent
    across reference years -- a beneficiary kept for 2015 is kept for 2025 too --
    so silver can still follow a patient through time on the dev subset.

    Args:
        csv_paths: Absolute paths to the extracted yearly CSVs.
        bene_idx: Zero-based index of the beneficiary-id column in each row.
        limit: Maximum number of distinct ids to return.

    Returns:
        Up to ``limit`` beneficiary ids, in first-seen order.
    """
    seen: dict[str, None] = {}  # insertion-ordered set
    for path in csv_paths:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=DELIMITER)
            next(reader, None)  # skip header
            for row in reader:
                if bene_idx >= len(row):
                    continue
                bene_id = row[bene_idx].strip()
                if bene_id and bene_id not in seen:
                    seen[bene_id] = None
                    if len(seen) >= limit:
                        return list(seen)
    return list(seen)


def _filter_to_cohort(path: str, bene_idx: int, allowed: set[str]) -> str:
    """Write a temp copy of ``path`` holding only rows whose id is in ``allowed``.

    Kept rows are written byte-for-byte as they appear in the source (the row is
    parsed only to read its id, never re-serialised), so the filtered file COPYs
    identically to the original -- bronze still preserves rows as-is, just fewer
    of them. The header line is carried over so COPY's ``HEADER true`` skips it.
    The caller deletes the temp file once COPYed.

    Args:
        path: Absolute path to a source yearly CSV.
        bene_idx: Zero-based index of the beneficiary-id column.
        allowed: Beneficiary ids to keep.

    Returns:
        Absolute path to the filtered temp CSV, created inside WORK_DIR.
    """
    fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=WORK_DIR)
    with (
        os.fdopen(fd, "w", newline="", encoding="utf-8") as out,
        open(path, newline="", encoding="utf-8") as fh,
    ):
        lines = iter(fh)
        header = next(lines, None)
        if header is not None:
            out.write(header)
        for line in lines:
            # Parse one line in isolation to read its id; the source has no
            # embedded newlines, so per-line parsing matches the whole-file read.
            parsed = list(csv.reader([line], delimiter=DELIMITER))
            fields = parsed[0] if parsed else []
            if bene_idx < len(fields) and fields[bene_idx].strip() in allowed:
                out.write(line)
    return tmp_path


def _dictionary_from_records(
    records: list[tuple[str | None, str | None, str | None, str | None]],
) -> dict[str, tuple[str, str]]:
    """Build the column -> (postgres_type, description) map from metadata rows.

    Each record is ``(long_name, name, ccw_type, description)`` read from the
    landed CCW dictionary table. Indexed by both names (upper-cased) so a column
    resolves whichever it's filed under; the CCW storage class maps to a Postgres
    type via CCW_TYPE_TO_PG. Columns absent from the metadata are not in here at
    all -- the caller defaults those to text (see UNDOCUMENTED_TYPE).

    Args:
        records: Rows of ``(long_name, name, type, description)``.

    Returns:
        Mapping of upper-cased variable name -> (postgres_type, description).
    """
    dictionary: dict[str, tuple[str, str]] = {}
    for long_name, name, ccw_type, description in records:
        pg_type = CCW_TYPE_TO_PG.get((ccw_type or "").strip(), UNDOCUMENTED_TYPE)
        desc = (description or "").strip()
        for key in (long_name, name):
            if key:
                dictionary.setdefault(key.strip().upper(), (pg_type, desc))
    return dictionary


@dag(
    dag_id="cms_beneficiary_bronze_load",
    description="Land the CMS synthetic beneficiary (patient) summary file (bronze).",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two concurrent runs would race on the same WORK_DIR
    default_args=default_args,
    doc_md=__doc__,
    tags=["cms", "bronze", "beneficiary", "patient"],
    params={
        "env": Param(
            DEFAULT_ENV,
            type="string",
            enum=ENVIRONMENTS,
            title="Target environment",
            description="Which Neon connection to load into (portfolio_healthcare_<env>).",
        ),
        "dev_beneficiary_limit": Param(
            DEV_DEFAULT_LIMIT,
            type="integer",
            minimum=1,
            title="Dev beneficiary limit",
            description=(
                "Development only: load just the first N distinct BENE_IDs, kept "
                "consistently across all years; the claim loads reuse this cohort. "
                "Ignored in production, which loads everyone."
            ),
        ),
    },
)
def cms_beneficiary_bronze_load() -> None:
    """Define the bronze-landing DAG for the CMS synthetic beneficiary file."""

    @task(execution_timeout=timedelta(minutes=15))
    def download_and_unzip() -> list[str]:
        """Download the source ZIP and extract the yearly CSVs it contains.

        Returns:
            Sorted list of absolute paths to the extracted CSVs, read off XCom by
            the next task. The list is small (eleven short strings), so passing it
            through XCom is fine.

        Raises:
            ConnectionError: The file could not be fetched (an HTTP status error,
                or the host was unreachable, which on a server usually means
                egress is blocked).
            ValueError: The download came back empty, or the archive held no CSVs.
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

        # Unlike the single-CSV outpatient file, this archive ships one CSV per
        # reference year. Take all of them; the load unions them and asserts they
        # share a header. An empty archive means the release layout changed.
        csv_paths = sorted(
            os.path.join(WORK_DIR, f) for f in os.listdir(WORK_DIR) if f.lower().endswith(".csv")
        )
        if not csv_paths:
            raise ValueError(f"no csv files found in {ZIP_PATH}")

        names = [os.path.basename(p) for p in csv_paths]
        log.info("staged %s csv files: %s", len(csv_paths), names)
        return csv_paths  # next task picks these up off XCom

    @task(execution_timeout=timedelta(minutes=10), outlets=[CCW_DICTIONARY_ASSET])
    def land_ccw_dictionary() -> str:
        """Land the CCW variable metadata as its own bronze table.

        Downloads the CMS BlueButton ``all_meta.csv`` and replaces the dictionary
        table with it, every column text -- a plain bronze landing of a source
        file. The beneficiary load reads types and column descriptions back out of
        this table in the same run, so the dictionary it applies is always the one
        just landed (no drift). This is the same table the outpatient load writes;
        both drop + rebuild it from the identical source file, so re-landing here
        keeps this DAG runnable on its own without diverging from outpatient.

        Returns:
            The fully-qualified dictionary table name.

        Raises:
            ConnectionError: The metadata CSV could not be fetched.
            ValueError: The metadata came back with no header row.
        """
        env = get_current_context()["params"]["env"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        conn_id = f"portfolio_healthcare_{env}"

        # Held in memory and inserted via csv.DictReader rather than COPYed from a
        # file: the source has blank trailing lines COPY would reject, and writing
        # a CSV next to the beneficiary files would pollute that task's CSV glob.
        # all_meta.csv is small (a few hundred rows), so this is cheap.
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
    def load_to_neon(csv_paths: list[str]) -> int:
        """Replace the Neon bronze table with the yearly CSVs' contents, typed.

        Each file COPYs *directly* into the typed table -- no intermediate all-text
        staging copy -- using types read from the CCW dictionary table landed
        upstream in this same run. Direct COPY halves peak Neon storage (the
        staging table held the whole dataset a second time) and works because CMS
        dates carry a textual month Postgres parses natively and CSV COPY reads an
        empty field as NULL. The cost is that a malformed value aborts that file's
        COPY rather than surfacing in a cast -- fine for this trusted source. Rows
        are preserved one-for-one across all years; each *documented* column is
        annotated with its CCW description as a COMMENT, and columns the codeset
        doesn't cover land as text with no comment rather than failing the load.

        In development the load is capped to the first ``dev_beneficiary_limit``
        distinct BENE_IDs (default 1000) so it fits a small Neon database. The kept
        ids are the same across every year -- chosen once, then used to filter all
        files. No separate cohort table is written: the resulting table *is* the
        cohort, which the claim loads read back to subset themselves so their dev
        rows still join. Production ignores the cap and loads every beneficiary.

        Each run drops and rebuilds the typed table, so it's idempotent: the table
        always ends up equal to the (possibly subset) files, never with duplicate
        rows. The per-file COPYs are separate transactions; a mid-load failure
        leaves a partially filled table that the next run replaces wholesale --
        still no duplicates, since a run replaces, never appends.

        The target environment comes from the run's `env` param (set on the
        trigger form), so the connection id is resolved here at runtime rather
        than baked in at parse time.

        Args:
            csv_paths: Absolute paths to the extracted CSVs, read off XCom.
                Assumed reachable from this worker (see WORK_DIR note on task
                locality).

        Returns:
            Number of rows loaded across all files.

        Raises:
            ValueError: A header has duplicate column names (which would make the
                COPY target ambiguous), the files don't all share one header
                (which would make a single union table wrong), or the dev cap is
                requested but the beneficiary-id column is absent from the header.
        """
        env = get_current_context()["params"]["env"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
        conn_id = f"portfolio_healthcare_{env}"
        hook = PostgresHook(postgres_conn_id=conn_id)

        # One union table across years only makes sense if every file has the same
        # columns in the same order. Assert it rather than trusting the release.
        header = _read_header(csv_paths[0])
        if len(set(header)) != len(header):
            raise ValueError(f"duplicate column names in header: {header}")
        for path in csv_paths[1:]:
            other = _read_header(path)
            if other != header:
                raise ValueError(
                    f"{os.path.basename(path)} header differs from {os.path.basename(csv_paths[0])}"
                )

        # Types + descriptions come from the CCW dictionary table landed upstream
        # in this same run (see land_ccw_dictionary). Unlike outpatient, the MBSF
        # layout is only partially documented there, so a column the codeset
        # doesn't know lands as text with no comment -- it isn't an error.
        records = hook.get_records(  # pyright: ignore[reportUnknownMemberType]
            f"SELECT long_name, name, type, description FROM {DICTIONARY_TABLE};"
        )
        dictionary = _dictionary_from_records(records)
        specs = {  # col -> (pg_type, description)
            col: dictionary.get(col.upper(), (UNDOCUMENTED_TYPE, "")) for col in header
        }
        documented = [col for col in header if col.upper() in dictionary]
        log.info(
            "typing %s columns from CCW metadata; %s undocumented -> text",
            len(documented),
            len(header) - len(documented),
        )

        idents = {col: _quote_ident(col) for col in header}
        col_list = ", ".join(idents[col] for col in header)
        typed_ddl = ",\n    ".join(f"{idents[col]} {specs[col][0]}" for col in header)

        # Only documented columns get a COMMENT; an undocumented column has an
        # empty description and is skipped so we don't stamp blank comments.
        comments = [
            f"COMMENT ON COLUMN {FQ_TABLE}.{idents[col]} IS {_quote_literal(specs[col][1])};"
            for col in header
            if specs[col][1]
        ]

        # Dev only: pick the cohort once. Production leaves `cohort` None and loads
        # every file whole. The cap is read from the run param. No separate list
        # table is kept -- the loaded cms_beneficiary table *is* the cohort, and
        # the claim loads read their dev subset straight from it (see outpatient).
        cohort: list[str] | None = None
        bene_idx = -1
        if env == "development":
            limit = int(get_current_context()["params"]["dev_beneficiary_limit"])  # pyright: ignore[reportTypedDictNotRequiredAccess]
            if BENE_ID_COLUMN not in header:
                raise ValueError(
                    f"{BENE_ID_COLUMN} not in header; can't pick a dev cohort: {header}"
                )
            bene_idx = header.index(BENE_ID_COLUMN)
            cohort = _collect_cohort(csv_paths, bene_idx, limit)
            log.info("dev subset: keeping %s distinct %s", len(cohort), BENE_ID_COLUMN)

        # Rebuild the typed table. COPY lands rows straight here -- no staging copy.
        hook.run(  # pyright: ignore[reportUnknownMemberType]
            [
                f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};",
                f"DROP TABLE IF EXISTS {FQ_TABLE};",
                f"CREATE TABLE {FQ_TABLE} (\n    {typed_ddl}\n);",
            ]
        )

        # COPY each yearly file directly into the typed table; HEADER true skips
        # the header per file. In dev each file is filtered to the cohort first
        # (a tiny temp file in WORK_DIR, deleted once COPYed) so the full file
        # never reaches Neon.
        copy_sql = (
            f"COPY {FQ_TABLE} ({col_list}) FROM STDIN "
            f"WITH (FORMAT csv, HEADER true, DELIMITER '{DELIMITER}')"
        )
        allowed = set(cohort) if cohort is not None else None
        for path in csv_paths:
            source = path
            if allowed is not None:
                source = _filter_to_cohort(path, bene_idx, allowed)
            try:
                hook.copy_expert(copy_sql, source)
            finally:
                if source != path:
                    _safe_remove(source)
            log.info("copied %s into %s", os.path.basename(path), FQ_TABLE)

        if comments:
            hook.run(comments)  # pyright: ignore[reportUnknownMemberType]

        rows = int(hook.get_first(f"SELECT count(*) FROM {FQ_TABLE};")[0])  # pyright: ignore[reportUnknownMemberType]
        log.info("loaded %s rows into %s via %s", f"{rows:,}", FQ_TABLE, conn_id)
        return rows

    # The load reads the dictionary table, so the landing must finish first; the
    # download runs in parallel with it. Both feed the single load.
    dictionary_ready = land_ccw_dictionary()
    loaded = load_to_neon(download_and_unzip())  # pyright: ignore[reportArgumentType]
    dictionary_ready >> loaded  # pyright: ignore[reportUnusedExpression]


bronze_load_dag = cms_beneficiary_bronze_load()

if __name__ == "__main__":
    bronze_load_dag.test()  # pyright: ignore[reportUnknownMemberType]
