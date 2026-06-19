"""
Bronze landing for the CMS synthetic outpatient claims file.

Pulls the published ZIP, unpacks the single CSV inside, and leaves it on the
worker's local disk for the next stage. Nothing is reshaped here: bronze takes
the file exactly as CMS ships it and lets the silver layer argue with it later.

Run by hand. The source is a static, point-in-time release, so there's nothing
to schedule against. Re-run it when CMS republishes, or when you point it at a
batch you generated yourself with Synthea.
"""

from __future__ import annotations

import logging
import os
import shutil
import urllib.error
import urllib.request
import zipfile
from datetime import timedelta
from typing import Any, Final

import pendulum
from airflow.sdk import dag, task  # pyright: ignore[reportUnknownVariableType]

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

    download_and_unzip()


bronze_load_dag = cms_outpatient_bronze_load()

if __name__ == "__main__":
    bronze_load_dag.test()  # pyright: ignore[reportUnknownMemberType]
