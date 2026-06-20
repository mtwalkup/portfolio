"""Generate the Markdown data dictionary for the CMS Outpatient bronze table.

Reads the *deployed* table straight from Postgres -- column order, Postgres type,
and the column COMMENT the load applied -- and writes a human-readable table to
``docs/``. The doc therefore mirrors exactly what's in the database; it is never
hand-authored. CCW storage class / length / source are joined in from the
``bronze.ccw_variable_metadata`` table that the load lands alongside the data.

Connection comes from the ``PG_DSN`` env var (a libpq URI). In CI that's a secret
pointing at the Neon database; locally, export it before running:

    PG_DSN=postgres://... python healthcare/airflow/scripts/build_data_dictionary.py

Run in CI as a freshness check: regenerate, then ``git diff --exit-code`` the
output. A non-empty diff means the committed doc has drifted from the table.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final, NamedTuple

import psycopg2

HERE: Final[Path] = Path(__file__).resolve().parent
OUTPUT_FILE: Final[Path] = HERE.parent / "docs" / "cms_outpatient_data_dictionary.md"
TABLE: Final[str] = "bronze.cms_outpatient"
DICTIONARY_TABLE: Final[str] = "bronze.ccw_variable_metadata"

# Column order, Postgres type, and COMMENT come from the live table's catalog;
# CCW class/length/source are joined in from the landed dictionary table. One
# LATERAL row per column keeps the join from multiplying rows.
_QUERY: Final[str] = f"""
SELECT a.attnum AS position,
       a.attname AS column_name,
       format_type(a.atttypid, a.atttypmod) AS pg_type,
       col_description(a.attrelid, a.attnum) AS description,
       m.type AS ccw_type,
       m.length AS ccw_length,
       m.source AS source
FROM pg_attribute a
LEFT JOIN LATERAL (
    SELECT type, length, source
    FROM {DICTIONARY_TABLE} d
    WHERE upper(d.long_name) = upper(a.attname) OR upper(d.name) = upper(a.attname)
    LIMIT 1
) m ON TRUE
WHERE a.attrelid = '{TABLE}'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
"""


class Column(NamedTuple):
    """One column of the deployed table, as read from the catalog."""

    position: int
    name: str
    pg_type: str
    description: str
    ccw_type: str
    ccw_length: str
    source: str


def _read_columns(dsn: str) -> list[Column]:
    """Read the deployed table's columns from Postgres.

    Args:
        dsn: libpq connection URI for the target database.

    Returns:
        The table's columns in ordinal order.

    Raises:
        RuntimeError: The target table doesn't exist (nothing to document).
    """
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (TABLE,))
        if cur.fetchone()[0] is None:
            raise RuntimeError(f"{TABLE} does not exist -- run the load first")
        cur.execute(_QUERY)
        rows = cur.fetchall()

    return [
        Column(
            position=position,
            name=name,
            pg_type=pg_type,
            description=(description or "").strip(),
            ccw_type=(ccw_type or "").strip(),
            ccw_length=(ccw_length or "").strip(),
            source=(source or "").strip(),
        )
        for position, name, pg_type, description, ccw_type, ccw_length, source in rows
    ]


def _render(columns: list[Column]) -> str:
    """Render the columns as a Markdown document.

    Args:
        columns: The deployed table's columns, in order.

    Returns:
        The full Markdown document.
    """
    pg_counts: dict[str, int] = {}
    for col in columns:
        pg_counts[col.pg_type] = pg_counts.get(col.pg_type, 0) + 1
    summary = ", ".join(f"{count} {pg_type}" for pg_type, count in sorted(pg_counts.items()))

    lines = [
        "# CMS Outpatient Claims — Data Dictionary",
        "",
        f"Generated from the deployed `{TABLE}` table — column order, Postgres type, and",
        "the column COMMENT are read straight from the database, so this doc mirrors what",
        "is actually loaded. CCW storage class / length / source are joined in from",
        f"`{DICTIONARY_TABLE}`. **Do not hand-edit** — regenerate with",
        "`scripts/build_data_dictionary.py`.",
        "",
        f"- **Columns:** {len(columns)} ({summary})",
        "- **Source of descriptions:** CCW/NCH variable metadata"
        " ([CMS BlueButton codesets](https://github.com/CMSgov/bluebutton-csv-codesets)),"
        " applied to the table as COMMENTs by the load",
        "",
        "| # | Column | Postgres type | CCW type | Len | Source | Description |",
        "|--:|--------|---------------|----------|----:|--------|-------------|",
    ]
    for col in columns:
        desc = col.description.replace("|", "\\|")
        lines.append(
            f"| {col.position} | `{col.name}` | `{col.pg_type}` | {col.ccw_type} "
            f"| {col.ccw_length} | {col.source} | {desc} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Read the deployed table and write the dictionary to the docs folder."""
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN is not set (libpq URI for the target database)")
    columns = _read_columns(dsn)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(_render(columns), encoding="utf-8")
    print(f"wrote {OUTPUT_FILE} ({len(columns)} columns)")


if __name__ == "__main__":
    main()
