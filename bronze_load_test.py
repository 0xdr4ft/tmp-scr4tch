#!/usr/bin/env python3
"""
Generic loading tests for the Bronze layer of a BigQuery data warehouse (GCP).

Given a table name, the script checks:
  1) number of records in the table
  2) completeness (NULL ratio) of each column
  3) correctness of full / incremental loading
  4) status of the Airflow (Cloud Composer) DAG with statistics
  5) final test status

Results are written to:
  - a plain-text log file  (bronze_test_<ts>.log)
  - a plain-text report    (bronze_test_<ts>.txt)   <-- attach to the Jira task

Usage:
    python bronze_load_test.py --config config.yaml --table bronze.orders
    python bronze_load_test.py --config config.yaml --all
    python bronze_load_test.py --config config.yaml --all --fail-on-warn

Requirements:
    pip install google-cloud-bigquery google-auth requests pyyaml
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover
    bigquery = None

LOG = logging.getLogger("bronze_load_test")

# --------------------------------------------------------------------------- #
# Status model
# --------------------------------------------------------------------------- #

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIPPED"
_SEVERITY = {SKIP: 0, PASS: 1, WARN: 2, FAIL: 3}


def worst(statuses: list[str]) -> str:
    """Aggregate a list of statuses into the most severe one."""
    if not statuses:
        return SKIP
    return max(statuses, key=lambda s: _SEVERITY.get(s, 0))


@dataclass
class CheckResult:
    section: str
    name: str
    status: str
    expected: str = ""
    actual: str = ""
    details: str = ""


@dataclass
class TableReport:
    table: str
    load_type: str
    started_at: datetime
    results: list[CheckResult] = field(default_factory=list)
    finished_at: datetime | None = None

    def add(self, *a, **kw) -> None:
        r = CheckResult(*a, **kw)
        self.results.append(r)
        LOG.info("[%s] %-38s %-7s %s", r.section, r.name, r.status,
                 f"actual={r.actual}" if r.actual else r.details)

    @property
    def final_status(self) -> str:
        return worst([r.status for r in self.results])

    def section_status(self, section: str) -> str:
        return worst([r.status for r in self.results if r.section == section])


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

DEFAULTS = {
    "null_threshold_pct": 5.0,          # WARN above this NULL ratio
    "null_fail_threshold_pct": 50.0,    # FAIL above this NULL ratio
    "row_count_tolerance_pct": 1.0,     # allowed drift vs. source row count
    "min_row_count": 1,
    "max_load_lag_hours": 24,           # data freshness SLA
    "audit_timestamp_column": "_ingestion_ts",
    "allow_duplicates": False,
}


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    merged = dict(DEFAULTS)
    merged.update(cfg.get("defaults") or {})
    cfg["defaults"] = merged
    return cfg


def table_config(cfg: dict, table: str) -> dict:
    for t in cfg.get("tables", []):
        if t["name"] == table:
            merged = dict(cfg["defaults"])
            merged.update(t)
            return merged
    # Unknown table -> run with pure defaults (generic mode)
    merged = dict(cfg["defaults"])
    merged["name"] = table
    merged.setdefault("load_type", "unknown")
    return merged


def window_clause(tc: dict) -> str:
    """WHERE limiting EVERY check to the configured time window.

    Filtering on the partitioning column means BigQuery reads only the relevant
    partitions. The dates are passed as string literals on purpose - BigQuery
    coerces them to DATE / DATETIME / TIMESTAMP, so the same clause works no
    matter which of the three the audit column is.
    """
    col = tc["audit_timestamp_column"]
    conds = []
    if tc.get("count_from"):
        conds.append(f"`{col}` >= '{tc['count_from']}'")
    if tc.get("count_to"):
        conds.append(f"`{col}` < '{tc['count_to']}'")
    return " WHERE " + " AND ".join(conds) if conds else ""


def window_label(tc: dict) -> str:
    if not (tc.get("count_from") or tc.get("count_to")):
        return "whole table"
    return f"{tc.get('count_from') or 'start'} .. {tc.get('count_to') or 'now'}"


def fq(cfg: dict, table: str) -> str:
    """Fully-qualified `project.dataset.table` in backticks."""
    parts = table.split(".")
    if len(parts) == 3:
        return f"`{table}`"
    if len(parts) == 2:
        return f"`{cfg['project_id']}.{table}`"
    raise ValueError(f"Table name must be dataset.table or project.dataset.table: {table}")


# --------------------------------------------------------------------------- #
# BigQuery helpers
# --------------------------------------------------------------------------- #

class BQ:
    def __init__(self, project: str, location: str | None = None):
        if bigquery is None:
            raise RuntimeError("google-cloud-bigquery is not installed")
        self.client = bigquery.Client(project=project)
        self.location = location
        self.bytes_billed = 0

    def query(self, sql: str) -> list[dict]:
        LOG.debug("SQL:\n%s", sql)
        job = self.client.query(sql, location=self.location)
        rows = [dict(r) for r in job.result()]
        self.bytes_billed += job.total_bytes_billed or 0
        return rows

    def one(self, sql: str) -> dict:
        rows = self.query(sql)
        return rows[0] if rows else {}

    def columns(self, cfg: dict, table: str) -> list[dict]:
        parts = table.split(".")
        project = parts[0] if len(parts) == 3 else cfg["project_id"]
        dataset, tbl = parts[-2], parts[-1]
        sql = f"""
            SELECT column_name, data_type, is_nullable
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{tbl}'
            ORDER BY ordinal_position
        """
        return self.query(sql)

    def table_exists(self, cfg: dict, table: str) -> bool:
        ref = fq(cfg, table).strip("`")
        try:
            self.client.get_table(ref)
            return True
        except Exception:
            return False

# --------------------------------------------------------------------------- #
# 1) Row count
# --------------------------------------------------------------------------- #

def check_row_count(bq: BQ, cfg: dict, tc: dict, rep: TableReport) -> int:
    table = tc["name"]
    row_count = bq.one(
        f"SELECT COUNT(*) AS c FROM {fq(cfg, table)}{window_clause(tc)}")["c"]

    status = PASS if row_count >= tc["min_row_count"] else FAIL
    rep.add("Row count", "Table is not empty", status,
            expected=f">= {tc['min_row_count']}", actual=f"{row_count:,}")
    return row_count


# --------------------------------------------------------------------------- #
# 2) Column completeness
# --------------------------------------------------------------------------- #

def check_completeness(bq: BQ, cfg: dict, tc: dict, rep: TableReport,
                       row_count: int) -> list[dict]:
    table = tc["name"]
    cols = bq.columns(cfg, table)
    if not cols:
        rep.add("Completeness", "Schema lookup", FAIL, details="No columns found")
        return []
    if row_count == 0:
        rep.add("Completeness", "Column completeness", SKIP, details="Table is empty")
        return []

    skip = set(tc.get("ignore_columns") or [])
    required = set(tc.get("required_columns") or [])
    scalar = [c for c in cols
              if c["column_name"] not in skip
              and not c["data_type"].startswith(("ARRAY", "STRUCT"))]
    if not scalar:
        rep.add("Completeness", "Column completeness", SKIP,
                details="No scalar columns left after ignore_columns")
        return []

    # One single scan for all columns
    exprs = ",\n  ".join(
        f"COUNTIF(`{c['column_name']}` IS NULL) AS `null__{c['column_name']}`"
        for c in scalar
    )
    counts = bq.one(f"SELECT\n  {exprs}\nFROM {fq(cfg, table)}{window_clause(tc)}")

    stats = []
    for c in scalar:
        name = c["column_name"]
        nulls = counts.get(f"null__{name}", 0)
        pct = nulls / row_count * 100
        if name in required or c["is_nullable"] == "NO":
            st = PASS if nulls == 0 else FAIL
            exp = "0% NULL (required)"
        elif pct > tc["null_fail_threshold_pct"]:
            st = FAIL
            exp = f"<= {tc['null_threshold_pct']}% NULL"
        elif pct > tc["null_threshold_pct"]:
            st = WARN
            exp = f"<= {tc['null_threshold_pct']}% NULL"
        else:
            st = PASS
            exp = f"<= {tc['null_threshold_pct']}% NULL"
        stats.append({"column": name, "type": c["data_type"],
                      "nulls": nulls, "pct": pct, "status": st})
        rep.add("Completeness", f"Column `{name}`", st,
                expected=exp, actual=f"{pct:.2f}% NULL ({nulls:,} rows)")

    # Fully empty columns are always worth flagging separately
    empty = [s["column"] for s in stats if s["nulls"] == row_count]
    if empty:
        rep.add("Completeness", "Fully empty columns", FAIL,
                expected="none", actual=", ".join(empty))
    return stats


# --------------------------------------------------------------------------- #
# 3a) Duplicate rows (no primary key needed)
# --------------------------------------------------------------------------- #

# Fallback only - used when neither the config nor the source schema tells us
# which columns the pipeline itself added.
_TECH_PREFIXES = ("_", "etl_", "dw_", "dwh_", "meta_", "audit_", "dbt_")
_TECH_SUFFIXES = ("_loaded_at", "_inserted_at", "_ingested_at", "_load_ts",
                  "_source_file", "_batch_id", "_run_id")


def technical_columns(bq: BQ, cfg: dict, tc: dict,
                      cols: list[dict]) -> tuple[list[str], str]:
    """Columns added by the pipeline, i.e. not part of the business record.

    Returns (column names, how they were determined) - the 'how' is reported so
    that the exclusion list is auditable instead of magic.
    """
    names = [c["column_name"] for c in cols]
    always = {tc["audit_timestamp_column"], *(tc.get("ignore_columns") or [])}

    explicit = tc.get("technical_columns")
    if explicit:
        found = [n for n in names if n in set(explicit) | always]
        return found, "from config"

    # Derived: whatever bronze has on top of the source table is technical.
    # Only works when the source is queryable from here, i.e. also in BigQuery.
    src_cfg = tc.get("source") or {}
    src = src_cfg.get("table")
    if src and str(src_cfg.get("system", "bigquery")).lower() == "bigquery":
        try:
            src_names = {c["column_name"] for c in bq.columns(cfg, src)}
        except Exception as exc:
            LOG.warning("Cannot read source schema %s: %s", src, exc)
            src_names = set()
        if src_names:
            found = [n for n in names if n not in src_names or n in always]
            return found, f"not present in source {src}"

    guessed = [n for n in names
               if n.lower().startswith(_TECH_PREFIXES)
               or n.lower().endswith(_TECH_SUFFIXES)
               or n in always]
    return guessed, "guessed from naming convention"


def check_duplicate_rows(bq: BQ, cfg: dict, tc: dict, rep: TableReport,
                         row_count: int) -> None:
    table = tc["name"]
    if row_count == 0:
        rep.add("Loading", "Duplicate rows", SKIP, details="Table is empty")
        return

    cols = bq.columns(cfg, table)
    if not cols:
        rep.add("Loading", "Duplicate rows", SKIP, details="No columns found")
        return

    tech, how = technical_columns(bq, cfg, tc, cols)
    if len(tech) == len(cols):
        rep.add("Loading", "Duplicate rows", SKIP,
                details="All columns classified as technical")
        return

    excluded = f" EXCEPT ({', '.join(f'`{c}`' for c in tech)})" if tech else ""
    dup = bq.one(f"""
        SELECT COUNT(*) AS rows_total,
               COUNT(DISTINCT TO_JSON_STRING(t)) AS distinct_rows
        FROM (SELECT AS STRUCT *{excluded}
              FROM {fq(cfg, table)}{window_clause(tc)}) AS t
    """)
    extra = dup["rows_total"] - dup["distinct_rows"]

    st = PASS if extra == 0 or tc["allow_duplicates"] else FAIL
    rep.add("Loading", "Duplicate rows (business columns)", st,
            expected="0 duplicated rows",
            actual=f"{extra:,} duplicated rows ({dup['distinct_rows']:,} unique)",
            details=f"excluded technical columns ({how}): "
                    f"{', '.join(tech) if tech else 'none'}")


# --------------------------------------------------------------------------- #
# 3b) Full / incremental loading correctness
# --------------------------------------------------------------------------- #

_BATCH_GRAINS = ("SECOND", "MINUTE", "HOUR", "DAY")


def _batch_expr(col: str, col_type: str, grain: str) -> str:
    """SQL expression identifying one load batch.

    DATE columns already have day granularity; DATETIME and TIMESTAMP need their
    own TRUNC function - using the wrong one is a type error in BigQuery.
    """
    if col_type == "DATE":
        return f"`{col}`"
    fn = "DATETIME_TRUNC" if col_type == "DATETIME" else "TIMESTAMP_TRUNC"
    return f"{fn}(`{col}`, {grain})"


def _literal(value: Any, col_type: str) -> str:
    if col_type == "DATE":
        return f"DATE '{value}'"
    if col_type == "DATETIME":
        return f"DATETIME '{value}'"
    return f"TIMESTAMP '{value}'"


def _as_utc(value: Any) -> datetime:
    """DATETIME/DATE come back without a timezone - read them as UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def check_load_correctness(bq: BQ, cfg: dict, tc: dict, rep: TableReport,
                           row_count: int) -> None:
    table = tc["name"]
    load_type = (tc.get("load_type") or "unknown").lower()
    audit_col = tc["audit_timestamp_column"]
    wm = tc.get("watermark_column")

    if row_count == 0:
        rep.add("Loading", "Load correctness", SKIP, details="Table is empty")
        return

    types = {c["column_name"]: c["data_type"].upper() for c in bq.columns(cfg, table)}
    if audit_col not in types:
        rep.add("Loading", "Audit column present", WARN,
                expected=audit_col, actual="missing",
                details="Cannot verify load pattern without a load timestamp column")
        return

    audit_type = types[audit_col]
    grain = str(tc.get("batch_granularity",
                       "DAY" if audit_type == "DATE" else "SECOND")).upper()
    if grain not in _BATCH_GRAINS:
        rep.add("Loading", "Batch granularity", WARN,
                expected="/".join(_BATCH_GRAINS), actual=grain,
                details="Unknown granularity, falling back to SECOND")
        grain = "SECOND"
    bexpr = _batch_expr(audit_col, audit_type, grain)
    win = window_clause(tc)

    # --- Batch / freshness statistics -------------------------------------- #
    batch = bq.one(f"""
        SELECT
          COUNT(DISTINCT {bexpr}) AS batches,
          MIN({bexpr}) AS first_load,
          MAX({bexpr}) AS last_load,
          COUNTIF({bexpr} = (SELECT MAX({bexpr}) FROM {fq(cfg, table)}{win}))
              AS rows_last_batch
        FROM {fq(cfg, table)}{win}
    """)

    last_load = _as_utc(batch["last_load"])
    lag_h = (datetime.now(timezone.utc) - last_load).total_seconds() / 3600
    st = PASS if lag_h <= tc["max_load_lag_hours"] else FAIL
    rep.add("Loading", "Data freshness", st,
            expected=f"<= {tc['max_load_lag_hours']}h",
            actual=f"{lag_h:.1f}h",
            details=f"last load = {batch['last_load']} "
                    f"({audit_col}, granularity {grain})")

    rep.add("Loading", "Load batches / rows in last batch", PASS,
            actual=f"{batch['batches']:,} batches, "
                   f"{batch['rows_last_batch']:,} rows in last batch")

    # --- Pattern-specific assertions --------------------------------------- #
    if load_type == "full":
        st = PASS if batch["rows_last_batch"] == row_count else FAIL
        rep.add("Loading", "FULL: single load batch", st,
                expected=f"{row_count:,} rows in one batch",
                actual=f"{batch['rows_last_batch']:,}",
                details="Rows from older batches indicate append instead of overwrite")

    elif load_type == "incremental":
        st = PASS if batch["batches"] > 1 else WARN
        rep.add("Loading", "INCREMENTAL: multiple batches", st,
                expected="> 1 batch", actual=f"{batch['batches']:,}",
                details="Only one batch may mean the table was reloaded in full")

        st = PASS if batch["rows_last_batch"] > 0 else FAIL
        rep.add("Loading", "INCREMENTAL: last run loaded data", st,
                expected="> 0 rows", actual=f"{batch['rows_last_batch']:,}")

        if not wm:
            rep.add("Loading", "INCREMENTAL: watermark", SKIP,
                    details="No watermark_column in config")
        elif wm not in types:
            rep.add("Loading", "INCREMENTAL: watermark", WARN,
                    expected=wm, actual="missing",
                    details="Watermark column not found in the table")
        else:
            w = bq.one(f"""
                SELECT
                  MAX(`{wm}`) AS max_wm,
                  MAX(IF({bexpr} < (SELECT MAX({bexpr}) FROM {fq(cfg, table)}{win}),
                         `{wm}`, NULL)) AS prev_max_wm,
                  COUNTIF(`{wm}` IS NULL) AS null_wm
                FROM {fq(cfg, table)}{win}
            """)
            moved = w["prev_max_wm"] is None or w["max_wm"] > w["prev_max_wm"]
            rep.add("Loading", f"INCREMENTAL: watermark `{wm}` advanced",
                    PASS if moved else FAIL,
                    expected="max(watermark) increased",
                    actual=f"{w['max_wm']}",
                    details=f"previous max = {w['prev_max_wm']}")
            rep.add("Loading", f"INCREMENTAL: `{wm}` not NULL",
                    PASS if w["null_wm"] == 0 else FAIL,
                    expected="0", actual=f"{w['null_wm']:,}")

            # Nothing left behind in the source?
            src_cfg = tc.get("source") or {}
            src = src_cfg.get("table")
            system = str(src_cfg.get("system", "bigquery")).lower()
            if src and system != "bigquery":
                rep.add("Loading", "INCREMENTAL: no pending source rows", SKIP,
                        details=f"Source system '{system}' is not queried yet")
            elif src and w["max_wm"] is not None:
                pending = bq.one(f"""
                    SELECT COUNT(*) AS c FROM {fq(cfg, src)}
                    WHERE `{wm}` > {_literal(w['max_wm'], types[wm])}
                """)["c"]
                rep.add("Loading", "INCREMENTAL: no pending source rows",
                        PASS if pending == 0 else FAIL,
                        expected="0", actual=f"{pending:,}",
                        details="Source rows newer than the bronze watermark")
    else:
        rep.add("Loading", "Load pattern assertions", SKIP,
                details=f"Unknown load_type='{load_type}' (expected full/incremental)")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bronze layer loading tests (BigQuery/GCP)")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--table", action="append", help="Table to test (repeatable)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Log the SQL being run")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)])

    cfg = load_config(args.config)
    tables = args.table or [t["name"] for t in cfg.get("tables", [])]
    if not tables:
        LOG.error("No tables to test. Use --table or define tables in the config.")
        return 2

    bq = BQ(cfg["project_id"], cfg.get("location"))
    statuses = []

    for table in tables:
        tc = table_config(cfg, table)
        rep = TableReport(table=table, load_type=tc.get("load_type", "unknown"),
                          started_at=datetime.now(timezone.utc))
        LOG.info("=" * 78)
        LOG.info("Testing table: %s (load_type=%s, window=%s)",
                 table, rep.load_type, window_label(tc))
        LOG.info("=" * 78)

        if not bq.table_exists(cfg, table):
            rep.add("Row count", "Table exists", FAIL, expected="exists", actual="not found")
        else:
            try:
                row_count = check_row_count(bq, cfg, tc, rep)
                check_completeness(bq, cfg, tc, rep, row_count)
                check_duplicate_rows(bq, cfg, tc, rep, row_count)
                check_load_correctness(bq, cfg, tc, rep, row_count)
            except Exception as exc:
                LOG.exception("Unexpected error while testing %s", table)
                rep.add("Runtime", "Test execution", FAIL,
                        details=f"{type(exc).__name__}: {exc}")

        rep.finished_at = datetime.now(timezone.utc)
        LOG.info("FINAL STATUS for %s: %s", table, rep.final_status)
        statuses.append(rep.final_status)

    overall = worst(statuses)
    LOG.info("-" * 78)
    LOG.info("OVERALL STATUS: %s", overall)
    LOG.info("Bytes billed: %.2f MB", bq.bytes_billed / 1024 / 1024)
    return 1 if overall == FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())