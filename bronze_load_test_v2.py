#!/usr/bin/env python3
"""
Loading tests for the Bronze layer: BigQuery checks plus the Oracle source.

Given a table name, the script checks:
  1) number of records in the table
  2) completeness (NULL ratio) of each column
  3) duplicated rows and correctness of full / incremental loading
  4) the Oracle source: row counts agree, nothing left behind in the source
  5) final test status

Every check is limited to the same time window (count_from / count_to in the
config), filtered on the partitioning column.

The Oracle part is optional: without the driver, without credentials or without
a `source: system: oracle` block those checks report SKIPPED and the BigQuery
tests run exactly as before. bronze_load_test.py is the version from before
this integration.

Usage:
    python bronze_load_test_v2.py --config config.yaml
    python bronze_load_test_v2.py --config config.yaml --table bronze.customers

Requirements:
    pip install google-cloud-bigquery pyyaml
    pip install oracledb            # only for the source checks
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import re
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


def _window_on(col: str, tc: dict) -> str:
    """WHERE for the configured window on the given column.

    The dates are passed as string literals on purpose - BigQuery coerces them
    to DATE / DATETIME / TIMESTAMP, so the same clause works no matter which of
    the three the column is.
    """
    conds = []
    if tc.get("count_from"):
        conds.append(f"`{col}` >= '{tc['count_from']}'")
    if tc.get("count_to"):
        conds.append(f"`{col}` < '{tc['count_to']}'")
    return " WHERE " + " AND ".join(conds) if conds else ""


def window_clause(tc: dict) -> str:
    """Window on the load timestamp - "what was loaded in this period".

    Deliberately NOT count_from / count_to: those are calendar dates chosen to
    compare against the source, while the load timestamp lives on a completely
    different scale (a table reloaded last week has every row stamped last
    week). Set load_from / load_to, or load_window_days for a rolling window;
    with none of them the loading checks look at the whole table.
    """
    col = tc["audit_timestamp_column"]
    load_from, load_to = tc.get("load_from"), tc.get("load_to")
    if not load_from and tc.get("load_window_days"):
        load_from = (datetime.now(timezone.utc)
                     - timedelta(days=int(tc["load_window_days"]))).date()
    return _window_on(col, {"count_from": load_from, "count_to": load_to})


def load_window_label(tc: dict) -> str:
    if not (tc.get("load_from") or tc.get("load_to") or tc.get("load_window_days")):
        return "whole table"
    if tc.get("load_window_days") and not tc.get("load_from"):
        return f"last {tc['load_window_days']} days"
    return f"{tc.get('load_from') or 'start'} .. {tc.get('load_to') or 'now'}"


def compare_clause(tc: dict) -> str:
    """Window on the watermark - "what changed in the source in this period".

    Used wherever BigQuery is compared against Oracle. Oracle has no idea when
    a row reached bronze, so the only column both sides share is the watermark;
    filtering each side on a different column would compare two different sets
    of rows and report a difference that is not there.
    """
    return _window_on(tc.get("watermark_column") or tc["audit_timestamp_column"], tc)


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

def oracle_source_of(tc: dict) -> tuple[str, str] | None:
    """(table, watermark column) when this table is fed from Oracle."""
    source = tc.get("source") or {}
    if str(source.get("system", "")).lower() != "oracle" or not source.get("table"):
        return None
    column = source.get("watermark_column") or (tc.get("watermark_column") or "").upper()
    return source["table"], column


def check_row_count(bq: BQ, cfg: dict, tc: dict, rep: TableReport) -> int:
    """Oracle first, then bronze, then the comparison - in that order."""
    table = tc["name"]
    src = oracle_source_of(tc)

    # --- 1) Oracle ---------------------------------------------------------- #
    src_count = None
    if src:
        src_table, src_column = src
        try:
            conn = oracle_connect(cfg)
            src_count = oracle_count(conn, src_table, src_column,
                                     since=tc.get("count_from"), until=tc.get("count_to"))
            rep.add("Row count", f"ORACLE  {src_table}", PASS,
                    actual=f"{src_count:,} rows",
                    details=f"window: {window_label(tc)} on {src_column}")
        except ImportError:
            rep.add("Row count", "ORACLE  row count", SKIP, details="oracledb not installed")
        except Exception as exc:
            rep.add("Row count", "ORACLE  row count", WARN,
                    details=f"{type(exc).__name__}: {exc}")

    # --- 2) BigQuery -------------------------------------------------------- #
    wm_col = tc.get("watermark_column") or tc["audit_timestamp_column"]
    row_count = bq.one(
        f"SELECT COUNT(*) AS c FROM {fq(cfg, table)}{compare_clause(tc)}")["c"]
    status = PASS if row_count >= tc["min_row_count"] else FAIL
    rep.add("Row count", f"GCP     {table}", status,
            expected=f">= {tc['min_row_count']}", actual=f"{row_count:,} rows",
            details=f"window: {window_label(tc)} on `{wm_col}`")

    # --- 3) Comparison ------------------------------------------------------ #
    if src_count is None:
        return row_count

    diff = row_count - src_count
    tol = tc["row_count_tolerance_pct"]
    diff_pct = abs(diff) / src_count * 100 if src_count else (100.0 if diff else 0.0)
    if diff == 0:
        st, note = PASS, "identical"
    elif diff < 0 and diff_pct <= tol:
        st, note = WARN, "source ahead - replication has not caught up"
    elif diff < 0:
        st, note = FAIL, "bronze is behind the source by more than the tolerance"
    else:
        st, note = FAIL, "bronze has MORE rows than the source - duplicates or a reload"
    rep.add("Row count", "COMPARE gcp vs oracle", st,
            expected=f"{src_count:,} (+-{tol}%)", actual=f"{row_count:,}",
            details=f"diff = {diff:+,} ({diff_pct:.3f}%) - {note}")
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
    counts = bq.one(f"SELECT\n  {exprs}\nFROM {fq(cfg, table)}{compare_clause(tc)}")

    # The same counts on the Oracle side, so both can be shown next to each other
    ora_nulls, ora_rows = oracle_null_counts(cfg, tc, [c["column_name"] for c in scalar])

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

        ora = ora_nulls.get(name.upper())
        ora_pct = (ora / ora_rows * 100) if ora is not None and ora_rows else None
        stats.append({"column": name, "type": c["data_type"], "nulls": nulls,
                      "pct": pct, "oracle_nulls": ora, "oracle_pct": ora_pct,
                      "status": st})
        actual = f"{pct:.2f}% NULL ({nulls:,} rows)"
        if ora_pct is not None:
            actual += f" | oracle {ora_pct:.2f}% ({ora:,} rows)"
        rep.add("Completeness", f"Column `{name}`", st, expected=exp, actual=actual)

    if ora_nulls:
        completeness_table(tc, stats, row_count, ora_rows, rep)

    # Fully empty columns are always worth flagging separately
    empty = [s["column"] for s in stats if s["nulls"] == row_count]
    if empty:
        rep.add("Completeness", "Fully empty columns", FAIL,
                expected="none", actual=", ".join(empty))
    return stats


def completeness_table(tc: dict, stats: list[dict], row_count: int,
                       ora_rows: int, rep: TableReport) -> None:
    """Side-by-side GCP vs Oracle NULL ratios, plus one verdict for the table."""
    tol = float(tc.get("null_match_tolerance_pp", 0.10))     # percentage points
    width = max([len("COLUMN")] + [len(s["column"]) for s in stats]) + 2

    LOG.info("")
    LOG.info("  %s%12s %12s %12s %12s  %s", "COLUMN".ljust(width),
             "GCP NULL%", "ORA NULL%", "GCP NULLS", "ORA NULLS", "MATCH")
    LOG.info("  %s", "-" * (width + 66))

    mismatched = []
    for s in stats:
        if s["oracle_pct"] is None:
            verdict = "n/a"
        elif abs(s["pct"] - s["oracle_pct"]) <= tol:
            verdict = "OK"
        else:
            verdict = "DIFF"
            mismatched.append(s["column"])
        LOG.info("  %s%11.2f%% %11s%s %12s %12s  %s",
                 s["column"].ljust(width), s["pct"],
                 f"{s['oracle_pct']:.2f}" if s["oracle_pct"] is not None else "-",
                 "%" if s["oracle_pct"] is not None else " ",
                 f"{s['nulls']:,}",
                 f"{s['oracle_nulls']:,}" if s["oracle_nulls"] is not None else "-",
                 verdict)
    LOG.info("  %s%11s  %11s  %12s %12s", "ROWS".ljust(width), "", "",
             f"{row_count:,}", f"{ora_rows:,}")
    LOG.info("")

    compared = [s for s in stats if s["oracle_pct"] is not None]
    if not compared:
        rep.add("Completeness", "GCP vs Oracle NULL ratios", SKIP,
                details="No column could be compared")
        return
    rep.add("Completeness", "GCP vs Oracle NULL ratios",
            WARN if mismatched else PASS,
            expected=f"same ratio (+-{tol}pp)",
            actual=f"{len(compared) - len(mismatched)}/{len(compared)} columns match",
            details=("differs: " + ", ".join(mismatched)) if mismatched else "")


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
                           row_count: int) -> Any:
    """Returns MAX(watermark) so the source check can ask Oracle what is newer."""
    table = tc["name"]
    load_type = (tc.get("load_type") or "unknown").lower()
    audit_col = tc["audit_timestamp_column"]
    wm = tc.get("watermark_column")

    max_wm = None
    if row_count == 0:
        rep.add("Loading", "Load correctness", SKIP, details="Table is empty")
        return max_wm

    types = {c["column_name"]: c["data_type"].upper() for c in bq.columns(cfg, table)}
    if audit_col not in types:
        rep.add("Loading", "Audit column present", WARN,
                expected=audit_col, actual="missing",
                details="Cannot verify load pattern without a load timestamp column")
        return max_wm

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

    # No rows in the load window: MAX() comes back NULL. That is a finding of
    # its own, not a crash - the watermark window may well have matched rows.
    if batch.get("last_load") is None:
        rep.add("Loading", "Rows in the load window", WARN,
                expected="> 0 rows", actual="0 rows",
                details=f"nothing has `{audit_col}` within {load_window_label(tc)}; "
                        f"the loading checks need that column, "
                        f"row counts use `{tc.get('watermark_column')}` instead")
        return max_wm

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
            max_wm = w["max_wm"]
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
                        details=f"Source system '{system}' is checked separately")
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
    return max_wm


# --------------------------------------------------------------------------- #
# 4) Oracle source system
# --------------------------------------------------------------------------- #

_ORACLE_CONN: Any = None            # one connection per run, opened on first use
_ORACLE_ERROR: Exception | None = None      # remembered so we ask for the password once
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")


def _oracle_ident(value: str, what: str) -> str:
    """Table and column names cannot be bound as parameters - validate them."""
    if not _IDENT.match(value or ""):
        raise ValueError(f"Invalid {what}: {value!r}")
    return value


def _oracle_dsn(oracle_cfg: dict) -> str:
    if oracle_cfg.get("dsn"):
        return str(oracle_cfg["dsn"])
    host, service = oracle_cfg.get("host"), oracle_cfg.get("service_name")
    if not host or not service:
        raise ValueError("Config needs oracle.dsn, or oracle.host + oracle.service_name")
    return f"{host}:{oracle_cfg.get('port', 1521)}/{service}"


def oracle_connect(cfg: dict) -> Any:
    """Connection shared by every table in one run - asks for credentials once."""
    global _ORACLE_CONN, _ORACLE_ERROR
    if _ORACLE_CONN is not None:
        return _ORACLE_CONN
    if _ORACLE_ERROR is not None:
        # Do not ask for the password again once it has already failed
        raise _ORACLE_ERROR

    import oracledb                  # raises ImportError -> caller reports SKIPPED

    oracle_cfg = cfg.get("oracle") or {}
    dsn = _oracle_dsn(oracle_cfg)
    user = (os.environ.get("ORACLE_USER") or oracle_cfg.get("user")
            or input(f"Oracle user for {dsn}: ").strip())
    password = os.environ.get("ORACLE_PASSWORD") or getpass.getpass(f"Password for {user}: ")

    LOG.info("Connecting to Oracle %s as %s", dsn, user)
    try:
        _ORACLE_CONN = oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as exc:
        _ORACLE_ERROR = exc
        raise
    LOG.info("Connected (Oracle %s)", _ORACLE_CONN.version)
    return _ORACLE_CONN


def oracle_close() -> None:
    global _ORACLE_CONN
    if _ORACLE_CONN is not None:
        _ORACLE_CONN.close()
        _ORACLE_CONN = None


def oracle_count(conn: Any, table: str, column: str | None = None, since: Any = None,
                 until: Any = None, above: Any = None) -> int:
    """COUNT(*) with the same window as the BigQuery side, or above a watermark."""
    _oracle_ident(table, "table name")
    conds, params = [], {}
    if column:
        _oracle_ident(column, "column name")
        for name, value, op in (("since", since, ">="), ("until", until, "<"),
                                ("above", above, ">")):
            if value is not None:
                conds.append(f"{column} {op} :{name}")
                params[name] = value

    sql = f"SELECT COUNT(*) FROM {table}"
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    LOG.debug("Oracle SQL: %s  %s", sql, params)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def oracle_null_counts(cfg: dict, tc: dict,
                       columns: list[str]) -> tuple[dict[str, int], int]:
    """NULLs per column on the Oracle side, in the same window as BigQuery.

    Returns ({COLUMN_NAME: nulls}, rows_in_window). Empty when the table has no
    Oracle source, the driver is missing or anything goes wrong - the BigQuery
    checks must never depend on it.
    """
    src = oracle_source_of(tc)
    if not src:
        return {}, 0
    table, column = src

    try:
        conn = oracle_connect(cfg)
    except ImportError:
        return {}, 0
    except Exception as exc:
        LOG.warning("Oracle unavailable, skipping the comparison: %s", exc)
        return {}, 0

    try:
        owner, _, tbl = table.upper().rpartition(".")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM all_tab_columns "
                "WHERE owner = :owner AND table_name = :tbl",
                owner=owner, tbl=tbl)
            available = {r[0] for r in cur.fetchall()}

        wanted = [c for c in columns if c.upper() in available]
        if not wanted:
            LOG.warning("None of the bronze columns exist in %s", table)
            return {}, 0

        # COUNT(col) skips NULLs, so COUNT(*) - COUNT(col) is the NULL count.
        # Numbered aliases keep us clear of Oracle identifier length limits.
        exprs = ", ".join(f"COUNT(*) - COUNT({c.upper()}) AS N{i}"
                          for i, c in enumerate(wanted))
        sql = f"SELECT COUNT(*) AS TOTAL, {exprs} FROM {table}"
        conds, params = [], {}
        if column and tc.get("count_from") is not None:
            conds.append(f"{column} >= :since")
            params["since"] = tc["count_from"]
        if column and tc.get("count_to") is not None:
            conds.append(f"{column} < :until")
            params["until"] = tc["count_to"]
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        LOG.debug("Oracle SQL: %s  %s", sql, params)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    except Exception as exc:
        LOG.warning("Oracle NULL counts failed: %s: %s", type(exc).__name__, exc)
        return {}, 0

    total = int(row[0])
    return {c.upper(): int(v) for c, v in zip(wanted, row[1:])}, total


def check_source_oracle(cfg: dict, tc: dict, rep: TableReport, bronze_count: int,
                        max_wm: Any = None) -> None:
    """Row counts agree, and nothing newer is left behind in the source."""
    source = tc.get("source") or {}
    table = source.get("table")
    column = source.get("watermark_column") or (tc.get("watermark_column") or "").upper()
    if not table:
        rep.add("Source", "Oracle source", SKIP, details="No source table in config")
        return

    try:
        conn = oracle_connect(cfg)
    except ImportError:
        rep.add("Source", "Oracle connection", SKIP, details="oracledb not installed")
        return
    except Exception as exc:
        rep.add("Source", "Oracle connection", WARN, details=f"{type(exc).__name__}: {exc}")
        return

    # Row counts are compared in check_row_count, next to the bronze count.
    # --- Anything newer than what bronze already has? ----------------------- #
    if max_wm is None:
        rep.add("Source", "Pending source rows", SKIP,
                details="No watermark value from bronze")
        return
    try:
        pending = oracle_count(conn, table, column, above=max_wm)
    except Exception as exc:
        rep.add("Source", "Pending source rows", WARN, details=f"{type(exc).__name__}: {exc}")
        return
    rep.add("Source", "Pending source rows", PASS if pending == 0 else WARN,
            expected="0", actual=f"{pending:,}",
            details=f"rows in Oracle newer than {max_wm}")


# --------------------------------------------------------------------------- #
# 5) Airflow / Cloud Composer
# --------------------------------------------------------------------------- #

def dag_id_for(tc: dict, af: dict) -> str:
    """Build the dag_id from the table name, unless the config states it.

        <dag_id_prefix><dag_prefix><TABLE NAME minus a stripped suffix>

    dag_prefix and dag_id_strip_suffixes can be set globally under `airflow:`
    or per table, because the naming differs between source systems.
    """
    if tc.get("dag_id"):
        return str(tc["dag_id"])

    name = tc["name"].split(".")[-1].upper()
    for suffix in tc.get("dag_id_strip_suffixes", af.get("dag_id_strip_suffixes")) or []:
        suffix = str(suffix).upper()
        if suffix and name.endswith(suffix):
            name = name[: -len(suffix)]
            break

    prefix = af.get("dag_id_prefix", "")
    extra = tc.get("dag_prefix", af.get("dag_prefix", ""))
    return f"{prefix}{extra}{name}"


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def check_airflow(cfg: dict, tc: dict, rep: TableReport) -> None:
    af = cfg.get("airflow") or {}
    base_url = af.get("base_url")
    if not af.get("enabled", True) or not base_url:
        rep.add("Airflow", "DAG status", SKIP, details="Airflow not configured")
        return

    try:
        from google.auth import default as google_auth_default
        from google.auth.transport.requests import AuthorizedSession
    except ImportError:
        rep.add("Airflow", "DAG status", SKIP, details="google-auth not installed")
        return

    dag_id = dag_id_for(tc, af)
    api = f"{base_url.rstrip('/')}/api/{af.get('api_version', 'v2')}"
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    # The corporate network inspects TLS, so the certificate never validates.
    if af.get("verify_ssl", True) is False:
        session.verify = False
        LOG.warning("TLS verification disabled for Airflow (airflow.verify_ssl: false)")
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass

    def get(path: str, **params) -> dict:
        r = session.request("GET", f"{api}{path}", params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    limit = int(af.get("history_runs", 10))
    try:
        runs = get(f"/dags/{dag_id}/dagRuns", order_by="-logical_date", limit=limit)
    except Exception as exc:      # network or permission problems must not kill the run
        rep.add("Airflow", f"DAG runs ({dag_id})", WARN, details=f"API error: {exc}")
        return

    dag_runs = runs.get("dag_runs", [])
    if not dag_runs:
        rep.add("Airflow", f"DAG runs ({dag_id})", FAIL, expected="at least 1 run",
                actual="0 runs", details="Wrong dag_id, or the DAG never ran")
        return

    last = dag_runs[0]
    state = last.get("state")
    st = {"success": PASS, "running": WARN, "queued": WARN}.get(state, FAIL)
    rep.add("Airflow", f"Last DAG run state ({dag_id})", st,
            expected="success", actual=str(state),
            details=f"run_id={last.get('dag_run_id')}, "
                    f"logical_date={last.get('logical_date') or last.get('run_after')}")

    # Duration of the last run against the average of the recent ones
    durations = []
    for r in dag_runs:
        start, end = _parse_ts(r.get("start_date")), _parse_ts(r.get("end_date"))
        if start and end:
            durations.append((end - start).total_seconds())
    if durations:
        last_dur, avg = durations[0], sum(durations) / len(durations)
        slow = len(durations) > 2 and last_dur > avg * float(af.get("duration_factor", 2.0))
        rep.add("Airflow", "Run duration vs average", WARN if slow else PASS,
                expected=f"~{avg / 60:.1f} min (avg of {len(durations)} runs)",
                actual=f"{last_dur / 60:.1f} min")

    succeeded = sum(1 for r in dag_runs if r.get("state") == "success")
    ratio = succeeded / len(dag_runs) * 100
    min_ratio = float(af.get("min_success_ratio_pct", 80))
    rep.add("Airflow", "Success ratio (recent runs)",
            PASS if ratio >= min_ratio else WARN,
            expected=f">= {min_ratio:.0f}%",
            actual=f"{ratio:.0f}% ({succeeded}/{len(dag_runs)})")

    # Task-level detail for the last run
    try:
        tis = get(f"/dags/{dag_id}/dagRuns/{last['dag_run_id']}/taskInstances")
        tasks = tis.get("task_instances", [])
    except Exception as exc:
        rep.add("Airflow", "Task instances (last run)", WARN, details=f"API error: {exc}")
        return

    by_state: dict[str, int] = {}
    for t in tasks:
        by_state[t.get("state") or "none"] = by_state.get(t.get("state") or "none", 0) + 1
    failed = [t["task_id"] for t in tasks if t.get("state") == "failed"]
    retries = sum((t.get("try_number") or 1) - 1 for t in tasks)
    rep.add("Airflow", "Task instances (last run)", FAIL if failed else PASS,
            expected="no failed tasks",
            actual=", ".join(f"{k}={v}" for k, v in sorted(by_state.items())),
            details=(f"failed: {', '.join(failed)}; " if failed else "")
                    + f"total retries: {retries}")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bronze loading tests (BigQuery + Oracle)")
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

    # Log in to Oracle up front. When a table declares an Oracle source, the
    # login is mandatory: without it half the checks would be meaningless, so
    # nothing runs at all - not even the BigQuery side.
    if any(oracle_source_of(table_config(cfg, t)) for t in tables):
        try:
            oracle_connect(cfg)
        except ImportError:
            LOG.error("oracledb is not installed - run `pip install oracledb`, "
                      "or remove the `source:` block to test BigQuery alone")
            return 2
        except Exception as exc:
            LOG.error("Oracle login failed: %s: %s", type(exc).__name__, exc)
            LOG.error("No tests were run. Fix the credentials or the connection "
                      "and try again.")
            return 2

    bq = BQ(cfg["project_id"], cfg.get("location"))
    statuses = []

    for table in tables:
        tc = table_config(cfg, table)
        rep = TableReport(table=table, load_type=tc.get("load_type", "unknown"),
                          started_at=datetime.now(timezone.utc))
        LOG.info("=" * 78)
        LOG.info("Testing table: %s (load_type=%s)", table, rep.load_type)
        LOG.info("  compare window: %s on `%s`", window_label(tc),
                 tc.get("watermark_column") or tc["audit_timestamp_column"])
        LOG.info("  load window   : %s on `%s`", load_window_label(tc),
                 tc["audit_timestamp_column"])
        LOG.info("=" * 78)

        if not bq.table_exists(cfg, table):
            rep.add("Row count", "Table exists", FAIL, expected="exists", actual="not found")
        else:
            # Each check is isolated: one blowing up must not cancel the rest,
            # otherwise a single bad column hides the Airflow status as well.
            def run(name: str, fn: Any, *args: Any) -> Any:
                try:
                    return fn(*args)
                except Exception as exc:
                    LOG.exception("Unexpected error in %s (%s)", name, table)
                    rep.add("Runtime", name, FAIL,
                            details=f"{type(exc).__name__}: {exc}")
                    return None

            row_count = run("Row count", check_row_count, bq, cfg, tc, rep)
            if row_count is not None:
                run("Completeness", check_completeness, bq, cfg, tc, rep, row_count)
                run("Duplicate rows", check_duplicate_rows, bq, cfg, tc, rep, row_count)
                max_wm = run("Load correctness", check_load_correctness,
                             bq, cfg, tc, rep, row_count)
                if oracle_source_of(tc):
                    run("Oracle source", check_source_oracle,
                        cfg, tc, rep, row_count, max_wm)
            run("Airflow", check_airflow, cfg, tc, rep)

        rep.finished_at = datetime.now(timezone.utc)
        LOG.info("FINAL STATUS for %s: %s", table, rep.final_status)
        statuses.append(rep.final_status)

    oracle_close()
    overall = worst(statuses)
    LOG.info("-" * 78)
    LOG.info("OVERALL STATUS: %s", overall)
    LOG.info("Bytes billed: %.2f MB", bq.bytes_billed / 1024 / 1024)
    return 1 if overall == FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())