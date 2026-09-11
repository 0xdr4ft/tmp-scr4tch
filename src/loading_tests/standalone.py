"""Checks for a framework with no source to compare against.

Nothing here reads a second database. The target has to answer for itself:
is it still being loaded, and how recently. The datasets are found from a
system name and a layer, the tables from their suffixes, so a table added
next week is tested without anyone editing a catalogue.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any

from .main import (FAIL, PASS, SKIP, CheckResult, TableReport, check_airflow,
                   now_local)

LOG = logging.getLogger("loading_tests")

_NAME = re.compile(r"^[A-Za-z0-9_$-]+$")
_ZONE = re.compile(r"^(UTC|[A-Za-z_]+/[A-Za-z_+-]+)$")


def _zone(value: str) -> str:
    """A time zone name, or nothing: it goes straight into the SQL."""
    name = str(value or "UTC").strip()
    if not _ZONE.match(name):
        raise ValueError(f"standalone.timezone is not a usable zone: {value!r}")
    return name


def _identifier(value: str, what: str) -> str:
    """A BigQuery name, or nothing: this is what keeps the SQL below safe."""
    name = str(value or "").strip()
    if not _NAME.match(name):
        raise ValueError(f"{what} is not a usable BigQuery name: {value!r}")
    return name


def ask_system() -> str | None:
    """Which source system to test. No default; the question repeats."""
    while True:
        try:
            answer = input("\nSource system to test (required): ").strip()
        except EOFError:
            return None
        if answer:
            return answer
        print("  A system name is required - nothing is tested without one")


def ask_layer(layers: list[str]) -> str | None:
    """Which layer of that system, picked from the ones the config names."""
    listed = "  ".join(f"{i}) {name}" for i, name in enumerate(layers, 1))
    while True:
        try:
            answer = input(f"\nLayer  {listed}\nChoice [1]: ").strip() or "1"
        except EOFError:
            return None
        if answer.isdigit() and 1 <= int(answer) <= len(layers):
            return layers[int(answer) - 1]
        if answer.lower() in {name.lower() for name in layers}:
            return next(n for n in layers if n.lower() == answer.lower())
        print(f"  Pick 1-{len(layers)}, or type the layer name")


def dataset_for(cfg: dict, layer: str, system: str) -> str:
    """The dataset holding one layer of one system, as the config spells it."""
    own = cfg.get("standalone") or {}
    pattern = str(own.get("dataset_pattern") or "{layer}_{system}")
    name = pattern.format(layer=layer, system=system)
    if own.get("lowercase_datasets", True):
        name = name.lower()
    return _identifier(name, "dataset name")


def dag_for(cfg: dict, layer: str, system: str) -> str:
    """The DAG loading one layer of one system, as the config spells it."""
    own = cfg.get("standalone") or {}
    pattern = str(own.get("dag_pattern") or "")
    if not pattern:
        return ""
    name = pattern.format(layer=layer, system=system)
    return name.lower() if own.get("dag_lowercase", True) else name


def read_tables(bq: Any, project: str, dataset: str,
                suffixes: list[str]) -> list[str]:
    """The tables of a dataset whose names end the way the config says."""
    sql = (f"SELECT table_name FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES "
           f"ORDER BY table_name")
    _, rows, _ = bq.rows(sql, 10000)
    wanted = tuple(str(s).lower() for s in suffixes)
    return [str(r[0]) for r in rows if str(r[0]).lower().endswith(wanted)]


TIME_TYPES = ("TIMESTAMP", "DATETIME", "DATE")


@dataclass
class TimeColumn:
    """The column a table is measured by, and how it came to be chosen."""
    name: str = ""
    kind: str = ""                   # TIMESTAMP / DATETIME / DATE
    chosen: str = ""                 # partitioning / config
    reason: str = ""                 # why there is none


def read_columns(bq: Any, project: str, dataset: str) -> dict[str, list[tuple]]:
    """(column, type, partitioning?) per table - enough to pick what to measure."""
    sql = (f"SELECT table_name, column_name, data_type, is_partitioning_column "
           f"FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS")
    _, rows, _ = bq.rows(sql, 100000)
    found: dict[str, list[tuple]] = {}
    for table, column, kind, partitioning in rows:
        found.setdefault(str(table), []).append(
            (str(column), str(kind).upper(), str(partitioning or "").upper() == "YES"))
    return found


def time_column(columns: list[tuple], candidates: list[str]) -> TimeColumn:
    """What to measure a table by: its partitioning column, else a named one.

    The partitioning column is asked for first because it is the only thing
    BigQuery itself calls out per table - no two tables have to agree on a name.
    Ingestion-time partitioning answers `_PARTITIONTIME`, which works the same way.
    """
    for name, kind, partitioning in columns:
        if partitioning and kind in TIME_TYPES:
            return TimeColumn(name=name, kind=kind, chosen="partitioning")

    by_name = {name.upper(): (name, kind) for name, kind, _ in columns}
    for wanted in candidates or []:
        found = by_name.get(str(wanted).upper())
        if found and found[1] in TIME_TYPES:
            return TimeColumn(name=found[0], kind=found[1], chosen="config")

    partitioned = [f"{n} ({k})" for n, k, p in columns if p]
    if partitioned:
        return TimeColumn(reason=f"partitioned by {', '.join(partitioned)}, which "
                                 f"is not a date or a timestamp")
    return TimeColumn(reason="the table is not partitioned by time, and none of "
                             "the columns named in the config is there")


def _now(kind: str, zone: str) -> str:
    return {"TIMESTAMP": "CURRENT_TIMESTAMP()",
            "DATETIME": f"CURRENT_DATETIME('{zone}')"}.get(kind, f"CURRENT_DATE('{zone}')")


def _since(col: TimeColumn, hours: int, zone: str) -> str:
    """The window clause, in whatever unit the column can actually answer."""
    if col.kind == "DATE":                      # a date cannot answer in hours
        days = max(1, -(-hours // 24))
        return f"{col.name} >= DATE_SUB({_now(col.kind, zone)}, INTERVAL {days} DAY)"
    unit = "TIMESTAMP" if col.kind == "TIMESTAMP" else "DATETIME"
    return (f"{col.name} >= {unit}_SUB({_now(col.kind, zone)}, "
            f"INTERVAL {hours} HOUR)")


def _age_minutes(col: TimeColumn, zone: str) -> str:
    if col.kind == "DATE":
        return f"DATE_DIFF({_now(col.kind, zone)}, MAX({col.name}), DAY) * 1440"
    unit = "TIMESTAMP" if col.kind == "TIMESTAMP" else "DATETIME"
    return f"{unit}_DIFF({_now(col.kind, zone)}, MAX({col.name}), MINUTE)"


def _measured_by(col: TimeColumn) -> str:
    return f"measured on {col.name} ({col.kind}, from the {col.chosen})"


def check_rows_last_hours(bq: Any, project: str, dataset: str, table: str,
                          col: TimeColumn, hours: int, least: int,
                          zone: str) -> CheckResult:
    """How much arrived recently. Nothing arriving is the thing to catch."""
    sql = (f"SELECT COUNT(*) AS n FROM `{project}.{dataset}.{table}` "
           f"WHERE {_since(col, hours, zone)}")
    started = time.perf_counter()
    _, rows, _ = bq.rows(sql, 1)
    loaded = int(rows[0][0]) if rows else 0
    window = f"{hours}h" if col.kind != "DATE" else f"{max(1, -(-hours // 24))}d"
    return CheckResult(
        section="Test cases", name="ROWS_LAST_HOURS",
        status=PASS if loaded >= least else FAIL,
        expected=f">= {least:,} rows in the last {window}",
        actual=f"{loaded:,} rows",
        details=_measured_by(col),
        rows_target=loaded, duration_s=round(time.perf_counter() - started, 2))


def check_last_insert(bq: Any, project: str, dataset: str, table: str,
                      col: TimeColumn, max_age: float, zone: str) -> CheckResult:
    """When the newest row arrived, and whether that is recent enough."""
    sql = (f"SELECT MAX({col.name}) AS last_insert, "
           f"{_age_minutes(col, zone)} AS age_min "
           f"FROM `{project}.{dataset}.{table}`")
    started = time.perf_counter()
    _, rows, _ = bq.rows(sql, 1)
    newest, age_min = (rows[0] if rows else (None, None))
    took = round(time.perf_counter() - started, 2)

    if newest is None:
        return CheckResult(
            section="Test cases", name="LAST_INSERT_AT", status=FAIL,
            expected=f"a row from the last {max_age:g}h",
            actual="the table is empty", details=_measured_by(col),
            duration_s=took)

    age = float(age_min or 0) / 60
    told = _measured_by(col)
    if col.kind == "DATE":
        told += "; a date column only tells the day, so the age is rounded"
    return CheckResult(
        section="Test cases", name="LAST_INSERT_AT",
        status=PASS if age <= max_age else FAIL,
        expected=f"not older than {max_age:g}h",
        actual=f"{newest} ({age:.1f}h ago)", details=told,
        last_load_at=str(newest), tolerance_h=max_age, duration_s=took)


def test_table(bq: Any, cfg: dict, project: str, dataset: str, table: str,
               columns: list[tuple], layer: str, system: str) -> TableReport:
    """Both checks for one table, in a report shaped like every other one."""
    own = cfg.get("standalone") or {}
    rep = TableReport(table=f"{dataset}.{table}", started_at=now_local())

    col = time_column(columns, own.get("loaded_at_columns") or [])
    if not col.name:
        for name in ("ROWS_LAST_HOURS", "LAST_INSERT_AT"):
            rep.add("Test cases", name, SKIP,
                    details=f"nothing to measure {table} by: {col.reason}")
        rep.finished_at = now_local()
        return rep

    LOG.info("  %s", _measured_by(col))
    hours = int(own.get("hours", 24))
    zone = _zone(own.get("timezone") or "UTC")
    for check in (check_rows_last_hours(bq, project, dataset, table, col, hours,
                                        int(own.get("min_rows", 1)), zone),
                  check_last_insert(bq, project, dataset, table, col,
                                    float(own.get("max_age_hours", hours)), zone)):
        rep.results.append(check)
        LOG.info("[%s] %-38s %-7s %s", check.section, check.name, check.status,
                 check.actual or check.details)
    rep.finished_at = now_local()
    return rep


def run_system(bq: Any, cfg: dict, system: str, layer: str) -> list[TableReport]:
    """Every table of one layer of one system, discovered and then checked."""
    project = str(cfg.get("project_id") or "")
    dataset = dataset_for(cfg, layer, system)
    own = cfg.get("standalone") or {}
    suffixes = own.get("table_suffixes") or ["_raw", "_std"]

    LOG.info("Dataset: %s.%s", project, dataset)
    tables = read_tables(bq, project, dataset, suffixes)
    if not tables:
        LOG.error("No table in %s ends with %s - nothing to test",
                  dataset, " or ".join(suffixes))
        return []
    LOG.info("Tables: %s", ", ".join(tables))
    columns = read_columns(bq, project, dataset)

    # The whole picture first: which table is measured by what, or by nothing.
    named = own.get("loaded_at_columns") or []
    LOG.info("Measured by: %s", ", ".join(
        f"{t} -> {c.name or 'nothing: ' + c.reason}"
        for t in tables for c in [time_column(columns.get(t, []), named)]))

    reports = []
    for table in tables:
        LOG.info("=" * 78)
        LOG.info("Testing table: %s.%s", dataset, table)
        LOG.info("=" * 78)
        reports.append(test_table(bq, cfg, project, dataset, table,
                                  columns.get(table, []), layer, system))

    # One DAG loads the whole layer, so its checks are not any one table's.
    dag = dag_for(cfg, layer, system)
    if dag:
        LOG.info("=" * 78)
        LOG.info("Airflow DAG: %s", dag)
        LOG.info("=" * 78)
        rep = TableReport(table=dataset, started_at=now_local())
        try:
            check_airflow(cfg, {"name": dataset, "dag_id": dag}, rep)
        except Exception as exc:
            rep.add("Runtime", "Airflow", FAIL, details=f"{type(exc).__name__}: {exc}")
        rep.finished_at = now_local()
        reports.append(rep)
    return reports
