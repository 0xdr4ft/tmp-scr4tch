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
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from .main import (FAIL, LOCAL_TZ, PASS, SKIP, WARN, CheckResult, TableReport,
                   _parse_ts, airflow_api, check_airflow, now_local)

try:
    from google.cloud import storage
except ImportError:  # pragma: no cover
    storage = None

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


@dataclass
class LoadRun:
    """The most recent DAG run that actually loaded, or why there is none."""
    started: datetime | None = None
    run_id: str = ""
    task: str = ""
    reason: str = ""


def final_task(get: Any, dag: str) -> tuple[str, str]:
    """(the task nothing runs after, why there is no single one)."""
    tasks = get(f"/dags/{quote(dag, safe='')}/tasks").get("tasks", [])
    leaves = [t["task_id"] for t in tasks if not t.get("downstream_task_ids")]
    if len(leaves) == 1:
        return leaves[0], ""
    return "", (f"{dag} ends in {len(leaves)} tasks ({', '.join(leaves) or 'none'}), "
                f"so there is no single final task to read an ingest from")


def newest_success(get: Any, path: str, task: str,
                   cutoff: datetime) -> tuple[dict | None, str]:
    """(the newest successful instance of `task`, why it cannot be trusted).

    One call over every run at once: Airflow filters in its own database, so how
    many empty runs came before the ingest does not change what is asked.
    """
    data = get(f"{path}/dagRuns/~/taskInstances", task_id=task, state="success",
               order_by="-start_date", limit=100,
               start_date_gte=f"{cutoff.astimezone(timezone.utc):%Y-%m-%dT%H:%M:%SZ}")
    found = data.get("task_instances", [])
    mine = [t for t in found if t.get("task_id") == task and t.get("state") == "success"]
    if found and not mine:
        # Instances of other tasks came back: the filters were not applied.
        return None, f"Airflow ignored the task_id and state filters for {task}"
    return (max(mine, key=lambda t: t.get("start_date") or "") if mine else None), ""


def last_load(cfg: dict, dag: str) -> LoadRun:
    """The newest run whose final task succeeded: an ingest, not an empty run.

    The DAG succeeds either way; only its final task tells the two apart, being
    skipped when there was nothing to take in.
    """
    own = cfg.get("standalone") or {}
    get, why = airflow_api(cfg)
    if get is None:
        return LoadRun(reason=why)

    days = int(own.get("lookback_days", 30))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    path = f"/dags/{quote(dag, safe='')}"
    try:
        task, why = final_task(get, dag)
        if not task:
            return LoadRun(reason=why)

        ti, why = newest_success(get, path, task, cutoff)
        if why:
            return LoadRun(task=task, reason=why)
        if not ti:
            return LoadRun(task=task, reason=f"{task} of {dag} has not succeeded "
                                             f"in the last {days} days")
        run_id = ti.get("dag_run_id") or ""
        run = get(f"{path}/dagRuns/{quote(run_id, safe='')}")
    except Exception as exc:
        return LoadRun(reason=f"the runs of {dag} could not be read: {exc}")

    return LoadRun(started=_parse_ts(run.get("start_date"))
                   or _parse_ts(ti.get("start_date")), run_id=run_id, task=task)


def _boundary(col: TimeColumn, since: datetime, zone: str) -> str:
    """A moment, spelled in whatever terms the column can be compared in."""
    moment = f"TIMESTAMP '{since.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S}+00'"
    if col.kind == "DATE":
        return f"DATE({moment}, '{zone}')"
    if col.kind == "DATETIME":
        return f"DATETIME({moment}, '{zone}')"
    if col.name.upper().startswith("_PARTITION"):
        # Ingestion time is cut to the partition, so the run's rows sit earlier.
        return f"TIMESTAMP_TRUNC({moment}, DAY)"
    return moment


def _shown(value: Any) -> str:
    """A value from BigQuery in the zone the rest of the report speaks."""
    if isinstance(value, datetime) and value.tzinfo:
        value = value.astimezone(LOCAL_TZ) if LOCAL_TZ else value
    return f"{value:%Y-%m-%d %H:%M:%S}" if isinstance(value, datetime) else str(value)


def _after(col: TimeColumn, since: datetime, zone: str) -> str:
    """Rows from that load on, in whatever terms the column can answer."""
    return f"{col.name} >= {_boundary(col, since, zone)}"


def check_last_insert_since_load(bq: Any, project: str, dataset: str, table: str,
                                 col: TimeColumn, load: LoadRun, slack: float,
                                 zone: str) -> CheckResult:
    """Whether the newest row came with the last load, not before it.

    Compared with the load and not with now: a DAG that had nothing to take in
    for a week has a week-old newest row, and that is not a fault.
    """
    if load.started is None:
        return CheckResult(section="Test cases", name="LAST_INSERT_AT",
                           status=SKIP, details=load.reason)
    since = load.started - timedelta(hours=slack)
    sql = (f"SELECT MAX({col.name}) AS last_insert, "
           f"MAX({col.name}) >= {_boundary(col, since, zone)} AS with_load "
           f"FROM `{project}.{dataset}.{table}`")
    started = time.perf_counter()
    _, rows, _ = bq.rows(sql, 1)
    newest, with_load = (rows[0] if rows else (None, None))
    took = round(time.perf_counter() - started, 2)

    local = load.started.astimezone(LOCAL_TZ) if LOCAL_TZ else load.started
    told = f"{_measured_by(col)}; load = {load.run_id}"
    if col.kind == "DATE":
        told += "; a date column only tells the day"
    return CheckResult(
        section="Test cases", name="LAST_INSERT_AT",
        status=PASS if with_load else FAIL,
        expected=f"newest row not before the last load (-{slack:g}h)",
        actual=(f"{_shown(newest)} (last load {local:%Y-%m-%d %H:%M:%S})"
                if newest is not None else "the table is empty"),
        details=told, last_load_at=f"{local:%Y-%m-%d %H:%M:%S}",
        tolerance_h=slack, duration_s=took)


def check_rows_since_load(bq: Any, project: str, dataset: str, table: str,
                          col: TimeColumn, load: LoadRun, least: int,
                          zone: str) -> CheckResult:
    """Whether the last run that loaded left rows behind."""
    if load.started is None:
        return CheckResult(section="Test cases", name="ROWS_SINCE_LAST_LOAD",
                           status=SKIP, details=load.reason)
    sql = (f"SELECT COUNT(*) AS n FROM `{project}.{dataset}.{table}` "
           f"WHERE {_after(col, load.started, zone)}")
    started = time.perf_counter()
    _, rows, _ = bq.rows(sql, 1)
    loaded = int(rows[0][0]) if rows else 0
    local = load.started.astimezone(LOCAL_TZ) if LOCAL_TZ else load.started
    return CheckResult(
        section="Test cases", name="ROWS_SINCE_LAST_LOAD",
        status=PASS if loaded >= least else FAIL,
        expected=f">= {least:,} rows since the last successful load",
        actual=f"{loaded:,} rows since {local:%Y-%m-%d %H:%M}",
        details=f"{_measured_by(col)}; load = {load.run_id} ({load.task} succeeded)",
        last_load_at=f"{local:%Y-%m-%d %H:%M:%S}", rows_target=loaded,
        duration_s=round(time.perf_counter() - started, 2))


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


# --------------------------------------------------------------------------- #
# What the validator threw out, told by the folders it leaves behind
# --------------------------------------------------------------------------- #

# run_id=scheduled__2026-09-07T07:15:00+00:00, and the manual spelling too.
_RUN_STAMP = re.compile(r"__(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?"
                        r"(?:Z|[+-]\d{2}:\d{2})?)$")


def run_age(folder: str) -> datetime | None:
    """When the run behind a folder name started, or None when it says nothing."""
    match = _RUN_STAMP.search(folder)
    if not match:
        return None
    try:
        stamp = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def folders(client: Any, bucket: str, prefix: str) -> set[str]:
    """The folder names directly under a prefix, without listing what is in them.

    A delimiter makes the API answer with prefixes instead of every object, so
    this stays cheap however many files a run wrote.
    """
    listing = client.list_blobs(bucket, prefix=prefix, delimiter="/")
    for _ in listing:                    # the prefixes only fill in once read
        pass
    return {p[len(prefix):].strip("/") for p in (listing.prefixes or set())}


def _gcs_paths(cfg: dict, system: str, table: str) -> dict[str, str]:
    gcs = (cfg.get("standalone") or {}).get("gcs") or {}
    # Bucket names cannot hold a capital, and the tree under them has none either.
    env = str((cfg.get("results") or {}).get("environment") or "").lower()
    fill = {"env": env, "system": system.lower(), "table": table.lower()}
    return {key: str(gcs.get(key) or "").format(**fill)
            for key in ("bucket_pattern", "archive_prefix", "invalid_prefix",
                        "valid_prefix")}


def check_validator(client: Any, cfg: dict, system: str,
                    table: str) -> list[CheckResult]:
    """Which runs the validator rejected, and which produced nothing at all.

    The archive holds one folder per run, so it is the list of runs that ever
    happened; only the newest few are judged. A folder of the same name under
    invalid/ means that run wrote rejected records; none under valid/ means it
    wrote nothing usable.
    """
    gcs = (cfg.get("standalone") or {}).get("gcs") or {}
    count = int(gcs.get("last_runs", 10))
    path = _gcs_paths(cfg, system, table)
    bucket = path["bucket_pattern"]

    started = time.perf_counter()
    seen = folders(client, bucket, path["archive_prefix"])
    # Newest first by the run's timestamp; a name without one cannot be placed.
    dated = sorted(((run_age(name), name) for name in seen if run_age(name)),
                   reverse=True)
    runs = [name for _, name in dated[:count]]
    took = round(time.perf_counter() - started, 2)

    where = f"gs://{bucket}/{path['archive_prefix']}"
    if not runs:
        told = (f"no run folder under {where}" if not seen else
                f"{len(seen)} folders under {where}, none named like a run")
        return [CheckResult(section="Test cases", name=name, status=SKIP,
                            details=told, duration_s=took)
                for name in ("REJECTED_RECORDS", "VALID_RECORDS")]

    invalid = folders(client, bucket, path["invalid_prefix"])
    valid = folders(client, bucket, path["valid_prefix"])
    rejected = [run for run in runs if run in invalid]
    empty = [run for run in runs if run not in valid]
    took = round(time.perf_counter() - started, 2)

    return [
        CheckResult(
            section="Test cases", name="REJECTED_RECORDS",
            status=FAIL if rejected else PASS,
            expected=f"no rejected records in the last {len(runs)} runs",
            actual=f"{len(rejected)} of the last {len(runs)} runs rejected records",
            details="; ".join(rejected), rows_target=len(rejected), duration_s=took),
        CheckResult(
            section="Test cases", name="VALID_RECORDS",
            status=WARN if empty else PASS,
            expected=f"accepted records in each of the last {len(runs)} runs",
            actual=(f"{len(runs) - len(empty)} of the last {len(runs)} runs "
                    f"wrote accepted records"),
            details="; ".join(empty), rows_target=len(runs) - len(empty),
            duration_s=took),
    ]


def validator_checks(cfg: dict, system: str, table: str) -> list[CheckResult]:
    """The validator checks, or one SKIP saying why they could not be made."""
    gcs = (cfg.get("standalone") or {}).get("gcs") or {}
    if not gcs.get("bucket_pattern"):
        return []
    if not str(table).lower().endswith(str(gcs.get("only_suffix") or "_raw")):
        return []                        # only the tables the validator writes

    if storage is None:
        why = "google-cloud-storage is not installed"
    else:
        why = ""
    if not why:
        try:
            client = storage.Client(project=gcs.get("project") or None)
            return check_validator(client, cfg, system, table)
        except Exception as exc:
            why = f"{type(exc).__name__}: {exc}"
    return [CheckResult(section="Test cases", name=name, status=SKIP, details=why)
            for name in ("REJECTED_RECORDS", "VALID_RECORDS")]


def test_table(bq: Any, cfg: dict, project: str, dataset: str, table: str,
               columns: list[tuple], layer: str, system: str,
               load: LoadRun | None = None) -> TableReport:
    """Every check for one table, in a report shaped like every other one.

    With a DAG to ask, rows are counted from its last successful load; without
    one, over the last hours in the config, as before.
    """
    own = cfg.get("standalone") or {}
    rep = TableReport(table=f"{dataset}.{table}", started_at=now_local())
    rows_check = "ROWS_SINCE_LAST_LOAD" if load else "ROWS_LAST_HOURS"

    col = time_column(columns, own.get("loaded_at_columns") or [])
    if not col.name:
        for name in (rows_check, "LAST_INSERT_AT"):
            rep.add("Test cases", name, SKIP,
                    details=f"nothing to measure {table} by: {col.reason}")
        rep.finished_at = now_local()
        return rep

    LOG.info("  %s", _measured_by(col))
    hours = int(own.get("hours", 24))
    zone = _zone(own.get("timezone") or "UTC")
    least = int(own.get("min_rows", 1))
    checks = [check_rows_since_load(bq, project, dataset, table, col, load, least, zone)
              if load else
              check_rows_last_hours(bq, project, dataset, table, col, hours, least, zone),
              check_last_insert_since_load(bq, project, dataset, table, col, load,
                                           float(own.get("load_slack_hours", 2)), zone)
              if load else
              check_last_insert(bq, project, dataset, table, col,
                                float(own.get("max_age_hours", hours)), zone)]
    checks += validator_checks(cfg, system, table)
    for check in checks:
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

    # One DAG loads the whole layer: its last real load is the same for every table.
    dag = dag_for(cfg, layer, system)
    # With Airflow switched off there is nothing to ask, so the hours window stays.
    load = last_load(cfg, dag) if dag and airflow_api(cfg)[0] is not None else None
    if load and load.started:
        local = load.started.astimezone(LOCAL_TZ) if LOCAL_TZ else load.started
        LOG.info("Last load: %s, run %s (%s succeeded) - rows are counted from here",
                 f"{local:%Y-%m-%d %H:%M:%S}", load.run_id, load.task)
    elif load:
        LOG.info("Last load: not found - %s", load.reason)

    reports = []
    for table in tables:
        LOG.info("=" * 78)
        LOG.info("Testing table: %s.%s", dataset, table)
        LOG.info("=" * 78)
        reports.append(test_table(bq, cfg, project, dataset, table,
                                  columns.get(table, []), layer, system, load))

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
