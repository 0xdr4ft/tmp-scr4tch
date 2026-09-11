"""The comparison framework: a source read alongside its target.

Every test case here is two queries - one per side - and a verdict on whether
the two answers agree. What the queries are comes from the catalogue; how much
they may differ comes from the load schedule in the registry.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .main import (BQ, CATALOGUE_KEYS, FAIL, PASS, REGISTRY_EXTRAS, REGISTRY_KEYS,
                   RUN_STAMP_FORMAT, SKIP, SOURCE_KEYS, TableReport, WARN, _oracle_ident,
                   _parse_ts, _run_stamp, airflow_api, ask_window, check_airflow,
                   croniter, dag_id_for, dag_prefix_of, dataset_of, layer_of,
                   now_local, oracle_close_all, save_results, table_config,
                   valid_date, worst, write_report)

LOG = logging.getLogger("loading_tests")


def catalogue_columns(cfg: dict) -> dict[str, str]:
    """The catalogue column names, as the config spells them."""
    cols = (cfg.get("oracle_meta") or {}).get("columns") or {}
    return {k: _oracle_ident(str(cols[k]), f"catalogue column {k}")
            for k in CATALOGUE_KEYS}


@dataclass
class TestCase:
    table: str
    name: str
    sql_oracle: str
    sql_gcp: str


def _lob(value: Any) -> Any:
    """Oracle returns long text as a LOB object; read it into a string."""
    return value.read() if hasattr(value, "read") else value


# Usually inside the table name: `:p_gcp_project.dataset.table`.
_PROJECT_PARAM = re.compile(r"[:@]p_gcp_project", re.IGNORECASE)


_PROJECT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")


def bind_project(sql: str, project: str) -> str:
    """Put project_id where the catalogue left the project placeholder.

    An identifier, not a value: it goes in bare, and whatever quoting the
    catalogue has around it stays - `:p_gcp_project.ds.t` becomes `project.ds.t`.
    """
    if not _PROJECT_PARAM.search(sql or ""):
        return sql
    if not _PROJECT_ID.match(str(project or "")):
        raise ValueError(f"the SQL needs the project, and project_id is "
                         f"missing or unusable: {project!r}")
    return _PROJECT_PARAM.sub(str(project), sql)


def catalogue_view(cfg: dict) -> str:
    view = (cfg.get("oracle_meta") or {}).get("tests_view")
    if not view:
        raise ValueError("Config needs oracle_meta.tests_view - the catalogue view")
    return _oracle_ident(str(view), "tests view name")


def read_catalogue(cfg: dict, conn: Any, tables: list[str]) -> list[TestCase]:
    """Test cases of the given tables, matched without regard to case.

    Only the catalogue's own name is asked for; the BigQuery one is already
    inside sql_gcp.
    """
    col = catalogue_columns(cfg)
    binds = {f"t{i}": t.strip().upper()
             for i, t in enumerate(dict.fromkeys(tables))}
    placeholders = ", ".join(f":{name}" for name in binds)
    sql = (f"SELECT {col['table_name']}, {col['test_case']}, "
           f"{col['sql_oracle']}, {col['sql_gcp']} "
           f"FROM {catalogue_view(cfg)} "
           f"WHERE UPPER({col['table_name']}) IN ({placeholders}) "
           f"ORDER BY 1, 2")
    LOG.debug("Oracle SQL: %s  %s", sql, binds)

    with conn.cursor() as cur:
        cur.execute(sql, binds)
        rows = cur.fetchall()

    # In here, so the dag_id and the comparison see a real name later on.
    project = str(cfg.get("project_id") or "")
    cases = []
    for table, name, sql_oracle, sql_gcp in rows:
        cases.append(TestCase(table=str(_lob(table)).strip(),
                              name=str(_lob(name) or "").strip() or "(unnamed)",
                              sql_oracle=str(_lob(sql_oracle) or "").strip(),
                              sql_gcp=bind_project(str(_lob(sql_gcp) or "").strip(),
                                                   project)))
    return cases


@dataclass
class Schedule:
    """What the registry says about how often a table is loaded."""
    cron: str = ""
    hours: float | None = None       # largest gap between two runs; None = no usable one
    state: str = "cron"              # cron | none | unreadable
    reason: str = ""                 # why there is no usable gap, for the report


# The ways the registry spells "no recurring schedule". `None` is Python's.
_NO_SCHEDULE = {"", "none", "null", "-", "manual", "@once", "@never", "@continuous"}


@dataclass
class TableInfo:
    """One table as the metadata describes it: how, how often, from where."""
    schedule: Schedule = field(default_factory=Schedule)
    extract: str = ""                # FULL / INCREMENTAL, shown but never acted on
    source: str = ""                 # the source system this table comes from
    source_type: str = ""            # as the source registry spells it
    framework: str = ""              # worked out from source_type, as config names it
    reason: str = ""                 # why the framework could not be worked out


def table_registry(cfg: dict) -> str:
    table = (cfg.get("oracle_meta") or {}).get("table_registry")
    if not table:
        raise ValueError("Config needs oracle_meta.table_registry - the registry "
                         "holding each table's load metadata")
    return _oracle_ident(str(table), "table registry name")


def source_registry(cfg: dict) -> str:
    table = (cfg.get("oracle_meta") or {}).get("source_registry")
    if not table:
        raise ValueError("Config needs oracle_meta.source_registry - the registry "
                         "naming each source and its type")
    return _oracle_ident(str(table), "source registry name")


def registry_columns(cfg: dict) -> dict[str, str]:
    """The table registry column names, as the config spells them.

    The extras are only there when the config names them; nothing breaks
    without one, the report simply says less.
    """
    cols = (cfg.get("oracle_meta") or {}).get("registry_columns") or {}
    wanted = REGISTRY_KEYS + tuple(k for k in REGISTRY_EXTRAS if cols.get(k))
    return {k: _oracle_ident(str(cols[k]), f"registry column {k}") for k in wanted}


def source_columns(cfg: dict) -> dict[str, str]:
    """The source registry column names, as the config spells them."""
    cols = (cfg.get("oracle_meta") or {}).get("source_columns") or {}
    return {k: _oracle_ident(str(cols[k]), f"source column {k}")
            for k in SOURCE_KEYS}


def framework_of(cfg: dict, source_type: str) -> tuple[str, str]:
    """(framework, why there is none) for a source type from the registry."""
    wanted = str(source_type or "").strip().lower()
    if not wanted:
        return "", "the source registry has no type for this source"
    for framework, types in (cfg.get("frameworks") or {}).items():
        if wanted in {str(t).strip().lower() for t in types or []}:
            return str(framework).strip().lower(), ""
    return "", (f"source type `{source_type}` is on none of the framework lists "
                f"in the config")


def cron_gap_hours(expr: str) -> float | None:
    """The largest gap between two runs of this cron, in hours, or None.

    The largest is what counts: `0 6 * * 1-5` waits 72h over the weekend, and a
    Monday test has to survive that. Measured from a fixed Monday, so it is stable.
    """
    if croniter is None or not expr:
        return None
    try:
        if not croniter.is_valid(expr):
            return None
        base = datetime(2024, 1, 1)                     # a Monday
        limit = base + timedelta(days=800)
        clock = croniter(expr, base)
        previous = clock.get_next(datetime)
        gap = 0.0
        for _ in range(400):                            # enough for the long gaps,
            nxt = clock.get_next(datetime)              # a cap for the dense crons
            if nxt > limit:
                break
            gap = max(gap, (nxt - previous).total_seconds() / 3600)
            previous = nxt
    except Exception:                                   # a shape croniter chokes on
        return None
    return round(gap, 2) or None


def as_schedule(raw: str) -> Schedule:
    """One registry value turned into hours, or into the reason there are none."""
    text = (raw or "").strip()
    if text.lower() in _NO_SCHEDULE:
        return Schedule(cron=text, state="none",
                        reason=f"the registry has no recurring schedule for this "
                               f"table ({text or 'the column is empty'}), so there "
                               f"is no interval to allow for")
    hours = cron_gap_hours(text)
    if hours is None:
        if croniter is None:
            return Schedule(cron=text, state="unreadable",
                            reason="croniter is not installed, so the schedule "
                                   f"`{text}` cannot be read - `pip install croniter`")
        return Schedule(cron=text, state="unreadable",
                        reason=f"the registry holds `{text}`, which is not a cron "
                               f"this can read - fix it in the metadata")
    return Schedule(cron=text, hours=hours)


def read_registry(cfg: dict, conn: Any, tables: list[str]) -> dict[str, TableInfo]:
    """What the metadata knows about each table, by upper-case table name.

    One statement: the table registry carries the schedule and the source name,
    the source registry turns that name into a type, and the type decides the
    framework. Outer-joined, so a source that is not registered is told apart
    from one whose type is simply blank.

    A table with no row of its own is absent from the result - a different thing
    from a row whose schedule is empty, and it reads differently in the report.
    """
    col = registry_columns(cfg)
    extract, source = col.get("extract_method"), col.get("source_name")
    binds = {f"t{i}": t.strip().upper()
             for i, t in enumerate(dict.fromkeys(tables))}
    placeholders = ", ".join(f":{name}" for name in binds)

    fields = [f"t.{col['table_name']}", f"t.{col['cron_table']}",
              f"t.{extract}" if extract else "NULL",
              f"t.{source}" if source else "NULL"]
    joined = ""
    if source and cfg.get("frameworks"):
        src = source_columns(cfg)
        fields.append(f"s.{src['source_type']}")
        joined = (f" LEFT JOIN {source_registry(cfg)} s "
                  f"ON UPPER(TRIM(s.{src['source_name']})) "
                  f"= UPPER(TRIM(t.{source}))")
    else:
        fields.append("NULL")

    sql = (f"SELECT {', '.join(fields)} FROM {table_registry(cfg)} t{joined} "
           f"WHERE UPPER(TRIM(t.{col['table_name']})) IN ({placeholders})")
    LOG.debug("Oracle SQL: %s  %s", sql, binds)

    with conn.cursor() as cur:
        cur.execute(sql, binds)
        rows = cur.fetchall()

    registry: dict[str, TableInfo] = {}
    for name, cron, method, source_name, source_type in rows:
        key = str(_lob(name) or "").strip().upper()
        found = TableInfo(schedule=as_schedule(str(_lob(cron) or "")),
                          extract=str(_lob(method) or "").strip(),
                          source=str(_lob(source_name) or "").strip(),
                          source_type=str(_lob(source_type) or "").strip())
        found.framework, found.reason = framework_of(cfg, found.source_type)
        seen = registry.get(key)
        if seen and seen.schedule.cron != found.schedule.cron:
            # Two rows, two schedules: the first one wins, but say so out loud.
            LOG.warning("  %s has more than one row in the registry (`%s` and "
                        "`%s`) - using the first", key, seen.schedule.cron,
                        found.schedule.cron)
            continue
        registry[key] = found
    return registry


def describe_registry(tables: list[str], registry: dict[str, TableInfo]) -> str:
    """One line on where the tested tables stand, so a gap is visible at once."""
    labels = {"cron": "from cron", "none": "without a schedule",
              "unreadable": "unreadable", "missing": "not in the registry"}
    counts: Counter[str] = Counter()
    frameworks: Counter[str] = Counter()
    for table in tables:
        found = registry.get(table.strip().upper())
        counts[found.schedule.state if found else "missing"] += 1
        frameworks[found.framework or "unknown" if found else "unknown"] += 1
    seen = ", ".join(f"{counts[state]} {label}"
                     for state, label in labels.items() if counts[state])
    return f"{seen} | frameworks: " + ", ".join(
        f"{n} {name}" for name, n in frameworks.most_common())


def tolerance_for(cfg: dict, case: TestCase,
                  registry: dict[str, TableInfo]) -> tuple[float, str, str]:
    """(hours, where the hours came from, why this case cannot be run).

    Only the test cases named in cron_tolerance_cases get a tolerance at all -
    everything else has to match exactly, as it always did. For those that do,
    the registry is the only source: no schedule, no test.
    """
    wanted = {str(n).strip().upper()
              for n in (cfg.get("cron_tolerance_cases") or [])}
    if case.name.strip().upper() not in wanted:
        return 0.0, "", ""

    found = registry.get(case.table.strip().upper())
    if found is None:
        return 0.0, "", (f"{case.table} has no row in the table registry - either "
                         f"it is not registered there, or the name is spelled "
                         f"differently; the tolerance cannot be worked out")
    if found.schedule.hours is None:
        return 0.0, "", found.schedule.reason

    buffer = float(cfg.get("tolerance_buffer_hours") or 0)
    return (found.schedule.hours + buffer,
            f"cron {found.schedule.cron} = {found.schedule.hours:g}h "
            f"+ {buffer:g}h buffer", "")


# ':p_date_from', ":p_date_from", @p_date_from or bare. Quotes are swallowed
# and put back; spaces are not, or `>= :p_date_from AND` comes out glued.
_DATE_PARAM = re.compile(r"""['"]?[:@]p_date_(from|to)['"]?""", re.IGNORECASE)


def strip_terminator(sql: str) -> str:
    """Drop the trailing ; or / the catalogue text usually carries.

    They belong to SQL*Plus, not to the statement - the driver answers them
    with ORA-00933.
    """
    sql = sql.strip()
    while sql.endswith((";", "/")):
        sql = sql[:-1].strip()
    return sql


def bind_dates(sql: str, date_from: str, date_to: str) -> tuple[str, int]:
    """Put the window into the SQL, returning it with the number of parameters."""
    count = 0

    def replace(match: re.Match) -> str:
        nonlocal count
        count += 1
        return f"'{date_from if match.group(1).lower() == 'from' else date_to}'"

    return _DATE_PARAM.sub(replace, sql), count


# The moment the last load took its cut, for a query that bounds itself by it.
_RUN_PARAM = re.compile(r"""['"]?[:@]p_last_airflow_run['"]?""", re.IGNORECASE)




def needs_last_run(sql: str) -> bool:
    return bool(_RUN_PARAM.search(sql or ""))


def bind_last_run(sql: str, stamp: str) -> str:
    """Put the last run's timestamp in, quoted, the way the window goes in."""
    return _RUN_PARAM.sub(f"'{stamp}'", sql)


@dataclass
class Diff:
    """The rows one side has and the other does not, as they were compared."""
    columns: list[str]
    only_oracle: list[tuple]
    only_gcp: list[tuple]
    # The same counts the `actual` line words, for the results table.
    identical: int = 0
    discrepancies: int = 0
    diff_columns: str = ""


@dataclass
class SideResult:
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    truncated: bool = False
    error: str = ""
    params: int = 0


# Either spelling: 2026-07-01T00:00:00 or 2026-07-01 00:00:00.000, zone or not.
_TS_TEXT = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})"
    r"(?:[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,9}))?)?"
    r"\s*(Z|[+-]\d{2}:?\d{2})?$")


def parse_timestamp(value: Any) -> datetime | None:
    """A timestamp given as text, or None when the text is not one."""
    match = _TS_TEXT.match(str(_lob(value)).strip())
    if not match:
        return None
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    stamp = datetime(int(year), int(month), int(day),
                     int(hour or 0), int(minute or 0), int(second or 0),
                     int((fraction or "0").ljust(6, "0")[:6]))
    if offset and offset != "Z":
        digits = offset[1:].replace(":", "")
        away = timedelta(hours=int(digits[:2]), minutes=int(digits[2:] or 0))
        stamp = stamp - away if offset[0] == "+" else stamp + away
    return stamp


def _format_stamp(stamp: datetime, decimals: int = 6) -> str:
    """One spelling for both sides. Midnight is a date, as a DATE column is.

    The fraction of a second is cut to `decimals` digits: Oracle TIMESTAMP(6)
    holds .187416 where BigQuery holds .187000, which is storage, not data.
    """
    if decimals < 6:
        step = 10 ** (6 - decimals)
        stamp = stamp.replace(microsecond=stamp.microsecond // step * step)
    if stamp.time() == datetime.min.time():
        return stamp.date().isoformat()
    return stamp.isoformat(sep=" ")          # fractions only when they are there


def normalise(value: Any, decimals: int, stamp_decimals: int = 6) -> Any:
    """One value in a form both databases can agree on.

    Numbers rounded, trailing spaces from CHAR columns dropped, midnight
    compared as a plain date. Timestamps written as text are treated as
    timestamps - the two sides spell them differently.
    """
    value = _lob(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return str(value)
        try:
            number = number.quantize(Decimal(1).scaleb(-decimals))
        except InvalidOperation:
            # More digits than the context allows; compare it as it came.
            pass
        return format(number.normalize(), "f")
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return _format_stamp(value, stamp_decimals)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()

    stamp = parse_timestamp(value)
    return _format_stamp(stamp, stamp_decimals) if stamp else str(value).rstrip()


def normalise_rows(rows: list[tuple], decimals: int,
                   stamp_decimals: int = 6) -> list[tuple]:
    return [tuple(normalise(v, decimals, stamp_decimals) for v in row)
            for row in rows]


def as_datetime(value: Any) -> datetime | None:
    """The value as a naive UTC timestamp, or None when it is not one."""
    value = _lob(value)
    if isinstance(value, datetime):
        return (value.astimezone(timezone.utc).replace(tzinfo=None)
                if value.tzinfo else value)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return parse_timestamp(value)


def drift_hours(left: Any, right: Any) -> float | None:
    """How far apart two timestamps are, or None when they are not timestamps."""
    first, second = as_datetime(left), as_datetime(right)
    if first is None or second is None:
        return None
    return abs((second - first).total_seconds()) / 3600


def run_oracle(conn: Any, sql: str, limit: int) -> SideResult:
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            fetched = cur.fetchmany(limit + 1)
        return SideResult(columns=columns, rows=[tuple(r) for r in fetched[:limit]],
                          truncated=len(fetched) > limit)
    except Exception as exc:
        return SideResult(error=f"{type(exc).__name__}: {exc}")


def run_gcp(bq: BQ, sql: str, limit: int) -> SideResult:
    try:
        columns, rows, truncated = bq.rows(sql, limit)
        return SideResult(columns=columns, rows=rows, truncated=truncated)
    except Exception as exc:
        return SideResult(error=f"{type(exc).__name__}: {exc}")


def align_columns(oracle: SideResult, gcp: SideResult) -> list[str] | None:
    """Column names shared by both sides, in the Oracle order, or None.

    None when the names cannot be trusted: they differ, or one is used twice.
    """
    left = [str(c).upper() for c in oracle.columns]
    right = [str(c).upper() for c in gcp.columns]
    if not left or len(set(left)) != len(left) or sorted(left) != sorted(right):
        return None
    return left


def reorder(rows: list[tuple], names: list[str], target: list[str]) -> list[tuple]:
    order = [names.index(n) for n in target]
    return [tuple(row[i] for i in order) for row in rows]


PAIRING_LIMIT = 200          # above this, pairing costs more than it explains


def _pair_rows(left: list[tuple], right: list[tuple]
               ) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """(pairs, left-overs, right-overs) - nearest counterparts by similarity.

    A guess, not a join: there is no key to join on. Two rows are called a pair
    only when most of their columns agree, so a row that is genuinely missing
    stays reported as missing instead of being read as a changed one.
    """
    if not left or not right or max(len(left), len(right)) > PAIRING_LIMIT:
        return [], left, right

    spare, pairs, lonely = list(right), [], []
    for row in left:
        near = min(spare, key=lambda r: _unlike(row, r), default=None)
        if near is not None and _unlike(row, near) * 2 <= len(row):
            spare.remove(near)
            pairs.append((row, near))
        else:
            lonely.append(row)
    return pairs, lonely, spare


def _unlike(left: tuple, right: tuple) -> int:
    return sum(1 for a, b in zip(left, right) if a != b)


def _describe_diff(pairs: list[tuple], lone_left: list, lone_right: list,
                   identical: int) -> str:
    """The counts for `actual`: what matched, and how the rest did not."""
    counts = [f"{identical:,} identical"]
    if pairs:
        counts.append(f"{len(pairs):,} discrepanc" + ("y" if len(pairs) == 1 else "ies"))
    if lone_left:
        counts.append(f"{len(lone_left):,} only in oracle")
    if lone_right:
        counts.append(f"{len(lone_right):,} only in gcp")
    return ", ".join(counts)


def _diff_columns(pairs: list[tuple], names: list[str]) -> str:
    """Which columns the paired rows disagree on, and how often."""
    tally: Counter[str] = Counter()
    for left, right in pairs:
        for i, (a, b) in enumerate(zip(left, right)):
            if a != b:
                tally[names[i] if i < len(names) else f"column {i + 1}"] += 1
    return ", ".join(f"{name} ({n} of {len(pairs)})" for name, n in tally.most_common())


def compare_sides(oracle: SideResult, gcp: SideResult, decimals: int, limit: int,
                  tolerance: float = 0, stamp_decimals: int = 6,
                  tolerance_note: str = "") -> tuple[str, str, str, Diff | None]:
    """(status, actual, details, differing rows) for one test case.

    The rows come back only when there are any to show. `tolerance` is how
    many hours two timestamps may differ and still count as equal, and
    `tolerance_note` says where that number came from.
    """
    broken = [f"{side} query failed: {res.error}"
              for side, res in (("Oracle", oracle), ("GCP", gcp)) if res.error]
    if broken:
        return FAIL, "not compared", "; ".join(broken), None

    if oracle.truncated or gcp.truncated:
        return (WARN, f"oracle {len(oracle.rows):,}+ rows | gcp {len(gcp.rows):,}+ rows",
                f"result larger than max_compare_rows ({limit:,}) - narrow the window "
                f"or raise the limit; nothing was compared", None)

    ora_rows = normalise_rows(oracle.rows, decimals, stamp_decimals)
    gcp_rows = normalise_rows(gcp.rows, decimals, stamp_decimals)

    if len(oracle.columns) != len(gcp.columns):
        return (FAIL, f"oracle {len(oracle.columns)} columns | "
                      f"gcp {len(gcp.columns)} columns",
                f"the two queries return different shapes: "
                f"oracle ({', '.join(oracle.columns)}) vs gcp ({', '.join(gcp.columns)})",
                None)

    # Both sides use the same aliases, so names decide which column is which.
    names = align_columns(oracle, gcp)
    note = ""
    if names is None:
        # One column a side has nothing to line up; two or more, and it does.
        if len(oracle.columns) > 1:
            note = (f"column names do not match, compared by position: "
                    f"oracle ({', '.join(oracle.columns)}) vs "
                    f"gcp ({', '.join(gcp.columns)})")
    else:
        gcp_rows = reorder(gcp_rows, [str(c).upper() for c in gcp.columns], names)

    def with_note(status: str, actual: str, details: str,
                  diff: Diff | None = None) -> tuple[str, str, str, Diff | None]:
        return status, actual, "; ".join(d for d in (details, note) if d), diff

    # The common case: one number against one number.
    if len(ora_rows) == 1 and len(gcp_rows) == 1 and len(ora_rows[0]) == 1:
        left, right = ora_rows[0][0], gcp_rows[0][0]
        actual = f"oracle {left} | gcp {right}"
        if left == right:
            return with_note(PASS, actual, "")

        # The two sides load at different moments, hence the tolerance.
        if tolerance:
            allowed = (f"{tolerance:g}h tolerance"
                       + (f" ({tolerance_note})" if tolerance_note else ""))
            drift = drift_hours(oracle.rows[0][0], gcp.rows[0][0])
            if drift is None:
                if left is None or right is None:
                    return with_note(FAIL, actual,
                                     f"one side has no value at all, so the "
                                     f"{allowed} has nothing to measure")
                return with_note(FAIL, actual,
                                 f"values differ and are not timestamps, so the "
                                 f"{allowed} does not apply")
            if drift <= tolerance:
                return with_note(PASS, actual,
                                 f"{drift:.2f}h apart, within the {allowed}")
            return with_note(FAIL, actual,
                             f"{drift:.2f}h apart, more than the {allowed}")
        try:
            apart = Decimal(str(right)) - Decimal(str(left))
            detail = f"difference (gcp - oracle) = {apart:+}"
        except (InvalidOperation, TypeError):
            detail = "values differ"
        return with_note(FAIL, actual, detail)

    # Whole rows, any width. Counter also catches differing row counts.
    rows_seen = f"oracle {len(ora_rows):,} rows | gcp {len(gcp_rows):,} rows"
    only_oracle = Counter(ora_rows) - Counter(gcp_rows)
    only_gcp = Counter(gcp_rows) - Counter(ora_rows)
    left_n, right_n = sum(only_oracle.values()), sum(only_gcp.values())
    if not left_n and not right_n:
        return with_note(PASS, rows_seen, "",
                         Diff(columns=names or [str(c) for c in oracle.columns],
                              only_oracle=[], only_gcp=[], identical=len(ora_rows)))

    # Counts and the columns behind them; the rows go to the CSV files under -v.
    labels = names or [str(c) for c in oracle.columns]
    left_rows, right_rows = list(only_oracle.elements()), list(only_gcp.elements())
    pairs, lone_oracle, lone_gcp = _pair_rows(left_rows, right_rows)
    counted = _describe_diff(pairs, lone_oracle, lone_gcp, len(ora_rows) - left_n)
    return with_note(FAIL, f"{rows_seen}, {counted}", "",
                     Diff(columns=labels, only_oracle=left_rows, only_gcp=right_rows,
                          identical=len(ora_rows) - left_n, discrepancies=len(pairs),
                          diff_columns=_diff_columns(pairs, labels) if pairs else ""))


def _file_name(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_") or "case"


def _sort_key(row: tuple) -> tuple:
    """Order rows the same way on both sides; None sorts as the empty string."""
    return tuple("" if v is None else str(v) for v in row)


def show_diff(case: TestCase, diff: Diff, out_dir: str, stamp: str,
              sample: int = 10) -> None:
    """Log the first differing rows and write one file per side.

    Two files, sorted the same way, so comparing them in an editor shows the
    differences and nothing else. The values are the ones actually compared.
    """
    header = [c.lower() for c in diff.columns]
    sides = {"oracle": sorted(diff.only_oracle, key=_sort_key),
             "gcp": sorted(diff.only_gcp, key=_sort_key)}
    if not any(sides.values()):
        return

    shown = [["side", *header]]
    shown += [[side, *row] for side, rows in sides.items() for row in rows[:sample]]
    widths = [max(len(str(r[i])) for r in shown) for i in range(len(shown[0]))]
    LOG.debug("  %s / %s - differing rows: %d oracle, %d gcp",
              case.table, case.name, len(sides["oracle"]), len(sides["gcp"]))

    # Which columns the near-matching rows disagree on, so the cause is visible.
    pairs, _, _ = _pair_rows(sides["oracle"], sides["gcp"])
    if pairs:
        LOG.debug("    discrepancies in: %s (paired by similarity, not by key)",
                  _diff_columns(pairs, diff.columns))
    for row in shown:
        LOG.debug("    " + "  ".join(str(v).ljust(w) for v, w in zip(row, widths)))

    base = f"bronze_test_{stamp}_{_file_name(case.table)}_{_file_name(case.name)}"
    for side, rows in sides.items():
        path = os.path.join(out_dir, f"{base}_{side}.csv")
        try:
            with open(path, "w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.writer(fh, delimiter=";")
                writer.writerow(header)
                writer.writerows(rows)
            LOG.debug("  %-6s -> %s", side, path)
        except Exception as exc:
            LOG.warning("  could not write the %s rows: %s: %s",
                        side, type(exc).__name__, exc)


def run_test_case(bq: BQ, source: Any, cfg: dict, case: TestCase,
                  registry: dict[str, TableInfo], last_run: LastRun,
                  date_from: str, date_to: str, rep: TableReport,
                  out_dir: str = ".", stamp: str = "") -> None:
    decimals = int(cfg["defaults"]["float_decimals"])
    limit = int(cfg["defaults"]["max_compare_rows"])

    if not case.sql_oracle or not case.sql_gcp:
        missing = "sql_oracle" if not case.sql_oracle else "sql_gcp"
        rep.add("Test cases", case.name, SKIP,
                details=f"the catalogue has no {missing} for this test case")
        return

    # A table the metadata calls someone else's is not ours to compare.
    found = registry.get(case.table.strip().upper())
    if cfg.get("frameworks"):
        can = {str(f).strip().lower()
               for f in (cfg.get("implemented_frameworks") or [])}
        if not registry:
            why = ("the table registry could not be read, or it returned nothing "
                   "at all - see the `Cannot read the table registry` line")
        elif found is None:
            why = f"{case.table} has no row in the table registry"
        elif found.reason:
            why = found.reason
        elif found.framework not in can:
            why = (f"{case.table} is loaded by {found.framework}, and only "
                   f"{', '.join(sorted(can)) or 'nothing'} is implemented")
        else:
            why = ""
        if why:
            rep.add("Test cases", case.name, SKIP, details=why)
            return

    # First: a case that cannot be judged is not worth paying BigQuery for.
    tolerance, tolerance_note, no_schedule = tolerance_for(cfg, case, registry)
    if no_schedule:
        rep.add("Test cases", case.name, SKIP, details=no_schedule)
        return

    sql_oracle, params_oracle = bind_dates(strip_terminator(case.sql_oracle),
                                           date_from, date_to)
    sql_gcp, params_gcp = bind_dates(strip_terminator(case.sql_gcp),
                                     date_from, date_to)

    # A case bounded by the last load: without that moment there is nothing to
    # compare against, so it is skipped rather than run against the wrong data.
    bounded = needs_last_run(sql_oracle) or needs_last_run(sql_gcp)
    if bounded:
        if not last_run.stamp:
            rep.add("Test cases", case.name, SKIP, details=last_run.reason)
            return
        sql_oracle = bind_last_run(sql_oracle, last_run.stamp)
        sql_gcp = bind_last_run(sql_gcp, last_run.stamp)

    LOG.info("Running test case %s / %s", case.table, case.name)
    started = time.perf_counter()
    oracle = run_oracle(source, sql_oracle, limit)
    gcp = run_gcp(bq, sql_gcp, limit)

    status, actual, details, diff = compare_sides(
        oracle, gcp, decimals, limit, tolerance,
        int(cfg["defaults"]["timestamp_decimals"]), tolerance_note)
    took = time.perf_counter() - started
    # With -v, the rows themselves, for reading side by side.
    if diff and LOG.isEnabledFor(logging.DEBUG):
        show_diff(case, diff, out_dir, stamp)
    if bounded:
        details = (details + "; " if details else "") + \
            f"p_last_airflow_run = {last_run.stamp} ({last_run.zone})"
    elif not params_oracle or not params_gcp:
        # Such a case ignores the window, so the dates change nothing in it.
        side = "sql_oracle" if not params_oracle else "sql_gcp"
        details = (details + "; " if details else "") + \
            f"no date parameter in {side} - the window was not applied there"
    rep.add("Test cases", case.name, status,
            expected="oracle = gcp", actual=actual, details=details,
            load_type=found.extract if found else "",
            cron=found.schedule.cron if found else "",
            tolerance_h=tolerance,
            last_load_at=last_run.stamp if bounded else "",
            rows_source=len(oracle.rows) if not oracle.error else None,
            rows_target=len(gcp.rows) if not gcp.error else None,
            rows_identical=diff.identical if diff else None,
            rows_discrepancies=diff.discrepancies if diff else None,
            diff_columns=diff.diff_columns if diff else "",
            duration_s=round(took, 2))


@dataclass
class LastRun:
    """When the last load took its cut, spelled the way the data spells time.

    One spelling for both sides, because both sides store the same one - which
    is the premise the whole comparison rests on, and what it checks every run.
    """
    stamp: str = ""
    zone: str = "utc"                # utc | local, whichever the data keeps
    reason: str = ""                 # why there is no moment at all


def last_airflow_run(cfg: dict, tc: dict) -> LastRun:
    """When the last load took its cut, or the reason there is no such moment.

    Successful runs only: a failed or still running one has moved no data, so
    bounding the source by it would hide rows that never reached bronze.
    """
    af = cfg.get("airflow") or {}
    zone = str(af.get("last_run_tz", "utc"))
    get, why = airflow_api(cfg)
    dag_id = dag_id_for(tc, af)
    if get is None:
        return LastRun(reason=f":p_last_airflow_run needs Airflow, and {why}")

    field_name = str(af.get("last_run_field", "start_date"))
    try:
        runs = get(f"/dags/{dag_id}/dagRuns", order_by="-logical_date",
                   limit=int(af.get("history_runs", 10)))
    except Exception as exc:
        return LastRun(reason=f"the runs of {dag_id} could not be read: {exc}")

    for run in runs.get("dag_runs", []):
        if run.get("state") != "success":
            continue
        stamp = _parse_ts(run.get(field_name))
        if stamp:
            return LastRun(stamp=_run_stamp(stamp, zone).strftime(RUN_STAMP_FORMAT),
                           zone=zone)
    return LastRun(reason=f"{dag_id} has no successful run with a {field_name} "
                          f"among the last {af.get('history_runs', 10)}")




def run_comparison(bq: BQ, meta: Any, source: Any, cfg: dict, typed: list[str],
                   args: Any, out_dir: str, stamp: str, log_path: str) -> int:
    """Every named table, compared side by side, then its Airflow DAG."""
    # --- Their test cases --------------------------------------------------- #
    try:
        cases = read_catalogue(cfg, meta, typed)
    except Exception as exc:
        LOG.error("Cannot read the test cases: %s: %s", type(exc).__name__, exc)
        oracle_close_all()
        return 2

    # The catalogue decides how the table is spelled from here on.
    tables = list(dict.fromkeys(c.table for c in cases))
    found = {t.upper() for t in tables}
    missing = [t for t in typed if t.strip().upper() not in found]
    if missing:
        LOG.error("The catalogue has no test case for: %s", ", ".join(missing))
    if not cases:
        oracle_close_all()
        return 2
    LOG.info("Test cases: %s", ", ".join(sorted({c.name for c in cases})))

    # --- How often those tables are loaded ---------------------------------- #
    # The registry is the only source; without one, those cases are skipped.
    registry: dict[str, TableInfo] = {}
    if cfg.get("cron_tolerance_cases") or cfg.get("frameworks"):
        if croniter is None:
            LOG.warning("croniter is not installed - no schedule can be read, so "
                        "the test cases needing one will be skipped")
        try:
            registry = read_registry(cfg, meta, tables)
        except Exception as exc:
            LOG.error("Cannot read the table registry: %s: %s",
                      type(exc).__name__, exc)
            LOG.error("Test cases that need it will be skipped.")
        LOG.info("Registry: %s", describe_registry(tables, registry))

    # --- Which window ------------------------------------------------------- #
    if args.date_from or args.date_to:
        try:
            date_from = valid_date(args.date_from or cfg["defaults"]["date_from"])
            date_to = valid_date(args.date_to
                                 or (now_local().date() - timedelta(days=1)).isoformat())
        except ValueError as exc:
            LOG.error("Bad date: %s", exc)
            oracle_close_all()
            return 2
    elif sys.stdin.isatty():
        date_from, date_to = ask_window(cfg)
    else:
        date_from = valid_date(cfg["defaults"]["date_from"])
        date_to = (now_local().date() - timedelta(days=1)).isoformat()
    window = f"{date_from} .. {date_to}"
    LOG.info("Window: %s", window)

    # --- Run ---------------------------------------------------------------- #
    reports = []
    for table in tables:
        found = registry.get(table.strip().upper())
        known = ", ".join(part for part in (
            found.framework if found else "",
            found.extract if found else "",
            f"cron {found.schedule.cron}" if found and found.schedule.cron else "")
            if part)
        rep = TableReport(table=table, started_at=now_local(), window=window,
                          load=found.extract if found else "")
        LOG.info("=" * 78)
        LOG.info("Testing table: %s%s", table, f" ({known})" if known else "")
        LOG.info("=" * 78)

        table_cases = [c for c in cases if c.table.upper() == table.upper()]

        # The source schema for the dag_id only appears in the sql_gcp dataset.
        tc = table_config(cfg, table)
        af = cfg.get("airflow") or {}
        dataset = next((d for d in (dataset_of(c.sql_gcp) for c in table_cases) if d),
                       None)
        if not tc.get("dag_id") and not tc.get("dag_prefix"):
            if dataset:
                tc["dag_prefix"] = dag_prefix_of(dataset, af.get("dataset_layer_prefixes"))
                LOG.info("  dag_id from dataset %s: %s", dataset, dag_id_for(tc, af))
            else:
                LOG.warning("  no dataset found in sql_gcp - dag_id without the schema")

        # Asked for only when a test case names it, so nobody pays for it twice.
        last_run = LastRun()
        if any(needs_last_run(c.sql_oracle) or needs_last_run(c.sql_gcp)
               for c in table_cases):
            last_run = last_airflow_run(cfg, tc)

        for case in table_cases:
            try:
                run_test_case(bq, source, cfg, case, registry, last_run,
                              date_from, date_to, rep, out_dir, stamp)
            except Exception as exc:
                LOG.exception("Unexpected error in test case %s (%s)", case.name, table)
                rep.add("Runtime", case.name, FAIL,
                        details=f"{type(exc).__name__}: {exc}")

        try:
            check_airflow(cfg, tc, rep)
        except Exception as exc:
            LOG.exception("Unexpected error in the Airflow check (%s)", table)
            rep.add("Runtime", "Airflow", FAIL, details=f"{type(exc).__name__}: {exc}")

        rep.finished_at = now_local()
        LOG.info("FINAL STATUS for %s: %s", table, rep.final_status)
        reports.append(rep)

        try:
            save_results(cfg, meta, rep, found,
                         layer=layer_of(dataset, af.get("dataset_layer_prefixes")),
                         date_from=date_from, date_to=date_to)
        except Exception as exc:
            LOG.warning("  results not saved for %s: %s: %s",
                        table, type(exc).__name__, exc)

    oracle_close_all()
    txt_path = write_report(reports, cfg, window, out_dir, stamp)

    overall = worst([r.final_status for r in reports])
    LOG.info("-" * 78)
    LOG.info("OVERALL STATUS: %s", overall)
    LOG.info("Bytes billed: %.2f MB", bq.bytes_billed / 1024 / 1024)
    LOG.info("Report  : %s", txt_path)
    LOG.info("Log file: %s", log_path)
    return 1 if overall == FAIL else 0
