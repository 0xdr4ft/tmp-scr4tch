#!/usr/bin/env python3
"""
Loading tests driven by the test catalogue kept in the metadata database.

Every test case comes from a view there: for each table it lists the cases, and
for each case the SQL to run on the source Oracle and the equivalent SQL to run
on BigQuery. This runs both, compares the two result sets and reports one check
per test case, then looks at the table's Airflow DAG. Adding a test case means
adding a row to that view.

What the run looks like:
  1) log in to BigQuery first (it is the one that may need a browser), then to
     the metadata database and the source Oracle
  2) ask which tables to test - one name, or several separated by commas,
     spelled as the catalogue spells them (the BigQuery side is already inside
     sql_gcp, so it never has to be named here)
  3) read their test cases from the catalogue
  4) ask for the date window (defaults: config date_from .. yesterday)
  5) run every test case on both sides and compare the results
  6) check the Airflow DAG of each table

The date parameters (:p_date_from / :p_date_to) are substituted into the SQL as
text, because in the catalogue they usually sit inside quotes - TO_DATE(':p_date
_from', 'RRRR-MM-DD') - where a real bind variable would never be seen. Only
dates that parse as YYYY-MM-DD are ever put there.

The GCP statements name the project the same way, as :p_gcp_project, and that
one is filled from project_id in the config.

Timestamps in the report and the log are Polish local time (Europe/Warsaw).

Usage:
    uv pip install -e .         # name the command in pyproject.toml
    loading-tests               # config.yaml from the current directory,
                                # or wherever LOADING_TESTS_CONFIG points
    loading-tests --table CUSTOMERS,ORDERS
    loading-tests --table CUSTOMERS --date-from 2026-06-01 --date-to 2026-08-24

Without installing:
    uv run python -m loading_tests --config config.yaml
"""

from __future__ import annotations

import argparse
import csv
import getpass
import itertools
import logging
import os
import re
import shutil
import subprocess
import sys
import textwrap
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import yaml

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover
    bigquery = None

LOG = logging.getLogger("loading_tests")

# --------------------------------------------------------------------------- #
# Local time
# --------------------------------------------------------------------------- #

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ: Any = ZoneInfo("Europe/Warsaw")
except Exception:                    # no tzdata (Windows without the package)
    LOCAL_TZ = None


def now_local() -> datetime:
    """Current time in Europe/Warsaw, or the machine's own zone as a fallback."""
    return datetime.now(LOCAL_TZ) if LOCAL_TZ else datetime.now().astimezone()


# --------------------------------------------------------------------------- #
# Status model
# --------------------------------------------------------------------------- #

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIPPED"
_SEVERITY = {SKIP: 0, PASS: 1, WARN: 2, FAIL: 3}


_COLOURS = {PASS: "\033[32m", WARN: "\033[33m", FAIL: "\033[31m", SKIP: "\033[90m"}
_RESET = "\033[0m"
_STATUS_WORD = re.compile(rf"\b({'|'.join(_COLOURS)})\b")


class ColourFormatter(logging.Formatter):
    """Colours the status words on the console. The log file stays plain."""

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        return _STATUS_WORD.sub(
            lambda m: f"{_COLOURS[m.group(1)]}{m.group(1)}{_RESET}", text)


def colour_works(stream: Any) -> bool:
    """Whether this console shows colours - and wants to.

    Redirected output gets none, NO_COLOR is honoured, and Windows needs the
    sequences switched on first or they print as gibberish.
    """
    if os.environ.get("NO_COLOR") or not getattr(stream, "isatty", None):
        return False
    if not stream.isatty():
        return False
    if os.name == "nt":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.GetStdHandle(-11)          # stdout
            mode = ctypes.c_ulong()
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                return False
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)   # VT processing
        except Exception:
            return False
    return True


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
    started_at: datetime
    window: str = ""
    results: list[CheckResult] = field(default_factory=list)
    finished_at: datetime | None = None

    def add(self, *a, **kw) -> None:
        r = CheckResult(*a, **kw)
        self.results.append(r)
        # A failure says "not compared" and puts the reason in the details.
        told = " ".join(part for part in (f"actual={r.actual}" if r.actual else "",
                                          r.details) if part)
        LOG.info("[%s] %-38s %-7s %s", r.section, r.name, r.status, told)

    @property
    def final_status(self) -> str:
        return worst([r.status for r in self.results])

    def section_status(self, section: str) -> str:
        return worst([r.status for r in self.results if r.section == section])

    @property
    def cases(self) -> int:
        return sum(1 for r in self.results if r.section == "Test cases")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

# Only the keys are fixed here; every value lives in the config file.
REQUIRED_DEFAULTS = ("date_from", "max_compare_rows", "float_decimals",
                     "timestamp_decimals")
CATALOGUE_KEYS = ("table_name", "test_case", "sql_oracle", "sql_gcp")


def load_config(path: str) -> dict[str, Any]:
    """Read the config and refuse to start with pieces missing from it."""
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    cfg["defaults"] = cfg.get("defaults") or {}

    columns = (cfg.get("oracle_meta") or {}).get("columns") or {}
    missing = [f"defaults.{k}" for k in REQUIRED_DEFAULTS
               if cfg["defaults"].get(k) is None]
    missing += [f"oracle_meta.columns.{k}" for k in CATALOGUE_KEYS
                if not columns.get(k)]
    if missing:
        raise ValueError(f"{path} is missing: {', '.join(missing)}")
    return cfg


def find_config(given: str | None) -> str:
    """The config file: what was asked for, or the first place it turns up.

    The last place looked at is the project directory the package was installed
    from, so the command works from anywhere without carrying a path around.
    """
    if given:
        return given
    if os.environ.get("LOADING_TESTS_CONFIG"):
        return os.environ["LOADING_TESTS_CONFIG"]

    here = os.path.join(os.getcwd(), "config.yaml")
    project = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for path in (here, os.path.join(project, "config.yaml")):
        if os.path.isfile(path):
            return path
    return here                          # so the error names the obvious place


def table_config(cfg: dict, table: str) -> dict:
    """Per-table settings (Airflow naming), matched without regard to case."""
    for t in cfg.get("tables", []):
        if str(t.get("name", "")).upper() == table.upper():
            merged = dict(cfg["defaults"])
            merged.update(t)
            return merged
    merged = dict(cfg["defaults"])
    merged["name"] = table
    return merged


def tolerance_for(cfg: dict, test_case: str) -> float:
    """Hours this test case may differ by, from tolerance_hours in the config."""
    tolerances = cfg.get("tolerance_hours") or {}
    for name, hours in tolerances.items():
        if str(name).strip().upper() == test_case.strip().upper():
            return float(hours)
    return 0.0


def catalogue_columns(cfg: dict) -> dict[str, str]:
    """The catalogue column names, as the config spells them."""
    cols = (cfg.get("oracle_meta") or {}).get("columns") or {}
    return {k: _oracle_ident(str(cols[k]), f"catalogue column {k}")
            for k in CATALOGUE_KEYS}


# --------------------------------------------------------------------------- #
# Oracle connections
# --------------------------------------------------------------------------- #

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#.]*(@[A-Za-z_][A-Za-z0-9_$#.]*)?$")
_CONNECTIONS: dict[str, Any] = {}
_CONNECT_ERRORS: dict[str, Exception] = {}


def _oracle_ident(value: str, what: str) -> str:
    """Table, view and column names cannot be bound as parameters - validate."""
    if not _IDENT.match(value or ""):
        raise ValueError(f"Invalid {what}: {value!r}")
    return value


def _oracle_dsn(block: dict) -> str:
    if block.get("dsn"):
        return str(block["dsn"])
    host, service = block.get("host"), block.get("service_name")
    if not host or not service:
        raise ValueError("Needs dsn, or host + service_name")
    return f"{host}:{block.get('port', 1521)}/{service}"


def oracle_connect(cfg: dict, key: str) -> Any:
    """Connect to the database configured under `key`, asking once per run.

    Credentials come from the environment first (<KEY>_USER / <KEY>_PASSWORD,
    e.g. ORACLE_META_USER), then from the config, then from the terminal.
    """
    if key in _CONNECTIONS:
        return _CONNECTIONS[key]
    if key in _CONNECT_ERRORS:
        raise _CONNECT_ERRORS[key]

    import oracledb                  # raises ImportError -> reported by the caller

    block = cfg.get(key) or {}
    if not block:
        raise ValueError(f"Config has no `{key}:` block")
    dsn = _oracle_dsn(block)
    label = block.get("label") or key
    prefix = key.upper()

    user = (os.environ.get(f"{prefix}_USER") or block.get("user")
            or input(f"User for {label} ({dsn}): ").strip())
    password = (os.environ.get(f"{prefix}_PASSWORD")
                or getpass.getpass(f"Password for {user}@{label}: "))

    LOG.info("Connecting to %s (%s) as %s", label, dsn, user)
    try:
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as exc:
        _CONNECT_ERRORS[key] = exc
        raise
    _CONNECTIONS[key] = conn
    LOG.info("Connected to %s (Oracle %s)", label, conn.version)
    return conn


def oracle_close_all() -> None:
    for key, conn in list(_CONNECTIONS.items()):
        try:
            conn.close()
        except Exception:
            pass
        _CONNECTIONS.pop(key, None)


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #

class BQ:
    def __init__(self, project: str, location: str | None = None):
        if bigquery is None:
            raise RuntimeError("google-cloud-bigquery is not installed")
        self.client = bigquery.Client(project=project)
        self.location = location
        self.bytes_billed = 0

    def rows(self, sql: str, limit: int) -> tuple[list[str], list[tuple], bool]:
        """(column names, rows, truncated) - one row more than the limit is read
        so that an oversized result can be recognised without reading it all."""
        LOG.debug("BigQuery SQL:\n%s", sql)
        job = self.client.query(sql, location=self.location)
        result = job.result()
        columns = [f.name for f in result.schema]
        fetched = [tuple(r.values()) for r in itertools.islice(result, limit + 1)]
        self.bytes_billed += job.total_bytes_billed or 0
        return columns, fetched[:limit], len(fetched) > limit

    def check_connection(self) -> None:
        list(self.client.query("SELECT 1", location=self.location).result())


GCLOUD_LOGIN = "gcloud auth application-default login"


def _needs_login(exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return any(word in text for word in
               ("defaultcredentials", "credential", "reauth", "unauthenticated",
                "could not automatically determine", "invalid_grant"))


def connect_bigquery(cfg: dict) -> BQ | None:
    """Connect to BigQuery, offering the gcloud login when there is none yet.

    The first run on a new machine fails on missing credentials, and the fix
    is one command - so it is offered rather than left to look up.
    """
    for attempt in (1, 2):
        try:
            bq = BQ(cfg["project_id"], cfg.get("location"))
            bq.check_connection()
            LOG.info("Connected to BigQuery (project %s, location %s)",
                     cfg["project_id"], cfg.get("location") or "-")
            return bq
        except Exception as exc:
            LOG.error("Cannot reach BigQuery: %s: %s", type(exc).__name__, exc)
            if not _needs_login(exc):
                return None                      # not an authentication problem
            if attempt == 2 or not sys.stdin.isatty():
                LOG.error("Log in with: %s", GCLOUD_LOGIN)
                return None
            try:
                answer = input(f"\nRun `{GCLOUD_LOGIN}` now? [y/N]: ").strip().lower()
            except EOFError:
                answer = ""
            if answer not in ("y", "yes", "t", "tak"):
                LOG.error("Log in with: %s", GCLOUD_LOGIN)
                return None
            # On Windows it is gcloud.cmd; the bare name gives WinError 2.
            command, *arguments = GCLOUD_LOGIN.split()
            executable = shutil.which(command)
            if not executable:
                LOG.error("`%s` is not on the PATH - log in with: %s",
                          command, GCLOUD_LOGIN)
                return None
            try:
                code = subprocess.call([executable, *arguments])
            except Exception as exc:
                LOG.error("Could not start `%s`: %s: %s",
                          GCLOUD_LOGIN, type(exc).__name__, exc)
                return None
            if code != 0:
                LOG.error("`%s` did not finish - log in and start again", GCLOUD_LOGIN)
                return None
    return None


# --------------------------------------------------------------------------- #
# 1) The test catalogue
# --------------------------------------------------------------------------- #

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


# --------------------------------------------------------------------------- #
# 2) Date parameters
# --------------------------------------------------------------------------- #

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


def valid_date(value: str) -> str:
    """YYYY-MM-DD or nothing. This is what keeps the substitution below safe."""
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date().isoformat()


def bind_dates(sql: str, date_from: str, date_to: str) -> tuple[str, int]:
    """Put the window into the SQL, returning it with the number of parameters."""
    count = 0

    def replace(match: re.Match) -> str:
        nonlocal count
        count += 1
        return f"'{date_from if match.group(1).lower() == 'from' else date_to}'"

    return _DATE_PARAM.sub(replace, sql), count


# --------------------------------------------------------------------------- #
# 3) Running one test case on both sides
# --------------------------------------------------------------------------- #

@dataclass
class Diff:
    """The rows one side has and the other does not, as they were compared."""
    columns: list[str]
    only_oracle: list[tuple]
    only_gcp: list[tuple]


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


def compare_sides(oracle: SideResult, gcp: SideResult, decimals: int, limit: int,
                  tolerance: float = 0,
                  stamp_decimals: int = 6) -> tuple[str, str, str, Diff | None]:
    """(status, actual, details, differing rows) for one test case.

    The rows come back only when there are any to show. `tolerance` is how
    many hours two timestamps may differ and still count as equal.
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
        note = (f"column names do not match, compared by position: "
                f"oracle ({', '.join(oracle.columns)}) vs gcp ({', '.join(gcp.columns)})")
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
            drift = drift_hours(oracle.rows[0][0], gcp.rows[0][0])
            if drift is None:
                return with_note(FAIL, actual,
                                 f"values differ and are not timestamps, so the "
                                 f"{tolerance}h tolerance does not apply")
            if drift <= tolerance:
                return with_note(PASS, actual,
                                 f"{drift:.2f}h apart, within the {tolerance}h tolerance")
            return with_note(FAIL, actual,
                             f"{drift:.2f}h apart, more than the {tolerance}h tolerance")
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
        return with_note(PASS, rows_seen, "")

    # Counts only - the rows themselves go to the CSV files under -v.
    counted = ", ".join(part for part in (
        f"{left_n:,} only in oracle" if left_n else "",
        f"{right_n:,} only in gcp" if right_n else "") if part)
    labels = names or [str(c) for c in oracle.columns]
    return with_note(FAIL, f"{rows_seen}, {counted}", "",
                     Diff(columns=labels,
                          only_oracle=list(only_oracle.elements()),
                          only_gcp=list(only_gcp.elements())))


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
                  date_from: str, date_to: str, rep: TableReport,
                  out_dir: str = ".", stamp: str = "") -> None:
    decimals = int(cfg["defaults"]["float_decimals"])
    limit = int(cfg["defaults"]["max_compare_rows"])

    if not case.sql_oracle or not case.sql_gcp:
        missing = "sql_oracle" if not case.sql_oracle else "sql_gcp"
        rep.add("Test cases", case.name, SKIP,
                details=f"the catalogue has no {missing} for this test case")
        return

    sql_oracle, params_oracle = bind_dates(strip_terminator(case.sql_oracle),
                                           date_from, date_to)
    sql_gcp, params_gcp = bind_dates(strip_terminator(case.sql_gcp),
                                     date_from, date_to)

    LOG.info("Running test case %s / %s", case.table, case.name)
    oracle = run_oracle(source, sql_oracle, limit)
    gcp = run_gcp(bq, sql_gcp, limit)

    tolerance = tolerance_for(cfg, case.name)
    status, actual, details, diff = compare_sides(
        oracle, gcp, decimals, limit, tolerance,
        int(cfg["defaults"]["timestamp_decimals"]))
    # With -v, the rows themselves, for reading side by side.
    if diff and LOG.isEnabledFor(logging.DEBUG):
        show_diff(case, diff, out_dir, stamp)
    if not params_oracle or not params_gcp:
        # Such a case ignores the window, so the dates change nothing in it.
        side = "sql_oracle" if not params_oracle else "sql_gcp"
        details = (details + "; " if details else "") + \
            f"no date parameter in {side} - the window was not applied there"
    rep.add("Test cases", case.name, status,
            expected="oracle = gcp", actual=actual, details=details)


# --------------------------------------------------------------------------- #
# 4) Airflow / Cloud Composer  (carried over from v2)
# --------------------------------------------------------------------------- #

# The dataset is whatever sits one dot before the table name.
_TABLE_REF = re.compile(
    r"\b(?:FROM|JOIN)\s+`?([A-Za-z0-9_$-]+(?:`?\s*\.\s*`?[A-Za-z0-9_$-]+)+)`?",
    re.IGNORECASE)


def dataset_of(sql: str) -> str | None:
    """The BigQuery dataset the query reads from, taken from its first table."""
    match = _TABLE_REF.search(sql or "")
    if not match:
        return None
    parts = [p for p in match.group(1).replace("`", "").replace(" ", "").split(".") if p]
    return parts[-2] if len(parts) >= 2 else None


def dag_prefix_of(dataset: str, layer_prefixes: Any) -> str:
    """The schema part of a dataset name, as the dag_id spells it.

    The layer in front of it is dropped: bronze_sys -> SYS_.
    """
    name = dataset.upper()
    for prefix in layer_prefixes or []:
        prefix = str(prefix).upper()
        if prefix and name.startswith(prefix):
            name = name[len(prefix):]
            break
    return f"{name}_" if name else ""


def dag_id_for(tc: dict, af: dict) -> str:
    """Build the dag_id from the table name, unless the config states it.

        <dag_id_prefix><dag_prefix><TABLE NAME minus a stripped suffix>

    dag_prefix is the source schema, taken from the sql_gcp dataset when the
    config does not give it.
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
# 5) Report file
# --------------------------------------------------------------------------- #

SECTIONS = ("Test cases", "Airflow")
_WIDTH = 100                      # report line width
DEFAULT_OUT_DIR = "./reports"     # used when neither --out-dir nor report_dir is set


def _banner(text: str, char: str = "=") -> list[str]:
    return [char * _WIDTH, f" {text}", char * _WIDTH]


def _section_bar(section: str, status: str) -> str:
    head = f"--- {section} "
    return head.ljust(_WIDTH - len(status) - 1, "-") + " " + status


def _detail_lines(details: str, indent: int) -> list[str]:
    pad = " " * indent
    return textwrap.wrap(details, width=_WIDTH - indent - 2,
                         initial_indent=f"{pad}> ", subsequent_indent=f"{pad}  ") \
        if details else []


def render_text(reports: list[TableReport], cfg: dict, window: str) -> str:
    overall = worst([r.final_status for r in reports])
    counts = {s: sum(1 for r in reports if r.final_status == s)
              for s in (PASS, WARN, FAIL, SKIP)}

    out = _banner("LOADING TEST REPORT - CATALOGUE TEST CASES")
    out += [
        f" Generated : {now_local():%Y-%m-%d %H:%M:%S %Z}",
        f" Project   : {cfg.get('project_id')}",
        f" Location  : {cfg.get('location') or '-'}",
        f" Window    : {window}",
        f" Tables    : {len(reports)}",
        "",
        f" FINAL TEST STATUS: {overall}",
        "=" * _WIDTH,
        "",
        "",
        "SUMMARY",
        "-" * _WIDTH,
    ]

    w_table = max([len("TABLE")] + [len(r.table) for r in reports]) + 2
    w_cases = len("CASES") + 2
    w_sec = max(len(s) for s in SECTIONS + (FAIL, WARN, PASS, SKIP)) + 2

    out.append("TABLE".ljust(w_table) + "CASES".ljust(w_cases) +
               "".join(s.upper().ljust(w_sec) for s in SECTIONS) + "STATUS")
    for r in reports:
        out.append(r.table.ljust(w_table) + str(r.cases).ljust(w_cases) +
                   "".join(r.section_status(s).ljust(w_sec) for s in SECTIONS) +
                   r.final_status)
    out += ["-" * _WIDTH,
            " " + "   ".join(f"{s} = {counts[s]}" for s in (PASS, WARN, FAIL, SKIP)),
            "", ""]

    for i, r in enumerate(reports, 1):
        title = f"[{i}/{len(reports)}]  {r.table}"
        out += ["=" * _WIDTH,
                f" {title}".ljust(_WIDTH - len(r.final_status) - 1) + " " + r.final_status,
                "=" * _WIDTH]
        dur = (r.finished_at - r.started_at).total_seconds() if r.finished_at else 0
        out += [f" test cases = {r.cases} | window = {r.window or '-'}"
                f" | duration = {dur:.1f}s", ""]

        w_name = max([len("CHECK")] + [len(c.name) for c in r.results]) + 2
        w_exp = max([len("EXPECTED")] + [len(c.expected or "-") for c in r.results]) + 2

        extra = [s for s in dict.fromkeys(c.section for c in r.results)
                 if s not in SECTIONS]
        for section in list(SECTIONS) + extra:
            checks = [c for c in r.results if c.section == section]
            if not checks:
                continue
            out.append(_section_bar(section, r.section_status(section)))
            out.append("STATUS".ljust(9) + "CHECK".ljust(w_name) +
                       "EXPECTED".ljust(w_exp) + "ACTUAL")
            for c in checks:
                out.append(c.status.ljust(9) + c.name.ljust(w_name) +
                           (c.expected or "-").ljust(w_exp) + (c.actual or "-"))
                out += _detail_lines(c.details, 9)
            out.append("")
        out.append("")

    issues = [(r.table, c) for r in reports for c in r.results
              if c.status in (WARN, FAIL)]
    out += _banner("ISSUES TO FOLLOW UP")
    if issues:
        w_tbl = max(len(t) for t, _ in issues) + 2
        w_sct = max(len(c.section) for _, c in issues) + 2
        # Wrapped, not cut - this part gets pasted into a ticket. The indent
        # stops short of the width so there is room left to write in.
        indent = " " * min(1 + 6 + w_tbl + w_sct, _WIDTH // 3)
        for table, c in issues:
            line = (f" {c.status.ljust(6)}{table.ljust(w_tbl)}{c.section.ljust(w_sct)}"
                    f"{c.name}: {c.actual or c.details or '-'}")
            out += textwrap.wrap(line, width=_WIDTH, subsequent_indent=indent) or [line]
    else:
        out.append(" None - all checks passed.")
    out += ["", f" FINAL TEST STATUS: {overall}", "=" * _WIDTH, ""]

    return "\n".join(out)


def write_report(reports: list[TableReport], cfg: dict, window: str,
                 out_dir: str, stamp: str) -> str:
    path = os.path.join(out_dir, f"bronze_test_{stamp}.txt")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(render_text(reports, cfg, window))
    return path


# --------------------------------------------------------------------------- #
# 6) The questions
# --------------------------------------------------------------------------- #

def ask_tables() -> list[str] | None:
    """Which tables to test - one name or several separated by commas.

    No default: the question repeats until a name is given.
    """
    while True:
        try:
            answer = input("\nTables to test (required), comma separated: ").strip()
        except EOFError:
            return None
        typed = [part.strip() for part in answer.split(",") if part.strip()]
        if typed:
            return list(dict.fromkeys(typed))
        print("  A table name is required - nothing is tested without one")


def ask_window(cfg: dict) -> tuple[str, str]:
    """The date window. Unlike the table, this one has defaults to accept."""
    default_from = valid_date(cfg["defaults"]["date_from"])
    default_to = (now_local().date() - timedelta(days=1)).isoformat()

    def ask(label: str, default: str) -> str:
        while True:
            try:
                answer = input(f"  {label} - Enter for {default}, "
                               f"or type a date: ").strip()
            except EOFError:
                return default
            try:
                return valid_date(answer or default)
            except ValueError:
                print("    Use the YYYY-MM-DD format, e.g. 2026-06-01")

    print("\nDate window (YYYY-MM-DD):")
    while True:
        date_from = ask("From", default_from)
        date_to = ask("To  ", default_to)
        if date_from <= date_to:
            print(f"  Window: {date_from} .. {date_to}")
            return date_from, date_to
        print("  The start of the window is after its end - both dates again")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Loading tests from the metadata catalogue (Oracle + BigQuery)")
    # Not required, so the installed command can be typed on its own.
    ap.add_argument("--config",
                    help="Path to YAML config. Without it: LOADING_TESTS_CONFIG, "
                         "then config.yaml here, then the one next to the "
                         "installed project")
    ap.add_argument("--table", action="append",
                    help="Table to test (repeatable, or comma separated); "
                         "skips the question")
    ap.add_argument("--date-from", help="Window start, YYYY-MM-DD (skips the question)")
    ap.add_argument("--date-to", help="Window end, YYYY-MM-DD (skips the question)")
    ap.add_argument("--out-dir", default=None,
                    help=f"Directory for the report and the log "
                         f"(default: report_dir from the config, else {DEFAULT_OUT_DIR})")
    ap.add_argument("-v", "--verbose", action="store_true", help="Log the SQL being run")
    args = ap.parse_args(argv)

    try:
        config_path = find_config(args.config)
        cfg = load_config(config_path)
    except Exception as exc:
        LOG.error("Config: %s: %s", type(exc).__name__, exc)
        return 2

    # A relative report_dir belongs to the config, not to wherever the command
    # was typed - otherwise reports scatter across the disk. --out-dir is the
    # user speaking, so it is left as given.
    out_dir = args.out_dir or cfg.get("report_dir") or DEFAULT_OUT_DIR
    if not args.out_dir and not os.path.isabs(out_dir):
        out_dir = os.path.normpath(
            os.path.join(os.path.dirname(os.path.abspath(config_path)), out_dir))
    os.makedirs(out_dir, exist_ok=True)

    stamp = now_local().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(out_dir, f"bronze_test_{stamp}.log")
    layout = "%(asctime)s %(levelname)-7s %(message)s"
    clock = (lambda secs: datetime.fromtimestamp(secs, LOCAL_TZ).timetuple()) \
        if LOCAL_TZ else None

    to_file = logging.FileHandler(log_path, encoding="utf-8")
    to_screen = logging.StreamHandler(sys.stdout)
    # The console may be coloured; the file must not be.
    to_file.setFormatter(logging.Formatter(layout))
    to_screen.setFormatter((ColourFormatter if colour_works(sys.stdout)
                            else logging.Formatter)(layout))
    handlers = [to_file, to_screen]
    for handler in handlers:
        if clock:
            handler.formatter.converter = clock
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        handlers=handlers)
    LOG.info("Config: %s", os.path.abspath(config_path))

    # --- Connections first, BigQuery ahead of the rest: it is the one that
    # may open a browser, and better to learn that before typing passwords. -- #
    bq = connect_bigquery(cfg)
    if bq is None:
        return 2

    connections = []
    for key in ("oracle_meta", "oracle"):
        try:
            connections.append(oracle_connect(cfg, key))
        except ImportError:
            LOG.error("oracledb is not installed - run `pip install oracledb`")
            return 2
        except Exception as exc:
            LOG.error("Cannot connect to `%s`: %s: %s", key, type(exc).__name__, exc)
            LOG.error("No tests were run.")
            oracle_close_all()
            return 2
    meta, source = connections

    # --- Which tables ------------------------------------------------------- #
    typed: list[str] | None
    if args.table:
        typed = list(dict.fromkeys(p.strip() for a in args.table
                                   for p in a.split(",") if p.strip()))
    elif sys.stdin.isatty():
        typed = ask_tables()
    else:
        LOG.error("A table is required: pass --table, there is no terminal to ask at.")
        oracle_close_all()
        return 2
    if not typed:
        LOG.error("A table is required - nothing was tested.")
        oracle_close_all()
        return 2

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
        rep = TableReport(table=table, started_at=now_local(), window=window)
        LOG.info("=" * 78)
        LOG.info("Testing table: %s", table)
        LOG.info("=" * 78)

        table_cases = [c for c in cases if c.table.upper() == table.upper()]
        for case in table_cases:
            try:
                run_test_case(bq, source, cfg, case, date_from, date_to, rep,
                              out_dir, stamp)
            except Exception as exc:
                LOG.exception("Unexpected error in test case %s (%s)", case.name, table)
                rep.add("Runtime", case.name, FAIL,
                        details=f"{type(exc).__name__}: {exc}")

        # The source schema for the dag_id only appears in the sql_gcp dataset.
        tc = table_config(cfg, table)
        af = cfg.get("airflow") or {}
        if not tc.get("dag_id") and not tc.get("dag_prefix"):
            dataset = next((d for d in (dataset_of(c.sql_gcp) for c in table_cases) if d),
                           None)
            if dataset:
                tc["dag_prefix"] = dag_prefix_of(dataset, af.get("dataset_layer_prefixes"))
                LOG.info("  dag_id from dataset %s: %s", dataset, dag_id_for(tc, af))
            else:
                LOG.warning("  no dataset found in sql_gcp - dag_id without the schema")
        try:
            check_airflow(cfg, tc, rep)
        except Exception as exc:
            LOG.exception("Unexpected error in the Airflow check (%s)", table)
            rep.add("Runtime", "Airflow", FAIL, details=f"{type(exc).__name__}: {exc}")

        rep.finished_at = now_local()
        LOG.info("FINAL STATUS for %s: %s", table, rep.final_status)
        reports.append(rep)

    oracle_close_all()
    txt_path = write_report(reports, cfg, window, out_dir, stamp)

    overall = worst([r.final_status for r in reports])
    LOG.info("-" * 78)
    LOG.info("OVERALL STATUS: %s", overall)
    LOG.info("Bytes billed: %.2f MB", bq.bytes_billed / 1024 / 1024)
    LOG.info("Report  : %s", txt_path)
    LOG.info("Log file: %s", log_path)
    return 1 if overall == FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
