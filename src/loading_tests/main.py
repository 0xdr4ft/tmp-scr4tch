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
import getpass
import itertools
import logging
import os
import re
import shutil
import subprocess
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

try:
    from croniter import croniter
except ImportError:  # pragma: no cover
    croniter = None

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
    # For the results table. Empty wherever the check has nothing of the kind.
    load_type: str = ""
    cron: str = ""
    tolerance_h: float | None = None
    last_load_at: str = ""
    rows_source: int | None = None
    rows_target: int | None = None
    rows_identical: int | None = None
    rows_discrepancies: int | None = None
    diff_columns: str = ""
    duration_s: float | None = None


@dataclass
class TableReport:
    table: str
    started_at: datetime
    window: str = ""
    load: str = ""                   # FULL / INCREMENTAL, from the source registry
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
REGISTRY_KEYS = ("table_name", "cron_table")
# Only there when the config names them; without one the report says less.
REGISTRY_EXTRAS = ("extract_method", "source_name")
SOURCE_KEYS = ("source_name", "source_type")


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

    meta = cfg.get("oracle_meta") or {}
    registry_cols = meta.get("registry_columns") or {}
    # Only the cases taking their tolerance from a schedule need the registry.
    if cfg.get("cron_tolerance_cases"):
        if not meta.get("table_registry"):
            missing.append("oracle_meta.table_registry")
        missing += [f"oracle_meta.registry_columns.{k}" for k in REGISTRY_KEYS
                    if not registry_cols.get(k)]

    # The framework is read from the metadata, so all of this has to be named.
    if cfg.get("frameworks"):
        if not meta.get("source_registry"):
            missing.append("oracle_meta.source_registry")
        if not registry_cols.get("source_name"):
            missing.append("oracle_meta.registry_columns.source_name")
        source_cols = meta.get("source_columns") or {}
        missing += [f"oracle_meta.source_columns.{k}" for k in SOURCE_KEYS
                    if not source_cols.get(k)]
    elif (cfg.get("results") or {}).get("enabled"):
        missing.append("frameworks (results need one, and it comes from the metadata)")
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


def load_env_file(config_path: str) -> str:
    """Read a `.env` next to the config into the environment; return the file used.

    `KEY=VALUE` a line, `#` starts a comment line only - a value may contain one,
    passwords being what they are. What is already exported wins, so a variable
    set in the shell still beats the file.
    """
    here = os.path.dirname(os.path.abspath(config_path))
    path = next((p for p in (os.path.join(here, ".env"),
                             os.path.join(os.getcwd(), ".env")) if os.path.isfile(p)), "")
    if not path:
        return ""
    # Windows keeps its permissions in the ACL, not in the mode bits.
    if os.name == "posix" and os.stat(path).st_mode & 0o077:
        LOG.warning("%s can be read by other users - `chmod 600` it", path)

    taken = []
    # utf-8-sig: Notepad writes a BOM, which would glue itself to the first name.
    with open(path, "r", encoding="utf-8-sig") as fh:
        for line in fh:
            line = line.strip().removeprefix("export ")
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            name, value = name.strip(), value.strip().strip("\"'")
            if name and name not in os.environ:
                os.environ[name] = value
                taken.append(name)
    LOG.info("Env: %s from %s", ", ".join(taken) or "nothing", path)
    return path


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


PASSWORD_TRIES = 3


def _bad_password(exc: Exception) -> bool:
    """Wrong credentials, the one failure that typing them again can fix."""
    text = f"{exc}".lower()
    return "ora-01017" in text or "logon denied" in text


def oracle_connect(cfg: dict, key: str) -> Any:
    """Connect to the database configured under `key`, asking once per run.

    Credentials come from the environment first (<KEY>_USER / <KEY>_PASSWORD,
    e.g. ORACLE_META_USER), then from the config, then from the terminal. A
    password typed at the terminal can be typed again; one that came from the
    environment cannot, so that a run nobody is watching still fails at once.
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
    password = os.environ.get(f"{prefix}_PASSWORD") or ""
    tries = 1 if password or not sys.stdin.isatty() else PASSWORD_TRIES

    LOG.info("Connecting to %s (%s) as %s", label, dsn, user)
    for attempt in range(1, tries + 1):
        if not password:
            password = getpass.getpass(f"Password for {user}@{label}: ")
        if not password:
            break                          # Enter on its own: the user gives up
        try:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
        except Exception as exc:
            if attempt == tries or not _bad_password(exc):
                _CONNECT_ERRORS[key] = exc
                raise
            LOG.warning("Wrong password for %s - attempt %d of %d, mind the "
                        "lockout", user, attempt, tries)
            password = ""
            continue
        _CONNECTIONS[key] = conn
        LOG.info("Connected to %s (Oracle %s)", label, conn.version)
        return conn

    error = _CONNECT_ERRORS.get(key) or ValueError(f"No password given for {label}")
    _CONNECT_ERRORS[key] = error
    raise error


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













# --------------------------------------------------------------------------- #
# 1b) How often each table is loaded
# --------------------------------------------------------------------------- #



























# --------------------------------------------------------------------------- #
# 2) Date parameters
# --------------------------------------------------------------------------- #





def valid_date(value: str) -> str:
    """YYYY-MM-DD or nothing. This is what keeps the substitution below safe."""
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date().isoformat()










# --------------------------------------------------------------------------- #
# 3) Running one test case on both sides
# --------------------------------------------------------------------------- #















































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


def _run_stamp(stamp: datetime, zone: str) -> datetime:
    """Airflow answers in UTC; this is the zone the source query reads it in.

    UTC by default, because that is what `normalise` compares in - an aware
    timestamp from either side ends up there. `local` is for a source that
    keeps wall-clock time instead.
    """
    if stamp.tzinfo is None:
        return stamp
    if str(zone).lower() == "local":
        return stamp.astimezone(LOCAL_TZ) if LOCAL_TZ else stamp.astimezone()
    return stamp.astimezone(timezone.utc).replace(tzinfo=None)


_AIRFLOW_API: Any = None


def airflow_api(cfg: dict) -> tuple[Any, str]:
    """A `get(path, **params)` for the REST API, or the reason there is none.

    Built once and kept, so a run with ten tables opens one session, not ten.
    """
    global _AIRFLOW_API
    if _AIRFLOW_API is not None:
        return _AIRFLOW_API

    af = cfg.get("airflow") or {}
    base_url = af.get("base_url")
    if not af.get("enabled", True) or not base_url:
        _AIRFLOW_API = (None, "Airflow not configured")
        return _AIRFLOW_API
    try:
        from google.auth import default as google_auth_default
        from google.auth.transport.requests import AuthorizedSession
    except ImportError:
        _AIRFLOW_API = (None, "google-auth not installed")
        return _AIRFLOW_API

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

    _AIRFLOW_API = (get, "")
    return _AIRFLOW_API






def check_airflow(cfg: dict, tc: dict, rep: TableReport) -> None:
    af = cfg.get("airflow") or {}
    get, why = airflow_api(cfg)
    if get is None:
        rep.add("Airflow", "DAG status", SKIP, details=why)
        return

    dag_id = dag_id_for(tc, af)
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
        out += [f" test cases = {r.cases}"
                + (f" | load = {r.load}" if r.load else "")
                + f" | window = {r.window or '-'} | duration = {dur:.1f}s", ""]

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


# --------------------------------------------------------------------------- #
# 5b) Results in the metadata database
# --------------------------------------------------------------------------- #

_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def layer_of(dataset: str, layer_prefixes: Any) -> str:
    """The layer a dataset name carries: bronze_sys -> BRONZE."""
    name = (dataset or "").upper()
    for prefix in layer_prefixes or []:
        prefix = str(prefix).upper()
        if prefix and name.startswith(prefix):
            return prefix.rstrip("_")
    return ""


def _fit(text: Any, size: int) -> Any:
    """Cut to what the column takes, counting bytes the way Oracle does."""
    if text is None or text == "":
        return None
    return str(text).encode("utf-8")[:size].decode("utf-8", "ignore")


def _column_prefix(value: Any, what: str) -> str:
    prefix = str(value or "")
    if prefix and not _PREFIX.match(prefix):
        raise ValueError(f"{what} is not a usable column prefix: {prefix!r}")
    return prefix


def _as_date(value: str) -> Any:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


RUN_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"        # Oracle: RRRR-MM-DD HH24:MI:SS


def _as_stamp(value: str) -> Any:
    try:
        return datetime.strptime(str(value).strip(), RUN_STAMP_FORMAT)
    except (ValueError, TypeError):
        return None


def save_results(cfg: dict, conn: Any, rep: TableReport, framework: str,
                 source: str, layer: str, date_from: str, date_to: str,
                 reason: str = "") -> None:
    """Write one table's report into the two metadata tables.

    The tests are the point; the record of them is not. Anything that goes
    wrong here is reported and swallowed, so a run still finishes.
    """
    res = cfg.get("results") or {}
    if not res.get("enabled"):
        return
    if not framework:
        # The framework column is NOT NULL and the metadata did not say.
        LOG.warning("  results not saved for %s: %s", rep.table,
                    reason or "the framework is not known")
        return

    runs = _oracle_ident(str(res.get("runs_table") or ""), "results.runs_table")
    rows = _oracle_ident(str(res.get("results_table") or ""), "results.results_table")
    head = _column_prefix(res.get("runs_column_prefix"), "results.runs_column_prefix")
    line = _column_prefix(res.get("results_column_prefix"),
                          "results.results_column_prefix")

    # DEFAULTs only fire when the column is absent, so the id ones stay out.
    header = (
        f"INSERT INTO {runs} ({head}FRAMEWORK, {head}ENVIRONMENT, {head}LAYER, "
        f"{head}SOURCE_TESTED, {head}TABLE_TESTED, {head}STARTED_AT, "
        f"{head}FINISHED_AT, {head}WINDOW_FROM, {head}WINDOW_TO, {head}RUN_BY, "
        f"{head}STATUS) "
        f"VALUES (:framework, :environment, :layer, :source, :table_name, "
        f":started, :finished, :win_from, :win_to, :run_by, :status) "
        f"RETURNING {head}ID INTO :run_id")

    try:
        _insert_results(conn, rep, framework, source, res, header, rows, line,
                        head, layer, date_from, date_to)
    except Exception:
        # Half a run recorded is worse than none: the next commit would keep it.
        conn.rollback()
        raise


def _insert_results(conn: Any, rep: TableReport, framework: str, source: str,
                    res: dict, header: str, rows: str, line: str, head: str,
                    layer: str, date_from: str, date_to: str) -> None:
    with conn.cursor() as cur:
        run_id = cur.var(int)
        cur.execute(header, {
            # One spelling, whichever path got here: the column is queried on it.
            "framework": str(framework).strip().lower(),
            "environment": str(res.get("environment") or "").strip().upper(),
            "layer": _fit(layer.upper(), 20),
            "source": _fit(source, 128),
            "table_name": _fit(rep.table, 256),
            "started": rep.started_at.replace(tzinfo=None),
            "finished": rep.finished_at.replace(tzinfo=None) if rep.finished_at else None,
            "win_from": _as_date(date_from),
            "win_to": _as_date(date_to),
            "run_by": _fit(getpass.getuser(), 64),
            "status": rep.final_status,
            "run_id": run_id,
        })
        taken = run_id.getvalue()
        parent = taken[0] if isinstance(taken, list) else taken

        detail = (
            f"INSERT INTO {rows} ({line}{head}ID, {line}SECTION, {line}TEST_CASE, "
            f"{line}STATUS, {line}LOAD_TYPE, {line}CRON, {line}TOLERANCE_H, "
            f"{line}LAST_LOAD_AT, {line}ROWS_SOURCE, {line}ROWS_TARGET, "
            f"{line}ROWS_IDENTICAL, {line}ROWS_DISCREPANCIES, {line}DIFF_COLUMNS, "
            f"{line}EXPECTED, {line}ACTUAL, {line}DETAILS, {line}DURATION_S) "
            f"VALUES (:parent, :section, :test_case, :status, :load_type, :cron, "
            f":tolerance_h, :last_load_at, :rows_source, :rows_target, "
            f":rows_identical, :rows_discrepancies, :diff_columns, :expected, "
            f":actual, :details, :duration_s)")
        cur.executemany(detail, [{
            "parent": parent,
            "section": _fit(r.section, 30),
            "test_case": _fit(r.name, 128),
            "status": r.status,
            "load_type": _fit(r.load_type, 20),
            "cron": _fit(r.cron, 100),
            "tolerance_h": r.tolerance_h,
            "last_load_at": _as_stamp(r.last_load_at),
            "rows_source": r.rows_source,
            "rows_target": r.rows_target,
            "rows_identical": r.rows_identical,
            "rows_discrepancies": r.rows_discrepancies,
            "diff_columns": _fit(r.diff_columns, 1000),
            "expected": _fit(r.expected, 400),
            "actual": _fit(r.actual, 1000),
            "details": _fit(r.details, 2000),
            "duration_s": r.duration_s,
        } for r in rep.results])
    conn.commit()
    LOG.info("  results saved: run %s, %d checks", parent, len(rep.results))


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

def pick_framework(cfg: dict, chosen: str | None) -> str | None:
    """Which tests to run, named the way the config names the frameworks."""
    known = [str(name) for name in (cfg.get("implemented_frameworks") or {})]
    if not known:
        LOG.error("Config implements no framework - see implemented_frameworks")
        return None
    if chosen:
        for name in known:
            if name.strip().lower() == chosen.strip().lower():
                return name
        LOG.error("Unknown framework %r - the config implements: %s",
                  chosen, ", ".join(known))
        return None
    if len(known) == 1 or not sys.stdin.isatty():
        return known[0]

    listed = "  ".join(f"{i}) {name}" for i, name in enumerate(known, 1))
    while True:
        try:
            answer = input(f"\nTests  {listed}\nChoice [1]: ").strip() or "1"
        except EOFError:
            return None
        if answer.isdigit() and 1 <= int(answer) <= len(known):
            return known[int(answer) - 1]
        if answer.lower() in {name.lower() for name in known}:
            return next(n for n in known if n.lower() == answer.lower())
        print(f"  Pick 1-{len(known)}, or type the name")


def framework_style(cfg: dict, framework: str) -> str:
    """`compare` when the tests need a source, `standalone` when they do not."""
    styles = cfg.get("implemented_frameworks") or {}
    return str(styles.get(framework) or "compare").strip().lower()


def run_without_source(bq: BQ, cfg: dict, framework: str, args: Any,
                       out_dir: str, stamp: str, log_path: str) -> int:
    """A framework whose tests ask the target alone: no catalogue, no source."""
    from . import standalone     # here, so the two modules do not import in a ring

    own = cfg.get("standalone") or {}
    system = args.system or (standalone.ask_system() if sys.stdin.isatty() else None)
    if not system:
        LOG.error("A source system is required: pass --system.")
        return 2
    layers = [str(name) for name in (own.get("layers") or [])]
    layer = args.layer or (standalone.ask_layer(layers) if sys.stdin.isatty() else None)
    if not layer:
        LOG.error("A layer is required: pass --layer.")
        return 2

    LOG.info("Framework: %s | system: %s | layer: %s", framework, system, layer)
    try:
        reports = standalone.run_system(bq, cfg, system, layer)
    except Exception as exc:
        LOG.error("Cannot test %s: %s: %s", system, type(exc).__name__, exc)
        return 2
    if not reports:
        return 2

    window = f"last {own.get('hours', 24)}h"
    for rep in reports:
        rep.window = window
    txt_path = write_report(reports, cfg, window, out_dir, stamp)

    if (cfg.get("results") or {}).get("enabled"):
        try:
            meta = oracle_connect(cfg, "oracle_meta")
            for rep in reports:
                save_results(cfg, meta, rep, framework, system, layer, "", "")
        except Exception as exc:
            LOG.warning("Results not saved: %s: %s", type(exc).__name__, exc)
        oracle_close_all()

    overall = worst([r.final_status for r in reports])
    LOG.info("-" * 78)
    LOG.info("OVERALL STATUS: %s", overall)
    LOG.info("Bytes billed: %.2f MB", bq.bytes_billed / 1024 / 1024)
    LOG.info("Report  : %s", txt_path)
    LOG.info("Log file: %s", log_path)
    return 1 if overall == FAIL else 0


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
    ap.add_argument("--framework", help="Which tests to run (skips the question)")
    ap.add_argument("--system", help="Source system, for the frameworks that need one")
    ap.add_argument("--layer", help="Layer of that system (skips the question)")
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
    load_env_file(config_path)

    # --- Connections first, BigQuery ahead of the rest: it is the one that
    # may open a browser, and better to learn that before typing passwords. -- #
    bq = connect_bigquery(cfg)
    if bq is None:
        return 2

    framework = pick_framework(cfg, args.framework)
    if framework is None:
        LOG.error("No framework chosen - nothing was tested.")
        return 2
    if framework_style(cfg, framework) != "compare":
        return run_without_source(bq, cfg, framework, args, out_dir, stamp, log_path)

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

    from . import compare    # here, so the two modules do not import in a ring

    return compare.run_comparison(bq, meta, source, cfg, typed, args,
                                  out_dir, stamp, log_path)


if __name__ == "__main__":
    raise SystemExit(main())
