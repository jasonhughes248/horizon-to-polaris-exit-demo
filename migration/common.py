"""Shared configuration, state files, and catalog REST helpers.

Configuration comes from `.env` at the repo root plus the process environment.
Secrets created during a run (PATs, Polaris principal credentials) are written
to `state/` with owner-only permissions and are never printed.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = REPO_ROOT / "state"
ENV_FILE = REPO_ROOT / ".env"

# The three demo users and the Snowflake role each one uses. The keys are the
# short names used everywhere else (Spark catalog suffixes, Polaris principals).
DEMO_USERS = {
    "rw": {"user": "HORIZON_DEMO_RW", "role": "HORIZON_DEMO_RW_ROLE"},
    "ro": {"user": "HORIZON_DEMO_RO", "role": "HORIZON_DEMO_RO_ROLE"},
    "none": {"user": "HORIZON_DEMO_NONE", "role": "HORIZON_DEMO_NONE_ROLE"},
}
# Demo roles are granted to SYSADMIN, so ACCOUNTADMIN (the admin connection) can
# inspect grants and drop the table that HORIZON_DEMO_RW_ROLE owns.
ADMIN_ROLE = "ACCOUNTADMIN"
NETWORK_POLICY = "HORIZON_EXIT_DEMO_SPARK_NP"
PAT_NAME = "HORIZON_EXIT_DEMO_PAT"


def load_env() -> None:
    """Load KEY=VALUE lines from .env without overriding real env vars."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.split(" #", 1)[0].strip()
        if value:  # an exported empty AWS_PROFILE breaks botocore's credential chain
            os.environ.setdefault(key.strip(), value)


def env(name: str, default: str | None = None) -> str:
    load_env()
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"{name} is not set (see .env.example)")
    return value


@dataclass(frozen=True)
class Names:
    database: str
    schema: str
    table: str
    warehouse: str
    external_volume: str

    @property
    def fqn(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


def names() -> Names:
    return Names(
        database=env("SF_DATABASE", "HORIZON_EXIT_DEMO_DB"),
        schema=env("SF_SCHEMA", "DEMO"),
        table=env("SF_TABLE", "EVENTS"),
        warehouse=env("SF_WAREHOUSE", "HORIZON_EXIT_DEMO_WH"),
        external_volume=env("SF_EXTERNAL_VOLUME", "HORIZON_EXIT_DEMO_VOL"),
    )


# ── State files ──────────────────────────────────────────────────────────────


def write_state(name: str, data: Any, secret: bool = False) -> Path:
    STATE_DIR.mkdir(exist_ok=True)
    path = STATE_DIR / name
    text = json.dumps(data, indent=2, default=str, sort_keys=True)
    if not secret:
        path.write_text(text)
        return path
    # Secret: write a temp file created 0600 (never briefly 0644), then rename
    # it over the target atomically so readers see old or new content, not partial.
    tmp = STATE_DIR / f".{name}.{os.getpid()}.tmp"
    tmp.unlink(missing_ok=True)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)  # explicit, independent of umask
        os.close(fd)
        tmp.write_text(text)  # existing file: mode stays 0600
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return path


def read_state(name: str) -> Any:
    path = STATE_DIR / name
    if not path.exists():
        raise RuntimeError(f"state/{name} not found; run the earlier step first")
    return json.loads(path.read_text())


def has_state(name: str) -> bool:
    return (STATE_DIR / name).exists()


# ── AWS ──────────────────────────────────────────────────────────────────────


def aws_session():
    """boto3 session from the standard credential chain (optionally AWS_PROFILE)."""
    import boto3

    load_env()
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None, region_name=env("AWS_REGION", "us-west-2"))


def bucket_base() -> str:
    return f"s3://{env('S3_BUCKET')}/{env('S3_PREFIX', 'horizon-to-polaris-exit').strip('/')}/"


# ── Snowflake ────────────────────────────────────────────────────────────────


def snowflake_admin(role: str = ADMIN_ROLE):
    """Connector session for the admin connection, switched to `role`."""
    import snowflake.connector

    conn = snowflake.connector.connect(connection_name=env("SNOWFLAKE_CONNECTION_NAME"))
    conn.cursor().execute(f"USE ROLE {role}")
    return conn


def snowflake_as(user_key: str):
    """Connector session as a demo user, authenticated with that user's PAT."""
    import snowflake.connector

    host = urlparse(env("SNOWFLAKE_ACCOUNT_URL")).netloc or env("SNOWFLAKE_ACCOUNT_URL")
    u = DEMO_USERS[user_key]
    return snowflake.connector.connect(
        account=host.split(".snowflakecomputing")[0],
        user=u["user"],
        role=u["role"],
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
        token=pats()[user_key],
        warehouse=names().warehouse,
    )


def run_sql(conn, statements: list[str], echo: bool = True) -> list[list[tuple]]:
    """Run statements in order; echo is off for statements that contain secrets."""
    results = []
    cur = conn.cursor()
    for stmt in statements:
        if echo:
            print(f"  sql> {stmt.splitlines()[0][:110]}")
        results.append(cur.execute(stmt).fetchall())
    return results


def query_dicts(conn, sql: str) -> list[dict]:
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Iceberg REST (Horizon and Polaris) ───────────────────────────────────────


def horizon_uri() -> str:
    return env("SNOWFLAKE_ACCOUNT_URL").rstrip("/") + "/polaris/api/catalog"


def polaris_uri() -> str:
    return env("CATALOG_URI").rstrip("/")


def pats() -> dict:
    return read_state("pats.json")


def polaris_principals() -> dict:
    return read_state("polaris_principals.json")


class RestCatalog:
    """Minimal Iceberg REST client: enough to load tables and list objects."""

    def __init__(self, uri: str, warehouse: str, client_id: str | None, secret: str, scope: str):
        self.uri = uri
        self.warehouse = warehouse
        self.client_id = client_id
        self.secret = secret
        self.scope = scope
        self._token: str | None = None
        self._prefix: str | None = None

    @classmethod
    def horizon(cls, user_key: str) -> "RestCatalog":
        role = DEMO_USERS[user_key]["role"]
        return cls(horizon_uri(), names().database, None, pats()[user_key], f"session:role:{role}")

    @classmethod
    def polaris(cls, user_key: str) -> "RestCatalog":
        creds = polaris_principals()[user_key]
        return cls(polaris_uri(), names().database, creds["client_id"], creds["client_secret"], "PRINCIPAL_ROLE:ALL")

    def token(self) -> str:
        if self._token is None:
            data = {"grant_type": "client_credentials", "client_secret": self.secret, "scope": self.scope}
            if self.client_id:
                data["client_id"] = self.client_id
            resp = requests.post(f"{self.uri}/v1/oauth/tokens", data=data, timeout=30)
            resp.raise_for_status()
            self._token = resp.json()["access_token"]
        return self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token()}", "X-Iceberg-Access-Delegation": "vended-credentials"}

    def prefix(self) -> str:
        if self._prefix is None:
            resp = requests.get(
                f"{self.uri}/v1/config", params={"warehouse": self.warehouse}, headers=self._headers(), timeout=30
            )
            resp.raise_for_status()
            cfg = resp.json()
            self._prefix = {**cfg.get("defaults", {}), **cfg.get("overrides", {})}.get("prefix", "")
        return self._prefix

    def get(self, path: str) -> requests.Response:
        base = f"{self.uri}/v1/{self.prefix()}" if self.prefix() else f"{self.uri}/v1"
        return requests.get(f"{base}/{path}", headers=self._headers(), timeout=60)

    def load_table(self, namespace: str, table: str) -> dict:
        resp = self.get(f"namespaces/{namespace}/tables/{table}")
        resp.raise_for_status()
        return resp.json()


def table_identity(load_table_response: dict) -> dict:
    """The parts of table metadata that must survive the migration unchanged."""
    md = load_table_response["metadata"]
    return {
        "table_uuid": md["table-uuid"],
        "format_version": md["format-version"],
        "current_snapshot_id": md.get("current-snapshot-id"),
        "snapshot_ids": sorted(s["snapshot-id"] for s in md.get("snapshots", [])),
        "location": md["location"],
        "metadata_location": load_table_response.get("metadata-location"),
    }


# Order-independent aggregate both Spark and Snowflake can compute identically.
FINGERPRINT_COLUMNS = ["row_count", "sum_id", "sum_amount", "min_event_ts", "max_event_ts", "distinct_categories"]


def fingerprint_sql(table_ref: str, quote: str = "") -> str:
    """quote='"' for Snowflake: Spark created lowercase column names."""
    c = {k: f"{quote}{k}{quote}" for k in ("id", "amount", "event_ts", "category")}
    return (
        f"SELECT COUNT(*) AS row_count, SUM({c['id']}) AS sum_id, "
        f"CAST(SUM({c['amount']}) AS DECIMAL(18,2)) AS sum_amount, "
        f"MIN({c['event_ts']}) AS min_event_ts, MAX({c['event_ts']}) AS max_event_ts, "
        f"COUNT(DISTINCT {c['category']}) AS distinct_categories FROM {table_ref}"
    )


def normalize_fingerprint(row) -> dict:
    """Normalize types so Spark and Snowflake results compare equal."""
    values = dict(zip(FINGERPRINT_COLUMNS, list(row)))
    out = {}
    for key, value in values.items():
        if key in ("min_event_ts", "max_event_ts") and value is not None:
            out[key] = value.replace(tzinfo=None).isoformat(timespec="microseconds")
        elif key == "sum_amount" and value is not None:
            out[key] = f"{float(value):.2f}"
        else:
            out[key] = int(value) if value is not None else None
    return out


# ── Who wrote what ───────────────────────────────────────────────────────────
# Every row's payload records the engine and catalog that wrote it. Each group
# has its own id range, so the groups never overlap.
SPARK_HORIZON = "spark via horizon"          # step 2, ids 1-3000
SNOWFLAKE_HORIZON = "snowflake via horizon"  # step 2, ids 5001-5100
SPARK_POLARIS = "spark via polaris"          # step 6, ids 6001-6010
SNOWFLAKE_POLARIS = "snowflake via polaris"  # step 7, ids 7001-7010
WRITER_IDS = {SNOWFLAKE_HORIZON: (5001, 100), SPARK_POLARIS: (6001, 10), SNOWFLAKE_POLARIS: (7001, 10)}


def writer_summary_sql(table_ref: str, quote: str = "") -> str:
    p = f"{quote}payload{quote}"
    return f"SELECT {p} AS written_by, COUNT(*) AS row_count FROM {table_ref} GROUP BY {p} ORDER BY {p}"


def writer_summary(rows) -> dict:
    return {str(r[0]): int(r[1]) for r in rows}


def print_writer_summary(title: str, summary: dict) -> None:
    print(f"\n{title}")
    for writer, count in summary.items():
        print(f"  {writer:<24}{count:>6} rows")


def tagged_rows_sql(table_ref: str, writer: str, dialect: str) -> list[str]:
    """Delete-then-insert of `writer`'s rows, so reruns don't add duplicates.

    dialect 'spark' or 'snowflake' (Snowflake needs quoted lowercase column names).
    """
    start, count = WRITER_IDS[writer]
    q = '"' if dialect == "snowflake" else ""
    ts = (lambda v: f"'{v}'::TIMESTAMP_NTZ") if dialect == "snowflake" else (lambda v: f"TIMESTAMP_NTZ '{v}'")
    values = ", ".join(
        f"({i}, {ts(f'2026-06-01 00:{(i - start) % 60:02d}:00')}, 'write', {i % 100}.50, '{writer}')"
        for i in range(start, start + count)
    )
    cols = ", ".join(f"{q}{c}{q}" for c in ("id", "event_ts", "category", "amount", "payload"))
    return [
        f"DELETE FROM {table_ref} WHERE {q}payload{q} = '{writer}'",
        f"INSERT INTO {table_ref} ({cols}) VALUES {values}",
    ]
