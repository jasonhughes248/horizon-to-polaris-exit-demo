"""Per-user access probes, run identically against Horizon and Polaris.

Each probe answers one yes/no question for one user. Errors count as "no"; the
error class is recorded so a caller can tell an auth denial from a bug.
"""

from __future__ import annotations

from migration.common import DEMO_USERS, names

# What RBAC must produce, on both catalogs.
EXPECTED = {
    "rw": {"sees_namespace": True, "sees_table": True, "reads": True, "writes": True},
    "ro": {"sees_namespace": True, "sees_table": True, "reads": True, "writes": False},
    "none": {"sees_namespace": False, "sees_table": False, "reads": False, "writes": False},
}
PROBE_ID = -1  # row id used by write probes; always cleaned up by the rw user


def _short_error(exc: Exception) -> str:
    text = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
    return text[:200]


def probe_user(spark, catalog: str) -> dict:
    n = names()
    t = f"{catalog}.{n.schema}.{n.table}"
    out: dict = {"errors": {}}

    def attempt(key, fn):
        try:
            out[key] = bool(fn())
        except Exception as exc:  # noqa: BLE001 - denial surfaces as many exception types
            out[key] = False
            out["errors"][key] = _short_error(exc)

    attempt("sees_namespace", lambda: n.schema.lower() in {r[0].lower() for r in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()})
    attempt("sees_table", lambda: n.table.lower() in {r.tableName.lower() for r in spark.sql(f"SHOW TABLES IN {catalog}.{n.schema}").collect()})
    attempt("reads", lambda: spark.sql(f"SELECT COUNT(*) FROM {t}").collect()[0][0] > 0)
    def write():
        from spark.sessions import write_once

        probe = f"INSERT INTO {t} VALUES ({PROBE_ID}, TIMESTAMP_NTZ '2026-01-01 00:00:00', 'probe', 0.00, '{catalog}')"
        # A denied write raises straight through; only an unclear catalog reply is re-checked.
        write_once(spark, t, lambda: spark.sql(probe).collect(),
                   lambda: spark.sql(f"SELECT COUNT(*) FROM {t} WHERE id = {PROBE_ID}").collect()[0][0] > 0,
                   f"{catalog} write probe")
        return True

    attempt("writes", write)
    return out


def cleanup_probe_rows(spark, rw_catalog: str) -> None:
    from spark.sessions import write_once

    n = names()
    t = f"{rw_catalog}.{n.schema}.{n.table}"
    write_once(spark, t, lambda: spark.sql(f"DELETE FROM {t} WHERE id = {PROBE_ID}"),
               lambda: spark.sql(f"SELECT COUNT(*) FROM {t} WHERE id = {PROBE_ID}").collect()[0][0] == 0,
               "probe cleanup")


def run_matrix(spark, prefix: str) -> dict:
    """Probe every user on one catalog server ('horizon' or 'polaris')."""
    result = {key: probe_user(spark, f"{prefix}_{key}") for key in DEMO_USERS}
    cleanup_probe_rows(spark, f"{prefix}_rw")
    return result


def probe_snowflake_user(conn, database: str, schema: str, table: str) -> dict:
    """Same four questions as probe_user, asked in Snowflake SQL as one demo user."""
    t = f'{database}."{schema}"."{table}"'
    out: dict = {"errors": {}}
    cur = conn.cursor()

    def attempt(key, fn):
        try:
            out[key] = bool(fn())
        except Exception as exc:  # noqa: BLE001 - denial surfaces as many exception types
            out[key] = False
            out["errors"][key] = _short_error(exc)

    attempt("sees_namespace", lambda: schema in {r[1] for r in cur.execute(f"SHOW SCHEMAS IN DATABASE {database}").fetchall()})
    attempt("sees_table", lambda: table in {r[1] for r in cur.execute(f'SHOW TABLES IN SCHEMA {database}."{schema}"').fetchall()})
    attempt("reads", lambda: cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] > 0)
    attempt(
        "writes",
        lambda: cur.execute(
            f'INSERT INTO {t} ("id", "event_ts", "category", "amount", "payload") '
            f"VALUES ({PROBE_ID}, '2026-01-01 00:00:00'::TIMESTAMP_NTZ, 'probe', 0.00, 'snowflake probe')"
        ) is not None,
    )
    return out


def run_snowflake_matrix(connect, database: str, schema: str, table: str) -> dict:
    """Probe every user in Snowflake. `connect(user_key)` returns a connection as that user."""
    result = {}
    for key in DEMO_USERS:
        with connect(key) as conn:
            result[key] = probe_snowflake_user(conn, database, schema, table)
    with connect("rw") as conn:
        conn.cursor().execute(f'DELETE FROM {database}."{schema}"."{table}" WHERE "id" = {PROBE_ID}')
    return result


def mismatches(matrix: dict) -> list[str]:
    return [
        f"{user}.{check}: expected {want}, got {matrix[user].get(check)}"
        for user, checks in EXPECTED.items()
        for check, want in checks.items()
        if matrix[user].get(check) != want
    ]


def print_matrix(title: str, matrix: dict) -> None:
    checks = list(EXPECTED["rw"])
    print(f"\n{title}")
    print("  user  " + "  ".join(f"{c:>14}" for c in checks))
    for user in DEMO_USERS:
        cells = []
        for c in checks:
            mark = "yes" if matrix[user][c] else "no"
            if matrix[user][c] != EXPECTED[user][c]:
                mark += " !!"
            cells.append(f"{mark:>14}")
        print(f"  {user:<5} " + "  ".join(cells))
