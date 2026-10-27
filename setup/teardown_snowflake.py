"""Drop every Snowflake object the demo created. Usage: python -m setup.teardown_snowflake"""

from migration.common import DEMO_USERS, NETWORK_POLICY, names, run_sql, snowflake_admin

STEP7_OBJECTS = ["DROP DATABASE IF EXISTS HORIZON_EXIT_DEMO_POLARIS_DB", "DROP CATALOG INTEGRATION IF EXISTS HORIZON_EXIT_DEMO_POLARIS_INT"]
# Names used before the users and roles got the HORIZON_DEMO_ prefix (runs on or before 2026-10-27).
LEGACY_USERS = ["DEMO_RW", "DEMO_RO", "DEMO_NONE"]
LEGACY_ROLES = ["DEMO_RW_ROLE", "DEMO_RO_ROLE", "DEMO_NONE_ROLE"]


def main() -> None:
    n = names()
    conn = snowflake_admin()
    stmts = list(STEP7_OBJECTS)
    stmts += [f"DROP USER IF EXISTS {u['user']}" for u in DEMO_USERS.values()]
    stmts += [f"DROP ROLE IF EXISTS {u['role']}" for u in DEMO_USERS.values()]
    stmts += [f"DROP USER IF EXISTS {u}" for u in LEGACY_USERS]
    stmts += [f"DROP ROLE IF EXISTS {r}" for r in LEGACY_ROLES]
    stmts += [
        f"DROP NETWORK POLICY IF EXISTS {NETWORK_POLICY}",
        f"DROP DATABASE IF EXISTS {n.database}",
        f"DROP WAREHOUSE IF EXISTS {n.warehouse}",
        "DROP RESOURCE MONITOR IF EXISTS HORIZON_EXIT_DEMO_RM",
        f"DROP EXTERNAL VOLUME IF EXISTS {n.external_volume}",
    ]
    run_sql(conn, stmts)


if __name__ == "__main__":
    main()
