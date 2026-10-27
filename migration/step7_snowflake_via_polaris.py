"""Step 7: Snowflake reads the migrated table back, through Polaris.

Snowflake is now just another engine: a catalog integration authenticates to
Polaris as the HORIZON_DEMO_RO principal (read-only), and a catalog-linked database
exposes the namespace. Polaris vends S3 credentials for its own task role, so
this works even though Snowflake's external volume role is denied.
Usage: python -m migration.step7_snowflake_via_polaris
"""

from __future__ import annotations

import time

from migration.common import (
    FINGERPRINT_COLUMNS, fingerprint_sql, names, normalize_fingerprint, polaris_principals, polaris_uri,
    query_dicts, read_state, run_sql, snowflake_admin, write_state,
)

INTEGRATION = "HORIZON_EXIT_DEMO_POLARIS_INT"
LINKED_DB = "HORIZON_EXIT_DEMO_POLARIS_DB"


def main() -> None:
    n = names()
    ro = polaris_principals()["ro"]
    conn = snowflake_admin()
    # The integration cannot be replaced while a linked database uses it (rerun,
    # or new Polaris credentials after recover_polaris). The linked database is
    # read-only and HORIZON_DEMO_RO cannot drop anything in Polaris, so dropping it only
    # removes Snowflake's view of the catalog.
    run_sql(conn, [f"DROP DATABASE IF EXISTS {LINKED_DB}"])
    print(f"  sql> CREATE OR REPLACE CATALOG INTEGRATION {INTEGRATION} ... (secret not echoed)")
    run_sql(conn, [f"""CREATE OR REPLACE CATALOG INTEGRATION {INTEGRATION}
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  REST_CONFIG = (
    CATALOG_URI = '{polaris_uri()}'
    CATALOG_NAME = '{n.database}'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = '{ro["client_id"]}'
    OAUTH_CLIENT_SECRET = '{ro["client_secret"]}'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE"""], echo=False)
    run_sql(conn, [
        f"SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('{INTEGRATION}')",
        f"CREATE OR REPLACE DATABASE {LINKED_DB} LINKED_CATALOG = (CATALOG = '{INTEGRATION}', ALLOWED_NAMESPACES = ('{n.schema}'), ALLOWED_WRITE_OPERATIONS = NONE)",
        f"USE WAREHOUSE {n.warehouse}",
    ])

    table = f'{LINKED_DB}."{n.schema}"."{n.table}"'
    row, last_error = None, None
    for _ in range(30):  # catalog-linked databases discover tables asynchronously
        try:
            row = query_dicts(conn, fingerprint_sql(table, quote='"'))[0]
            break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc).splitlines()[0]
            time.sleep(10)
    if row is None:
        raise SystemExit(f"step 7 FAILED: Snowflake could not read {table}: {last_error}")

    fp = normalize_fingerprint([row[c] for c in FINGERPRINT_COLUMNS])
    expected = read_state("baseline.json")["fingerprint"]
    write_state("step7_snowflake_via_polaris.json", {"table": table, "fingerprint": fp, "matches_baseline": fp == expected})
    if fp != expected:
        raise SystemExit(f"step 7 FAILED: fingerprint {fp} != baseline {expected}")
    print(f"step 7 complete: Snowflake reads {table} through Polaris; fingerprint matches baseline")


if __name__ == "__main__":
    main()
