"""Step 7: Snowflake reads and writes the migrated table, through Polaris.

Snowflake is now just another engine: a catalog integration authenticates to
Polaris as the HORIZON_DEMO_RW principal, and a writable catalog-linked database
exposes the namespace. Polaris vends S3 credentials for its own task role, so
this works even though Snowflake's external volume role is denied.

Inside Snowflake, grants on the linked database decide what each demo user may do:
HORIZON_DEMO_RW_ROLE reads and writes, HORIZON_DEMO_RO_ROLE only reads, and
HORIZON_DEMO_NONE_ROLE gets nothing. The same users are tried from Snowflake SQL,
HORIZON_DEMO_RW writes 10 rows ("snowflake via polaris"), and Spark reads them back.
Usage: python -m migration.step7_snowflake_via_polaris
"""

from __future__ import annotations

import time

from migration.common import (
    FINGERPRINT_COLUMNS, SNOWFLAKE_HORIZON, SNOWFLAKE_POLARIS, SPARK_HORIZON, SPARK_POLARIS, DEMO_USERS, RestCatalog,
    fingerprint_sql, names, normalize_fingerprint, polaris_principals, polaris_uri, print_writer_summary, query_dicts,
    read_state, run_sql, snowflake_admin, snowflake_as, tagged_rows_sql, write_state, writer_summary,
    writer_summary_sql,
)
from spark.sessions import add_polaris_catalogs, spark, table_ref
from tests.access_matrix import mismatches, print_matrix, run_snowflake_matrix

INTEGRATION = "HORIZON_EXIT_DEMO_POLARIS_INT"
LINKED_DB = "HORIZON_EXIT_DEMO_POLARIS_DB"


def drop_linked_db_sql() -> list[str]:
    """Make the linked database read-only before dropping it, so no drop can reach Polaris."""
    return [
        f"ALTER DATABASE IF EXISTS {LINKED_DB} UPDATE LINKED_CATALOG SET ALLOWED_WRITE_OPERATIONS = NONE",
        f"DROP DATABASE IF EXISTS {LINKED_DB}",
    ]


def grants_sql(schema: str, table: str) -> list[str]:
    s, t = f'{LINKED_DB}."{schema}"', f'{LINKED_DB}."{schema}"."{table}"'
    rw, ro = DEMO_USERS["rw"]["role"], DEMO_USERS["ro"]["role"]
    return [
        *[f"GRANT USAGE ON DATABASE {LINKED_DB} TO ROLE {r}" for r in (rw, ro)],
        *[f"GRANT USAGE ON SCHEMA {s} TO ROLE {r}" for r in (rw, ro)],
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {t} TO ROLE {rw}",
        f"GRANT SELECT ON TABLE {t} TO ROLE {ro}",
        # HORIZON_DEMO_NONE_ROLE: nothing
    ]


def main() -> None:
    n = names()
    rw = polaris_principals()["rw"]
    conn = snowflake_admin()
    # The integration cannot be replaced while a linked database uses it (rerun,
    # or new Polaris credentials after recover_polaris). Dropping the linked
    # database removes only Snowflake's view of the catalog.
    run_sql(conn, drop_linked_db_sql())
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
    OAUTH_CLIENT_ID = '{rw["client_id"]}'
    OAUTH_CLIENT_SECRET = '{rw["client_secret"]}'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE"""], echo=False)
    run_sql(conn, [
        f"SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('{INTEGRATION}')",
        f"CREATE OR REPLACE DATABASE {LINKED_DB} LINKED_CATALOG = (CATALOG = '{INTEGRATION}', ALLOWED_NAMESPACES = ('{n.schema}'), ALLOWED_WRITE_OPERATIONS = ALL)",
        f"USE WAREHOUSE {n.warehouse}",
    ])

    table = f'{LINKED_DB}."{n.schema}"."{n.table}"'
    found, last_error = False, None
    for _ in range(30):  # catalog-linked databases discover tables asynchronously
        try:
            query_dicts(conn, f"SELECT COUNT(*) AS c FROM {table}")
            found = True
            break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc).splitlines()[0]
            time.sleep(10)
    if not found:
        raise SystemExit(f"step 7 FAILED: Snowflake could not read {table}: {last_error}")
    run_sql(conn, grants_sql(n.schema, n.table))
    bad = []

    # The same three users, in Snowflake, after the exit.
    sf_matrix = run_snowflake_matrix(snowflake_as, LINKED_DB, n.schema, n.table)
    print_matrix("Polaris access (Snowflake SQL, same users after migration)", sf_matrix)
    bad += [f"snowflake {m}" for m in mismatches(sf_matrix)]

    # Snowflake keeps writing after the exit, as the same user, through Polaris.
    with snowflake_as("rw") as rw_conn:
        for stmt in tagged_rows_sql(table, SNOWFLAKE_POLARIS, "snowflake"):
            rw_conn.cursor().execute(stmt)

    # Both engines must now see exactly the same table, written by both of them.
    s = spark()
    add_polaris_catalogs()
    t = table_ref("polaris_ro")
    s.sql(f"REFRESH TABLE {t}")
    spark_writers = writer_summary(s.sql(writer_summary_sql(t)).collect())
    sf_writers = writer_summary(conn.cursor().execute(writer_summary_sql(table, quote='"')).fetchall())
    print_writer_summary("Rows by writer (Spark and Snowflake agree)" if spark_writers == sf_writers
                         else "Rows by writer, Spark's view (Snowflake disagrees)", spark_writers)
    if spark_writers != sf_writers:
        bad.append(f"writer summaries differ: Spark {spark_writers} vs Snowflake {sf_writers}")
    baseline = read_state("baseline.json")
    expected = {SNOWFLAKE_HORIZON: baseline["writers"][SNOWFLAKE_HORIZON], SNOWFLAKE_POLARIS: 10,
                SPARK_HORIZON: baseline["writers"][SPARK_HORIZON], SPARK_POLARIS: 10}
    if spark_writers != expected:
        bad.append(f"rows by writer: expected {expected}, got {spark_writers}")

    spark_fp = normalize_fingerprint(s.sql(fingerprint_sql(t)).collect()[0])
    row = query_dicts(conn, fingerprint_sql(table, quote='"'))[0]
    sf_fp = normalize_fingerprint([row[c] for c in FINGERPRINT_COLUMNS])
    if spark_fp != sf_fp:
        bad.append(f"fingerprints differ: Spark {spark_fp} vs Snowflake {sf_fp}")

    # Snowflake committed through Polaris; remember the catalog's head for recovery.
    head = RestCatalog.polaris("ro").load_table(n.schema, n.table).get("metadata-location")
    if head:
        write_state("last_committed.json", {"metadata_location": head})
    write_state("step7_snowflake_via_polaris.json", {
        "table": table, "fingerprint": sf_fp, "matches_spark": spark_fp == sf_fp,
        "writers": spark_writers, "snowflake_matrix": sf_matrix,
    })
    if bad:
        raise SystemExit("step 7 FAILED:\n  " + "\n  ".join(bad))
    print(f"step 7 complete: Snowflake reads and writes {table} through Polaris; "
          f"both engines see the same {spark_fp['row_count']} rows from all four writers")


if __name__ == "__main__":
    main()
