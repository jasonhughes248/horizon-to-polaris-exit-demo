"""Step 3: show RBAC on Horizon with the three users, from both engines, then capture the baseline.

Each user is tried from local Spark (through Horizon IRC) and from Snowflake SQL; both
must give the same answers. Both engines must also agree on who wrote which rows.
Order matters: the matrices' write probes add snapshots, so the baseline is taken after them. Usage: python -m migration.step3_horizon_demo
"""

from __future__ import annotations

from migration.common import (
    FINGERPRINT_COLUMNS, RestCatalog, fingerprint_sql, names, normalize_fingerprint, print_writer_summary, query_dicts,
    snowflake_admin, snowflake_as, table_identity, write_state, writer_summary, writer_summary_sql,
)
from spark.sessions import spark, table_ref
from tests.access_matrix import mismatches, print_matrix, run_matrix, run_snowflake_matrix


def main() -> None:
    s = spark()
    n = names()

    matrix = run_matrix(s, "horizon")
    print_matrix("Horizon access (Spark, one identity per user)", matrix)
    bad = mismatches(matrix)
    sf_matrix = run_snowflake_matrix(snowflake_as, n.database, n.schema, n.table)
    print_matrix("Horizon access (Snowflake SQL, same users)", sf_matrix)
    bad += [f"snowflake {m}" for m in mismatches(sf_matrix)]
    s.sql(f"REFRESH TABLE {table_ref('horizon_ro')}")  # Snowflake's probe commits since Spark last looked

    identity = table_identity(RestCatalog.horizon("rw").load_table(n.schema, n.table))
    spark_fp = normalize_fingerprint(s.sql(fingerprint_sql(table_ref("horizon_ro"))).collect()[0])

    conn = snowflake_admin()
    conn.cursor().execute(f"USE WAREHOUSE {n.warehouse}")
    row = query_dicts(conn, fingerprint_sql(n.fqn, quote='"'))[0]
    snowflake_fp = normalize_fingerprint([row[c] for c in FINGERPRINT_COLUMNS])
    spark_writers = writer_summary(s.sql(writer_summary_sql(table_ref("horizon_ro"))).collect())
    sf_writers = writer_summary(conn.cursor().execute(writer_summary_sql(n.fqn, quote='"')).fetchall())
    print_writer_summary("Rows by writer (Spark and Snowflake agree)" if spark_writers == sf_writers
                         else "Rows by writer, Spark's view (Snowflake disagrees)", spark_writers)
    if spark_writers != sf_writers:
        bad.append(f"writer summaries differ: Spark {spark_writers} vs Snowflake {sf_writers}")

    baseline = {"identity": identity, "fingerprint": spark_fp, "snowflake_fingerprint": snowflake_fp, "horizon_matrix": matrix,
                "horizon_snowflake_matrix": sf_matrix, "writers": spark_writers}
    write_state("baseline.json", baseline)
    print(f"\nbaseline: uuid {identity['table_uuid']}, {len(identity['snapshot_ids'])} snapshots, {spark_fp['row_count']} rows")
    if spark_fp != snowflake_fp:
        bad.append(f"Spark and Snowflake fingerprints differ: {spark_fp} vs {snowflake_fp}")
    if bad:
        raise SystemExit("step 3 FAILED:\n  " + "\n  ".join(bad))
    print("step 3 complete")


if __name__ == "__main__":
    main()
