"""Step 3: show RBAC on Horizon with the three users, then capture the baseline.

Order matters: the matrix's write probe adds snapshots, so the baseline is taken
after it. Usage: python -m migration.step3_horizon_demo
"""

from __future__ import annotations

from migration.common import fingerprint_sql, names, normalize_fingerprint, query_dicts, snowflake_admin, table_identity, write_state, RestCatalog, FINGERPRINT_COLUMNS
from spark.sessions import spark, table_ref
from tests.access_matrix import mismatches, print_matrix, run_matrix


def main() -> None:
    s = spark()
    n = names()

    matrix = run_matrix(s, "horizon")
    print_matrix("Horizon access (Spark, one identity per user)", matrix)
    bad = mismatches(matrix)

    identity = table_identity(RestCatalog.horizon("rw").load_table(n.schema, n.table))
    spark_fp = normalize_fingerprint(s.sql(fingerprint_sql(table_ref("horizon_ro"))).collect()[0])

    conn = snowflake_admin()
    conn.cursor().execute(f"USE WAREHOUSE {n.warehouse}")
    row = query_dicts(conn, fingerprint_sql(n.fqn, quote='"'))[0]
    snowflake_fp = normalize_fingerprint([row[c] for c in FINGERPRINT_COLUMNS])

    baseline = {"identity": identity, "fingerprint": spark_fp, "snowflake_fingerprint": snowflake_fp, "horizon_matrix": matrix}
    write_state("baseline.json", baseline)
    print(f"\nbaseline: uuid {identity['table_uuid']}, {len(identity['snapshot_ids'])} snapshots, {spark_fp['row_count']} rows")
    if spark_fp != snowflake_fp:
        bad.append(f"Spark and Snowflake fingerprints differ: {spark_fp} vs {snowflake_fp}")
    if bad:
        raise SystemExit("step 3 FAILED:\n  " + "\n  ".join(bad))
    print("step 3 complete")


if __name__ == "__main__":
    main()
