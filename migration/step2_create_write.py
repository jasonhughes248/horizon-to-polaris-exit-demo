"""Step 2: HORIZON_DEMO_RW creates the v3 table through Horizon IRC and writes history.

HORIZON_DEMO_RW writes from both engines: local PySpark through Horizon IRC (3 appends,
1 update, 1 delete), then Snowflake SQL as the same user (1 insert). Each row's payload
records which engine wrote it. Spark then reads Snowflake's rows back through Horizon.
Usage: python -m migration.step2_create_write
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from migration.common import SNOWFLAKE_HORIZON, SPARK_HORIZON, WRITER_IDS, names, snowflake_as, tagged_rows_sql, write_state, writer_summary, writer_summary_sql, print_writer_summary
from spark.sessions import spark, table_ref, write_once

CATEGORIES = ["click", "view", "purchase", "signup"]
BASE_TS = datetime(2026, 1, 1, 0, 0, 0)


def batch(start: int, count: int) -> list[tuple]:
    """Deterministic rows: same input -> same fingerprint on every run."""
    return [
        (i, BASE_TS + timedelta(minutes=17 * i), CATEGORIES[i % 4], Decimal(i * 37 % 1000) / 10, SPARK_HORIZON)
        for i in range(start, start + count)
    ]


def main() -> None:
    s = spark()
    t = table_ref("horizon_rw")
    s.sql(f"DROP TABLE IF EXISTS {t}")  # rerun from scratch; table and RBAC are recreated identically
    s.sql(
        f"""CREATE TABLE {t} (
              id BIGINT, event_ts TIMESTAMP_NTZ, category STRING, amount DECIMAL(10,2), payload STRING)
            USING iceberg TBLPROPERTIES ('format-version' = '3')"""
    )
    schema = "id BIGINT, event_ts TIMESTAMP_NTZ, category STRING, amount DECIMAL(10,2), payload STRING"
    # Every write is checked against the table before any retry, so none can apply twice.
    count = lambda where: s.sql(f"SELECT COUNT(*) FROM {t} WHERE {where}").collect()[0][0]  # noqa: E731
    for start in (1, 1001, 2001):
        write_once(s, t, lambda start=start: s.createDataFrame(batch(start, 1000), schema).writeTo(t).append(),
                   lambda start=start: count(f"id BETWEEN {start} AND {start + 999}") == 1000, f"append {start}-{start + 999}")
    # Sets each purchase's amount from its id (the original amount + 1), so a repeat is harmless.
    bumped = "CAST((id * 37 % 1000) / 10.0 + 1 AS DECIMAL(10,2))"
    write_once(s, t, lambda: s.sql(f"UPDATE {t} SET amount = {bumped} WHERE category = 'purchase'"),
               lambda: count(f"category = 'purchase' AND amount != {bumped}") == 0, "update purchases")
    write_once(s, t, lambda: s.sql(f"DELETE FROM {t} WHERE id % 10 = 0"),
               lambda: count("id % 10 = 0") == 0, "delete id % 10 = 0")

    # Snowflake writes too, as the same user, to the table it manages through Horizon.
    n = names()
    start, rows = WRITER_IDS[SNOWFLAKE_HORIZON]
    with snowflake_as("rw") as conn:
        cur = conn.cursor()
        sf_count = lambda: cur.execute(  # noqa: E731
            f'SELECT COUNT(*) FROM {n.fqn} WHERE "id" BETWEEN {start} AND {start + rows - 1}').fetchone()[0]
        for attempt in range(1, 5):
            try:
                if sf_count() == 0:
                    cur.execute(tagged_rows_sql(n.fqn, SNOWFLAKE_HORIZON, "snowflake")[1])
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == 4:
                    raise
                print(f"  Snowflake insert: unclear result ({str(exc).splitlines()[0][:80]}); checking before retrying")
    s.sql(f"REFRESH TABLE {t}")
    summary = writer_summary(s.sql(writer_summary_sql(t)).collect())
    print_writer_summary("Rows by writer (read by Spark through Horizon)", summary)

    snapshots = s.sql(f"SELECT snapshot_id, operation FROM {t}.snapshots ORDER BY committed_at").collect()
    props = {r.key: r.value for r in s.sql(f"SHOW TBLPROPERTIES {t}").collect()}
    result = {
        "snapshots": [{"snapshot_id": r.snapshot_id, "operation": r.operation} for r in snapshots],
        "format_version": props.get("format-version"),
        "row_count": s.table(t).count(),
        "writers": summary,
    }
    write_state("step2_create_write.json", result)

    # Only the final table state decides pass/fail.
    # Spark: 3 appends, update, delete of id % 10 = 0 (2700 rows); Snowflake: 1 insert (100 rows)
    expected = {"snapshots": 6, "format_version": "3", "row_count": 2800,
                "writers": {SNOWFLAKE_HORIZON: 100, SPARK_HORIZON: 2700}}
    got = {"snapshots": len(snapshots), "format_version": result["format_version"], "row_count": result["row_count"],
           "writers": summary}
    if got != expected:
        raise SystemExit(f"step 2 FAILED: expected {expected}, got {got}")
    print(f"step 2 complete: {got['snapshots']} snapshots, format-version {got['format_version']}, {got['row_count']} rows "
          f"({summary[SPARK_HORIZON]} from Spark, {summary[SNOWFLAKE_HORIZON]} from Snowflake)")


if __name__ == "__main__":
    main()
