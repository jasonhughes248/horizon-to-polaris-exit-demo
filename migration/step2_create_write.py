"""Step 2: HORIZON_DEMO_RW creates the v3 table through Horizon IRC and writes history.

Every write comes from local PySpark as HORIZON_DEMO_RW. Snowflake never runs DML on the
table. Produces several snapshots: 3 appends, 1 update, 1 delete.
Usage: python -m migration.step2_create_write
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from migration.common import write_state
from spark.sessions import spark, table_ref

CATEGORIES = ["click", "view", "purchase", "signup"]
BASE_TS = datetime(2026, 1, 1, 0, 0, 0)


def batch(start: int, count: int) -> list[tuple]:
    """Deterministic rows: same input -> same fingerprint on every run."""
    return [
        (i, BASE_TS + timedelta(minutes=17 * i), CATEGORIES[i % 4], Decimal(i * 37 % 1000) / 10, f"payload-{i}")
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
    for start in (1, 1001, 2001):
        s.createDataFrame(batch(start, 1000), schema).writeTo(t).append()
    s.sql(f"UPDATE {t} SET amount = amount + 1 WHERE category = 'purchase'")
    s.sql(f"DELETE FROM {t} WHERE id % 10 = 0")

    snapshots = s.sql(f"SELECT snapshot_id, operation FROM {t}.snapshots ORDER BY committed_at").collect()
    props = {r.key: r.value for r in s.sql(f"SHOW TBLPROPERTIES {t}").collect()}
    result = {
        "snapshots": [{"snapshot_id": r.snapshot_id, "operation": r.operation} for r in snapshots],
        "format_version": props.get("format-version"),
        "row_count": s.table(t).count(),
    }
    write_state("step2_create_write.json", result)

    # Iceberg retries a commit that loses a race and logs the conflict as a WARN stack
    # trace; only the final table state decides pass/fail.
    expected = {"snapshots": 5, "format_version": "3", "row_count": 3000 - 300}  # 3 appends, update, delete of id % 10 = 0
    got = {"snapshots": len(snapshots), "format_version": result["format_version"], "row_count": result["row_count"]}
    if got != expected:
        raise SystemExit(f"step 2 FAILED: expected {expected}, got {got}")
    print(f"step 2 complete: {got['snapshots']} snapshots, format-version {got['format_version']}, {got['row_count']} rows")


if __name__ == "__main__":
    main()
