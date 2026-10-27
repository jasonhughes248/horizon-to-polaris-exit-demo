"""Re-check, days later, that every file Polaris references still exists.

Snowflake purges files of a dropped table after its retention period. The deny
in step 5 should make that impossible; run this after DATA_RETENTION_TIME_IN_DAYS
(1 day here) plus margin.  Usage: python -m tests.recheck_files
"""

from migration.step6_polaris_demo import missing_files, referenced_files
from spark.sessions import add_polaris_catalogs, spark


def main() -> None:
    s = spark()
    add_polaris_catalogs()
    files = referenced_files(s, "polaris_ro")
    missing = missing_files(files)
    print(f"{len(files)} referenced files, {len(missing)} missing")
    if missing:
        raise SystemExit("\n".join(missing[:20]))


if __name__ == "__main__":
    main()
