"""Step 6: same three-user demo on Polaris, plus proof every file survived.

  - same table: uuid, format and location equal the baseline; every baseline
    snapshot is retained and the baseline head is an ancestor of the current
    head (the write probe below adds snapshots, so reruns stay valid)
  - data at the baseline snapshot and at the current head both equal the
    baseline fingerprint (write probes are net-zero)
  - access matrix matches the Horizon matrix
  - every file referenced by any snapshot exists in S3 (HEAD on each object)
  - HORIZON_DEMO_RW can still commit a write through Polaris

Usage: python -m migration.step6_polaris_demo
"""

from __future__ import annotations

from urllib.parse import urlparse

from migration.common import RestCatalog, aws_session, fingerprint_sql, names, normalize_fingerprint, read_state, table_identity, write_state
from spark.sessions import add_polaris_catalogs, spark, table_ref
from tests.access_matrix import mismatches, print_matrix, run_matrix


def referenced_files(s, catalog: str) -> set[str]:
    t = table_ref(catalog)
    files = {r[0] for r in s.sql(f"SELECT file_path FROM {t}.all_files").collect()}
    files |= {r[0] for r in s.sql(f"SELECT path FROM {t}.all_manifests").collect()}
    files |= {r[0] for r in s.sql(f"SELECT manifest_list FROM {t}.snapshots").collect()}
    files |= {r[0] for r in s.sql(f"SELECT file FROM {t}.metadata_log_entries").collect()}
    return files


def missing_files(paths: set[str]) -> list[str]:
    s3 = aws_session().client("s3")  # local AWS identity, not Snowflake's or Polaris's
    missing = []
    for p in sorted(paths):
        u = urlparse(p)
        try:
            s3.head_object(Bucket=u.netloc, Key=u.path.lstrip("/"))
        except s3.exceptions.ClientError:
            missing.append(p)
    return missing


def identity_problems(md: dict, baseline_identity: dict) -> list[str]:
    """Same table as the baseline, possibly with later commits on top."""
    ident = table_identity({"metadata": md})
    bad = [f"{k}: {baseline_identity[k]} -> {ident[k]}" for k in ("table_uuid", "format_version", "location")
           if ident[k] != baseline_identity[k]]
    missing = set(baseline_identity["snapshot_ids"]) - set(ident["snapshot_ids"])
    if missing:
        bad.append(f"baseline snapshots missing: {sorted(missing)}")
    by_id = {s["snapshot-id"]: s for s in md.get("snapshots", [])}
    head, cur, seen = baseline_identity["current_snapshot_id"], md.get("current-snapshot-id"), set()
    while cur is not None and cur != head and cur not in seen:
        seen.add(cur)
        cur = by_id.get(cur, {}).get("parent-snapshot-id")
    if cur != head:
        bad.append(f"baseline head {head} is not an ancestor of current snapshot {md.get('current-snapshot-id')}")
    return bad


def main() -> None:
    n = names()
    baseline = read_state("baseline.json")
    s = spark()
    add_polaris_catalogs()
    bad = []

    loaded = RestCatalog.polaris("ro").load_table(n.schema, n.table)
    identity = table_identity(loaded)
    bad += identity_problems(loaded["metadata"], baseline["identity"])
    fp = normalize_fingerprint(s.sql(fingerprint_sql(table_ref("polaris_ro"))).collect()[0])
    if fp != baseline["fingerprint"]:
        bad.append(f"fingerprint at head: {baseline['fingerprint']} -> {fp}")
    head = baseline["identity"]["current_snapshot_id"]
    fp_base = normalize_fingerprint(
        s.sql(fingerprint_sql(f"{table_ref('polaris_ro')} VERSION AS OF {head}")).collect()[0])
    if fp_base != baseline["fingerprint"]:
        bad.append(f"fingerprint at baseline snapshot {head}: {baseline['fingerprint']} -> {fp_base}")

    # Time travel to the oldest snapshot (step 2's first append) still works: history came along.
    first = s.sql(f"SELECT snapshot_id FROM {table_ref('polaris_ro')}.snapshots ORDER BY committed_at LIMIT 1").collect()[0][0]
    first_count = s.sql(f"SELECT COUNT(*) FROM {table_ref('polaris_ro')} VERSION AS OF {first}").collect()[0][0]
    if first_count != 1000:
        bad.append(f"oldest snapshot {first} has {first_count} rows, expected step 2's first 1000-row append")

    files = referenced_files(s, "polaris_ro")
    missing = missing_files(files)
    if missing:
        bad.append(f"{len(missing)} of {len(files)} referenced files missing, e.g. {missing[:3]}")

    matrix = run_matrix(s, "polaris")
    print_matrix("Polaris access (Spark, same users after migration)", matrix)
    bad += mismatches(matrix)
    fp_after = normalize_fingerprint(s.sql(fingerprint_sql(table_ref("polaris_ro"))).collect()[0])
    if fp_after != baseline["fingerprint"]:
        bad.append(f"fingerprint after write probe (must be net-zero): {fp_after}")

    # The write probe commits through Polaris; remember the catalog's head for recovery.
    head = RestCatalog.polaris("ro").load_table(n.schema, n.table).get("metadata-location")
    if head:
        write_state("last_committed.json", {"metadata_location": head})
    write_state("step6_polaris_demo.json", {
        "identity": identity, "fingerprint": fp, "first_snapshot_rows": first_count,
        "referenced_files": len(files), "missing_files": missing, "polaris_matrix": matrix,
    })
    if bad:
        raise SystemExit("step 6 FAILED:\n  " + "\n  ".join(bad))
    print(f"step 6 complete: {len(files)} files present, history intact (first snapshot {first_count} rows)")


if __name__ == "__main__":
    main()
