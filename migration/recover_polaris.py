"""Rebuild Polaris after its in-memory state was lost (task restart or redeploy).

Step 4 cannot be rerun after step 5: the table is gone from Snowflake. Everything
Polaris needs is still recoverable without Snowflake:

  - the table: newest metadata.json in S3 under the table location (it includes
    commits made through Polaris after cutover), checked to be the same table
    (UUID) and a descendant of the baseline head; falls back to the location
    recorded in state/step4_register.json
  - grants: the Snowflake grants recorded by step 4, re-mapped with the current
    mapping (migration/grants.py)
  - principals: saved credentials are reused if they still authenticate; a
    restarted Polaris has no principals, so new credentials are issued and step 7
    is rerun so Snowflake's catalog integration gets HORIZON_DEMO_RW's new secret

Usage: python -m migration.recover_polaris [--force] [--skip-step7]
"""

from __future__ import annotations

import argparse
import json
import sys
from urllib.parse import urlparse

from migration.common import aws_session, has_state, names, read_state, write_state
from migration.step4_register import apply_polaris, recorded_grants


def _metadata_seq(key: str) -> int:
    """00012-<uuid>.metadata.json -> 12 (-1 if the name has no numeric prefix)."""
    head = key.rsplit("/", 1)[-1].split("-", 1)[0]
    return int(head) if head.isdigit() else -1


def descends_from(md: dict, ancestor: int) -> bool:
    by_id = {s["snapshot-id"]: s for s in md.get("snapshots", [])}
    cur = md.get("current-snapshot-id")
    seen = set()
    while cur is not None and cur not in seen:
        if cur == ancestor:
            return True
        seen.add(cur)
        cur = by_id.get(cur, {}).get("parent-snapshot-id")
    return False


def _norm(uri: str) -> str:
    """Normalize an S3 URI for prefix comparisons (s3a/s3n -> s3, no trailing /)."""
    for scheme in ("s3a://", "s3n://"):
        if uri.startswith(scheme):
            uri = "s3://" + uri[len(scheme):]
    return uri.rstrip("/")


def _current_schema(md: dict) -> dict | None:
    sid = md.get("current-schema-id")
    for sc in md.get("schemas", []):
        if sc.get("schema-id") == sid:
            return {"schema-id": sid, "fields": sc.get("fields", [])}
    return None


def _structure_problem(md: dict, uri: str, table_loc: str) -> str | None:
    """Reasons a metadata file cannot be this table's metadata at all."""
    if _norm(md.get("location", "")) != table_loc:
        return f"location {md.get('location')} != {table_loc}"
    if not _norm(uri).startswith(table_loc + "/"):
        return "metadata file outside table location"
    for s in md.get("snapshots", []):
        ml = s.get("manifest-list")
        if not ml or not _norm(ml).startswith(table_loc + "/"):
            return f"snapshot {s.get('snapshot-id')} manifest-list {ml} outside table location"
    if _current_schema(md) is None:
        return "current schema missing"
    return None


def newest_metadata_location(baseline_identity: dict, fallback: str, anchor: str | None = None) -> str:
    """Newest metadata.json that is a validated descendant of the last committed metadata.

    The anchor is the last metadata location known to be catalog-committed
    (state/last_committed.json, else step 4's registration). Without an anchor the
    oldest structurally valid file containing the baseline head is used. A newer file
    is accepted only if, BEFORE anything is registered:
      - its `location` equals the baseline location, and the file and every
        snapshot manifest-list are under that location
      - every baseline snapshot id is present and the baseline head and the previous
        accepted head are ancestors of its current snapshot
      - its current schema equals the anchor's (schema evolution needs --anchor reset)
      - its metadata-log contains the current chain tip (strict continuity; a
        sibling that cites an earlier file is skipped); two valid files with the
        same sequence extending the same tip are a fork and abort recovery
    Files that fail are skipped; planted or divergent files therefore never win.
    """
    table_loc = _norm(baseline_identity["location"])
    loc = urlparse(table_loc)
    prefix = loc.path.lstrip("/") + "/metadata/"
    s3 = aws_session().client("s3")  # local AWS identity
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=loc.netloc, Prefix=prefix):
        objects += [o for o in page.get("Contents", []) if o["Key"].endswith(".metadata.json")]
    objects.sort(key=lambda o: (_metadata_seq(o["Key"]), o["LastModified"]))

    base_ids = set(baseline_identity.get("snapshot_ids") or [])
    base_head = baseline_identity["current_snapshot_id"]
    cands = []
    for o in objects:
        uri = f"s3://{loc.netloc}/{o['Key']}"
        md = json.loads(s3.get_object(Bucket=loc.netloc, Key=o["Key"])["Body"].read())
        if md.get("table-uuid") != baseline_identity["table_uuid"]:
            continue
        problem = _structure_problem(md, uri, table_loc)
        ids = {s["snapshot-id"] for s in md.get("snapshots", [])}
        if not problem and not base_ids <= ids:
            problem = "baseline snapshots missing"
        if not problem and not descends_from(md, base_head):
            problem = "baseline head is not an ancestor"
        if problem:
            print(f"  skip {o['Key']}: {problem}")
            continue
        cands.append((uri, md))

    if anchor is not None:
        idx = next((i for i, (u, _) in enumerate(cands) if _norm(u) == _norm(anchor)), None)
        if idx is None:
            raise SystemExit(f"recover FAILED: last committed metadata {anchor} not found or invalid; "
                             "refusing to pick a replacement")
    else:
        idx = 0 if cands else None
    if idx is None:
        return fallback

    anchor_uri, anchor_md = cands[idx]
    ref_schema = baseline_identity.get("schema") or _current_schema(anchor_md)
    accepted = [(anchor_uri, anchor_md)]

    def extends(tip, md):
        tip_uri, tip_md = tip
        log = {_norm(e.get("metadata-file", "")) for e in md.get("metadata-log", [])}
        if _norm(tip_uri) not in log:
            return f"metadata-log does not contain chain tip {tip_uri}"
        if _current_schema(md) != ref_schema:
            return "current schema differs from committed schema"
        if not descends_from(md, tip_md.get("current-snapshot-id")):
            return f"not a descendant of {tip_uri}"
        return None

    rest = cands[idx + 1:]
    i = 0
    while i < len(rest):
        uri, md = rest[i]
        problem = extends(accepted[-1], md)
        if problem:
            print(f"  skip {uri}: {problem}")
            i += 1
            continue
        # Fork: another valid file at the same sequence also extends this tip.
        seq = _metadata_seq(uri)
        rivals = [u for u, m in rest[i + 1:] if _metadata_seq(u) == seq and extends(accepted[-1], m) is None]
        if rivals:
            raise SystemExit(f"recover FAILED: fork after {accepted[-1][0]}: {uri} and {rivals[0]} "
                             "both extend it; refusing to choose")
        accepted.append((uri, md))
        i += 1
    return accepted[-1][0]


def record_last_committed(metadata_location: str) -> None:
    write_state("last_committed.json", {"metadata_location": metadata_location})


def last_committed(default: str) -> str:
    if has_state("last_committed.json"):
        return read_state("last_committed.json")["metadata_location"]
    return default


def main(argv: list[str] = ()) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="re-point an existing registration to the newest metadata")
    ap.add_argument("--skip-step7", action="store_true", help="do not rerun step 7 when HORIZON_DEMO_RW's secret changed")
    args = ap.parse_args(list(argv))

    n = names()
    baseline = read_state("baseline.json")
    step4 = read_state("step4_register.json")
    metadata_location = newest_metadata_location(baseline["identity"], step4["metadata_location"],
                                                 anchor=last_committed(step4["metadata_location"]))
    print(f"  metadata: {metadata_location}")

    result = apply_polaris(metadata_location, recorded_grants(), force=args.force)
    identity = result["identity"]
    if identity["table_uuid"] != baseline["identity"]["table_uuid"]:
        raise SystemExit(f"recover FAILED: registered uuid {identity['table_uuid']} != baseline")
    missing = set(baseline["identity"]["snapshot_ids"]) - set(identity["snapshot_ids"])
    if missing:
        raise SystemExit(f"recover FAILED: baseline snapshots missing: {sorted(missing)}")

    write_state("step4_register.json", {**step4, "grants": result["mapping"]})
    record_last_committed(metadata_location)
    write_state("recover_polaris.json", {"metadata_location": metadata_location, "identity": identity,
                                         "rotated_principals": result["rotated"]})
    print(f"recover complete: {n.fqn} registered at metadata seq {_metadata_seq(metadata_location)}, "
          f"{len(identity['snapshot_ids'])} snapshots")

    if "HORIZON_DEMO_RW" in result["rotated"]:
        if args.skip_step7:
            print("  NOTE: HORIZON_DEMO_RW has new credentials; rerun step 7: python -m migration.step7_snowflake_via_polaris")
        else:
            print("  HORIZON_DEMO_RW has new credentials; rerunning step 7 to update Snowflake's catalog integration")
            from migration import step7_snowflake_via_polaris

            step7_snowflake_via_polaris.main()


if __name__ == "__main__":
    main(sys.argv[1:])
