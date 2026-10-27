"""Step 4: freeze the table, register it in Polaris, and recreate the RBAC.

  1. Read every grant on the three demo roles (direct + future)
  2. Freeze: disable HORIZON_DEMO_RW, the only writer, then read the final metadata location
  3. Create the Polaris catalog (= database) and namespace (= schema)
  4. Register the table at Snowflake's metadata file (no data copied)
  5. Users -> principals, roles -> principal roles + catalog roles, grants -> privileges
     Grants on the Polaris side are reconciled exactly: extras are revoked.

After step 5 (table dropped in Snowflake) this step cannot freeze or read the
table again; it refuses and points to `python -m migration.recover_polaris`.
An existing Polaris registration that points at a different metadata file
(commits made through Polaris) is never re-pointed unless --force is given.

Usage: python -m migration.step4_register [--force]
"""

from __future__ import annotations

import argparse
import json
import sys

from migration.common import (
    DEMO_USERS, bucket_base, env, has_state, names, query_dicts, read_state, run_sql, snowflake_admin, table_identity, write_state,
)
from migration.grants import map_role_grants, normalize_future_grants, normalize_show_grants
from migration.polaris_admin import PolarisAdmin


def cut_over() -> bool:
    """Step 5 ran: the table no longer exists in Snowflake and must not be re-read."""
    return has_state("step5_cutover.json")


def recorded_grants() -> dict:
    """Snowflake grants captured by the original step 4 run (before cutover)."""
    return {k: v["snowflake"] for k, v in read_state("step4_register.json")["grants"].items()}


def read_snowflake_grants(conn) -> dict:
    n = names()
    if cut_over() and has_state("step4_register.json"):
        print("  grants: using the Snowflake grants recorded before cutover (state/step4_register.json)")
        return recorded_grants()
    future = normalize_future_grants(query_dicts(conn, f"SHOW FUTURE GRANTS IN SCHEMA {n.database}.{n.schema}"))
    out = {}
    for key, u in DEMO_USERS.items():
        direct = normalize_show_grants(query_dicts(conn, f"SHOW GRANTS TO ROLE {u['role']}"))
        out[key] = direct + [g for g in future if g["grantee"] == u["role"]]
    return out


def freeze_and_locate(conn) -> str:
    n = names()
    if cut_over():
        # HORIZON_DEMO_RW is already disabled and the table is dropped in Snowflake;
        # Snowflake's final metadata location was recorded before cutover.
        loc = (read_state("step4_register.json") if has_state("step4_register.json") else read_state("baseline.json")["identity"])
        print("  freeze: skipped (already cut over); using recorded Snowflake metadata location")
        return loc["metadata_location"]
    run_sql(conn, [f"ALTER USER {DEMO_USERS['rw']['user']} SET DISABLED = TRUE"])
    info = json.loads(run_sql(conn, [f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{n.fqn}')"])[0][0][0])
    if info.get("status") not in (None, "success"):
        raise RuntimeError(f"SYSTEM$GET_ICEBERG_TABLE_INFORMATION: {info}")
    return info["metadataLocation"]


def catalog_body() -> dict:
    n = names()
    return {
        "name": n.database,
        "type": "INTERNAL",
        "readOnly": False,
        "properties": {
            "default-base-location": bucket_base() + "polaris",
            # Snowflake's table paths are not under Polaris's namespace layout.
            "polaris.config.allow.unstructured.table.location": "true",
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [bucket_base()],
            "region": env("AWS_REGION", "us-west-2"),
            "roleArn": env("ECS_TASK_ROLE_ARN"),
        },
    }


def reconcile(admin: PolarisAdmin, catalog: str, role: str, wanted: list[dict]) -> None:
    def k(g):
        return (g["type"], tuple(g.get("namespace", [])), g.get("tableName", ""), g["privilege"])

    have = {k(g): g for g in admin.list_grants(catalog, role)}
    want = {k(g): g for g in wanted}
    for key in want.keys() - have.keys():
        admin.grant(catalog, role, want[key])
    for key in have.keys() - want.keys():
        admin.revoke(catalog, role, have[key])


def apply_polaris(metadata_location: str, sf_grants: dict, force: bool = False) -> dict:
    """Create catalog, namespace, registration, roles, grants and principals.

    Idempotent. Returns {'identity', 'mapping', 'principals', 'rotated'}.
    """
    n = names()
    admin = PolarisAdmin()
    created = admin.create_catalog(catalog_body())
    print(f"  polaris catalog {n.database}: {'created' if created else 'exists'}")
    admin.create_namespace(n.database, n.schema)
    registered = admin.register_table(n.database, n.schema, n.table, metadata_location, force=force)
    if registered.get("conflict"):
        raise SystemExit(
            f"refusing: {n.table} is already registered in Polaris at {registered.get('metadata-location')}, "
            f"not {metadata_location} (commits were made through Polaris). Rerun with --force to re-point it.")
    identity = table_identity(registered)

    mapping, principals, rotated = {}, {}, []
    for key, u in DEMO_USERS.items():
        role = u["role"]
        m = map_role_grants(sf_grants[key], n.database, n.schema, n.table)
        admin.create_principal_role(role)
        admin.create_catalog_role(n.database, role)
        reconcile(admin, n.database, role, m["polaris"])
        admin.assign_catalog_role(role, n.database, role)
        creds = admin.create_principal(u["user"])
        if creds.pop("rotated", False):
            rotated.append(u["user"])
        principals[key] = creds
        admin.assign_principal_role(u["user"], role)
        mapping[key] = {"snowflake": sf_grants[key], **m}
        print(f"  {u['user']}: {len(m['polaris'])} Polaris privileges, {len(m['unmapped'])} Snowflake grants not applicable")
    write_state("polaris_principals.json", principals, secret=True)
    return {"identity": identity, "mapping": mapping, "principals": principals, "rotated": rotated}


def main(argv: list[str] = ()) -> None:  # run_all calls main() with no args
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="re-point an existing Polaris registration")
    args = ap.parse_args(list(argv))
    n = names()
    if cut_over():
        raise SystemExit("step 4 already ran and step 5 dropped the table in Snowflake; "
                         "to rebuild a restarted Polaris run: python -m migration.recover_polaris")
    conn = snowflake_admin()
    sf_grants = read_snowflake_grants(conn)
    metadata_location = freeze_and_locate(conn)
    baseline = read_state("baseline.json")
    if metadata_location != baseline["identity"]["metadata_location"]:
        raise SystemExit(f"table changed after baseline: {metadata_location} != {baseline['identity']['metadata_location']}")

    result = apply_polaris(metadata_location, sf_grants, force=args.force)
    identity = result["identity"]
    write_state("step4_register.json", {"metadata_location": metadata_location, "identity": identity, "grants": result["mapping"]})
    write_state("last_committed.json", {"metadata_location": identity["metadata_location"] or metadata_location})
    diffs = {k: (baseline["identity"][k], identity[k]) for k in identity if identity[k] != baseline["identity"][k]}
    if diffs:
        raise SystemExit(f"step 4 FAILED: registered table differs from baseline: {diffs}")
    print(f"step 4 complete: registered {n.fqn} with uuid {identity['table_uuid']}, {len(identity['snapshot_ids'])} snapshots")


if __name__ == "__main__":
    main(sys.argv[1:])
