"""Step 0: AWS storage, Snowflake external volume, RBAC, three demo users, PATs.

RBAC is created before the table exists, so the table is born governed:

  HORIZON_DEMO_RW_ROLE    USAGE db/schema/ext volume, CREATE ICEBERG TABLE on schema
                  (owns the table it creates -> read + write)
  HORIZON_DEMO_RO_ROLE    USAGE db/schema, FUTURE SELECT on Iceberg tables in schema
  HORIZON_DEMO_NONE_ROLE  nothing on the database or schema

Idempotent: safe to rerun. PATs are rotated on every run and written to
state/pats.json (0600). Usage:  python -m setup.run_setup
"""

from __future__ import annotations

import json
import time

import requests

from migration.common import (
    DEMO_USERS,
    NETWORK_POLICY,
    PAT_NAME,
    aws_session,
    bucket_base,
    env,
    names,
    query_dicts,
    run_sql,
    snowflake_admin,
    write_state,
)

EXTERNAL_ID = "horizon-to-polaris-exit"


def s3_policy(bucket: str, prefix: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject", "s3:DeleteObjectVersion"],
                "Resource": f"arn:aws:s3:::{bucket}/{prefix}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*"]}},
            },
        ],
    }


def trust_policy(principal_arn: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": principal_arn},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": EXTERNAL_ID}},
            }
        ],
    }


def setup_aws() -> str:
    """Create the dedicated bucket and Snowflake's IAM role. Returns the role ARN."""
    sess = aws_session()
    bucket, prefix = env("S3_BUCKET"), env("S3_PREFIX", "horizon-to-polaris-exit").strip("/")
    region = env("AWS_REGION", "us-west-2")
    s3, iam = sess.client("s3"), sess.client("iam")
    account = sess.client("sts").get_caller_identity()["Account"]

    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    if bucket not in existing:
        print(f"  aws> create bucket {bucket}")
        kwargs = {} if region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region}}
        s3.create_bucket(Bucket=bucket, **kwargs)
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={k: True for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")},
        )
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": [{"Key": "project", "Value": "horizon-to-polaris-exit"}]})

    role_name = env("SNOWFLAKE_EXTVOL_ROLE_NAME", "horizon-to-polaris-exit-snowflake")
    try:
        role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        print(f"  aws> create role {role_name} (placeholder trust; fixed after DESC EXTERNAL VOLUME)")
        role_arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy(f"arn:aws:iam::{account}:root")),
            Tags=[{"Key": "project", "Value": "horizon-to-polaris-exit"}],
        )["Role"]["Arn"]
        time.sleep(10)  # IAM propagation before Snowflake validates the role
    iam.put_role_policy(RoleName=role_name, PolicyName="horizon-exit-demo-s3", PolicyDocument=json.dumps(s3_policy(bucket, prefix)))
    # A leftover deny from a previous cutover would break the new run.
    try:
        iam.delete_role_policy(RoleName=role_name, PolicyName="horizon-exit-demo-deny")
        print("  aws> removed leftover horizon-exit-demo-deny policy from a previous run")
    except iam.exceptions.NoSuchEntityException:
        pass
    return role_arn


def setup_external_volume(conn, role_arn: str) -> None:
    n = names()
    run_sql(conn, [
        f"""CREATE EXTERNAL VOLUME IF NOT EXISTS {n.external_volume}
  STORAGE_LOCATIONS = ((
    NAME = 'horizon-exit-demo-s3'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = '{bucket_base()}'
    STORAGE_AWS_ROLE_ARN = '{role_arn}'
    STORAGE_AWS_EXTERNAL_ID = '{EXTERNAL_ID}'
  ))
  ALLOW_WRITES = TRUE""",
    ])
    desc = query_dicts(conn, f"DESC EXTERNAL VOLUME {n.external_volume}")
    loc = next(json.loads(r["property_value"]) for r in desc if r["property"].startswith("STORAGE_LOCATION_1"))
    sf_principal = loc["STORAGE_AWS_IAM_USER_ARN"]

    iam = aws_session().client("iam")
    role_name = role_arn.rsplit("/", 1)[-1]
    print(f"  aws> set trust on {role_name} for Snowflake's IAM user")
    iam.update_assume_role_policy(RoleName=role_name, PolicyDocument=json.dumps(trust_policy(sf_principal)))

    for attempt in range(6):
        result = json.loads(run_sql(conn, [f"SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('{n.external_volume}')"])[0][0][0])
        if result.get("success"):
            print("  external volume verified")
            return
        time.sleep(10)
    raise RuntimeError(f"external volume verification failed: {result}")


def rbac_statements(cidr: str) -> list[str]:
    n = names()
    rw, ro, none = (DEMO_USERS[k]["role"] for k in ("rw", "ro", "none"))
    schema = f"{n.database}.{n.schema}"
    stmts = [
        f"CREATE WAREHOUSE IF NOT EXISTS {n.warehouse} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE",
        "CREATE RESOURCE MONITOR IF NOT EXISTS HORIZON_EXIT_DEMO_RM WITH CREDIT_QUOTA = 5 TRIGGERS ON 100 PERCENT DO SUSPEND",
        f"ALTER WAREHOUSE {n.warehouse} SET RESOURCE_MONITOR = HORIZON_EXIT_DEMO_RM",
        f"CREATE DATABASE IF NOT EXISTS {n.database} CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = {n.external_volume} DATA_RETENTION_TIME_IN_DAYS = 1",
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
    ]
    for role in (rw, ro, none):
        stmts += [f"CREATE ROLE IF NOT EXISTS {role}", f"GRANT ROLE {role} TO ROLE SYSADMIN"]
    stmts += [
        # read + write
        f"GRANT USAGE ON DATABASE {n.database} TO ROLE {rw}",
        f"GRANT USAGE ON SCHEMA {schema} TO ROLE {rw}",
        f"GRANT USAGE ON EXTERNAL VOLUME {n.external_volume} TO ROLE {rw}",
        f"GRANT CREATE ICEBERG TABLE ON SCHEMA {schema} TO ROLE {rw}",
        # read only
        f"GRANT USAGE ON DATABASE {n.database} TO ROLE {ro}",
        f"GRANT USAGE ON SCHEMA {schema} TO ROLE {ro}",
        f"GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA {schema} TO ROLE {ro}",
        # HORIZON_DEMO_NONE_ROLE: intentionally nothing
        f"CREATE NETWORK POLICY IF NOT EXISTS {NETWORK_POLICY} ALLOWED_IP_LIST = ('{cidr}')",
        f"ALTER NETWORK POLICY {NETWORK_POLICY} SET ALLOWED_IP_LIST = ('{cidr}')",
    ]
    for u in DEMO_USERS.values():
        stmts += [
            f"CREATE USER IF NOT EXISTS {u['user']} TYPE = SERVICE DEFAULT_ROLE = {u['role']}",
            f"ALTER USER {u['user']} SET DISABLED = FALSE NETWORK_POLICY = {NETWORK_POLICY}",
            f"GRANT ROLE {u['role']} TO USER {u['user']}",
        ]
    return stmts


def issue_pats(conn) -> dict:
    tokens = {}
    cur = conn.cursor()
    for key, u in DEMO_USERS.items():
        existing = cur.execute(f"SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER {u['user']}").fetchall()
        if any(row[0].upper() == PAT_NAME for row in existing):
            cur.execute(f"ALTER USER {u['user']} REMOVE PROGRAMMATIC ACCESS TOKEN {PAT_NAME}")
        cur.execute(
            f"ALTER USER {u['user']} ADD PROGRAMMATIC ACCESS TOKEN {PAT_NAME} "
            f"ROLE_RESTRICTION = '{u['role']}' DAYS_TO_EXPIRY = 7 COMMENT = 'horizon-to-polaris-exit demo'"
        )
        cols = [c[0].lower() for c in cur.description]
        tokens[key] = dict(zip(cols, cur.fetchone()))["token_secret"]
        print(f"  issued PAT for {u['user']} (role restriction {u['role']})")
    return tokens


def detect_cidr() -> str:
    try:
        return env("ALLOWED_IP_CIDR")
    except RuntimeError:
        ip = requests.get("https://checkip.amazonaws.com", timeout=10).text.strip()
        print(f"  ALLOWED_IP_CIDR not set; using current public IP {ip}/32")
        return f"{ip}/32"


def main() -> None:
    print("AWS: dedicated bucket + Snowflake IAM role")
    role_arn = setup_aws()
    conn = snowflake_admin()
    print("Snowflake: external volume")
    setup_external_volume(conn, role_arn)
    print("Snowflake: warehouse, database, roles, grants, users")
    run_sql(conn, rbac_statements(detect_cidr()))
    print("Snowflake: PATs")
    write_state("pats.json", issue_pats(conn), secret=True)
    write_state("step0_setup.json", {"extvol_role_arn": role_arn, "users": DEMO_USERS, "completed_at": time.time()})
    print("step 0 complete")


if __name__ == "__main__":
    main()
