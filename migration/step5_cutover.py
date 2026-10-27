"""Step 5: cut Snowflake off from the files without losing them.

  1. Deny Snowflake's IAM role all access to the bucket
  2. Wait until the deny is in effect: SYSTEM$VERIFY_EXTERNAL_VOLUME must fail
  3. Only then DROP ICEBERG TABLE in Snowflake (removes it from Horizon)

Order matters: Snowflake deletes files of a dropped table it considers its own (after the
retention period). Denying first means there is no moment at which the table is dropped
and Snowflake can still reach the files. If the DROP is refused after the deny, nothing is
lost: the table still exists in Snowflake and the step reports how to undo the deny.

Usage: python -m migration.step5_cutover
"""

from __future__ import annotations

import json
import time

from migration.common import aws_session, env, names, run_sql, snowflake_admin, write_state

DENY_POLICY = "horizon-exit-demo-deny"


def deny_document(bucket: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Deny", "Action": "s3:*", "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]}
        ],
    }


def wait_until_denied(conn, volume: str) -> dict:
    """Poll Snowflake's own storage check until it fails. IAM is eventually consistent."""
    verify: dict = {}
    for _ in range(12):
        time.sleep(10)
        verify = json.loads(run_sql(conn, [f"SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('{volume}')"])[0][0][0])
        if not verify.get("success"):
            return verify
    return verify


def main() -> None:
    n = names()
    role_name = env("SNOWFLAKE_EXTVOL_ROLE_NAME", "horizon-to-polaris-exit-snowflake")
    if env("ECS_TASK_ROLE_ARN").rsplit("/", 1)[-1] == role_name:
        raise SystemExit("refusing: Snowflake's role and Polaris's task role are the same")

    iam = aws_session().client("iam")
    iam.put_role_policy(RoleName=role_name, PolicyName=DENY_POLICY, PolicyDocument=json.dumps(deny_document(env("S3_BUCKET"))))
    revoked_at = time.time()
    print(f"  aws> deny policy {DENY_POLICY} on {role_name}")

    conn = snowflake_admin()
    verify = wait_until_denied(conn, n.external_volume)
    if verify.get("success"):
        raise SystemExit(
            "step 5 FAILED: Snowflake can still reach the bucket after the deny; table NOT dropped. "
            "Check the policy on the role, then rerun step 5."
        )
    print("  Snowflake's storage check now fails: deny is in effect")

    try:
        run_sql(conn, [f"DROP ICEBERG TABLE IF EXISTS {n.fqn}"])
    except Exception as exc:  # noqa: BLE001 - report any refusal with recovery instructions
        raise SystemExit(
            f"step 5 FAILED: DROP refused after the deny ({str(exc).splitlines()[0]}). The table still "
            f"exists in Snowflake. To undo the deny: aws iam delete-role-policy --role-name {role_name} "
            f"--policy-name {DENY_POLICY}"
        ) from None
    dropped_at = time.time()

    # Written only after the DROP succeeds: step 4 and recovery treat this file as "cut over".
    write_state("step5_cutover.json", {"revoked_at": revoked_at, "dropped_at": dropped_at, "verify_after_revoke": verify})
    print(f"step 5 complete: Snowflake denied bucket access, then table dropped ({dropped_at - revoked_at:.0f}s later)")


if __name__ == "__main__":
    main()
