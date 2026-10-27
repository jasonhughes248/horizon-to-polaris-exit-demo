#!/usr/bin/env bash
# teardown.sh: remove everything this demo created.
#
#   ./cloud/teardown.sh            ECS service, cluster, task defs, SG, root secret, Polaris IAM roles
#   ./cloud/teardown.sh --all      also Snowflake objects, Snowflake's IAM role, and the bucket
#                                  (asks before deleting the bucket's contents)
#
# Only touches resources named horizon-to-polaris-exit* or HORIZON_EXIT_DEMO_* / DEMO_*.

set -euo pipefail
log() { echo "[$(date '+%H:%M:%S')] $*"; }

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
set -a
# shellcheck disable=SC1090
_ENV_TMP=$(mktemp)  # bash 3.2 (macOS) can't `source <(...)`
grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | sed 's/[[:space:]]#.*$//' > "$_ENV_TMP"
source "$_ENV_TMP"; rm -f "$_ENV_TMP"
set +a
[[ -z "${AWS_PROFILE:-}" ]] && unset AWS_PROFILE
AWS_REGION="${AWS_REGION:-us-west-2}"
NAME="horizon-to-polaris-exit"

log "Deleting ECS service and cluster"
if aws ecs describe-services --cluster "$NAME" --services polaris --region "$AWS_REGION" \
    --query "services[?status=='ACTIVE'].serviceArn" --output text 2>/dev/null | grep -q .; then
  aws ecs update-service --cluster "$NAME" --service polaris --desired-count 0 --region "$AWS_REGION" >/dev/null
  aws ecs delete-service --cluster "$NAME" --service polaris --force --region "$AWS_REGION" >/dev/null
  aws ecs wait services-inactive --cluster "$NAME" --services polaris --region "$AWS_REGION"
fi
aws ecs delete-cluster --cluster "$NAME" --region "$AWS_REGION" >/dev/null 2>&1 || true

# shellcheck source=cloud/purge-task-defs.sh
source "$ROOT_DIR/cloud/purge-task-defs.sh"
TDS=$(for ST in ACTIVE INACTIVE; do
  aws ecs list-task-definitions --family-prefix "$NAME-polaris" --status "$ST" --region "$AWS_REGION" \
    --query "taskDefinitionArns[]" --output text
done | tr '\t' '\n' | grep -v '^None$' | grep . || true)
purge_task_defs $TDS

SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$NAME-polaris-sg" \
  --query "SecurityGroups[0].GroupId" --output text --region "$AWS_REGION")
if [[ -n "$SG_ID" && "$SG_ID" != "None" ]]; then
  for _ in $(seq 1 12); do
    aws ec2 delete-security-group --group-id "$SG_ID" --region "$AWS_REGION" 2>/dev/null && break
    sleep 10  # ENI release lags service deletion
  done
fi

if aws secretsmanager describe-secret --secret-id "$NAME/polaris-root" --region "$AWS_REGION" >/dev/null 2>&1; then
  aws secretsmanager delete-secret --secret-id "$NAME/polaris-root" --force-delete-without-recovery \
    --region "$AWS_REGION" >/dev/null
  log "Deleted secret $NAME/polaris-root"
fi

delete_role() {
  aws iam get-role --role-name "$1" >/dev/null 2>&1 || return 0
  for P in $(aws iam list-role-policies --role-name "$1" --query "PolicyNames[]" --output text); do
    aws iam delete-role-policy --role-name "$1" --policy-name "$P"
  done
  for A in $(aws iam list-attached-role-policies --role-name "$1" --query "AttachedPolicies[].PolicyArn" --output text); do
    aws iam detach-role-policy --role-name "$1" --policy-arn "$A"
  done
  aws iam delete-role --role-name "$1"
  log "Deleted IAM role $1"
}
delete_role "$NAME-polaris-task"
delete_role "$NAME-polaris-exec"

if [[ "${1:-}" == "--all" ]]; then
  log "Dropping Snowflake demo objects"
  (cd "$ROOT_DIR" && python3 -m setup.teardown_snowflake)
  delete_role "${SNOWFLAKE_EXTVOL_ROLE_NAME:-horizon-to-polaris-exit-snowflake}"
  read -r -p "Delete s3://${S3_BUCKET} and ALL its contents? [y/N] " ANSWER
  if [[ "$ANSWER" == "y" ]]; then
    aws s3 rb "s3://${S3_BUCKET}" --force --region "$AWS_REGION"
  fi
fi
log "Teardown complete"
