#!/usr/bin/env bash
# deploy-polaris.sh: run Apache Polaris on ECS Fargate behind a Caddy HTTPS
# sidecar (Let's Encrypt cert for <public-ip>.sslip.io).
#
# Persistence is IN-MEMORY. If the task restarts, every catalog, principal, and
# grant is gone (the table files in S3 are not). Fine for a one-session demo,
# not for anything else. Polaris supports a relational JDBC metastore; see
# https://polaris.apache.org/releases/1.8.0/metastores/
#
# Creates only infrastructure. Catalog, namespace, principals, and grants are
# created by migration/step4_register.py from Snowflake's metadata.
#
# Credentials: AWS CLI default chain. Nothing AWS-related is written to .env.
# Writes POLARIS_ROOT_CREDENTIAL (generated if empty), CATALOG_URI, and the IAM
# role ARNs into .env.
#
# Usage: ./cloud/deploy-polaris.sh [--rotate-root]
#
# --rotate-root issues a new root credential (.env + Secrets Manager). Rotation also
# happens automatically if any task definition revision still holds
# POLARIS_BOOTSTRAP_CREDENTIALS in plaintext env. Old revisions are deregistered and
# permanently deleted (DeleteTaskDefinitions), since INACTIVE ones stay describable.

set -euo pipefail
ROTATE_ROOT=0
for ARG in "$@"; do
  case "$ARG" in
    --rotate-root) ROTATE_ROOT=1 ;;
    *) echo "Usage: $0 [--rotate-root]"; exit 2 ;;
  esac
done
log() { echo "[$(date '+%H:%M:%S')] $*"; }

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
[[ -f "$ENV_FILE" ]] || { echo "Error: .env not found (cp .env.example .env)"; exit 1; }

set_env() {  # set_env KEY VALUE: replace or append in .env; the value goes via env, not argv
  _SET_ENV_VALUE="$2" python3 - "$ENV_FILE" "$1" <<'PY'
import os, sys, re, pathlib
path, key, value = pathlib.Path(sys.argv[1]), sys.argv[2], os.environ["_SET_ENV_VALUE"]
text = path.read_text()
line = f"{key}={value}"
text, n = re.subn(rf"^{re.escape(key)}=.*$", lambda _: line, text, flags=re.M)
if n == 0:
    text += ("" if text.endswith("\n") else "\n") + line + "\n"
path.write_text(text)
PY
}

set -a
# shellcheck disable=SC1090
_ENV_TMP=$(mktemp)  # bash 3.2 (macOS) can't `source <(...)`
grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$' | sed 's/[[:space:]]#.*$//' > "$_ENV_TMP"
source "$_ENV_TMP"; rm -f "$_ENV_TMP"
set +a
[[ -z "${AWS_PROFILE:-}" ]] && unset AWS_PROFILE

: "${S3_BUCKET:?S3_BUCKET is required in .env}"
AWS_REGION="${AWS_REGION:-us-west-2}"
S3_PREFIX="${S3_PREFIX:-horizon-to-polaris-exit}"
POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris:1.8.0}"
SNOWFLAKE_EXTVOL_ROLE_NAME="${SNOWFLAKE_EXTVOL_ROLE_NAME:-horizon-to-polaris-exit-snowflake}"

NAME="horizon-to-polaris-exit"
CLUSTER_NAME="$NAME"
SERVICE_NAME="polaris"
TASK_FAMILY="$NAME-polaris"
TASK_ROLE="$NAME-polaris-task"
EXEC_ROLE="$NAME-polaris-exec"
SG_NAME="$NAME-polaris-sg"
TAGS="key=project,value=$NAME"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

list_task_defs() {  # every revision still describable (ACTIVE and INACTIVE)
  for ST in ACTIVE INACTIVE; do
    aws ecs list-task-definitions --family-prefix "$TASK_FAMILY" --status "$ST" --region "$AWS_REGION" \
      --query "taskDefinitionArns[]" --output text
  done | tr '\t' '\n' | grep -v '^None$' | grep . || true
}

# A revision with the root credential in plaintext env leaks it to anyone with
# ecs:DescribeTaskDefinition, so that credential must be considered burned.
for TD in $(list_task_defs); do
  if aws ecs describe-task-definition --task-definition "$TD" --region "$AWS_REGION" \
      --query "taskDefinition.containerDefinitions[].environment[?name=='POLARIS_BOOTSTRAP_CREDENTIALS'].name" \
      --output text | grep -q .; then
    log "Revision ${TD##*/} has the root credential in plaintext env: rotating"
    ROTATE_ROOT=1
  fi
done

if [[ -z "${POLARIS_ROOT_CREDENTIAL:-}" || "$ROTATE_ROOT" == 1 ]]; then
  POLARIS_ROOT_CREDENTIAL="root:$(openssl rand -hex 24)"
  set_env POLARIS_ROOT_CREDENTIAL "$POLARIS_ROOT_CREDENTIAL"
  log "Generated new POLARIS_ROOT_CREDENTIAL (stored in .env and Secrets Manager)"
fi
ROOT_ID="${POLARIS_ROOT_CREDENTIAL%%:*}"
ROOT_SECRET="${POLARIS_ROOT_CREDENTIAL#*:}"
log "=== Deploying $POLARIS_IMAGE to ECS (account $ACCOUNT_ID, $AWS_REGION) ==="

if [[ "$TASK_ROLE" == "$SNOWFLAKE_EXTVOL_ROLE_NAME" ]]; then
  echo "Error: Polaris task role must differ from Snowflake's external volume role"; exit 1
fi

# ── Network ──────────────────────────────────────────────────────────────────
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
  --query "Vpcs[0].VpcId" --output text --region "$AWS_REGION")
[[ -n "$VPC_ID" && "$VPC_ID" != "None" ]] || { echo "Error: no default VPC in $AWS_REGION"; exit 1; }
SUBNET_IDS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" "Name=map-public-ip-on-launch,Values=true" \
  --query "Subnets[*].SubnetId" --output text --region "$AWS_REGION" | tr '\t' ',')

SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC_ID" \
  --query "SecurityGroups[0].GroupId" --output text --region "$AWS_REGION")
if [[ -z "$SG_ID" || "$SG_ID" == "None" ]]; then
  SG_ID=$(aws ec2 create-security-group --group-name "$SG_NAME" --vpc-id "$VPC_ID" \
    --description "Polaris for horizon-to-polaris-exit demo" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=$NAME}]" \
    --query GroupId --output text --region "$AWS_REGION")
  log "Created security group $SG_ID"
fi
# 80: ACME HTTP-01 challenge. 443: HTTPS to Polaris via Caddy. Nothing else.
# 8182 (Quarkus management: health AND Prometheus metrics, which can't be turned
# off at runtime) and 8181 (plain HTTP) are never opened; any earlier 8182 rule
# is revoked. Readiness is checked through HTTPS instead.
for PORT in 80 443; do
  if ! aws ec2 describe-security-groups --group-ids "$SG_ID" --region "$AWS_REGION" \
      --query "SecurityGroups[0].IpPermissions[?FromPort==\`$PORT\`]" --output text | grep -q .; then
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port "$PORT" \
      --cidr 0.0.0.0/0 --region "$AWS_REGION" >/dev/null
  fi
done
for CIDR in $(aws ec2 describe-security-groups --group-ids "$SG_ID" --region "$AWS_REGION" \
    --query "SecurityGroups[0].IpPermissions[?FromPort==\`8182\`].IpRanges[].CidrIp" --output text); do
  aws ec2 revoke-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 8182 \
    --cidr "$CIDR" --region "$AWS_REGION" >/dev/null
  log "Revoked 8182 ingress from $CIDR"
done

# ── IAM ──────────────────────────────────────────────────────────────────────
ECS_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

ensure_role() {  # ensure_role NAME TRUST_JSON
  if ! aws iam get-role --role-name "$1" >/dev/null 2>&1; then
    aws iam create-role --role-name "$1" --assume-role-policy-document "$2" --tags "Key=project,Value=$NAME" >/dev/null
    log "Created IAM role $1"
    sleep 10
  fi
}

ensure_role "$EXEC_ROLE" "$ECS_TRUST"
aws iam attach-role-policy --role-name "$EXEC_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
ECS_TASK_EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EXEC_ROLE}"

# Root credential lives in Secrets Manager; the task definition only references it.
# The value goes through a 0600 temp file (file://), never a command-line argument.
SECRET_NAME="$NAME/polaris-root"
_SECRET_TMP=$(mktemp)
chmod 600 "$_SECRET_TMP"
printf '%s' "POLARIS,${ROOT_ID},${ROOT_SECRET}" > "$_SECRET_TMP"
if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  aws secretsmanager put-secret-value --secret-id "$SECRET_NAME" --secret-string "file://$_SECRET_TMP" \
    --region "$AWS_REGION" >/dev/null
else
  aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string "file://$_SECRET_TMP" \
    --tags "Key=project,Value=$NAME" --region "$AWS_REGION" >/dev/null
  log "Created secret $SECRET_NAME"
fi
rm -f "$_SECRET_TMP"
ROOT_SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" \
  --query ARN --output text)
aws iam put-role-policy --role-name "$EXEC_ROLE" --policy-name read-polaris-root-secret \
  --policy-document "$(python3 "$ROOT_DIR/cloud/render.py" secret-read-policy "$ROOT_SECRET_ARN")"

# Task role: ECS trust + self-assume, so Polaris can vend down-scoped STS
# credentials for this role to Spark and Snowflake.
ensure_role "$TASK_ROLE" "$ECS_TRUST"
ECS_TASK_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${TASK_ROLE}"
RENDER="python3 $ROOT_DIR/cloud/render.py"
aws iam update-assume-role-policy --role-name "$TASK_ROLE" \
  --policy-document "$($RENDER task-trust "$ECS_TASK_ROLE_ARN")"
aws iam put-role-policy --role-name "$TASK_ROLE" --policy-name s3-access \
  --policy-document "$($RENDER s3-policy "$S3_BUCKET" "$S3_PREFIX")"
set_env ECS_TASK_ROLE_ARN "$ECS_TASK_ROLE_ARN"
set_env ECS_TASK_EXECUTION_ROLE_ARN "$ECS_TASK_EXECUTION_ROLE_ARN"

# ── ECS ──────────────────────────────────────────────────────────────────────
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com >/dev/null 2>&1 || true
if ! aws ecs describe-clusters --clusters "$CLUSTER_NAME" --region "$AWS_REGION" \
    --query "clusters[?status=='ACTIVE'].clusterName" --output text | grep -q .; then
  aws ecs create-cluster --cluster-name "$CLUSTER_NAME" --tags "$TAGS" --region "$AWS_REGION" >/dev/null
  log "Created ECS cluster $CLUSTER_NAME"
fi

CONTAINER_DEF=$($RENDER containers "$POLARIS_IMAGE" "$AWS_REGION" "$ROOT_SECRET_ARN")

# Every existing revision is removed after the new one is live: deregister, then
# permanently delete (INACTIVE revisions are otherwise still describable).
OLD_TDS=$(list_task_defs)
aws ecs register-task-definition --family "$TASK_FAMILY" --network-mode awsvpc \
  --requires-compatibilities FARGATE --cpu 512 --memory 1024 \
  --execution-role-arn "$ECS_TASK_EXECUTION_ROLE_ARN" --task-role-arn "$ECS_TASK_ROLE_ARN" \
  --container-definitions "$CONTAINER_DEF" --tags "$TAGS" --region "$AWS_REGION" >/dev/null

NETWORK_CONFIG="awsvpcConfiguration={subnets=[$SUBNET_IDS],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"
if aws ecs describe-services --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" --region "$AWS_REGION" \
    --query "services[?status=='ACTIVE'].serviceArn" --output text | grep -q .; then
  log "Updating ECS service (in-memory state will be reset)"
  aws ecs update-service --cluster "$CLUSTER_NAME" --service "$SERVICE_NAME" \
    --task-definition "$TASK_FAMILY" --force-new-deployment --region "$AWS_REGION" >/dev/null
else
  aws ecs create-service --cluster "$CLUSTER_NAME" --service-name "$SERVICE_NAME" \
    --task-definition "$TASK_FAMILY" --desired-count 1 --launch-type FARGATE \
    --network-configuration "$NETWORK_CONFIG" --tags "$TAGS" --region "$AWS_REGION" >/dev/null
fi

log "Waiting for the service to stabilize (~1-2 min)"
aws ecs wait services-stable --cluster "$CLUSTER_NAME" --services "$SERVICE_NAME" --region "$AWS_REGION"
# shellcheck source=cloud/purge-task-defs.sh
source "$ROOT_DIR/cloud/purge-task-defs.sh"
purge_task_defs $OLD_TDS

TASK_ARN=$(aws ecs list-tasks --cluster "$CLUSTER_NAME" --service-name "$SERVICE_NAME" \
  --desired-status RUNNING --query "taskArns[0]" --output text --region "$AWS_REGION")
ENI_ID=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$TASK_ARN" --region "$AWS_REGION" \
  --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value" --output text)
PUBLIC_IP=$(aws ec2 describe-network-interfaces --network-interface-ids "$ENI_ID" --region "$AWS_REGION" \
  --query "NetworkInterfaces[0].Association.PublicIp" --output text)
DOMAIN="$(echo "$PUBLIC_IP" | tr '.' '-').sslip.io"
CATALOG_URI="https://${DOMAIN}/api/catalog"

log "Waiting for Caddy TLS and Polaris on $DOMAIN"
for _ in $(seq 1 60); do curl -s -o /dev/null "https://${DOMAIN}/api/catalog/v1/config" && break; sleep 5; done
# The secret is read by curl from a 0600 file (client_secret@file), never put in argv.
_TOKEN_TMP=$(mktemp)
trap 'rm -f "$_TOKEN_TMP"' EXIT
chmod 600 "$_TOKEN_TMP"
printf '%s' "$ROOT_SECRET" > "$_TOKEN_TMP"
TOKEN_STATUS=000
for _ in $(seq 1 24); do
  TOKEN_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${CATALOG_URI}/v1/oauth/tokens" \
    --data-urlencode grant_type=client_credentials --data-urlencode "client_id=${ROOT_ID}" \
    --data-urlencode "client_secret@${_TOKEN_TMP}" --data-urlencode scope=PRINCIPAL_ROLE:ALL)
  [[ "$TOKEN_STATUS" == "200" ]] && break
  sleep 5
done
rm -f "$_TOKEN_TMP"
[[ "$TOKEN_STATUS" == "200" ]] || { echo "Error: root token request returned $TOKEN_STATUS"; exit 1; }

VERSION=$(aws ecs describe-task-definition --task-definition "$TASK_FAMILY" --region "$AWS_REGION" \
  --query "taskDefinition.containerDefinitions[?name=='polaris'].image" --output text)
set_env CATALOG_URI "$CATALOG_URI"
log "=== Polaris ($VERSION) ready at $CATALOG_URI (written to .env) ==="
