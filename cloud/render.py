"""Render JSON documents for deploy-polaris.sh (keeps quoting out of bash 3.2).

    python3 cloud/render.py task-trust <task-role-arn>
    python3 cloud/render.py s3-policy <bucket> <prefix>
    python3 cloud/render.py containers <polaris-image> <aws-region> <root-secret-arn>
    python3 cloud/render.py secret-read-policy <secret-arn>

The Polaris root credential is never rendered into the task definition: ECS
injects it from Secrets Manager (`secrets` / valueFrom) at container start.
"""

import json
import sys


def task_trust(arn: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"},
        {"Effect": "Allow", "Principal": {"AWS": arn}, "Action": ["sts:AssumeRole", "sts:TagSession"]},
    ]}


def s3_policy(bucket: str, prefix: str) -> dict:
    p = prefix.strip("/")
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"],
         "Resource": f"arn:aws:s3:::{bucket}/{p}/*"},
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": f"arn:aws:s3:::{bucket}",
         "Condition": {"StringLike": {"s3:prefix": [f"{p}/*", p]}}},
    ]}


def secret_read_policy(arn: str) -> dict:
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": arn}]}


def containers(image: str, region: str, root_secret_arn: str) -> list:
    caddy = ("IP=$(wget -qO- https://checkip.amazonaws.com | tr . -); "
             'exec caddy reverse-proxy --from "${IP}.sslip.io" --to localhost:8181')
    return [
        {"name": "polaris", "image": image, "essential": True,
         "portMappings": [{"containerPort": 8181}],  # 8182 (health+metrics) stays private
         "secrets": [{"name": "POLARIS_BOOTSTRAP_CREDENTIALS", "valueFrom": root_secret_arn}],
         "environment": [
             {"name": "polaris.realm-context.realms", "value": "POLARIS"},
             {"name": "quarkus.otel.sdk.disabled", "value": "true"},
             {"name": "AWS_REGION", "value": region}]},
        {"name": "caddy", "image": "caddy:2", "essential": True,
         "portMappings": [{"containerPort": 80}, {"containerPort": 443}],
         "command": ["sh", "-c", caddy]},
    ]


if __name__ == "__main__":
    kind, *args = sys.argv[1:]
    print(json.dumps({"task-trust": task_trust, "s3-policy": s3_policy, "containers": containers,
                      "secret-read-policy": secret_read_policy}[kind](*args)))
