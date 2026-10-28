"""One local SparkSession with one Iceberg REST catalog per (catalog server, user).

    horizon_rw / horizon_ro / horizon_none   Snowflake Horizon IRC, PAT auth
    polaris_rw / polaris_ro / polaris_none   Apache Polaris on ECS, client credentials

Each catalog authenticates as a different identity, so switching users in the
demo is just switching the catalog name in SQL. Polaris catalogs are added after
step 4 creates the principals (Spark accepts catalog config at runtime).
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from migration.common import DEMO_USERS, env, has_state, horizon_uri, names, pats, polaris_principals, polaris_uri

_spark: SparkSession | None = None
LOG_CONFIG = Path(__file__).resolve().parent / "log4j2.properties"


def _packages() -> str:
    v = env("ICEBERG_VERSION", "1.10.0")
    return f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{v},org.apache.iceberg:iceberg-aws-bundle:{v}"


def _rest_conf(name: str, uri: str, warehouse: str, credential: str, scope: str) -> dict[str, str]:
    p = f"spark.sql.catalog.{name}"
    return {
        p: "org.apache.iceberg.spark.SparkCatalog",
        f"{p}.type": "rest",
        f"{p}.rest.auth.type": "oauth2",
        f"{p}.uri": uri,
        f"{p}.oauth2-server-uri": f"{uri}/v1/oauth/tokens",
        f"{p}.warehouse": warehouse,
        f"{p}.credential": credential,
        f"{p}.scope": scope,
        f"{p}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"{p}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{p}.client.region": env("AWS_REGION", "us-west-2"),
        # Each check must hit the catalog, not a cached table from a previous user.
        f"{p}.cache-enabled": "false",
    }


def horizon_conf() -> dict[str, str]:
    tokens = pats()
    conf = {}
    for key, u in DEMO_USERS.items():
        conf |= _rest_conf(f"horizon_{key}", horizon_uri(), names().database, tokens[key], f"session:role:{u['role']}")
    return conf


def polaris_conf() -> dict[str, str]:
    creds = polaris_principals()
    conf = {}
    for key in DEMO_USERS:
        c = creds[key]
        conf |= _rest_conf(
            f"polaris_{key}", polaris_uri(), names().database, f"{c['client_id']}:{c['client_secret']}", "PRINCIPAL_ROLE:ALL"
        )
    return conf


def spark() -> SparkSession:
    global _spark
    if _spark is None:
        builder = (
            SparkSession.builder.appName("horizon-to-polaris-exit")
            .master("local[2]")
            .config("spark.jars.packages", _packages())
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.session.timeZone", "UTC")
            # Snowflake writes strings with DELTA_LENGTH_BYTE_ARRAY encoding, which Iceberg's
            # vectorized Parquet reader rejects; the row-based reader handles it.
            .config("spark.sql.iceberg.vectorization.enabled", "false")
            .config("spark.ui.enabled", "false")
            # Hide Spark's routine retry and expected-denial stack traces (see the file).
            .config("spark.driver.extraJavaOptions", f'"-Dlog4j2.configurationFile=file:{LOG_CONFIG}"')
            # local mode only: avoid binding the driver to a LAN/VPN address
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )
        for k, v in horizon_conf().items():
            builder = builder.config(k, v)
        _spark = builder.getOrCreate()
        if has_state("polaris_principals.json"):
            add_polaris_catalogs()
    return _spark


def add_polaris_catalogs() -> None:
    s = _spark or spark()
    for k, v in polaris_conf().items():
        s.conf.set(k, v)


def table_ref(catalog: str) -> str:
    n = names()
    return f"{catalog}.{n.schema}.{n.table}"


# Horizon occasionally answers a commit with a 500 ("Indeterminate result during conflict
# resolution check") or a conflict that Iceberg's own retries don't resolve. A blind retry
# could apply a write twice, so each write comes with a check of whether it already landed.
RETRYABLE = ("CommitStateUnknownException", "CommitFailedException", "Indeterminate result", "concurrently modified")


def write_once(s, table: str, write, landed, label: str, attempts: int = 4) -> None:
    """Run `write()` until `landed()` is true, re-checking the table after any unclear commit."""
    import time

    for attempt in range(1, attempts + 1):
        try:
            write()
            return
        except Exception as exc:  # noqa: BLE001 - Py4J wraps the Java exception
            text = str(exc)
            if not any(m in text for m in RETRYABLE) or attempt == attempts:
                raise
            time.sleep(2 * attempt)
            s.sql(f"REFRESH TABLE {table}")
            if landed():
                print(f"  {label}: the catalog's reply was unclear, but the write had landed")
                return
            print(f"  {label}: the catalog's reply was unclear and the write had not landed; retrying")
