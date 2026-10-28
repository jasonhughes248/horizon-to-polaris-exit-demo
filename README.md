# horizon-to-polaris-exit

Moves a Snowflake-managed Iceberg table and its access controls from Snowflake Horizon Catalog
to self-hosted Apache Polaris, without copying data. Before and after the move, local Spark and
Snowflake both read and write the same table as the same users, and every row records which
engine wrote it.

Work through the sections in order: **Prerequisites → One-time setup → Every new shell → Run
the demo**. Each section assumes the ones before it are done.

## 1. Prerequisites

Check that each command gives the expected result before going further.

| Need | Check | Expected |
|---|---|---|
| Python 3.11 | `python3.11 --version` | `Python 3.11.x` |
| Java 17 (PySpark) | `/opt/homebrew/opt/openjdk@17/bin/java -version` (or your Java 17 path) | `version "17...` |
| AWS CLI v2, logged in | `aws sts get-caller-identity` | your account ID, no error |
| A Snowflake named connection for an admin user who can use ACCOUNTADMIN, in `~/.snowflake/config.toml` or `connections.toml` | `snow connection test -c <name>` (or log in another way) | connects |
| Snowflake account on AWS with Iceberg REST (Horizon) access | — | — |
| AWS permissions to create an S3 bucket, IAM roles, Secrets Manager secrets, and ECS Fargate resources in the default VPC | — | — |

The demo creates billable resources: an S3 bucket, an ECS Fargate task, an XS warehouse (with
a 5-credit resource monitor). Tear them down when finished (see [Cleanup](#6-cleanup)).

## 2. One-time setup

From the repo root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Then edit `.env`. You must fill in these four values; leave the rest at their defaults:

| Key | What to put |
|---|---|
| `SNOWFLAKE_CONNECTION_NAME` | the named connection from Prerequisites |
| `SNOWFLAKE_ACCOUNT_URL` | `https://<orgname>-<accountname>.snowflakecomputing.com` for that account |
| `S3_BUCKET` | a **new**, globally unique bucket name, not one other workloads use: step 5 revokes Snowflake's direct access to this bucket |
| `ALLOWED_IP_CIDR` | the IP range the demo's Snowflake service users may connect from, e.g. `203.0.113.7/32`. If blank, step 0 uses your current public IP |

Set `AWS_PROFILE` / `AWS_REGION` only if you don't use the default AWS profile or `us-west-2`.
Leave `POLARIS_ROOT_CREDENTIAL`, `CATALOG_URI`, `ECS_TASK_ROLE_ARN` and
`ECS_TASK_EXECUTION_ROLE_ARN` empty; step 1 fills them in.

## 3. Every new shell

Run these in every terminal before any step. Without them, `python` may be a different
interpreter (for example Anaconda's) that fails with `ModuleNotFoundError: No module named
'snowflake'`, and Spark fails to start without Java 17.

```bash
cd /path/to/horizon-to-polaris-exit       # the repo root; all commands run from here
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk@17   # your Java 17 path
```

Optional sanity check (no cloud access, a few seconds):

```bash
python -m pytest tests -q -m offline
```

## 4. Run the demo

Run the steps in order. Each one stops with `FAILED` and a reason if its check fails; fix that
before moving on.

| Step | Command | Result |
|---|---|---|
| 0 | `python -m setup.run_setup` | dedicated S3 bucket, IAM role, external volume, and three users with RBAC (created before the table exists) |
| 1 | `./cloud/deploy-polaris.sh` | Polaris 1.8.0 on ECS Fargate behind a Caddy HTTPS sidecar (~2 minutes) |
| 2 | `python -m migration.step2_create_write` | `HORIZON_DEMO_RW` creates a v3 table from local PySpark through Horizon's Iceberg REST endpoint and writes history (appends, update, delete), then writes more rows from Snowflake SQL; Spark reads Snowflake's rows back |
| 3 | `python -m migration.step3_horizon_demo` | the three users are tried on Horizon from Spark and from Snowflake (below); both engines must agree on who wrote what; then a baseline is recorded |
| 4 | `python -m migration.step4_register` | writes are frozen; the database becomes a catalog and the schema a namespace; the table is registered at its existing metadata file; users, roles and grants are recreated in Polaris |
| 5 | `python -m migration.step5_cutover` | Snowflake's IAM role is denied access to the bucket; once Snowflake's own storage check confirms the deny, `DROP ICEBERG TABLE` in Snowflake; the write freeze ends |
| 6 | `python -m migration.step6_polaris_demo` | the same three users are tried on Polaris from Spark; identity, history and data are checked; every referenced file must still exist; `HORIZON_DEMO_RW` writes new rows from Spark through Polaris |
| 7 | `python -m migration.step7_snowflake_via_polaris` | Snowflake connects to Polaris (catalog integration + writable catalog-linked database); the three users are tried from Snowflake; `HORIZON_DEMO_RW` writes new rows from Snowflake; both engines must see the same rows from all four writers |

Steps 2–7 also run in one go with `python -m migration.run_all` (`--from N --to M` for a range).

**Reading the output:** each step ends with exactly one line, either `step N complete: ...` or
`step N FAILED: ...`. That line is the result. Above it you'll see Spark's dependency download
messages, a progress bar, and the step's own tables (access matrices, rows by writer).

Spark's routine stack traces are hidden by `spark/log4j2.properties`:

- Iceberg retrying a commit that lost a race ("concurrently modified"). Iceberg reloads the
  table and commits again, and the step's own checks confirm the final table.
- The cleanup after that retry. Horizon's vended credentials don't allow `s3:DeleteObject`, so
  one unreferenced file is left in S3.
- Denied writes in the access checks, which are the expected result for `HORIZON_DEMO_RO` and
  `HORIZON_DEMO_NONE`.

A real failure still stops the step with `FAILED` and the error. To see everything Spark logs,
set `rootLogger.level = info` and remove the `off` loggers in that file.

Horizon occasionally answers a commit with an unclear result (an HTTP 500, "Indeterminate result
during conflict resolution check"), even when nothing else is writing. Every write in the demo
checks the table before trying again, so a write is never applied twice. If that happens you'll
see one line such as `append 1-1000: the catalog's reply was unclear and the write had not landed;
retrying`, and the step carries on.

Step 5 is the cutover: Polaris becomes the table's catalog, and Horizon stops managing it. The
table's data and full history stay in your bucket, untouched, and Snowflake keeps reading and
writing the table through Polaris (step 7). The cutover removes Snowflake's direct bucket access so that
Horizon's cleanup of a dropped table can't touch files Polaris now owns. To confirm no file was
removed, leave everything running past the dropped table's retention period (1 day) and run
`python -m tests.recheck_files`.

### Before and after

**After step 3 (before migration):** Horizon is the catalog. Spark connects as each demo user
with that user's PAT; Snowflake SQL runs as the same users with the same PATs. Storage access goes
through Snowflake's external volume IAM role.

```mermaid
flowchart LR
    subgraph local["Your machine"]
        spark["PySpark<br/>HORIZON_DEMO_RW · HORIZON_DEMO_RO · HORIZON_DEMO_NONE<br/>(one catalog per user, PAT auth)"]
    end
    subgraph sf["Snowflake account"]
        horizon["Horizon Catalog<br/>Iceberg REST endpoint"]
        objs["HORIZON_EXIT_DEMO_DB.DEMO.EVENTS<br/>roles HORIZON_DEMO_RW_ROLE · HORIZON_DEMO_RO_ROLE · HORIZON_DEMO_NONE_ROLE"]
        wh["Snowflake SQL<br/>same three users (PAT auth)"]
        horizon --- objs
        wh --- objs
    end
    subgraph aws["Your AWS account"]
        role["IAM role<br/>horizon-to-polaris-exit-snowflake"]
        s3[("S3 bucket<br/>table data + metadata + history")]
    end
    spark -- "Iceberg REST<br/>(RBAC per user)" --> horizon
    horizon -- "vended credentials" --> spark
    spark -- "read / write files" --> s3
    wh -- "read / write<br/>(RBAC per user)" --> objs
    objs -- "external volume" --> role
    role -- "read / write" --> s3
```

**After step 7 (after migration):** Polaris is the catalog for the same table, pointing at the same
files. The database, schema, users, roles and grants were recreated as Polaris objects. Spark
connects as the same three users (now Polaris principals). Snowflake reads and writes the table
through Polaris like any other engine, and its own grants keep the same three users' rights.
Storage access now goes through Polaris's IAM role; Snowflake's own role is denied, so Horizon's
cleanup of the dropped table can't touch the files.

```mermaid
flowchart LR
    subgraph local["Your machine"]
        spark["PySpark<br/>HORIZON_DEMO_RW · HORIZON_DEMO_RO · HORIZON_DEMO_NONE<br/>(one catalog per user, principal auth)"]
    end
    subgraph ecs["Your AWS account: ECS Fargate"]
        caddy["Caddy<br/>HTTPS :443"]
        polaris["Apache Polaris 1.8.0<br/>catalog HORIZON_EXIT_DEMO_DB · namespace DEMO · table EVENTS<br/>principals + principal roles + catalog roles"]
        caddy --> polaris
    end
    subgraph sf["Snowflake account"]
        sfsql["Snowflake SQL<br/>same three users (PAT auth)"]
        cld["Catalog-linked database<br/>HORIZON_EXIT_DEMO_POLARIS_DB (writable)<br/>catalog integration as HORIZON_DEMO_RW<br/>Snowflake grants per role"]
        horizon["Horizon<br/>(table dropped)"]
        sfsql -- "read / write<br/>(RBAC per user)" --> cld
    end
    subgraph aws["Your AWS account: storage"]
        prole["IAM role<br/>horizon-to-polaris-exit-polaris-task"]
        srole["IAM role<br/>horizon-to-polaris-exit-snowflake<br/>(explicit deny on bucket)"]
        s3[("S3 bucket<br/>same files, same history")]
    end
    spark -- "Iceberg REST<br/>(RBAC per user)" --> caddy
    cld -- "Iceberg REST" --> caddy
    polaris -- "vended credentials" --> spark
    polaris -- "vended credentials" --> cld
    polaris -. "assumes" .-> prole
    prole -- "read / write" --> s3
    spark -- "read / write files" --> s3
    cld -- "read / write files" --> s3
    srole -. "denied" .-x s3
    horizon ~~~ srole
```

### The three users

| User | Can see schema | Can see table | Reads | Writes |
|---|---|---|---|---|
| `HORIZON_DEMO_RW` | yes | yes | yes | yes |
| `HORIZON_DEMO_RO` | yes | yes | yes | no |
| `HORIZON_DEMO_NONE` | no | no | no | no |

The same matrix is checked four times, and must come out the same each time:

| | Spark | Snowflake |
|---|---|---|
| Before (Horizon) | step 3 | step 3 |
| After (Polaris) | step 6 | step 7 |

Spark uses a separate catalog per user. Snowflake connects as each user with that user's PAT.

### Who wrote what

Every row's `payload` column records the engine and catalog that wrote it, so from step 2 onward
the table always holds rows from both engines. After step 7, both engines report the same thing:

```text
Rows by writer (Spark and Snowflake agree)
  snowflake via horizon      100 rows
  snowflake via polaris       10 rows
  spark via horizon         2700 rows
  spark via polaris           10 rows
```

To check it yourself in Snowflake:

```sql
SELECT "payload" AS written_by, COUNT(*) FROM HORIZON_EXIT_DEMO_POLARIS_DB."DEMO"."EVENTS" GROUP BY 1;
```

## 5. What moves

| Snowflake | Polaris |
|---|---|
| database | catalog |
| schema | namespace |
| Iceberg table + every snapshot | registered table (same UUID, snapshots, files) |
| user | principal |
| role | principal role + catalog role |
| USAGE on database | `NAMESPACE_LIST` |
| USAGE on schema | `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST` |
| SELECT on table | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` on that table |
| future grants (any privilege) | not mapped: Polaris has no future-grant equivalent, and a namespace grant would also cover existing tables; recorded as unmapped in `state/step4_register.json` |
| INSERT, UPDATE, DELETE | `TABLE_WRITE_DATA`, `TABLE_WRITE_PROPERTIES` |
| OWNERSHIP on table | `TABLE_FULL_METADATA` + read + write |
| CREATE ICEBERG TABLE | `TABLE_CREATE` on the namespace |
| warehouse, external volume grants | not applicable; listed in `state/step4_register.json` |

### Credentials

- **Snowflake admin:** the named connection in `SNOWFLAKE_CONNECTION_NAME`.
- **Demo users:** service users with programmatic access tokens, created by step 0 and written
  to `state/pats.json`.
- **AWS:** the default AWS CLI credential chain. Nothing AWS-related is written to `.env`.
- **Polaris:** the root credential is generated into `.env` and stored in AWS Secrets Manager
  (`horizon-to-polaris-exit/polaris-root`); the ECS task definition references the secret and
  never contains its value. Principal credentials are written to `state/polaris_principals.json`.
  Rerunning step 4 reuses saved principal credentials while they still authenticate.
  `./cloud/deploy-polaris.sh --rotate-root` issues a new root credential.
- **Snowflake after the exit:** step 7's catalog integration authenticates to Polaris as the
  `HORIZON_DEMO_RW` principal, because Snowflake needs a writer. Inside Snowflake, grants on
  the linked database decide who may use it: `HORIZON_DEMO_RW_ROLE` can read and write,
  `HORIZON_DEMO_RO_ROLE` can only read, and `HORIZON_DEMO_NONE_ROLE` gets nothing.
- **Polaris endpoint:** only 80/443 are open (443 is TLS via Caddy). Polaris's plain-HTTP
  port 8181 and its management port 8182 (health and Prometheus metrics; metrics export is a
  Quarkus build-time setting and can't be disabled at runtime) are never opened; the deploy
  script checks readiness over HTTPS.

`.env` and `state/` are gitignored. Secret files are owner-read-only.

### Caveats

- **Polaris persistence is in-memory.** If the ECS task restarts (or `deploy-polaris.sh` is
  rerun), Polaris forgets every catalog, principal and grant; the table's files in S3 are
  unaffected. Before step 5 you can rerun step 4. After step 5, Horizon no longer manages the
  table, so rebuild Polaris from the files and `state/` with
  `python -m migration.recover_polaris`, then rerun step 6. Recovery:
  - registers the newest `*.metadata.json` under the table location that is a validated
    descendant of the last catalog-committed metadata (`state/last_committed.json`, written by
    steps 4 and 6 and by recovery): same UUID and `location`, all manifest lists under the table
    location, baseline snapshots and head ancestry kept, schema unchanged, and a metadata-log
    that contains the current chain tip. Siblings are skipped; two valid files extending the
    same tip abort recovery as a fork. Planted or divergent files are skipped before anything is
    registered.
  - does not open manifests, so data-file paths inside them are not validated. Anyone with write
    access to the table prefix could already overwrite data files directly, so this is accepted
    risk. `state/` is treated as a trusted local anchor.
  - recreates roles and grants from the Snowflake grants recorded in `state/step4_register.json`,
    issues new principal credentials, and reruns step 7 so Snowflake's catalog integration gets
    HORIZON_DEMO_RW's new secret.

  Use Polaris's JDBC metastore for anything beyond a demo.
- Step 4 never re-points an existing Polaris registration at a different metadata file (that
  would discard commits made through Polaris); pass `--force` to do so deliberately.
- The Polaris catalog sets `polaris.config.allow.unstructured.table.location=true`, because
  Snowflake's table paths don't follow Polaris's namespace-derived layout.
- Snowflake hides tables a role has no privileges on, even when the role has schema USAGE.
  Polaris `TABLE_LIST` lists every table in the namespace. With one table that both readers can
  query, the two behave the same.
- Keep `ICEBERG_VERSION` at 1.11.0 or later. Snowflake writes string columns with
  `DELTA_LENGTH_BYTE_ARRAY` encoding, and Iceberg's vectorized Parquet reader (on in
  `spark/sessions.py`) supports that encoding only from 1.11.0. With an older Iceberg, Spark
  fails to read Snowflake's rows.
- The catalog-linked database is writable, and Snowflake passes table drops through to Polaris.
  Dropping `EVENTS` in `HORIZON_EXIT_DEMO_POLARIS_DB` would drop it from Polaris too. Step 7 and
  teardown set the database read-only before dropping the database itself.
- Step 4 freezes writes by disabling `HORIZON_DEMO_RW`, which also disables its PAT. Step 5
  re-enables both once the table is out of Horizon.

## 6. Cleanup

From the repo root, with the steps in [Every new shell](#3-every-new-shell) done:

```bash
./cloud/teardown.sh          # ECS, security group, root-credential secret, Polaris IAM roles
./cloud/teardown.sh --all    # also Snowflake objects, Snowflake's IAM role, and (after a prompt) the bucket and all its data
```

To run the demo again from scratch, tear down with `--all`, then reset local state:

```bash
python -m setup.reset    # archives state/ to state-archive/<timestamp>/ and blanks step 1's .env values
```

Then start again at step 0. Reset touches nothing in Snowflake or AWS; the archive (gitignored,
owner-only) keeps the previous run's credentials and evidence.

## License

Apache License 2.0; see [LICENSE](LICENSE).
