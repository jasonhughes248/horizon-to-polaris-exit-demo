"""Map Snowflake grants to Polaris grants. Pure functions, no I/O (unit-tested offline).

Input rows come from SHOW GRANTS TO ROLE and SHOW FUTURE GRANTS IN SCHEMA,
normalized to {privilege, granted_on, name, grantee, future}. Output is, per role,
the Polaris catalog-role grant bodies plus the Snowflake grants with no Polaris
equivalent. Mapping rationale lives in orchestration/DECISIONS.md.
"""

from __future__ import annotations

READ = ["TABLE_READ_DATA", "TABLE_READ_PROPERTIES"]
WRITE = ["TABLE_WRITE_DATA", "TABLE_WRITE_PROPERTIES"]

TABLE_PRIVS = {
    "SELECT": READ,
    "INSERT": WRITE,
    "UPDATE": WRITE,
    "DELETE": WRITE,
    "TRUNCATE": WRITE,
    "OWNERSHIP": ["TABLE_FULL_METADATA", *READ, *WRITE],
}
SCHEMA_PRIVS = {
    "USAGE": ["NAMESPACE_READ_PROPERTIES", "TABLE_LIST"],
    "CREATE ICEBERG TABLE": ["TABLE_CREATE"],
}
DATABASE_PRIVS = {"USAGE": ["NAMESPACE_LIST"]}
TABLE_KINDS = {"TABLE", "ICEBERG_TABLE", "ICEBERG TABLE"}
# Account-level objects with no catalog equivalent; reported, never mapped.
NOT_APPLICABLE = {"WAREHOUSE", "EXTERNAL_VOLUME", "EXTERNAL VOLUME", "ACCOUNT", "ROLE", "INTEGRATION"}


def split_name(name: str) -> list[str]:
    """Split a Snowflake identifier path on unquoted dots.

    Unquoted parts are case-insensitive (upper-cased); quoted parts keep their
    exact text, with "" unescaped to ". Raises ValueError on an unbalanced quote.
    """
    parts: list[str] = []
    cur: list[str] = []
    i, quoted, in_quotes = 0, False, False
    while i < len(name):
        ch = name[i]
        if in_quotes:
            if ch == '"':
                if i + 1 < len(name) and name[i + 1] == '"':
                    cur.append('"')
                    i += 1
                else:
                    in_quotes = False
            else:
                cur.append(ch)
        elif ch == '"':
            in_quotes = quoted = True
        elif ch == ".":
            text = "".join(cur)
            parts.append(text if quoted else text.strip().upper())
            cur, quoted = [], False
        else:
            cur.append(ch)
        i += 1
    if in_quotes:
        raise ValueError(f"unbalanced quote in identifier {name!r}")
    text = "".join(cur)
    parts.append(text if quoted else text.strip().upper())
    return parts


def _ident(name: str) -> str:
    """Canonical form of a configured (unquoted) object name."""
    return split_name(name)[0]


def normalize_show_grants(rows: list[dict]) -> list[dict]:
    """SHOW GRANTS TO ROLE rows (lower-cased column names) -> normalized grants."""
    return [
        {"privilege": r["privilege"].upper(), "granted_on": r["granted_on"].upper(), "name": r["name"],
         "grantee": r["grantee_name"].upper(), "future": False}
        for r in rows
    ]


def normalize_future_grants(rows: list[dict]) -> list[dict]:
    """SHOW FUTURE GRANTS IN SCHEMA rows -> normalized grants (future=True)."""
    return [
        {"privilege": r["privilege"].upper(), "granted_on": r["grant_on"].upper(), "name": r["name"],
         "grantee": r["grantee_name"].upper(), "future": True}
        for r in rows
        if str(r.get("grant_to", "ROLE")).upper() == "ROLE"
    ]


def map_grant(grant: dict, database: str, schema: str, table: str) -> tuple[list[dict], str | None]:
    """One Snowflake grant -> (Polaris grant bodies, reason if unmapped)."""
    kind, priv = grant["granted_on"], grant["privilege"]
    if kind in NOT_APPLICABLE:
        return [], f"{kind} has no Polaris equivalent"
    if grant.get("future"):
        # Snowflake future grants apply only to objects created later. Polaris has
        # no equivalent: a namespace grant also covers existing tables, so mapping
        # it would widen access. Existing tables get access only from direct grants.
        return [], "future grant: Polaris has no future-grant equivalent (namespace grants also cover existing tables)"
    try:
        parts = split_name(grant["name"])
    except ValueError as exc:
        return [], str(exc)
    if parts[0] != _ident(database) or (kind == "DATABASE" and len(parts) != 1):
        return [], "outside the migrated database"

    if kind == "DATABASE":
        privs = DATABASE_PRIVS.get(priv)
        return ([{"type": "catalog", "privilege": p} for p in privs], None) if privs else ([], f"database {priv} not mapped")

    if len(parts) < 2 or parts[1] != _ident(schema) or (kind == "SCHEMA" and len(parts) != 2):
        return [], "outside the migrated schema"
    ns = [schema]

    if kind == "SCHEMA":
        privs = SCHEMA_PRIVS.get(priv)
        if not privs:
            return [], f"schema {priv} not mapped"
        return [{"type": "namespace", "namespace": ns, "privilege": p} for p in privs], None

    if kind in TABLE_KINDS:
        privs = TABLE_PRIVS.get(priv)
        if not privs:
            return [], f"table {priv} not mapped"
        if len(parts) != 3 or parts[2] != _ident(table):
            return [], "a table that is not being migrated"
        return [{"type": "table", "namespace": ns, "tableName": table, "privilege": p} for p in privs], None

    return [], f"{kind} not mapped"


def _key(g: dict) -> tuple:
    return (g["type"], tuple(g.get("namespace", [])), g.get("tableName", ""), g["privilege"])


def map_role_grants(grants: list[dict], database: str, schema: str, table: str) -> dict:
    """Grants for one role -> {'polaris': [...deduped, sorted], 'unmapped': [...]}"""
    mapped: dict[tuple, dict] = {}
    unmapped = []
    for g in grants:
        bodies, reason = map_grant(g, database, schema, table)
        for b in bodies:
            mapped[_key(b)] = b
        if reason:
            unmapped.append({**g, "reason": reason})
    return {"polaris": [mapped[k] for k in sorted(mapped)], "unmapped": unmapped}
