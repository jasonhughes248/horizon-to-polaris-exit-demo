"""Offline tests for the Snowflake -> Polaris grant mapping. No network."""

import pytest

from migration.grants import map_grant, map_role_grants

pytestmark = pytest.mark.offline
DB, SCHEMA, TABLE = "HORIZON_EXIT_DEMO_DB", "DEMO", "EVENTS"


def g(privilege, granted_on, name, future=False):
    return {"privilege": privilege, "granted_on": granted_on, "name": name, "grantee": "R", "future": future}


def privs(result):
    return {(b["type"], b.get("tableName"), b["privilege"]) for b in result["polaris"]}


def test_read_only_role():
    r = map_role_grants([
        g("USAGE", "DATABASE", DB),
        g("USAGE", "SCHEMA", f"{DB}.{SCHEMA}"),
        g("SELECT", "ICEBERG_TABLE", f"{DB}.{SCHEMA}.<ICEBERG_TABLE>", future=True),
        g("SELECT", "TABLE", f"{DB}.{SCHEMA}.{TABLE}"),
    ], DB, SCHEMA, TABLE)
    p = privs(r)
    assert ("catalog", None, "NAMESPACE_LIST") in p
    assert ("namespace", None, "TABLE_READ_DATA") not in p  # future grants are not mapped
    assert ("table", TABLE, "TABLE_READ_DATA") in p
    assert not any("WRITE" in x[2] or x[2] in ("TABLE_CREATE", "TABLE_FULL_METADATA") for x in p)
    assert [u["privilege"] for u in r["unmapped"]] == ["SELECT"] and "future" in r["unmapped"][0]["reason"]


def test_owner_gets_write_and_create():
    r = map_role_grants([
        g("OWNERSHIP", "TABLE", f'"{DB}"."{SCHEMA}"."{TABLE}"'),
        g("CREATE ICEBERG TABLE", "SCHEMA", f"{DB}.{SCHEMA}"),
    ], DB, SCHEMA, TABLE)
    p = privs(r)
    assert {("table", TABLE, "TABLE_WRITE_DATA"), ("table", TABLE, "TABLE_FULL_METADATA"), ("namespace", None, "TABLE_CREATE")} <= p


def test_no_grants_maps_to_nothing():
    assert map_role_grants([], DB, SCHEMA, TABLE) == {"polaris": [], "unmapped": []}


@pytest.mark.parametrize("grant,reason", [
    (g("USAGE", "WAREHOUSE", "HORIZON_EXIT_DEMO_WH"), "no Polaris equivalent"),
    (g("USAGE", "EXTERNAL_VOLUME", "HORIZON_EXIT_DEMO_VOL"), "no Polaris equivalent"),
    (g("USAGE", "DATABASE", "OTHER_DB"), "outside the migrated database"),
    (g("USAGE", "SCHEMA", f"{DB}.OTHER"), "outside the migrated schema"),
    (g("SELECT", "TABLE", f"{DB}.{SCHEMA}.OTHER"), "not being migrated"),
    (g("MONITOR", "DATABASE", DB), "not mapped"),
])
def test_unmapped_reported_not_granted(grant, reason):
    bodies, why = map_grant(grant, DB, SCHEMA, TABLE)
    assert bodies == [] and reason in why


def test_dedup_and_deterministic_order():
    grants = [g("INSERT", "TABLE", f"{DB}.{SCHEMA}.{TABLE}"), g("UPDATE", "TABLE", f"{DB}.{SCHEMA}.{TABLE}")]
    a = map_role_grants(grants, DB, SCHEMA, TABLE)["polaris"]
    b = map_role_grants(list(reversed(grants)), DB, SCHEMA, TABLE)["polaris"]
    assert a == b and len(a) == 2


@pytest.mark.parametrize("name,expected", [
    ('HORIZON_EXIT_DEMO_DB.DEMO.EVENTS', ["HORIZON_EXIT_DEMO_DB", "DEMO", "EVENTS"]),
    ('horizon_exit_demo_db.demo."Events"', ["HORIZON_EXIT_DEMO_DB", "DEMO", "Events"]),
    ('DB."A.B".T', ["DB", "A.B", "T"]),
    ('DB."say ""hi"".x".T', ["DB", 'say "hi".x', "T"]),
])
def test_split_name_quote_aware(name, expected):
    from migration.grants import split_name

    assert split_name(name) == expected
