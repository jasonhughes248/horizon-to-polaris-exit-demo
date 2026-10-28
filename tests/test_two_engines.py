"""Offline tests for the two-engine pieces: writer tags, Snowflake-as-user connections, step 7 SQL."""

import pytest

import migration.common as common
import migration.step7_snowflake_via_polaris as step7
from migration.common import SNOWFLAKE_POLARIS, SPARK_POLARIS, WRITER_IDS, tagged_rows_sql, writer_summary, writer_summary_sql

pytestmark = pytest.mark.offline


def test_writer_id_ranges_are_disjoint_and_clear_of_spark_history():
    spans = sorted((start, start + count - 1) for start, count in WRITER_IDS.values())
    assert spans[0][0] > 3000  # Spark's history uses ids 1-3000
    assert all(a[1] < b[0] for a, b in zip(spans, spans[1:]))


@pytest.mark.parametrize("dialect,col,ts", [("spark", "payload", "TIMESTAMP_NTZ '"), ("snowflake", '"payload"', "::TIMESTAMP_NTZ")])
def test_tagged_rows_delete_then_insert(dialect, col, ts):
    delete, insert = tagged_rows_sql("db.s.t", SPARK_POLARIS, dialect)
    assert delete == f"DELETE FROM db.s.t WHERE {col} = '{SPARK_POLARIS}'"
    assert insert.startswith("INSERT INTO db.s.t (") and ts in insert
    assert insert.count(f"'{SPARK_POLARIS}'") == WRITER_IDS[SPARK_POLARIS][1]


def test_writer_summary_sql_quotes_for_snowflake():
    assert '"payload"' in writer_summary_sql("t", quote='"')
    assert '"' not in writer_summary_sql("t")
    assert writer_summary([("a", 2), ("b", 3)]) == {"a": 2, "b": 3}


def test_snowflake_as_uses_pat_and_role(monkeypatch):
    seen = {}

    class FakeConnector:
        @staticmethod
        def connect(**kw):
            seen.update(kw)
            return object()

    import sys
    import types

    fake_pkg = types.ModuleType("snowflake")
    fake_pkg.connector = FakeConnector
    monkeypatch.setitem(sys.modules, "snowflake", fake_pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", FakeConnector)
    monkeypatch.setattr(common, "pats", lambda: {"ro": "secret-ro"})
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT_URL", "https://myorg-myacct.snowflakecomputing.com")
    common.snowflake_as("ro")
    assert seen["account"] == "myorg-myacct"
    assert seen["user"] == "HORIZON_DEMO_RO" and seen["role"] == "HORIZON_DEMO_RO_ROLE"
    assert seen["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN" and seen["token"] == "secret-ro"


def test_step7_makes_linked_db_read_only_before_dropping():
    alter, drop = step7.drop_linked_db_sql()
    assert "ALLOWED_WRITE_OPERATIONS = NONE" in alter and drop.startswith("DROP DATABASE")


def test_step7_grants_ro_select_only_and_none_nothing():
    stmts = step7.grants_sql("DEMO", "EVENTS")
    ro = [s for s in stmts if s.endswith("HORIZON_DEMO_RO_ROLE")]
    assert any("GRANT SELECT ON TABLE" in s for s in ro)
    assert not any(w in s for s in ro for w in ("INSERT", "UPDATE", "DELETE"))
    assert not any("HORIZON_DEMO_NONE_ROLE" in s for s in stmts)
    assert SNOWFLAKE_POLARIS  # tag constant importable
