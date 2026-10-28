"""Offline tests for spark.sessions.write_once: retry only when an unclear write did not land."""

import pytest

import spark.sessions as sessions

pytestmark = pytest.mark.offline


class FakeSpark:
    def __init__(self):
        self.statements = []

    def sql(self, stmt):
        self.statements.append(stmt)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)


def unclear():
    raise RuntimeError("org.apache.iceberg.exceptions.CommitStateUnknownException: Indeterminate result")


def test_unclear_reply_but_landed_is_not_written_again():
    s, calls = FakeSpark(), []
    sessions.write_once(s, "t", lambda: (calls.append(1), unclear()), lambda: True, "x")
    assert calls == [1]
    assert s.statements == ["REFRESH TABLE t"]


def test_unclear_reply_not_landed_is_retried():
    s, calls = FakeSpark(), []

    def write():
        calls.append(1)
        if len(calls) == 1:
            unclear()

    sessions.write_once(s, "t", write, lambda: False, "x")
    assert calls == [1, 1]


def test_denied_write_raises_without_retry():
    s, calls = FakeSpark(), []

    def write():
        calls.append(1)
        raise RuntimeError("ForbiddenException: principal not authorized")

    with pytest.raises(RuntimeError, match="Forbidden"):
        sessions.write_once(s, "t", write, lambda: False, "x")
    assert calls == [1] and s.statements == []


def test_gives_up_after_attempts():
    s, calls = FakeSpark(), []
    with pytest.raises(RuntimeError, match="Indeterminate"):
        sessions.write_once(s, "t", lambda: (calls.append(1), unclear()), lambda: False, "x", attempts=3)
    assert calls == [1, 1, 1]
