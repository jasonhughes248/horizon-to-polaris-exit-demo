"""Offline tests for setup.reset. Uses a temp repo layout; never touches the real state/ or .env."""

import os
import stat
from datetime import datetime

import pytest

import setup.reset as reset

pytestmark = pytest.mark.offline


@pytest.fixture
def repo(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    (state / ".gitkeep").write_text("")
    env_file = tmp_path / ".env"
    monkeypatch.setattr(reset, "STATE_DIR", state)
    monkeypatch.setattr(reset, "ENV_FILE", env_file)
    monkeypatch.setattr(reset, "ARCHIVE_ROOT", tmp_path / "state-archive")
    return tmp_path


def test_archives_state_and_keeps_gitkeep(repo):
    (repo / "state" / "baseline.json").write_text("{}")
    (repo / "state" / "pats.json").write_text("{}")
    moved = reset.archive_state(datetime(2026, 10, 27, 9, 0, 0))
    assert sorted(moved) == ["baseline.json", "pats.json"]
    assert [p.name for p in (repo / "state").iterdir()] == [".gitkeep"]
    dest = repo / "state-archive" / "20261027-090000"
    assert sorted(p.name for p in dest.iterdir()) == ["baseline.json", "pats.json"]
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o700


def test_empty_state_is_a_no_op(repo):
    assert reset.archive_state() == []
    assert not (repo / "state-archive").exists()


def test_blanks_only_step1_values(repo):
    (repo / ".env").write_text(
        "S3_BUCKET=keep-me\nCATALOG_URI=https://x/api/catalog\nPOLARIS_ROOT_CREDENTIAL=root:s\n"
        "ECS_TASK_ROLE_ARN=arn:a\nECS_TASK_EXECUTION_ROLE_ARN=\nSNOWFLAKE_CONNECTION_NAME=c\n"
    )
    cleared = reset.blank_step1_env()
    assert sorted(cleared) == ["CATALOG_URI", "ECS_TASK_ROLE_ARN", "POLARIS_ROOT_CREDENTIAL"]
    text = (repo / ".env").read_text()
    assert "S3_BUCKET=keep-me" in text and "SNOWFLAKE_CONNECTION_NAME=c" in text
    for k in reset.STEP1_KEYS:
        assert f"\n{k}=\n" in "\n" + text
