"""Reset local run state so the next run starts clean at step 0.

  - moves every file in state/ (except .gitkeep) to state-archive/<timestamp>/ (0700)
  - blanks the values step 1 writes into .env: POLARIS_ROOT_CREDENTIAL, CATALOG_URI,
    ECS_TASK_ROLE_ARN, ECS_TASK_EXECUTION_ROLE_ARN

Touches nothing in Snowflake or AWS. Run ./cloud/teardown.sh --all first if the previous
run's resources still exist; otherwise the archived credentials are the only record of them.
Usage: python -m setup.reset
"""

from __future__ import annotations

import os
import re
import shutil
import stat
from datetime import datetime

from migration.common import ENV_FILE, REPO_ROOT, STATE_DIR

STEP1_KEYS = ["POLARIS_ROOT_CREDENTIAL", "CATALOG_URI", "ECS_TASK_ROLE_ARN", "ECS_TASK_EXECUTION_ROLE_ARN"]
ARCHIVE_ROOT = REPO_ROOT / "state-archive"


def archive_state(now: datetime | None = None) -> list[str]:
    files = [p for p in STATE_DIR.iterdir() if p.name != ".gitkeep"] if STATE_DIR.exists() else []
    if not files:
        return []
    stamp = (now or datetime.now()).strftime("%Y%m%d-%H%M%S")
    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)
    dest, n = ARCHIVE_ROOT / stamp, 1
    while dest.exists():  # two resets in the same second
        n += 1
        dest = ARCHIVE_ROOT / f"{stamp}-{n}"
    dest.mkdir()
    os.chmod(ARCHIVE_ROOT, stat.S_IRWXU)
    os.chmod(dest, stat.S_IRWXU)  # archived files include credentials
    for p in files:
        shutil.move(str(p), dest / p.name)
    return [p.name for p in files]


def blank_step1_env() -> list[str]:
    if not ENV_FILE.exists():
        return []
    text = ENV_FILE.read_text()
    # same leniency as load_env: optional spaces around the key
    cleared = [k for k in STEP1_KEYS if re.search(rf"^[ \t]*{k}[ \t]*=[ \t]*\S.*$", text, flags=re.M)]
    for k in STEP1_KEYS:
        text = re.sub(rf"^[ \t]*{k}[ \t]*=.*$", f"{k}=", text, flags=re.M)
    ENV_FILE.write_text(text)
    return cleared


def main() -> None:
    moved = archive_state()
    cleared = blank_step1_env()
    print(f"  state/: archived {len(moved)} file(s) to state-archive/" if moved else "  state/: already empty")
    print(f"  .env: cleared {', '.join(cleared)}" if cleared else "  .env: step-1 values already empty")
    print("reset complete: start again at step 0")


if __name__ == "__main__":
    main()
