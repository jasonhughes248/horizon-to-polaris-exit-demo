"""Run the steps in order. Step 1 (ECS deploy) is a shell script and runs separately.

    python -m migration.run_all                 # steps 2-7
    python -m migration.run_all --from 4 --to 5
"""

from __future__ import annotations

import argparse
import importlib

STEPS = {
    2: "migration.step2_create_write",
    3: "migration.step3_horizon_demo",
    4: "migration.step4_register",
    5: "migration.step5_cutover",
    6: "migration.step6_polaris_demo",
    7: "migration.step7_snowflake_via_polaris",
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="start", type=int, default=2)
    p.add_argument("--to", dest="end", type=int, default=7)
    args = p.parse_args()
    for num in range(args.start, args.end + 1):
        print(f"\n===== step {num}: {STEPS[num].split('.')[-1]} =====")
        importlib.import_module(STEPS[num]).main()


if __name__ == "__main__":
    main()
