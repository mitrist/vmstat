"""
Расчёт командного зачёта по этапам Кубка.

Примеры:
  python compute_team_scoring.py --cup-id 7 --year 2026
  python compute_team_scoring.py --cup-id 7 --year 2026 --stages-file ".cursor/etapi.md"
"""

from __future__ import annotations

import argparse
from pathlib import Path

import marathon_queries as mq


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute team scoring tables for a cup season.")
    p.add_argument("--db", default=str(mq.DEFAULT_DB), help="Path to marathon.db")
    p.add_argument("--cup-id", type=int, required=True, help="Cup ID")
    p.add_argument("--year", type=int, required=True, help="Season year")
    p.add_argument(
        "--stages-file",
        default=str(mq.DEFAULT_STAGES_FILE),
        help="Path to stage mapping file (.cursor/etapy.yaml)",
    )
    p.add_argument("--rule-version", default="team_v1", help="Rule version tag")
    return p.parse_args()


def main() -> int:
    a = parse_args()
    db = Path(a.db)
    if not db.is_file():
        print(f"DB not found: {db}")
        return 2
    stage_map = mq.load_stage_index_map(Path(a.stages_file))
    if not stage_map:
        print(f"Stage map is empty: {a.stages_file}")
        return 3
    out = mq.compute_team_scoring_for_cup_year(
        db_path=db,
        cup_id=int(a.cup_id),
        year=int(a.year),
        stage_map=stage_map,
        rule_version=str(a.rule_version),
    )
    print(
        f"Done: stage_rows={out['stage_rows']}, "
        f"member_rows={out['member_rows']}, team_rows={out['team_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

