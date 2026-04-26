"""
Пересчёт локальных очков кубка в marathon.db (таблицы cup_scoring_computed_*).

Пример:
  python compute_cup_scoring.py --cup-id 1 --year 2026
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cup_scoring  # noqa: E402

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", str(ROOT / "marathon.db")))


def main() -> None:
    ap = argparse.ArgumentParser(description="Пересчёт cup_scoring_computed_* по правилам 2026_run_v1")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Путь к marathon.db")
    ap.add_argument("--cup-id", type=int, required=True, help="id кубка (cups.id)")
    ap.add_argument("--year", type=int, default=2026, help="Календарный год этапов")
    args = ap.parse_args()

    if not args.db.is_file():
        print(f"База не найдена: {args.db}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    try:
        n_fin, n_tot = cup_scoring.compute_run_cup_2026(conn, args.cup_id, args.year)
    finally:
        conn.close()
    print(f"Готово: записей finishes={n_fin}, totals={n_tot} (cup_id={args.cup_id}, year={args.year})")


if __name__ == "__main__":
    main()
