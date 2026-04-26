"""
Заполняет только таблицу profile_cup_results для уже имеющихся строк в profiles.

Для каждого profile_id вызываются те же запросы, что в sync._sync_profile_cup_results:
  GET /api/v2/profile/{id}/competition-results/years/
  GET /api/v2/profile/{id}/cup-results/?year=YYYY

Запуск (долго: ~2 запроса на год активности на каждый профиль):
  python fill_profile_cup_results.py
  python fill_profile_cup_results.py --only-missing     # только у кого ещё нет строк в profile_cup_results
  python fill_profile_cup_results.py --min-id 2000 --max-id 5000
  python fill_profile_cup_results.py --include-empty    # и «пустые» id без ФИО (обычно 404)
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from pathlib import Path

from sync import DB_PATH, make_session, _sync_profile_cup_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser(description="Заполнить profile_cup_results из API")
    ap.add_argument("--db", type=Path, default=DB_PATH)
    ap.add_argument(
        "--only-missing",
        action="store_true",
        help="Только профили, у которых ещё нет ни одной строки в profile_cup_results",
    )
    ap.add_argument(
        "--include-empty",
        action="store_true",
        help="Включать строки profiles без имени/фамилии (часто мусор после обхода несуществующих id)",
    )
    ap.add_argument("--min-id", type=int, default=None)
    ap.add_argument("--max-id", type=int, default=None)
    ap.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Пауза сек после каждого профиля (дополнительно к паузам внутри _sync_profile_cup_results)",
    )
    args = ap.parse_args()

    if not args.db.exists():
        raise SystemExit(f"База не найдена: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS profile_cup_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            year INTEGER,
            cup_id INTEGER,
            cup_title TEXT,
            distance_id INTEGER,
            distance_name TEXT,
            place_abs INTEGER,
            place_gender INTEGER,
            place_group INTEGER,
            group_name TEXT,
            total_points REAL,
            raw TEXT
        )
        """
    )
    conn.commit()

    sql = """
        SELECT p.id FROM profiles p
        WHERE (? OR TRIM(COALESCE(p.first_name,'')) != '' OR TRIM(COALESCE(p.last_name,'')) != '')
    """
    params: list = [1 if args.include_empty else 0]

    if args.only_missing:
        sql += """
          AND NOT EXISTS (
            SELECT 1 FROM profile_cup_results r WHERE r.profile_id = p.id
          )
        """

    if args.min_id is not None:
        sql += " AND p.id >= ?"
        params.append(args.min_id)
    if args.max_id is not None:
        sql += " AND p.id <= ?"
        params.append(args.max_id)

    sql += " ORDER BY p.id"

    pids = [r[0] for r in conn.execute(sql, params).fetchall()]
    total = len(pids)
    log.info("Профилей к обработке: %s", total)

    session = make_session()
    t0 = time.perf_counter()
    for i, pid in enumerate(pids):
        try:
            _sync_profile_cup_results(conn, session, pid)
        except Exception as e:
            log.warning("profile %s: %s", pid, e)
            conn.rollback()
        if args.delay > 0:
            time.sleep(args.delay)
        if (i + 1) % 100 == 0 or (i + 1) == total:
            elapsed = time.perf_counter() - t0
            log.info("  [%s/%s]  %.1f сек", i + 1, total, elapsed)

    n_rows = conn.execute("SELECT COUNT(*) FROM profile_cup_results").fetchone()[0]
    conn.close()
    log.info("Готово. Строк в profile_cup_results: %s", n_rows)


if __name__ == "__main__":
    main()
