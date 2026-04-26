"""
Импорт profiles.csv в marathon.db (схема crawler_full).

CSV (из crawler.py / экспорт) → колонки marathon.profiles:
  stat_kilometers → stat_km
  active_years, error, fetched_at → JSON в raw (плюс при необходимости полный снимок)

Связи: таблицы results и cup_results уже содержат profile_id; после INSERT в profiles
       внешние ключи SQLite начинают указывать на существующие строки
       (при PRAGMA foreign_keys=ON).

Запуск:
  python import_profiles_csv.py
  python import_profiles_csv.py --csv profiles.csv --db marathon.db
  python import_profiles_csv.py --skip-not-found    # не грузить строки с 404
  python import_profiles_csv.py --crawl-log         # заполнить crawl_log для профилей
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path


def _opt_int(s: str) -> int | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_active_years(val: str) -> list | str:
    val = (val or "").strip()
    if not val:
        return []
    if val.startswith("["):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


def _crawl_status(error: str) -> str:
    e = (error or "").strip().lower()
    if "404" in e or "not_found" in e:
        return "not_found"
    if e:
        return "error"
    return "ok"


def main() -> None:
    ap = argparse.ArgumentParser(description="Импорт profiles.csv в marathon.db")
    ap.add_argument("--csv", type=Path, default=Path("profiles.csv"))
    ap.add_argument("--db", type=Path, default=Path("marathon.db"))
    ap.add_argument(
        "--skip-not-found",
        action="store_true",
        help="Пропускать строки, где error указывает на отсутствие профиля (404)",
    )
    ap.add_argument(
        "--crawl-log",
        action="store_true",
        help="Добавить записи в crawl_log (entity=profile), чтобы sync не дёргал API зря",
    )
    args = ap.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"Файл не найден: {args.csv}")
    if not args.db.exists():
        raise SystemExit(f"База не найдена: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")

    inserted = 0
    skipped = 0

    with args.csv.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        expected = {
            "id",
            "first_name",
            "last_name",
            "second_name",
            "gender",
            "age",
            "birth_year",
            "city",
            "city_id",
            "region",
            "region_id",
            "country",
            "club",
            "active_years",
            "stat_competitions",
            "stat_kilometers",
            "stat_marathons",
            "stat_first",
            "stat_second",
            "stat_third",
            "error",
            "fetched_at",
        }
        if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
            raise SystemExit(
                f"Неверный заголовок CSV. Нужны колонки: {sorted(expected)}\n"
                f"Есть: {reader.fieldnames}"
            )

        for row in reader:
            err = (row.get("error") or "").strip()
            if args.skip_not_found and _crawl_status(err) == "not_found":
                skipped += 1
                continue

            pid = _opt_int(row["id"])
            if pid is None:
                skipped += 1
                continue

            active = _parse_active_years(row.get("active_years") or "")
            raw_obj = {
                "active_years": active,
                "error": err or None,
                "fetched_at": (row.get("fetched_at") or "").strip() or None,
                "source": "import_profiles_csv.py",
            }
            raw = json.dumps(raw_obj, ensure_ascii=False)

            conn.execute(
                """
                INSERT OR REPLACE INTO profiles (
                    id, first_name, last_name, second_name, gender, age, birth_year,
                    city, city_id, region, region_id, country, club,
                    stat_competitions, stat_km, stat_marathons,
                    stat_first, stat_second, stat_third, raw
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    pid,
                    (row.get("first_name") or "").strip(),
                    (row.get("last_name") or "").strip(),
                    (row.get("second_name") or "").strip(),
                    (row.get("gender") or "").strip(),
                    _opt_int(row.get("age") or ""),
                    _opt_int(row.get("birth_year") or ""),
                    (row.get("city") or "").strip(),
                    _opt_int(row.get("city_id") or ""),
                    (row.get("region") or "").strip(),
                    _opt_int(row.get("region_id") or ""),
                    (row.get("country") or "").strip(),
                    (row.get("club") or "").strip(),
                    _opt_int(row.get("stat_competitions") or ""),
                    _opt_int(row.get("stat_kilometers") or ""),
                    _opt_int(row.get("stat_marathons") or ""),
                    _opt_int(row.get("stat_first") or ""),
                    _opt_int(row.get("stat_second") or ""),
                    _opt_int(row.get("stat_third") or ""),
                    raw,
                ),
            )

            if args.crawl_log:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO crawl_log (entity, entity_id, status)
                    VALUES ('profile', ?, ?)
                    """,
                    (pid, _crawl_status(err)),
                )

            inserted += 1

    conn.commit()
    conn.close()
    print(f"Готово: записано/обновлено профилей: {inserted}, пропущено: {skipped}")
    print(f"База: {args.db.resolve()}")


if __name__ == "__main__":
    main()
