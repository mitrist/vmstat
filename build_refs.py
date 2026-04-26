"""
Строим справочники из уже собранных данных profiles.jsonl / profiles.db.

Не требует дополнительных запросов к сайту — всё уже есть в базе.

Что создаём:
  ref_cities        — id, название, регион, страна + статистика участников
  ref_competitions  — id, название, вид спорта, даты проведения + статистика
  ref_groups        — уникальные названия групп (возрастные категории)

Запуск:
    python build_refs.py

Результат: обновляет profiles.db (добавляет таблицы-справочники)
"""

import sqlite3
import json
from pathlib import Path

DB_PATH    = Path("profiles.db")
JSONL_PATH = Path("profiles.jsonl")


def init_ref_tables(conn: sqlite3.Connection):
    conn.executescript("""
        -- Справочник городов
        CREATE TABLE IF NOT EXISTS ref_cities (
            city_id     INTEGER PRIMARY KEY,
            city_name   TEXT,
            region_id   INTEGER,
            region_name TEXT,
            country     TEXT,
            athletes    INTEGER DEFAULT 0   -- сколько участников из города
        );

        -- Справочник соревнований
        CREATE TABLE IF NOT EXISTS ref_competitions (
            competition_id    INTEGER PRIMARY KEY,
            competition_title TEXT,
            sport             TEXT,
            first_date        TEXT,
            last_date         TEXT,
            years_held        TEXT,   -- JSON-список годов
            total_starts      INTEGER DEFAULT 0,
            unique_athletes   INTEGER DEFAULT 0
        );

        -- Справочник групп (возрастные категории)
        CREATE TABLE IF NOT EXISTS ref_groups (
            group_name TEXT PRIMARY KEY,
            sport      TEXT,
            count      INTEGER DEFAULT 0
        );
    """)
    conn.commit()


def build_city_ref(conn: sqlite3.Connection):
    """Собираем уникальные города из таблицы profiles."""
    print("Строим ref_cities...")

    conn.execute("DELETE FROM ref_cities")

    rows = conn.execute("""
        SELECT city_id, city AS city_name, region, region_id, country,
               COUNT(*) AS athletes
        FROM profiles
        WHERE city_id IS NOT NULL AND city_id > 0
        GROUP BY city_id
    """).fetchall()

    conn.executemany("""
        INSERT OR REPLACE INTO ref_cities
          (city_id, city_name, region_id, region_name, country, athletes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [(r[0], r[1], r[3], r[2], r[4], r[5]) for r in rows])

    conn.commit()
    print(f"  Городов найдено: {len(rows)}")


def build_competition_ref(conn: sqlite3.Connection):
    """Собираем уникальные соревнования из таблицы results."""
    print("Строим ref_competitions...")

    conn.execute("DELETE FROM ref_competitions")

    rows = conn.execute("""
        SELECT
            competition_id,
            competition_title,
            sport,
            MIN(competition_date) AS first_date,
            MAX(competition_date) AS last_date,
            COUNT(*) AS total_starts,
            COUNT(DISTINCT profile_id) AS unique_athletes,
            GROUP_CONCAT(DISTINCT year ORDER BY year) AS years
        FROM results
        WHERE competition_id > 0
        GROUP BY competition_id
        ORDER BY total_starts DESC
    """).fetchall()

    conn.executemany("""
        INSERT OR REPLACE INTO ref_competitions
          (competition_id, competition_title, sport,
           first_date, last_date, years_held,
           total_starts, unique_athletes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r[0], r[1], r[2], r[3], r[4],
         json.dumps(r[7].split(",") if r[7] else []),
         r[5], r[6])
        for r in rows
    ])

    conn.commit()
    print(f"  Соревнований найдено: {len(rows)}")


def build_group_ref(conn: sqlite3.Connection):
    """Уникальные группы (возрастные категории)."""
    print("Строим ref_groups...")

    conn.execute("DELETE FROM ref_groups")

    rows = conn.execute("""
        SELECT group_name, sport, COUNT(*) AS cnt
        FROM results
        WHERE group_name != ''
        GROUP BY group_name, sport
        ORDER BY cnt DESC
    """).fetchall()

    conn.executemany("""
        INSERT OR REPLACE INTO ref_groups (group_name, sport, count)
        VALUES (?, ?, ?)
    """, rows)

    conn.commit()
    print(f"  Групп найдено: {len(rows)}")


def enrich_from_jsonl(conn: sqlite3.Connection):
    """
    Дополнительное обогащение из JSONL —
    подтягиваем region_id и country которые могут отсутствовать в profiles.
    """
    if not JSONL_PATH.exists():
        return

    print("Обогащаем из JSONL...")
    cities_extra = {}

    with JSONL_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
                cid = p.get("city_id")
                if cid and cid not in cities_extra:
                    cities_extra[cid] = {
                        "city_id":   cid,
                        "city_name": p.get("city", ""),
                        "region_id": p.get("region_id"),
                        "region_name": p.get("region", ""),
                        "country":   p.get("country", ""),
                    }
            except Exception:
                continue

    for cid, info in cities_extra.items():
        conn.execute("""
            UPDATE ref_cities SET
                city_name   = COALESCE(NULLIF(?, ''), city_name),
                region_id   = COALESCE(?, region_id),
                region_name = COALESCE(NULLIF(?, ''), region_name),
                country     = COALESCE(NULLIF(?, ''), country)
            WHERE city_id = ?
        """, (info["city_name"], info["region_id"],
              info["region_name"], info["country"], cid))

    conn.commit()
    print(f"  Обработано городов из JSONL: {len(cities_extra)}")


def print_summary(conn: sqlite3.Connection):
    def q(sql):
        return conn.execute(sql).fetchall()

    print("\n" + "="*60)
    print("  ref_cities — топ-20 городов по участникам")
    print("="*60)
    for r in q("""
        SELECT city_id, city_name, region_name, athletes
        FROM ref_cities ORDER BY athletes DESC LIMIT 20
    """):
        print(f"  [{r[0]:4d}] {r[1]:25s} {r[2]:30s} {r[3]} уч.")

    print("\n" + "="*60)
    print("  ref_competitions — топ-20 событий по стартам")
    print("="*60)
    for r in q("""
        SELECT competition_id, competition_title, sport,
               total_starts, unique_athletes, years_held
        FROM ref_competitions ORDER BY total_starts DESC LIMIT 20
    """):
        years = json.loads(r[5]) if r[5] else []
        print(f"  [{r[0]:4d}] {r[1]:35s} {r[2]:5s} "
              f"{r[3]:4d} ст. {r[4]:4d} уч.  годы: {','.join(years)}")

    print("\n" + "="*60)
    print("  ref_groups — возрастные группы")
    print("="*60)
    for r in q("""
        SELECT group_name, sport, count
        FROM ref_groups ORDER BY count DESC LIMIT 30
    """):
        print(f"  {r[0]:20s} {r[1]:6s} {r[2]} раз")


def main():
    if not DB_PATH.exists():
        print(f"❌ Файл {DB_PATH} не найден. Сначала запусти crawler.py")
        return

    conn = sqlite3.connect(DB_PATH)
    init_ref_tables(conn)
    build_city_ref(conn)
    build_competition_ref(conn)
    build_group_ref(conn)
    enrich_from_jsonl(conn)
    print_summary(conn)
    conn.close()

    print(f"\n✅ Справочники добавлены в {DB_PATH}")
    print("Таблицы: ref_cities, ref_competitions, ref_groups")


if __name__ == "__main__":
    main()
