"""
ШАГ 3: Просмотр и проверка собранных данных.
Запускай после того, как step2 набрал данные.

    python step3_explore.py
"""

import sqlite3
import json
from pathlib import Path

DB_PATH = Path("profiles.db")


def run(sql: str, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def print_table(rows: list[dict]):
    if not rows:
        print("  (нет данных)")
        return
    keys = list(rows[0].keys())
    widths = [max(len(str(r.get(k, ""))) for r in rows + [{}]) + 2 for k in keys]
    widths = [max(w, len(k) + 2) for w, k in zip(widths, keys)]
    header = " | ".join(k.ljust(w) for k, w in zip(keys, widths))
    print("  " + header)
    print("  " + "-" * len(header))
    for row in rows[:30]:
        print("  " + " | ".join(str(row.get(k, "")).ljust(w) for k, w in zip(keys, widths)))
    if len(rows) > 30:
        print(f"  ... и ещё {len(rows)-30} строк")


if __name__ == "__main__":
    section("Общая статистика")
    stats = run("""
        SELECT
            (SELECT COUNT(*) FROM crawl_log WHERE status='ok') AS profiles_ok,
            (SELECT COUNT(*) FROM crawl_log WHERE status='not_found') AS not_found,
            (SELECT COUNT(*) FROM crawl_log WHERE status='error') AS errors,
            (SELECT COUNT(*) FROM results) AS total_results
    """)
    print_table(stats)

    section("Топ-10 участников по числу стартов")
    print_table(run("""
        SELECT p.id, p.name, p.city, p.gender,
               COUNT(r.id) AS races,
               SUM(r.distance_km) AS total_km
        FROM profiles p
        JOIN results r ON r.profile_id = p.id
        GROUP BY p.id
        ORDER BY races DESC
        LIMIT 10
    """))

    section("Распределение по видам спорта")
    print_table(run("""
        SELECT sport,
               COUNT(*) AS starts,
               COUNT(DISTINCT profile_id) AS athletes,
               ROUND(AVG(distance_km), 1) AS avg_km
        FROM results
        WHERE sport != ''
        GROUP BY sport
        ORDER BY starts DESC
    """))

    section("Топ городов по участникам")
    print_table(run("""
        SELECT city, COUNT(*) AS athletes
        FROM profiles
        WHERE city != '' AND error IS NULL
        GROUP BY city
        ORDER BY athletes DESC
        LIMIT 15
    """))

    section("Популярные события")
    print_table(run("""
        SELECT event_name,
               COUNT(*) AS finishers,
               COUNT(DISTINCT profile_id) AS athletes,
               MIN(event_date) AS first_date,
               MAX(event_date) AS last_date
        FROM results
        WHERE event_name != ''
        GROUP BY event_name
        ORDER BY finishers DESC
        LIMIT 15
    """))

    section("Пример данных одного участника (первый с результатами)")
    profiles_with_results = run("""
        SELECT DISTINCT profile_id FROM results LIMIT 1
    """)
    if profiles_with_results:
        pid = profiles_with_results[0]["profile_id"]
        profile = run("SELECT * FROM profiles WHERE id=?", (pid,))
        print(f"\n  Профиль #{pid}:")
        print_table(profile)
        print(f"\n  Результаты:")
        print_table(run("""
            SELECT event_name, event_date, distance_km, sport,
                   finish_time, place_abs, category
            FROM results WHERE profile_id=?
            ORDER BY event_date
        """, (pid,)))

    section("Возрастное распределение")
    print_table(run("""
        SELECT
            CASE
                WHEN age < 20 THEN 'до 20'
                WHEN age < 35 THEN '20-34'
                WHEN age < 50 THEN '35-49'
                WHEN age < 65 THEN '50-64'
                ELSE '65+'
            END AS age_group,
            COUNT(*) AS athletes
        FROM profiles
        WHERE age IS NOT NULL
        GROUP BY age_group
        ORDER BY age_group
    """))
