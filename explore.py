"""
Просмотр и проверка собранных данных.
Запускай после crawler.py.

    python explore.py
"""

import sqlite3, json
from pathlib import Path

DB_PATH = Path("profiles.db")

def q(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def section(t):
    print(f"\n{'='*62}\n  {t}\n{'='*62}")

def tbl(rows, limit=20):
    if not rows:
        print("  (нет данных)"); return
    keys = list(rows[0].keys())
    ws = [max(max(len(str(r.get(k,''))) for r in rows), len(k)) + 1 for k in keys]
    hdr = " | ".join(k.ljust(w) for k,w in zip(keys,ws))
    print("  "+hdr)
    print("  "+"-"*len(hdr))
    for row in rows[:limit]:
        print("  "+" | ".join(str(row.get(k,'')).ljust(w) for k,w in zip(keys,ws)))
    if len(rows)>limit: print(f"  ... ещё {len(rows)-limit}")

section("Общая статистика сбора")
tbl(q("""
    SELECT
        (SELECT COUNT(*) FROM crawl_log WHERE status='ok')        AS собрано,
        (SELECT COUNT(*) FROM crawl_log WHERE status='not_found') AS не_найдено,
        (SELECT COUNT(*) FROM crawl_log WHERE status='error')     AS ошибок,
        (SELECT COUNT(*) FROM results)                             AS всего_стартов,
        (SELECT COUNT(DISTINCT competition_id) FROM results)       AS уникальных_событий
"""))

section("Топ-15 участников по числу стартов")
tbl(q("""
    SELECT p.id,
           p.last_name || ' ' || p.first_name AS имя,
           p.gender AS пол, p.age AS возраст, p.city AS город,
           COUNT(r.result_id) AS стартов,
           ROUND(SUM(r.distance_km)) AS км,
           p.stat_first AS победы
    FROM profiles p
    JOIN results r ON r.profile_id = p.id
    WHERE p.error IS NULL OR p.error = ''
    GROUP BY p.id
    ORDER BY стартов DESC
    LIMIT 15
"""))

section("Распределение по видам спорта")
tbl(q("""
    SELECT sport AS вид,
           COUNT(*) AS стартов,
           COUNT(DISTINCT profile_id) AS участников,
           ROUND(AVG(distance_km),1) AS средняя_дист_км
    FROM results WHERE sport != ''
    GROUP BY sport ORDER BY стартов DESC
"""))

section("Топ-10 событий по числу финишёров")
tbl(q("""
    SELECT competition_title AS событие,
           competition_id AS id,
           COUNT(*) AS финишёров,
           COUNT(DISTINCT profile_id) AS участников,
           sport AS вид,
           MIN(competition_date) AS дата
    FROM results
    WHERE competition_title != ''
    GROUP BY competition_id
    ORDER BY финишёров DESC
    LIMIT 10
"""))

section("Топ-15 городов по числу участников")
tbl(q("""
    SELECT city AS город, region AS регион,
           COUNT(*) AS участников
    FROM profiles
    WHERE (error IS NULL OR error='') AND city != ''
    GROUP BY city ORDER BY участников DESC
    LIMIT 15
"""))

section("Возрастное распределение")
tbl(q("""
    SELECT
        CASE
            WHEN age < 20        THEN 'до 20'
            WHEN age BETWEEN 20 AND 34 THEN '20–34'
            WHEN age BETWEEN 35 AND 49 THEN '35–49'
            WHEN age BETWEEN 50 AND 64 THEN '50–64'
            ELSE '65+'
        END AS группа,
        COUNT(*) AS участников,
        ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM profiles WHERE age IS NOT NULL),1) AS процент
    FROM profiles WHERE age IS NOT NULL
    GROUP BY группа ORDER BY группа
"""))

section("Пол")
tbl(q("""
    SELECT gender AS пол, COUNT(*) AS участников
    FROM profiles WHERE gender != '' AND (error IS NULL OR error='')
    GROUP BY gender
"""))

section("Активность по годам")
tbl(q("""
    SELECT year AS год,
           COUNT(*) AS стартов,
           COUNT(DISTINCT profile_id) AS участников,
           COUNT(DISTINCT competition_id) AS событий
    FROM results
    GROUP BY year ORDER BY год DESC
"""))

section("Пример профиля (первый с наибольшим числом стартов)")
rows = q("""
    SELECT profile_id FROM results
    GROUP BY profile_id ORDER BY COUNT(*) DESC LIMIT 1
""")
if rows:
    pid = rows[0]["profile_id"]
    print(f"\n  Профиль #{pid}:")
    tbl(q("SELECT id,last_name,first_name,gender,age,city,region,club,"
          "stat_competitions,stat_kilometers,stat_first FROM profiles WHERE id=?", (pid,)))
    print(f"\n  Последние 10 стартов:")
    tbl(q("""
        SELECT year, competition_title, competition_date,
               ROUND(distance_km,1) AS км, sport,
               finish_time, place_abs, group_name
        FROM results WHERE profile_id=?
        ORDER BY competition_date DESC LIMIT 10
    """, (pid,)))
