"""
Слой SQL-запросов к marathon.db для analytics.py и Streamlit-дашборда.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", "marathon.db"))


@contextmanager
def connect(db_path: Path | str | None = None) -> Iterator[sqlite3.Connection]:
    path = Path(db_path) if db_path is not None else DEFAULT_DB
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def q_all(db_path: Path | str, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connect(db_path) as c:
        return [dict(r) for r in c.execute(sql, params).fetchall()]


def q_one(db_path: Path | str, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connect(db_path) as c:
        row = c.execute(sql, params).fetchone()
        return dict(row) if row else None


def db_exists(db_path: Path | str | None = None) -> bool:
    return Path(db_path or DEFAULT_DB).is_file()


def ensure_cup_scoring_schema(db_path: Path | str | None = None) -> None:
    """Создаёт таблицы cup_scoring_computed_* в существующей БД (CREATE IF NOT EXISTS)."""
    import cup_scoring

    path = Path(db_path) if db_path is not None else DEFAULT_DB
    if not path.is_file():
        return
    with connect(path) as c:
        cup_scoring.ensure_cup_scoring_tables(c)


# ── Сводка и сезон ────────────────────────────────────────────────────────


def query_summary_row(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT
          (SELECT COUNT(*) FROM competitions)               AS соревнований,
          (SELECT COUNT(*) FROM distances)                  AS дистанций,
          (SELECT COUNT(*) FROM results)                    AS результатов,
          (SELECT COUNT(DISTINCT profile_id) FROM results WHERE profile_id IS NOT NULL)
                                                            AS уникальных_участников,
          (SELECT COUNT(*) FROM cups)                       AS кубков,
          (SELECT COUNT(*) FROM cup_results)               AS результатов_кубков,
          (SELECT COUNT(*) FROM profiles)                  AS профилей
        """,
    )


def query_events_by_year_sport(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT year AS год, sport AS вид,
               COUNT(*) AS событий,
               SUM(cs.total_members) AS стартов
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE year IS NOT NULL
        GROUP BY year, sport
        ORDER BY year DESC, событий DESC
        """,
    )


def query_distinct_years(db_path: Path | str) -> list[int]:
    rows = q_all(
        db_path,
        "SELECT DISTINCT year AS y FROM competitions WHERE year IS NOT NULL ORDER BY y DESC",
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_season_metrics(db_path: Path | str, year: int) -> dict[str, Any] | None:
    return q_one(
        db_path,
        """
        SELECT
          COUNT(DISTINCT c.id) AS events,
          COUNT(CASE WHEN r.dnf = 0 THEN 1 END) AS finishes,
          COUNT(DISTINCT CASE WHEN r.dnf = 0 AND r.profile_id IS NOT NULL
                              THEN r.profile_id END) AS unique_athletes,
          ROUND(COALESCE(SUM(CASE WHEN r.dnf = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS total_km
        FROM competitions c
        LEFT JOIN results r ON r.competition_id = c.id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE c.year = ?
        """,
        (year,),
    )


def query_season_sport_breakdown(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.sport AS вид,
               COUNT(DISTINCT c.id) AS событий,
               COUNT(CASE WHEN r.dnf = 0 THEN 1 END) AS финишей
        FROM competitions c
        LEFT JOIN results r ON r.competition_id = c.id
        WHERE c.year = ?
        GROUP BY c.sport
        ORDER BY финишей DESC
        """,
        (year,),
    )


def query_season_monthly_finishes(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT substr(c.date, 1, 7) AS month,
               COUNT(r.id) AS finishes
        FROM competitions c
        JOIN results r ON r.competition_id = c.id AND r.dnf = 0
        WHERE c.year = ? AND c.date IS NOT NULL AND length(c.date) >= 7
        GROUP BY month
        ORDER BY month
        """,
        (year,),
    )


def query_competitions_for_year(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.id, c.title AS событие, c.date AS дата, c.sport AS вид,
               cs.total_members AS участников
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE c.year = ?
        ORDER BY c.date DESC NULLS LAST, c.id
        """,
        (year,),
    )


# ── Участники (топы и распределения) ───────────────────────────────────────


def query_participants_top(db_path: Path | str, limit: int = 20) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT p.id,
               p.last_name || ' ' || p.first_name AS имя,
               p.gender AS пол, p.age AS возраст,
               p.city AS город,
               COUNT(r.id) AS стартов,
               ROUND(SUM(r.finish_time_sec)/3600.0, 1) AS часов_на_трассе,
               p.stat_first AS побед
        FROM profiles p
        JOIN results r ON r.profile_id = p.id AND r.dnf = 0
        GROUP BY p.id
        ORDER BY стартов DESC
        LIMIT {int(limit)}
        """,
    )


def query_gender_age_distribution(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT
            CASE p.gender WHEN 'm' THEN 'Мужчины' ELSE 'Женщины' END AS пол,
            CASE
                WHEN p.age < 20        THEN 'до 20'
                WHEN p.age BETWEEN 20 AND 34 THEN '20–34'
                WHEN p.age BETWEEN 35 AND 49 THEN '35–49'
                WHEN p.age BETWEEN 50 AND 64 THEN '50–64'
                ELSE '65+'
            END AS возраст,
            COUNT(DISTINCT p.id) AS участников,
            COUNT(r.id) AS стартов
        FROM profiles p
        JOIN results r ON r.profile_id = p.id
        WHERE p.gender != ''
        GROUP BY пол, возраст
        ORDER BY пол, возраст
        """,
    )


def query_cities_top(db_path: Path | str, limit: int = 15) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT p.city AS город, p.region AS регион,
               COUNT(DISTINCT p.id) AS участников,
               COUNT(r.id) AS стартов
        FROM profiles p
        JOIN results r ON r.profile_id = p.id
        WHERE p.city != ''
        GROUP BY p.city
        ORDER BY участников DESC
        LIMIT {int(limit)}
        """,
    )


def query_profile_search(db_path: Path | str, needle: str, limit: int = 50) -> list[dict[str, Any]]:
    n = (needle or "").strip()
    if not n:
        return []
    lim = int(limit)
    if n.isdigit():
        return q_all(
            db_path,
            f"SELECT id, first_name, last_name, city, gender, age FROM profiles WHERE id = ? LIMIT {lim}",
            (int(n),),
        )
    pat = f"%{n}%"
    return q_all(
        db_path,
        f"""
        SELECT id, first_name, last_name, city, gender, age
        FROM profiles
        WHERE last_name LIKE ? OR first_name LIKE ?
        ORDER BY last_name, first_name
        LIMIT {lim}
        """,
        (pat, pat),
    )


# ── События (отчёты и карточка) ─────────────────────────────────────────────


def query_competitions_top(db_path: Path | str, limit: int = 20) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.id, c.title AS событие, c.year AS год,
               c.sport AS вид,
               cs.total_members AS участников,
               cs.male AS м, cs.female AS ж,
               cs.regions AS регионов
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        ORDER BY участников DESC NULLS LAST
        LIMIT {int(limit)}
        """,
    )


def query_avg_finish_by_sport_distance(db_path: Path | str, limit: int = 30) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.sport AS вид,
               d.name AS дистанция,
               ROUND(d.distance_km, 1) AS км,
               COUNT(r.id) AS финишей,
               MIN(r.finish_time) AS лучшее,
               TIME(ROUND(AVG(r.finish_time_sec)), 'unixepoch') AS среднее
        FROM results r
        JOIN distances d ON d.id = r.distance_id
        JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND r.finish_time_sec IS NOT NULL
          AND d.distance_km > 0
        GROUP BY c.sport, d.id
        ORDER BY c.sport, км DESC
        LIMIT {int(limit)}
        """,
    )


def query_dnf_events_top(db_path: Path | str, limit: int = 15) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.title AS событие, c.year AS год,
               cs.total_members AS стартов,
               cs.dnf AS сходов,
               ROUND(100.0 * cs.dnf / NULLIF(cs.total_members, 0), 1) AS dnf_pct
        FROM competition_stats cs
        JOIN competitions c ON c.id = cs.competition_id
        WHERE cs.dnf > 0
        ORDER BY dnf_pct DESC
        LIMIT {int(limit)}
        """,
    )


def query_competition_header(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.title, c.date, c.year, c.sport,
               cs.total_members AS участников,
               cs.male AS мужчин, cs.female AS женщин,
               cs.teams AS команд, cs.regions AS регионов,
               cst.status
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        LEFT JOIN competition_status cst ON cst.competition_id = c.id
        WHERE c.id = ?
        """,
        (comp_id,),
    )


def query_competition_distances(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT d.id, d.name, d.distance_km AS км, d.is_relay AS эстафета,
               COUNT(r.id) AS финишёров
        FROM distances d
        LEFT JOIN results r ON r.distance_id = d.id AND r.dnf = 0
        WHERE d.competition_id = ?
        GROUP BY d.id
        """,
        (comp_id,),
    )


def query_competition_top10(db_path: Path | str, comp_id: int, limit: int = 30) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT r.place_abs AS место,
               p.last_name || ' ' || p.first_name AS участник,
               p.city AS город, r.group_name AS группа,
               r.finish_time AS время, d.name AS дистанция
        FROM results r
        LEFT JOIN profiles p ON p.id = r.profile_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.competition_id = ? AND r.place_abs <= 10 AND r.dnf = 0
        ORDER BY d.id, r.place_abs
        LIMIT {int(limit)}
        """,
        (comp_id,),
    )


def query_competition_groups(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT r.group_name AS группа,
               COUNT(*) AS участников,
               MIN(r.finish_time) AS лучшее_время
        FROM results r
        WHERE r.competition_id = ? AND r.dnf = 0 AND r.group_name != ''
        GROUP BY r.group_name
        ORDER BY группа
        """,
        (comp_id,),
    )


# ── Кубки ─────────────────────────────────────────────────────────────────


def query_cups_summary(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.id, c.title AS кубок, c.year AS год, c.sport AS вид,
               COUNT(DISTINCT cc.competition_id) AS событий,
               COUNT(DISTINCT cr.profile_id) AS участников
        FROM cups c
        LEFT JOIN cup_competitions cc ON cc.cup_id = c.id
        LEFT JOIN cup_results cr ON cr.cup_id = c.id
        GROUP BY c.id
        ORDER BY год DESC
        """,
    )


def query_cup_leaders(db_path: Path | str, limit: int = 40) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT cu.title AS кубок,
               p.last_name || ' ' || p.first_name AS участник,
               cr.group_name AS группа,
               cr.place_abs AS место,
               ROUND(cr.total_points, 1) AS очков,
               cr.competitions_count AS стартов_в_кубке
        FROM cup_results cr
        JOIN cups cu ON cu.id = cr.cup_id
        LEFT JOIN profiles p ON p.id = cr.profile_id
        WHERE cr.place_abs <= 10
        ORDER BY cu.id, cr.distance_id, cr.place_abs
        LIMIT {int(limit)}
        """,
    )


def query_profile_cup_result_years(db_path: Path | str) -> list[int]:
    """Годы, для которых в базе есть строки profile_cup_results."""
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT year AS y
        FROM profile_cup_results
        WHERE year IS NOT NULL
        ORDER BY y DESC
        """,
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_profile_cup_summaries_for_year(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    """
    Сводка кубков за год по profile_cup_results: один ряд на cup_id.
    """
    return q_all(
        db_path,
        """
        SELECT
            pcr.cup_id AS id,
            MAX(COALESCE(cu.title, pcr.cup_title)) AS кубок,
            COUNT(DISTINCT pcr.profile_id) AS участников
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        WHERE pcr.year = ? AND pcr.cup_id IS NOT NULL
        GROUP BY pcr.cup_id
        ORDER BY MAX(COALESCE(cu.title, pcr.cup_title))
        """,
        (year,),
    )


def query_profile_cup_detail_for_year_cup(
    db_path: Path | str, year: int, cup_id: int
) -> list[dict[str, Any]]:
    """
    Строки profile_cup_results за год и кубок + ФИО из profiles.
    Места из официальной таблицы cup_results (JOIN по cup_id, profile_id, distance_id):
    cup_place_abs / cup_place_gender / cup_place_group; pcr_place_abs — fallback из profile_cup_results.

    Колонка ``очков``: из ``cup_results.raw`` — массив ``competition_points[]`` (поля
    ``competition_id`` + ``points``), элемент с ``competition_id`` как у отображаемого этапа
    (тот же подзапрос, что для «Событие»); иначе скаляр в ``$.competition_points`` для старых
    дампов; иначе ``cup_scoring_computed_finishes`` / ``profile_cup_results.total_points``.
    """
    ensure_cup_scoring_schema(db_path)
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    comp_one = _sql_profile_cup_stage_competition_id_one("pcr")
    # Массив объектов {competition_id, points, ...} — см. API кубка в cup_results.raw
    pts_from_cr_array = f"""COALESCE(
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.competition_points') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      ),
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.competitionPoints') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      ),
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.result.competition_points') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      )
    )"""
    pts_from_cr_scalar = """(
      CASE
        WHEN cr.raw IS NULL OR TRIM(COALESCE(cr.raw, '')) = '' THEN NULL
        WHEN json_type(cr.raw, '$.competition_points') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.competition_points') AS REAL)
        WHEN json_type(cr.raw, '$.competitionPoints') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.competitionPoints') AS REAL)
        WHEN json_type(cr.raw, '$.result.competition_points') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.result.competition_points') AS REAL)
        WHEN json_type(cr.raw, '$.result.competitionPoints') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.result.competitionPoints') AS REAL)
        ELSE NULL
      END
    )"""
    pts_from_cr_raw = f"""COALESCE({pts_from_cr_array}, {pts_from_cr_scalar})"""
    return q_all(
        db_path,
        f"""
        SELECT
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            COALESCE(p.last_name, '') AS last_name,
            COALESCE(p.gender, '') AS gender,
            {ev} AS событие,
            {dist} AS дистанция,
            pcr.group_name AS группа,
            COALESCE(
              {pts_from_cr_raw},
              (
                SELECT CAST(f.points_awarded AS REAL)
                FROM cup_scoring_computed_finishes f
                WHERE f.rule_version = '2026_run_v1'
                  AND f.cup_id = pcr.cup_id
                  AND f.year = pcr.year
                  AND f.profile_id = pcr.profile_id
                  AND f.competition_id = ({comp_one})
                LIMIT 1
              ),
              ROUND(pcr.total_points, 1)
            ) AS очков,
            pcr.raw AS raw,
            pcr.place_abs AS pcr_place_abs,
            cr.place_abs AS cup_place_abs,
            cr.place_gender AS cup_place_gender,
            cr.place_group AS cup_place_group
        FROM profile_cup_results pcr
        LEFT JOIN profiles p ON p.id = pcr.profile_id
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.year = ? AND pcr.cup_id = ?
        ORDER BY {dist},
                 (COALESCE(cr.place_abs, pcr.place_abs) IS NULL),
                 COALESCE(cr.place_abs, pcr.place_abs),
                 участник
        """,
        (year, cup_id),
    )


def cup_detail_resolve_display_place(
    row: dict[str, Any],
    gender_mode: str,
    age_group: str,
) -> Any:
    """
    Значение колонки «место» для таблицы результатов кубка:
    при выбранной возрастной группе — cup_results.place_group;
    при фильтре «Мужчины»/«Женщины» — cup_results.place_gender;
    в абсолютном зачёте — cup_results.place_abs.
    Если в cup_results нет строки — fallback на place_abs из profile_cup_results (pcr_place_abs).
    """
    pcr_abs = row.get("pcr_place_abs")
    ca = row.get("cup_place_abs")
    cg = row.get("cup_place_gender")
    cgr = row.get("cup_place_group")
    if age_group and str(age_group).strip() and age_group != "Все":
        return cgr if cgr is not None else pcr_abs
    if gender_mode in ("Мужчины", "Женщины"):
        return cg if cg is not None else pcr_abs
    return ca if ca is not None else pcr_abs


def query_cup_team_score_rows(db_path: Path | str, cup_id: int, year: int) -> list[dict[str, Any]]:
    """
    Строки очков кубка (как на вкладке «Личное первенство»): profile_cup_results,
    только участники, у которых в финишах соревнований этого кубка за год
    указана команда (results.team через cup_competitions).
    Очки — total_points из profile_cup_results; команда — MIN(TRIM(team)) по финишам в зачёте кубка.
    """
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT
            tm.team AS команда,
            pcr.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            {cupn} AS кубок,
            {ev} AS событие,
            {dist} AS дистанция,
            COALESCE(pcr.group_name, '') AS группа,
            pcr.total_points AS очков,
            pcr.raw AS raw,
            COALESCE(cr.place_abs, pcr.place_abs) AS место_абс
        FROM profile_cup_results pcr
        INNER JOIN (
            SELECT r.profile_id, MIN(TRIM(r.team)) AS team
            FROM results r
            INNER JOIN cup_competitions cc
              ON cc.competition_id = r.competition_id AND cc.cup_id = ?
            INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
            WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
            GROUP BY r.profile_id
        ) tm ON tm.profile_id = pcr.profile_id
        LEFT JOIN profiles p ON p.id = pcr.profile_id
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.year = ? AND pcr.cup_id = ?
        ORDER BY tm.team, {dist}, участник
        """,
        (cup_id, year, year, cup_id),
    )


def query_profile_cup_results_lines_for_member(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> list[dict[str, Any]]:
    """
    Все строки **profile_cup_results** участника по кубку и году (одна строка ответа = одна строка БД):
    событие, дистанция, место абсолют (cup_results / fallback pcr), очки, raw для времени.
    """
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    return q_all(
        db_path,
        f"""
        SELECT
            {ev} AS событие,
            {dist} AS дистанция,
            COALESCE(cr.place_abs, pcr.place_abs) AS место_абс,
            pcr.total_points AS очков,
            pcr.raw AS raw
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )


def query_profile_cup_team_member_competition_rows(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> list[dict[str, Any]]:
    """
    Все финиши участника в соревнованиях кубка за год: по строке на **results**
    (cup_competitions связывает competition_id с cup_id).
    Колонка ``очков`` — ``profile_cup_results.total_points`` только при явном совпадении
    ``competition.id`` / ``competition_id`` / ``result.competition.id`` в **raw** с
    ``results.competition_id`` (в т.ч. через CAST). Сопоставление по названию — в UI
    через ``map_profile_cup_points_by_title_distance`` (только текст из **raw**), иначе
    снова схлопывается в один этап.
    """
    return q_all(
        db_path,
        """
        SELECT
            c.title AS событие,
            COALESCE(NULLIF(TRIM(d.name), ''), '') AS дистанция,
            r.place_abs AS место_абс,
            r.competition_id AS competition_id,
            (
              SELECT pcr.total_points
              FROM profile_cup_results pcr
              WHERE pcr.profile_id = r.profile_id
                AND pcr.cup_id = ?
                AND pcr.year = ?
                AND (
                  json_extract(pcr.raw, '$.competition.id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.competition.id') AS INTEGER) = r.competition_id
                  OR json_extract(pcr.raw, '$.competition_id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.competition_id') AS INTEGER) = r.competition_id
                  OR json_extract(pcr.raw, '$.result.competition.id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.result.competition.id') AS INTEGER) = r.competition_id
                )
              ORDER BY pcr.id
              LIMIT 1
            ) AS очков,
          COALESCE(NULLIF(TRIM(r.finish_time), ''), '') AS время
        FROM results r
        INNER JOIN cup_competitions cc
          ON cc.competition_id = r.competition_id AND cc.cup_id = ?
        INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
        INNER JOIN distances d ON d.id = r.distance_id AND d.competition_id = c.id
        WHERE r.profile_id = ?
          AND COALESCE(r.dnf, 0) = 0
        ORDER BY c.date ASC, c.id ASC, d.id ASC
        """,
        (cup_id, year, cup_id, year, profile_id),
    )


def aggregate_team_cup_points_top_five(members: list[dict[str, Any]]) -> int:
    """
    Командный зачёт: сумма индивидуальных очков пяти лучших участников.
    По каждому участнику — сумма int(round(total_points)) по его строкам profile_cup_results в этой выборке.
    """
    from collections import defaultdict

    by_pid: dict[int, int] = defaultdict(int)
    for r in members:
        pid = int(r["profile_id"])
        v = r.get("очков")
        try:
            row_pts = int(round(float(v))) if v is not None else 0
        except (TypeError, ValueError):
            row_pts = 0
        by_pid[pid] += row_pts
    scores = sorted(by_pid.values(), reverse=True)
    return int(sum(scores[:5]))


def parse_profile_cup_raw_age_group_label(raw: str | None) -> str:
    """
    Метка возрастной/зачётной группы из profile_cup_results.raw
    (JSON элемента списка API …/profile/{id}/cup-results/?year=).
    """
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    group = item.get("group")
    if isinstance(group, dict):
        name = group.get("name") or group.get("title")
        if name is not None and str(name).strip():
            return str(name).strip()
        try:
            af = group.get("age_from")
            at = group.get("age_to")
            if af is not None and at is not None:
                return f"{int(af)}-{int(at)}"
        except (TypeError, ValueError):
            pass
    return ""


def parse_profile_cup_raw_finish_time(raw: str | None) -> str:
    """Время финиша из JSON profile_cup_results.raw (если API отдал)."""
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    for key in ("total_time", "finish_time", "time", "totalTime", "result_time"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    res = item.get("result")
    if isinstance(res, dict):
        for key in ("total_time", "finish_time", "time", "total_time"):
            v = res.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def parse_profile_cup_raw_event_title(raw: str | None) -> str:
    """Название события из JSON profile_cup_results.raw (если API отдал вложенный объект)."""
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    comp = item.get("competition")
    if isinstance(comp, dict):
        t = comp.get("title") or comp.get("title_short") or comp.get("name")
        if t is not None and str(t).strip():
            return str(t).strip()
    for key in ("competition_title", "event_title", "marathon_title"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    res = item.get("result")
    if isinstance(res, dict):
        c2 = res.get("competition")
        if isinstance(c2, dict):
            t2 = c2.get("title") or c2.get("title_short") or c2.get("name")
            if t2 is not None and str(t2).strip():
                return str(t2).strip()
    return ""


def parse_profile_cup_raw_competition_id(raw: str | None) -> int | None:
    """Идентификатор соревнования из JSON profile_cup_results.raw (связь со строкой results.competition_id)."""
    if not raw or not str(raw).strip():
        return None
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(item, dict):
        return None
    def _to_int_id(v: Any) -> int | None:
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        try:
            return int(float(str(v).strip()))
        except (TypeError, ValueError):
            return None

    for key in ("competition_id", "competitionId", "event_id", "marathon_id", "eventId"):
        got = _to_int_id(item.get(key))
        if got is not None:
            return got
    mar = item.get("marathon")
    if isinstance(mar, dict):
        got = _to_int_id(mar.get("id"))
        if got is not None:
            return got
    comp = item.get("competition")
    if isinstance(comp, dict):
        for k in ("id", "pk", "competition_id"):
            got = _to_int_id(comp.get(k))
            if got is not None:
                return got
    res = item.get("result")
    if isinstance(res, dict):
        c2 = res.get("competition")
        if isinstance(c2, dict):
            got = _to_int_id(c2.get("id"))
            if got is not None:
                return got
    return None


def map_profile_cup_points_by_competition_id(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> dict[int, float]:
    """
    Словарь competition_id → total_points из **profile_cup_results** (по ``raw``).
    При нескольких строках с одним id берётся последняя по ``pcr.id``.
    """
    rows = q_all(
        db_path,
        """
        SELECT id, raw, total_points
        FROM profile_cup_results
        WHERE profile_id = ? AND cup_id = ? AND year = ?
        ORDER BY id
        """,
        (profile_id, cup_id, year),
    )
    out: dict[int, float] = {}
    for r in rows:
        cid = parse_profile_cup_raw_competition_id(r.get("raw"))
        if cid is None:
            continue
        v = r.get("total_points")
        try:
            out[cid] = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            out[cid] = 0.0
    return out


def norm_cup_match_title(title: str | None) -> str:
    """Нормализация названия для сопоставления строк кубка (без года в скобках)."""
    if not title or not str(title).strip():
        return ""
    s = str(title).strip().casefold()
    if " (" in s:
        s = s.split(" (", 1)[0].strip()
    return " ".join(s.split())


def norm_cup_match_distance(dist: str | None) -> str:
    """Нормализация подписи дистанции."""
    if not dist or not str(dist).strip():
        return ""
    s = str(dist).strip().casefold().replace(",", ".")
    s = s.replace("км", "km")
    return " ".join(s.split())


def profile_cup_raw_event_title_strings(raw: str | None) -> list[str]:
    """Все варианты названия этапа из ``profile_cup_results.raw`` (как для карты очков)."""
    raw_titles: list[str] = []

    def _add_title(s: str | None) -> None:
        if s is None or not str(s).strip():
            return
        t0 = str(s).strip()
        if t0 not in raw_titles:
            raw_titles.append(t0)

    _add_title(parse_profile_cup_raw_event_title(raw))
    try:
        item = json.loads(raw or "")
    except (json.JSONDecodeError, TypeError):
        item = None
    if isinstance(item, dict):
        comp = item.get("competition")
        if isinstance(comp, dict):
            for k in ("title_short", "title", "name"):
                v2 = comp.get(k)
                if v2 is not None and str(v2).strip():
                    _add_title(str(v2))
        rc = item.get("result")
        if isinstance(rc, dict):
            c3 = rc.get("competition")
            if isinstance(c3, dict):
                for k in ("title_short", "title", "name"):
                    v3 = c3.get(k)
                    if v3 is not None and str(v3).strip():
                        _add_title(str(v3))
    return raw_titles


def profile_cup_title_norms_from_raw(raw: str | None) -> set[str]:
    """Нормализованные названия этапа из ``raw``."""
    out: set[str] = set()
    for t in profile_cup_raw_event_title_strings(raw):
        n = norm_cup_match_title(t)
        if n:
            out.add(n)
    return out


def profile_cup_distance_norms_from_pcr_row(row: dict[str, Any]) -> set[str]:
    """Нормализованные подписи дистанции из строки ``profile_cup_results`` (+ ``cd``)."""
    out: set[str] = set()
    for k in ("distance_name", "cd_name"):
        v = row.get(k)
        if v is not None and str(v).strip():
            n = norm_cup_match_distance(str(v).strip())
            if n:
                out.add(n)
    return out


def assign_profile_cup_points_to_result_lines(
    db_path: Path | str,
    profile_id: int,
    cup_id: int,
    year: int,
    lines: list[dict[str, Any]],
) -> list[int | None]:
    """
    Для строк из ``query_profile_cup_team_member_competition_rows`` (есть ``competition_id``)
    сопоставляет каждую строку финиша с **одной** записью ``profile_cup_results`` без повторов:
    сначала по ``competition.id`` в ``raw``, иначе по паре (нормализованное название, дистанция).

    Для таблицы только из ``profile_cup_results`` (без ключа ``competition_id``) возвращает
    список из ``None`` — там очки уже в колонке ``очков``.
    """
    if not lines:
        return []
    if not all("competition_id" in ln for ln in lines):
        return [None] * len(lines)

    pcr_rows = q_all(
        db_path,
        """
        SELECT pcr.id, pcr.raw, pcr.total_points, pcr.distance_name, cd.name AS cd_name
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )
    if not pcr_rows:
        return [None] * len(lines)

    used: set[int] = set()
    out: list[int | None] = []

    def _pts_int(row: dict[str, Any]) -> int | None:
        v = row.get("total_points")
        try:
            return int(round(float(v))) if v is not None else None
        except (TypeError, ValueError):
            return None

    for line in lines:
        best: dict[str, Any] | None = None
        lcid = line.get("competition_id")
        lcid_i: int | None = None
        if lcid is not None:
            try:
                lcid_i = int(lcid)
            except (TypeError, ValueError):
                lcid_i = None

        if lcid_i is not None:
            for r in pcr_rows:
                rid = int(r["id"])
                if rid in used:
                    continue
                if parse_profile_cup_raw_competition_id(r.get("raw")) == lcid_i:
                    best = r
                    break

        if best is None:
            lt = norm_cup_match_title((line.get("событие") or "").strip())
            ld = norm_cup_match_distance((line.get("дистанция") or "").strip())
            if lt and ld:
                cands: list[dict[str, Any]] = []
                for r in pcr_rows:
                    rid = int(r["id"])
                    if rid in used:
                        continue
                    tns = profile_cup_title_norms_from_raw(r.get("raw"))
                    dns = profile_cup_distance_norms_from_pcr_row(r)
                    if lt in tns and ld in dns:
                        cands.append(r)
                if len(cands) == 1:
                    best = cands[0]
                elif len(cands) > 1:
                    best = min(cands, key=lambda x: int(x["id"]))

        if best is not None:
            used.add(int(best["id"]))
            out.append(_pts_int(best))
        else:
            out.append(None)

    return out


def _map_cup_register_title_distance_keys(
    out: dict[tuple[str, str], float], titles: list[str], d: str, pts: float
) -> None:
    """Пишет ключи (title_norm, d) либо (title_norm, '') если дистанции в строке pcr нет."""
    seen_t: set[str] = set()
    for tit in titles:
        t = norm_cup_match_title(tit)
        if not t or t in seen_t:
            continue
        seen_t.add(t)
        if d:
            out[(t, d)] = pts
        else:
            out[(t, "")] = pts


def map_profile_cup_points_by_title_distance(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> dict[tuple[str, str], float]:
    """
    Ключ (нормализованное название, нормализованная дистанция) → ``total_points``
    по строкам **profile_cup_results**.

    Используются только названия из **raw** (и поля ``competition.*`` внутри JSON), без
    «разрешённого» заголовка этапа из SQL: иначе для нескольких строк кубка получается
    один и тот же этап (последний по дате), все ключи схлопываются и в UI остаётся одна
    сумма очков.
    """
    rows = q_all(
        db_path,
        """
        SELECT pcr.raw, pcr.total_points, pcr.distance_name, cd.name AS cd_name
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )
    out: dict[tuple[str, str], float] = {}
    for r in rows:
        d_raw = (r.get("distance_name") or "").strip()
        if not d_raw:
            d_raw = (r.get("cd_name") or "").strip()
        d = norm_cup_match_distance(d_raw)
        v = r.get("total_points")
        try:
            pts = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            pts = 0.0
        raw_titles = profile_cup_raw_event_title_strings(r.get("raw"))
        if not raw_titles and not d:
            continue
        if raw_titles:
            _map_cup_register_title_distance_keys(out, raw_titles, d, pts)
        elif d:
            # только дистанция без названия — не создаём ключи, чтобы не размазать очки
            pass
    return out


def _sql_profile_cup_stage_event_title_one(pcr: str = "pcr") -> str:
    """Одно соревнование (последнее по дате среди подходящих под строку profile_cup_results)."""
    return f"""(
      SELECT c2.title
      FROM cup_competitions cc2
      INNER JOIN competitions c2 ON c2.id = cc2.competition_id AND c2.year = {pcr}.year
      INNER JOIN results r2 ON r2.competition_id = c2.id
        AND r2.profile_id = {pcr}.profile_id AND COALESCE(r2.dnf, 0) = 0
      LEFT JOIN distances d2 ON d2.id = r2.distance_id AND d2.competition_id = c2.id
      LEFT JOIN cup_distances cd2 ON cd2.id = {pcr}.distance_id AND cd2.cup_id = {pcr}.cup_id
      WHERE cc2.cup_id = {pcr}.cup_id
        AND (
          cd2.id IS NULL
          OR (
            (d2.distance_km IS NOT NULL AND cd2.distance_km IS NOT NULL
              AND ABS(d2.distance_km - cd2.distance_km) < 0.06)
            OR (TRIM(COALESCE(d2.name, '')) != ''
              AND TRIM(d2.name) = TRIM(COALESCE(cd2.name, '')))
            OR (TRIM(COALESCE({pcr}.distance_name, '')) != ''
              AND TRIM(d2.name) = TRIM({pcr}.distance_name))
          )
        )
      ORDER BY c2.date DESC, c2.id DESC
      LIMIT 1
    )"""


def _sql_profile_cup_stage_competition_id_one(pcr: str = "pcr") -> str:
    """Тот же этап, что ``_sql_profile_cup_stage_event_title_one``, но ``competitions.id``."""
    return f"""(
      SELECT c2.id
      FROM cup_competitions cc2
      INNER JOIN competitions c2 ON c2.id = cc2.competition_id AND c2.year = {pcr}.year
      INNER JOIN results r2 ON r2.competition_id = c2.id
        AND r2.profile_id = {pcr}.profile_id AND COALESCE(r2.dnf, 0) = 0
      LEFT JOIN distances d2 ON d2.id = r2.distance_id AND d2.competition_id = c2.id
      LEFT JOIN cup_distances cd2 ON cd2.id = {pcr}.distance_id AND cd2.cup_id = {pcr}.cup_id
      WHERE cc2.cup_id = {pcr}.cup_id
        AND (
          cd2.id IS NULL
          OR (
            (d2.distance_km IS NOT NULL AND cd2.distance_km IS NOT NULL
              AND ABS(d2.distance_km - cd2.distance_km) < 0.06)
            OR (TRIM(COALESCE(d2.name, '')) != ''
              AND TRIM(d2.name) = TRIM(COALESCE(cd2.name, '')))
            OR (TRIM(COALESCE({pcr}.distance_name, '')) != ''
              AND TRIM(d2.name) = TRIM({pcr}.distance_name))
          )
        )
      ORDER BY c2.date DESC, c2.id DESC
      LIMIT 1
    )"""


def _sql_profile_cup_stage_event_display(pcr: str = "pcr") -> str:
    """
    Одна строка на запись profile_cup_results: одно название события
    (по связке cup_competitions + results + дистанция этапа), иначе из raw JSON.
    """
    one = _sql_profile_cup_stage_event_title_one(pcr)
    return f"""COALESCE(
      NULLIF(TRIM(CAST({one} AS TEXT)), ''),
      NULLIF(TRIM(json_extract({pcr}.raw, '$.competition.title')), ''),
      NULLIF(TRIM(json_extract({pcr}.raw, '$.competition.title_short')), '')
    )"""


def _sql_profile_cup_distance_display(pcr: str = "pcr", cd: str = "cd") -> str:
    """Дистанция: из API-строки или из cup_distances по id."""
    return f"""COALESCE(
      NULLIF(TRIM({pcr}.distance_name), ''),
      {cd}.name,
      ''
    )"""


def _sql_profile_cup_name_from_cups(pcr: str = "pcr", cu: str = "cu") -> str:
    """Название кубка из таблицы cups, иначе из строки profile_cup_results."""
    return f"COALESCE({cu}.title, {pcr}.cup_title)"


# ── Профиль ───────────────────────────────────────────────────────────────


def query_profile_row(db_path: Path | str, pid: int) -> dict[str, Any] | None:
    return q_one(db_path, "SELECT * FROM profiles WHERE id=?", (pid,))


def query_profile_results_history(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.year AS год, c.title AS событие, c.sport AS вид,
               d.name AS дистанция, ROUND(d.distance_km,1) AS км,
               r.finish_time AS время,
               r.place_abs AS место_абс,
               r.place_gender AS место_пол,
               r.group_name AS группа
        FROM results r
        JOIN competitions c ON c.id = r.competition_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND r.dnf = 0
        ORDER BY c.date DESC
        """,
        (pid,),
    )


def query_profile_cup_rows(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT pcr.year AS год,
               {cupn} AS кубок,
               {ev} AS событие,
               {dist} AS дистанция,
               pcr.place_abs AS место, pcr.group_name AS группа,
               ROUND(pcr.total_points, 1) AS очков
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ?
        ORDER BY год DESC, кубок, событие
        """,
        (pid,),
    )


def parse_profile_active_years(raw: str | None) -> list[int]:
    """Годы активности из JSON в profiles.raw (ключ active_years, см. import_profiles_csv)."""
    if not raw or not str(raw).strip():
        return []
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(obj, dict):
        return []
    ay = obj.get("active_years")
    if not isinstance(ay, list):
        return []
    out: list[int] = []
    for x in ay:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return sorted(set(out), reverse=True)


def query_profile_participation_years(db_path: Path | str, pid: int) -> list[int]:
    """Годы, в которых у участника есть финиш в событиях или строка в profile_cup_results."""
    rows = q_all(
        db_path,
        """
        SELECT y FROM (
            SELECT DISTINCT c.year AS y
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE r.profile_id = ? AND r.dnf = 0 AND c.year IS NOT NULL
            UNION
            SELECT DISTINCT year AS y
            FROM profile_cup_results
            WHERE profile_id = ? AND year IS NOT NULL
        )
        ORDER BY y DESC
        """,
        (pid, pid),
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_profile_results_history_for_year(
    db_path: Path | str, pid: int, year: int
) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.year AS год, c.title AS событие, c.sport AS вид,
               d.name AS дистанция, ROUND(d.distance_km,1) AS км,
               r.finish_time AS время,
               r.place_abs AS место_абс,
               r.place_gender AS место_пол,
               r.group_name AS группа
        FROM results r
        JOIN competitions c ON c.id = r.competition_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND r.dnf = 0 AND c.year = ?
        ORDER BY c.date DESC
        """,
        (pid, year),
    )


def query_profile_cup_rows_for_year(
    db_path: Path | str, pid: int, year: int
) -> list[dict[str, Any]]:
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT pcr.year AS год,
               {cupn} AS кубок,
               {ev} AS событие,
               {dist} AS дистанция,
               pcr.place_abs AS место, pcr.group_name AS группа,
               ROUND(pcr.total_points, 1) AS очков
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.year = ?
        ORDER BY кубок, событие
        """,
        (pid, year),
    )


# ── Общая статистика (фильтры: год, вид спорта, кубок) ───────────────────────


def build_competition_filter_sql(
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> tuple[str, list[Any]]:
    """Фильтр по алиасу `c` (competitions). Пустой список / None = без ограничения по этому полю."""
    parts: list[str] = []
    params: list[Any] = []
    if years:
        parts.append("c.year IN (" + ",".join("?" * len(years)) + ")")
        params.extend(years)
    if sports:
        parts.append("c.sport IN (" + ",".join("?" * len(sports)) + ")")
        params.extend(sports)
    if cup_ids:
        parts.append(
            "c.id IN (SELECT competition_id FROM cup_competitions WHERE cup_id IN ("
            + ",".join("?" * len(cup_ids))
            + "))"
        )
        params.extend(cup_ids)
    return (" AND ".join(parts) if parts else "1=1"), params


def query_distinct_sports(db_path: Path | str) -> list[str]:
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT sport AS s FROM competitions
        WHERE sport IS NOT NULL AND TRIM(sport) != ''
        ORDER BY sport
        """,
    )
    return [str(r["s"]) for r in rows if r.get("s") is not None]


def query_cups_for_filter(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        "SELECT id, title, year FROM cups ORDER BY year DESC, title",
    )


def query_general_stats_cards(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> dict[str, Any]:
    """Карточки: события, уникальные участники (финиши), регионы, команды, страны."""
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)

    events = q_one(db_path, f"SELECT COUNT(*) AS n FROM competitions c WHERE {w}", params)
    total_events = int(events["n"]) if events else 0

    part = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT r.profile_id) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.profile_id IS NOT NULL AND r.dnf = 0 AND {w}
        """,
        params,
    )
    total_participants = int(part["n"]) if part and part.get("n") is not None else 0

    reg = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT reg_key) AS n FROM (
            SELECT CASE
                WHEN p.region_id IS NOT NULL THEN 'id:' || CAST(p.region_id AS TEXT)
                WHEN TRIM(COALESCE(p.region, '')) != '' THEN 'n:' || TRIM(p.region)
                ELSE NULL
            END AS reg_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE {w}
        ) t
        WHERE reg_key IS NOT NULL
        """,
        params,
    )
    regions = int(reg["n"]) if reg and reg.get("n") is not None else 0

    teams = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT TRIM(r.team)) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != '' AND {w}
        """,
        params,
    )
    total_teams = int(teams["n"]) if teams and teams.get("n") is not None else 0

    cntr = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT c_key) AS n FROM (
            SELECT CASE
                WHEN TRIM(COALESCE(p.country, '')) != '' THEN TRIM(p.country)
                ELSE NULL
            END AS c_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE {w}
        ) t
        WHERE c_key IS NOT NULL
        """,
        params,
    )
    countries = int(cntr["n"]) if cntr and cntr.get("n") is not None else 0

    return {
        "total_events": total_events,
        "total_participants": total_participants,
        "regions_distinct": regions,
        "teams_distinct": total_teams,
        "countries_distinct": countries,
    }


def query_chart_events_by_year(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT c.year AS year, COUNT(*) AS events
        FROM competitions c
        WHERE c.year IS NOT NULL AND {w}
        GROUP BY c.year
        ORDER BY c.year
        """,
        params,
    )


def query_chart_unique_participants_by_year(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT c.year AS year, COUNT(DISTINCT r.profile_id) AS participants
        FROM competitions c
        INNER JOIN results r ON r.competition_id = c.id
        WHERE c.year IS NOT NULL AND r.profile_id IS NOT NULL AND r.dnf = 0 AND {w}
        GROUP BY c.year
        ORDER BY c.year
        """,
        params,
    )


def query_chart_events_by_sport(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT COALESCE(NULLIF(TRIM(c.sport), ''), 'other') AS sport, COUNT(*) AS n
        FROM competitions c
        WHERE {w}
        GROUP BY sport
        ORDER BY n DESC
        """,
        params,
    )


def query_chart_participants_by_gender(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT COALESCE(NULLIF(TRIM(p.gender), ''), 'не указан') AS gender,
               COUNT(DISTINCT p.id) AS n
        FROM profiles p
        INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE {w}
        GROUP BY gender
        ORDER BY n DESC
        """,
        params,
    )


# ── Команды (MVP: поле team в results) ─────────────────────────────────────


def query_teams_top(
    db_path: Path | str,
    year: int | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    if year is not None:
        return q_all(
            db_path,
            f"""
            SELECT TRIM(r.team) AS команда,
                   COUNT(*) AS финишей,
                   COUNT(DISTINCT r.profile_id) AS участников
            FROM results r
            JOIN competitions c ON c.id = r.competition_id
            WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
              AND c.year = ?
            GROUP BY команда
            ORDER BY финишей DESC
            LIMIT {int(limit)}
            """,
            (year,),
        )
    return q_all(
        db_path,
        f"""
        SELECT TRIM(r.team) AS команда,
               COUNT(*) AS финишей,
               COUNT(DISTINCT r.profile_id) AS участников
        FROM results r
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
        GROUP BY команда
        ORDER BY финишей DESC
        LIMIT {int(limit)}
        """,
    )


def query_team_names_for_select(
    db_path: Path | str,
    search: str | None,
    limit: int = 500,
) -> list[str]:
    """Уникальные названия команд из финишей (dnf=0), опционально поиск подстроки (без учёта регистра)."""
    lim = int(limit)
    base = """
        SELECT DISTINCT TRIM(r.team) AS name
        FROM results r
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
    """
    if search and search.strip():
        pat = f"%{search.strip()}%"
        rows = q_all(
            db_path,
            base + " AND LOWER(TRIM(r.team)) LIKE LOWER(?) ORDER BY name LIMIT ?",
            (pat, lim),
        )
    else:
        rows = q_all(db_path, base + " ORDER BY name LIMIT ?", (lim,))
    return [str(r["name"]) for r in rows if r.get("name")]


def query_team_stats(db_path: Path | str, team_name: str) -> dict[str, Any] | None:
    """
    Статистика по точному названию команды (TRIM(results.team)).
    Участники — уникальные profile_id с финишами; финиши — все строки results с dnf=0.
    """
    t = team_name.strip()
    if not t:
        return None
    agg = q_one(
        db_path,
        """
        SELECT
          COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants,
          COUNT(*) AS finishes,
          COUNT(DISTINCT c.year) AS active_years_n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        """,
        (t,),
    )
    if not agg or int(agg.get("finishes") or 0) == 0:
        return None
    yrows = q_all(
        db_path,
        """
        SELECT DISTINCT c.year AS y
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        ORDER BY c.year
        """,
        (t,),
    )
    years_sorted = [int(x["y"]) for x in yrows if x.get("y") is not None]
    return {
        "participants": int(agg.get("participants") or 0),
        "finishes": int(agg.get("finishes") or 0),
        "active_years_count": int(agg.get("active_years_n") or 0),
        "active_years_list": years_sorted,
    }


def query_team_year_options_for_cups(db_path: Path | str, team_name: str) -> list[int]:
    """Годы для фильтра кубков: годы активности команды в событиях ∪ годы строк profile_cup_results её участников."""
    t = team_name.strip()
    if not t:
        return []
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT y FROM (
            SELECT pcr.year AS y
            FROM profile_cup_results pcr
            WHERE pcr.profile_id IN (
                SELECT DISTINCT r.profile_id
                FROM results r
                WHERE r.dnf = 0 AND TRIM(r.team) = ? AND r.profile_id IS NOT NULL
            )
            UNION
            SELECT c.year AS y
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        )
        ORDER BY y DESC
        """,
        (t, t),
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_team_cup_points_for_year(
    db_path: Path | str,
    team_name: str,
    year: int,
) -> list[dict[str, Any]]:
    """Очки в кубках (profile_cup_results) для участников команды за выбранный год."""
    t = team_name.strip()
    return q_all(
        db_path,
        """
        SELECT
            p.id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS athlete,
            pcr.cup_title AS cup,
            pcr.distance_name AS distance,
            pcr.place_abs AS place,
            pcr.group_name AS cup_group,
            ROUND(COALESCE(pcr.total_points, 0), 2) AS points
        FROM profile_cup_results pcr
        INNER JOIN profiles p ON p.id = pcr.profile_id
        WHERE pcr.year = ?
          AND pcr.profile_id IN (
              SELECT DISTINCT r.profile_id
              FROM results r
              WHERE r.dnf = 0 AND TRIM(r.team) = ? AND r.profile_id IS NOT NULL
          )
        ORDER BY pcr.cup_title, pcr.distance_name, athlete
        """,
        (year, t),
    )


def query_data_health(db_path: Path | str) -> dict[str, Any]:
    """Мини-чеклист для дашборда / документации."""
    row = q_one(
        db_path,
        """
        SELECT
          (SELECT COUNT(*) FROM results WHERE profile_id IS NOT NULL) AS results_with_profile,
          (SELECT COUNT(*) FROM results) AS results_total,
          (SELECT COUNT(*) FROM profiles) AS profiles_count,
          (SELECT COUNT(*) FROM profile_cup_results) AS profile_cup_rows
        """,
    )
    return row or {}
