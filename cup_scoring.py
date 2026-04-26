"""
Расчёт очков кубка по локальным правилам (не из API profile_cup_results).

Статическая версия: ``RULE_2026_RUN_V1`` — беговый кубок 2026 (этапы 1–6 / 7–8,
дистанционные корзины, сумма 7 лучших из N этапов кубка).

Таблицы: ``cup_scoring_computed_finishes``, ``cup_scoring_computed_totals``.
"""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

RULE_2026_RUN_V1 = "2026_run_v1"

# Верхняя граница «10–21 км» на этапах 7–8: покрывает 21,097 км
STAGE7_8_HALF_MAX_KM = 22.0

_CUP_SCORING_DDL = """
CREATE TABLE IF NOT EXISTS cup_scoring_computed_finishes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version        TEXT NOT NULL,
    cup_id              INTEGER NOT NULL,
    year                INTEGER NOT NULL,
    profile_id          INTEGER NOT NULL,
    result_id           INTEGER NOT NULL,
    competition_id      INTEGER NOT NULL,
    distance_id         INTEGER NOT NULL,
    stage_index         INTEGER NOT NULL,
    distance_km         REAL,
    place_for_score     INTEGER,
    points_awarded      INTEGER NOT NULL,
    rule_label          TEXT,
    computed_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(rule_version, cup_id, year, result_id)
);

CREATE TABLE IF NOT EXISTS cup_scoring_computed_totals (
    rule_version    TEXT NOT NULL,
    cup_id          INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    profile_id      INTEGER NOT NULL,
    points_best7    INTEGER NOT NULL,
    stages_json     TEXT,
    computed_at     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (rule_version, cup_id, year, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_cup_score_fin_cup
    ON cup_scoring_computed_finishes(cup_id, year);
CREATE INDEX IF NOT EXISTS idx_cup_score_fin_prof
    ON cup_scoring_computed_finishes(profile_id);
"""


def ensure_cup_scoring_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(_CUP_SCORING_DDL)
    conn.commit()


def _km_from_distance_row(distance_km: Any, dist_name: str | None) -> float | None:
    try:
        if distance_km is not None and str(distance_km).strip() != "":
            v = float(distance_km)
            if v > 0:
                return v
    except (TypeError, ValueError):
        pass
    if not dist_name or not str(dist_name).strip():
        return None
    m = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(?:км|km)",
        str(dist_name).strip().casefold().replace(",", "."),
    )
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _place_for_score(place_gender: Any, place_abs: Any) -> int | None:
    try:
        pg = int(place_gender) if place_gender is not None else None
        if pg is not None and pg > 0:
            return pg
    except (TypeError, ValueError):
        pass
    try:
        pa = int(place_abs) if place_abs is not None else None
        if pa is not None and pa > 0:
            return pa
    except (TypeError, ValueError):
        pass
    return None


def points_2026_run_stages_1_6_ge_7km(place: int) -> int:
    """Этапы 1–6, дистанция ≥ 7 км."""
    if place < 1:
        return 0
    m = {1: 600, 2: 598, 3: 596, 4: 594, 5: 592, 6: 590, 7: 589}
    if place in m:
        return m[place]
    return max(0, 589 - (place - 7))


def points_2026_run_stages_1_6_5_to_6km(place: int) -> int:
    """Этапы 1–6, 5–6 км включительно; то же для 5 км на этапах 7–8."""
    if place < 1:
        return 0
    m = {1: 598, 2: 596, 3: 594, 4: 592, 5: 590, 6: 589}
    if place in m:
        return m[place]
    return max(0, 589 - (place - 6))


def points_2026_run_stages_7_8_10_to_half(place: int) -> int:
    """Этапы 7–8, 10 км … до полумарафона (до STAGE7_8_HALF_MAX_KM км)."""
    if place < 1:
        return 0
    if place == 1:
        return 602
    if place == 2:
        return 600
    if place == 3:
        return 599
    return max(0, 599 - (place - 3))


def points_2026_run_for_finish(
    stage_index: int,
    distance_km: float,
    place: int | None,
) -> tuple[int, str]:
    """Возвращает (очки, метка правила)."""
    if place is None or place < 1:
        return 0, "no_place"

    d = float(distance_km)

    if 1 <= stage_index <= 6:
        if d >= 7.0:
            return points_2026_run_stages_1_6_ge_7km(place), "2026_s1_6_d_ge_7km"
        if 5.0 <= d <= 6.0:
            return points_2026_run_stages_1_6_5_to_6km(place), "2026_s1_6_d_5_6km"
        return 0, "2026_s1_6_no_band"

    if 7 <= stage_index <= 8:
        if 10.0 <= d <= STAGE7_8_HALF_MAX_KM:
            return points_2026_run_stages_7_8_10_to_half(place), "2026_s7_8_d_10_half"
        if 4.8 <= d <= 5.2:
            return points_2026_run_stages_1_6_5_to_6km(place), "2026_s7_8_d_5km"
        if 2.0 <= d <= 3.2:
            return 0, "2026_s7_8_d_2_3km_no_points"
        return 0, "2026_s7_8_no_band"

    return 0, "stage_out_of_1_8"


def _load_stage_map(
    conn: sqlite3.Connection, cup_id: int, year: int
) -> dict[int, int]:
    """competition_id -> stage_index (1..N по дате)."""
    rows = conn.execute(
        """
        SELECT c.id AS competition_id
        FROM cup_competitions cc
        INNER JOIN competitions c ON c.id = cc.competition_id AND c.year = ?
        WHERE cc.cup_id = ?
        ORDER BY c.date ASC, c.id ASC
        """,
        (year, cup_id),
    ).fetchall()
    return {int(r[0]): i + 1 for i, r in enumerate(rows)}


def compute_run_cup_2026(
    conn: sqlite3.Connection,
    cup_id: int,
    year: int,
    rule_version: str = RULE_2026_RUN_V1,
) -> tuple[int, int]:
    """
    Пересчитывает очки для кубка ``cup_id`` и календарного года ``year``.
    Возвращает (число строк finishes, число строк totals).
    """
    ensure_cup_scoring_tables(conn)
    stage_by_comp = _load_stage_map(conn, cup_id, year)
    conn.execute(
        "DELETE FROM cup_scoring_computed_finishes WHERE rule_version=? AND cup_id=? AND year=?",
        (rule_version, cup_id, year),
    )
    conn.execute(
        "DELETE FROM cup_scoring_computed_totals WHERE rule_version=? AND cup_id=? AND year=?",
        (rule_version, cup_id, year),
    )
    if not stage_by_comp:
        conn.commit()
        return 0, 0

    cur = conn.execute(
        """
        SELECT r.id AS result_id, r.profile_id, r.competition_id, r.distance_id,
               r.place_abs, r.place_gender,
               COALESCE(d.distance_km, 0) AS distance_km, d.name AS dist_name
        FROM results r
        INNER JOIN cup_competitions cc
          ON cc.competition_id = r.competition_id AND cc.cup_id = ?
        INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
        INNER JOIN distances d ON d.id = r.distance_id AND d.competition_id = c.id
        WHERE COALESCE(r.dnf, 0) = 0
          AND COALESCE(r.is_relay, 0) = 0
          AND r.profile_id IS NOT NULL
        """,
        (cup_id, year),
    )
    n_fin = 0
    for row in cur.fetchall():
        rid, pid, comp_id, dist_id, pabs, pgen, dkm, dname = row
        comp_id = int(comp_id)
        stage = stage_by_comp.get(comp_id)
        if stage is None:
            continue
        km = _km_from_distance_row(dkm, dname)
        pl = _place_for_score(pgen, pabs)
        if km is None:
            pts, lab = 0, "no_distance_km"
        else:
            pts, lab = points_2026_run_for_finish(stage, float(km), pl)
        conn.execute(
            """
            INSERT INTO cup_scoring_computed_finishes (
                rule_version, cup_id, year, profile_id, result_id,
                competition_id, distance_id, stage_index, distance_km,
                place_for_score, points_awarded, rule_label, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """,
            (
                rule_version,
                cup_id,
                year,
                int(pid),
                int(rid),
                comp_id,
                int(dist_id),
                int(stage),
                float(km) if km is not None else None,
                pl,
                int(pts),
                lab,
            ),
        )
        n_fin += 1

    prof_rows = conn.execute(
        """
        SELECT profile_id, stage_index, MAX(points_awarded) AS mx
        FROM cup_scoring_computed_finishes
        WHERE rule_version = ? AND cup_id = ? AND year = ?
        GROUP BY profile_id, stage_index
        """,
        (rule_version, cup_id, year),
    ).fetchall()

    n_stages = len(stage_by_comp)
    by_prof: dict[int, dict[int, int]] = {}
    for pid, stg, mx in prof_rows:
        pid_i = int(pid)
        st_i = int(stg)
        by_prof.setdefault(pid_i, {})
        by_prof[pid_i][st_i] = max(by_prof[pid_i].get(st_i, 0), int(mx))

    n_tot = 0
    for pid, stage_max in by_prof.items():
        vec = [stage_max.get(s, 0) for s in range(1, n_stages + 1)]
        vec.sort(reverse=True)
        best7 = sum(vec[:7])
        conn.execute(
            """
            INSERT INTO cup_scoring_computed_totals (
                rule_version, cup_id, year, profile_id, points_best7, stages_json, computed_at
            ) VALUES (?,?,?,?,?,?,datetime('now'))
            """,
            (
                rule_version,
                cup_id,
                year,
                pid,
                int(best7),
                json.dumps({"per_stage": vec, "n_stages": n_stages}, ensure_ascii=False),
            ),
        )
        n_tot += 1

    conn.commit()
    return n_fin, n_tot
