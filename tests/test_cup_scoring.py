"""Тесты расчёта очков кубка (cup_scoring)."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import cup_scoring as cs


def test_points_2026_stages_1_6_ge_7km() -> None:
    assert cs.points_2026_run_stages_1_6_ge_7km(1) == 600
    assert cs.points_2026_run_stages_1_6_ge_7km(6) == 590
    assert cs.points_2026_run_stages_1_6_ge_7km(7) == 589
    assert cs.points_2026_run_stages_1_6_ge_7km(8) == 588


def test_points_2026_stages_1_6_5_6km() -> None:
    assert cs.points_2026_run_stages_1_6_5_to_6km(1) == 598
    assert cs.points_2026_run_stages_1_6_5_to_6km(6) == 589
    assert cs.points_2026_run_stages_1_6_5_to_6km(7) == 588


def test_points_2026_stages_7_8_half() -> None:
    assert cs.points_2026_run_stages_7_8_10_to_half(1) == 602
    assert cs.points_2026_run_stages_7_8_10_to_half(3) == 599
    assert cs.points_2026_run_stages_7_8_10_to_half(4) == 598


def test_points_2026_for_finish_bands() -> None:
    assert cs.points_2026_run_for_finish(3, 10.0, 1)[0] == 600
    assert cs.points_2026_run_for_finish(3, 5.5, 1)[0] == 598
    assert cs.points_2026_run_for_finish(7, 21.097, 1)[0] == 602
    assert cs.points_2026_run_for_finish(7, 5.0, 1)[0] == 598
    assert cs.points_2026_run_for_finish(7, 2.5, 10)[0] == 0
    assert cs.points_2026_run_for_finish(7, 42.0, 1)[0] == 0


def test_compute_run_cup_2026_integration() -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    p = Path(path)
    try:
        conn = sqlite3.connect(p)
        conn.executescript(
            """
            CREATE TABLE cups (id INTEGER PRIMARY KEY, title TEXT, year INTEGER, sport TEXT);
            CREATE TABLE competitions (
                id INTEGER PRIMARY KEY, title TEXT, date TEXT, year INTEGER, sport TEXT);
            CREATE TABLE distances (
                id INTEGER PRIMARY KEY, competition_id INTEGER, name TEXT, distance_km REAL);
            CREATE TABLE results (
                id INTEGER PRIMARY KEY, competition_id INTEGER, distance_id INTEGER,
                profile_id INTEGER, dnf INTEGER DEFAULT 0, is_relay INTEGER DEFAULT 0,
                place_abs INTEGER, place_gender INTEGER);
            CREATE TABLE cup_competitions (cup_id INTEGER, competition_id INTEGER);

            INSERT INTO cups VALUES (9, 'Test Cup', 2026, 'run');
            INSERT INTO competitions VALUES (101, 'A', '2026-01-01', 2026, 'run');
            INSERT INTO competitions VALUES (102, 'B', '2026-02-01', 2026, 'run');
            INSERT INTO distances VALUES (1, 101, '10 km', 10.0);
            INSERT INTO distances VALUES (2, 102, '21 km', 21.0);
            INSERT INTO results VALUES (501, 101, 1, 500, 0, 0, 1, 1);
            INSERT INTO results VALUES (502, 102, 2, 500, 0, 0, 3, 3);
            INSERT INTO cup_competitions VALUES (9, 101);
            INSERT INTO cup_competitions VALUES (9, 102);
            """
        )
        n_fin, n_tot = cs.compute_run_cup_2026(conn, 9, 2026)
        conn.close()
        assert n_fin == 2
        assert n_tot == 1

        conn = sqlite3.connect(p)
        pts = conn.execute(
            "SELECT points_awarded, stage_index FROM cup_scoring_computed_finishes ORDER BY result_id"
        ).fetchall()
        assert pts[0] == (600, 1)
        assert pts[1] == (596, 2)
        tot = conn.execute(
            "SELECT points_best7 FROM cup_scoring_computed_totals WHERE profile_id=500"
        ).fetchone()[0]
        assert tot == 600 + 596
        conn.close()
    finally:
        p.unlink(missing_ok=True)
