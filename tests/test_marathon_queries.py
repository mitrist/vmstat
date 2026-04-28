"""Тесты слоя marathon_queries на временной SQLite."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

import marathon_queries as mq


@pytest.fixture
def sample_db() -> Path:
    fd, path = tempfile.mkstemp(suffix=".db")
    import os

    os.close(fd)
    p = Path(path)
    conn = sqlite3.connect(p)
    conn.executescript(
        """
        CREATE TABLE competitions (
            id INTEGER PRIMARY KEY, title TEXT, date TEXT, year INTEGER, sport TEXT
        );
        CREATE TABLE distances (
            id INTEGER PRIMARY KEY, competition_id INTEGER, name TEXT, distance_km REAL,
            is_relay INTEGER DEFAULT 0
        );
        CREATE TABLE results (
            id INTEGER PRIMARY KEY, competition_id INTEGER, distance_id INTEGER,
            profile_id INTEGER, dnf INTEGER DEFAULT 0, finish_time_sec REAL,
            team TEXT, place_abs INTEGER, place_gender INTEGER, place_group INTEGER,
            group_name TEXT, finish_time TEXT, raw TEXT
        );
        CREATE TABLE profiles (
            id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, second_name TEXT,
            gender TEXT, age INTEGER, birth_year INTEGER, city TEXT, city_id INTEGER,
            region TEXT, region_id INTEGER, country TEXT, club TEXT,
            stat_competitions INTEGER, stat_km INTEGER, stat_marathons INTEGER,
            stat_first INTEGER, stat_second INTEGER, stat_third INTEGER, raw TEXT
        );
        CREATE TABLE cups (id INTEGER PRIMARY KEY, title TEXT, year INTEGER, sport TEXT, raw TEXT);
        CREATE TABLE cup_competitions (cup_id INTEGER, competition_id INTEGER);
        CREATE TABLE cup_distances (
            id INTEGER PRIMARY KEY, cup_id INTEGER, name TEXT, distance_km REAL, sport TEXT, raw TEXT
        );
        CREATE TABLE cup_results (
            id INTEGER PRIMARY KEY, cup_id INTEGER, distance_id INTEGER, profile_id INTEGER,
            place_abs INTEGER, place_gender INTEGER, place_group INTEGER,
            group_id INTEGER, group_name TEXT, total_points REAL, competitions_count INTEGER, raw TEXT
        );
        CREATE TABLE profile_cup_results (
            id INTEGER PRIMARY KEY, profile_id INTEGER, year INTEGER, cup_id INTEGER,
            cup_title TEXT, distance_id INTEGER, distance_name TEXT,
            place_abs INTEGER, group_name TEXT, total_points REAL, raw TEXT
        );
        CREATE TABLE cup_scoring_computed_finishes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_version TEXT NOT NULL,
            cup_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            result_id INTEGER NOT NULL,
            competition_id INTEGER NOT NULL,
            distance_id INTEGER NOT NULL,
            stage_index INTEGER NOT NULL,
            distance_km REAL,
            place_for_score INTEGER,
            points_awarded INTEGER NOT NULL,
            rule_label TEXT,
            computed_at TEXT,
            UNIQUE(rule_version, cup_id, year, result_id)
        );
        CREATE TABLE cup_scoring_computed_totals (
            rule_version TEXT NOT NULL,
            cup_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            points_best7 INTEGER NOT NULL,
            stages_json TEXT,
            computed_at TEXT,
            PRIMARY KEY (rule_version, cup_id, year, profile_id)
        );
        CREATE TABLE competition_stats (
            competition_id INTEGER PRIMARY KEY, total_members INTEGER,
            male INTEGER, female INTEGER, teams INTEGER, regions INTEGER, dnf INTEGER, raw TEXT
        );
        CREATE TABLE competition_status (competition_id INTEGER PRIMARY KEY, status TEXT);

        INSERT INTO competitions VALUES (1, 'Test Marathon', '2024-06-01', 2024, 'run');
        INSERT INTO distances VALUES (10, 1, '42 km', 42.0, 0);
        INSERT INTO profiles VALUES (100, 'Ivan', 'Testov', '', 'm', 30, 1994, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 1, 42, 0, 0, 0, 0,
            '{"active_years": [2024, 2023, 2019]}');
        INSERT INTO profiles VALUES (101, 'Petr', 'Secondov', '', 'm', 28, 1996, 'Cherepovets',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO competitions VALUES (2, 'Spring Half', '2023-05-01', 2023, 'run');
        INSERT INTO distances VALUES (11, 2, '21 km', 21.0, 0);
        INSERT INTO results VALUES (1, 1, 10, 100, 0, 3600.0, 'Team A', 1, 1, 2, 'M40', '01:00:00', '{}');
        INSERT INTO results VALUES (2, 2, 11, 100, 0, 7200.0, 'Team A', 5, 2, 4, 'M40', '02:00:00', '{}');
        INSERT INTO results VALUES (3, 1, 10, 101, 0, 3500.0, 'Team B', 2, 2, 1, 'M40', '00:58:00', '{}');
        INSERT INTO results VALUES (9, 2, 11, 100, 1, NULL, 'Team A', NULL, NULL, NULL, 'M40', '', '{}');
        INSERT INTO cup_competitions VALUES (1, 1);
        INSERT INTO cup_competitions VALUES (2, 2);
        INSERT INTO cups VALUES (1, 'Super Cup', 2024, 'run', '{}');
        INSERT INTO cups VALUES (2, 'Mini Cup', 2024, 'run', '{}');
        INSERT INTO cup_distances VALUES (1, 1, '21 km', 21.0, 'run', '{}');
        INSERT INTO cup_distances VALUES (2, 2, '10 km', 10.0, 'run', '{}');
        INSERT INTO cup_distances VALUES (3, 1, '42 km', 42.0, 'run', '{}');
        INSERT INTO cup_results
          (id, cup_id, distance_id, profile_id, place_abs, place_gender, place_group,
           group_id, group_name, total_points, competitions_count, raw)
        VALUES (9001, 1, 3, 100, 88, 40, 3, NULL, 'M40', 99.5, 1,
            '{"competition_points": [{"competition_id": 1, "points": 88.25}]}');
        INSERT INTO cup_results
          (id, cup_id, distance_id, profile_id, place_abs, place_gender, place_group,
           group_id, group_name, total_points, competitions_count, raw)
        VALUES (9002, 2, 2, 100, 15, 7, 2, NULL, 'M40', 10.0, 1, '{}');
        INSERT INTO cup_results
          (id, cup_id, distance_id, profile_id, place_abs, place_gender, place_group,
           group_id, group_name, total_points, competitions_count, raw)
        VALUES (9003, 1, 1, 101, 3, 2, 1, NULL, 'M40', 150.0, 1, '{}');
        INSERT INTO profile_cup_results
          (id, profile_id, year, cup_id, cup_title, distance_id, distance_name,
           place_abs, group_name, total_points, raw)
        VALUES (1, 100, 2024, 1, 'Super Cup', 3, '42 km', 2, 'M40', 99.5,
            '{"group": {"name": "Мужчины 40-44", "age_from": 40, "age_to": 44},
              "competition": {"id": 1}}');
        INSERT INTO profile_cup_results
          (id, profile_id, year, cup_id, cup_title, distance_id, distance_name,
           place_abs, group_name, total_points, raw)
        VALUES (2, 100, 2023, 1, 'Super Cup', 1, '21 km', 3, 'M40', 50.0, '{}');
        INSERT INTO profile_cup_results
          (id, profile_id, year, cup_id, cup_title, distance_id, distance_name,
           place_abs, group_name, total_points, raw)
        VALUES (3, 100, 2024, 2, 'Mini Cup', 2, '10 km', 1, 'M40', 10.0,
            '{"group": {"age_from": 35, "age_to": 39}}');
        INSERT INTO profile_cup_results
          (id, profile_id, year, cup_id, cup_title, distance_id, distance_name,
           place_abs, group_name, total_points, raw)
        VALUES (4, 101, 2024, 1, 'Super Cup', 1, '21 km', 1, 'M40', 150.0,
            '{"competition": {"id": 1, "title": "Контрольный забег (2024)"}}');
        INSERT INTO competition_stats
          (competition_id, total_members, male, female, teams, regions, dnf, raw)
        VALUES (1, 3, 2, 1, 2, 1, 0, NULL);
        INSERT INTO competition_stats
          (competition_id, total_members, male, female, teams, regions, dnf, raw)
        VALUES (2, 1, 1, 0, 0, 0, 0, NULL);
        """
    )
    conn.commit()
    conn.close()
    yield p
    p.unlink(missing_ok=True)


def test_season_metrics(sample_db: Path) -> None:
    m = mq.query_season_metrics(sample_db, 2024)
    assert m is not None
    assert m["events"] == 1
    assert m["finishes"] == 2
    assert m["unique_athletes"] == 2
    assert m["total_km"] == 84.0


def test_profile_row_and_history(sample_db: Path) -> None:
    row = mq.query_profile_row(sample_db, 100)
    assert row is not None
    assert row["first_name"] == "Ivan"
    hist = mq.query_profile_results_history(sample_db, 100)
    assert len(hist) == 2
    assert mq.parse_profile_active_years(row.get("raw")) == [2024, 2023, 2019]
    assert mq.query_profile_participation_years(sample_db, 100) == [2024, 2023]
    h24 = mq.query_profile_results_history_for_year(sample_db, 100, 2024)
    assert len(h24) == 1 and h24[0]["км"] == 42.0
    h23 = mq.query_profile_results_history_for_year(sample_db, 100, 2023)
    assert len(h23) == 1 and h23[0]["км"] == 21.0
    assert mq.query_profile_results_history_for_year(sample_db, 100, 2019) == []
    c24 = mq.query_profile_cup_rows_for_year(sample_db, 100, 2024)
    assert len(c24) == 2
    pts = {float(r["очков"]) for r in c24}
    assert pts == {99.5, 10.0}
    assert all("событие" in r and "дистанция" in r for r in c24)
    petr_c24 = mq.query_profile_cup_rows_for_year(sample_db, 101, 2024)
    petr_row = next(x for x in petr_c24 if float(x["очков"]) == 150.0)
    assert "Контрольный" in (petr_row.get("событие") or "")
    c23 = mq.query_profile_cup_rows_for_year(sample_db, 100, 2023)
    assert len(c23) == 1 and c23[0]["очков"] == 50.0


def test_profile_search_by_id(sample_db: Path) -> None:
    rows = mq.query_profile_search(sample_db, "100", 10)
    assert len(rows) == 1
    assert rows[0]["id"] == 100
    erows = mq.query_profile_search_enriched(sample_db, "100", 10)
    assert len(erows) == 1
    assert erows[0]["id"] == 100
    assert int(erows[0]["starts_total"]) >= 1


def test_profile_search_enriched_text(sample_db: Path) -> None:
    rows = mq.query_profile_search_enriched(sample_db, "Test", 10)
    assert len(rows) >= 1
    first = rows[0]
    assert "last_year" in first
    assert "starts_total" in first


def test_teams_top(sample_db: Path) -> None:
    rows = mq.query_teams_top(sample_db, year=2024, limit=10)
    assert len(rows) == 2
    names = {r["команда"] for r in rows}
    assert names == {"Team A", "Team B"}


def test_general_stats_cards(sample_db: Path) -> None:
    c = mq.query_general_stats_cards(sample_db, years=[2024], sports=["run"], cup_ids=None)
    assert c["total_events"] == 1
    assert c["total_participants"] == 2
    assert c["regions_distinct"] >= 1
    assert c["teams_distinct"] == 2
    assert c["countries_distinct"] >= 1


def test_profile_cup_page_queries(sample_db: Path) -> None:
    assert mq.query_profile_cup_result_years(sample_db) == [2024, 2023]
    s24 = mq.query_profile_cup_summaries_for_year(sample_db, 2024)
    assert len(s24) == 2
    ids_titles = {(r["id"], r["кубок"]) for r in s24}
    assert ids_titles == {(1, "Super Cup"), (2, "Mini Cup")}
    d = mq.query_profile_cup_detail_for_year_cup(sample_db, 2024, 1)
    assert len(d) == 2
    ivan = next(r for r in d if "Testov" in (r.get("участник") or ""))
    assert float(ivan["очков"]) == 88.25
    assert "событие" in ivan and "дистанция" in ivan
    assert ivan["last_name"] == "Testov"
    assert ivan["gender"] == "m"
    assert ivan["cup_place_abs"] == 88
    assert ivan["cup_place_gender"] == 40
    assert ivan["cup_place_group"] == 3
    assert ivan["pcr_place_abs"] == 2
    assert mq.cup_detail_resolve_display_place(ivan, "Абсолютный зачёт", "Все") == 88
    assert mq.cup_detail_resolve_display_place(ivan, "Мужчины", "Все") == 40
    assert mq.cup_detail_resolve_display_place(ivan, "Мужчины", "Мужчины 40-44") == 3
    assert mq.parse_profile_cup_raw_age_group_label(ivan.get("raw")) == "Мужчины 40-44"
    d2 = mq.query_profile_cup_detail_for_year_cup(sample_db, 2024, 2)
    assert mq.parse_profile_cup_raw_age_group_label(d2[0].get("raw")) == "35-39"


def test_parse_profile_cup_raw_age_group_label() -> None:
    assert mq.parse_profile_cup_raw_age_group_label("") == ""
    assert mq.parse_profile_cup_raw_age_group_label("not json") == ""
    assert mq.parse_profile_cup_raw_age_group_label('{"group": {"name": "M40"}}') == "M40"
    assert mq.parse_profile_cup_raw_age_group_label('{"group": {"age_from": 20, "age_to": 29}}') == "20-29"


def test_profile_cup_team_member_competition_rows(sample_db: Path) -> None:
    rows = mq.query_profile_cup_team_member_competition_rows(sample_db, 100, 1, 2024)
    assert len(rows) == 1
    assert rows[0]["событие"] == "Test Marathon"
    assert rows[0]["дистанция"] == "42 km"
    assert rows[0]["место_абс"] == 1
    assert rows[0]["время"] == "01:00:00"
    assert float(rows[0]["очков"]) == 99.5
    assert rows[0]["competition_id"] == 1
    r101 = mq.query_profile_cup_team_member_competition_rows(sample_db, 101, 1, 2024)
    assert len(r101) == 1
    assert r101[0]["событие"] == "Test Marathon"
    assert float(r101[0]["очков"]) == 150.0


def test_assign_profile_cup_points_to_result_lines(sample_db: Path) -> None:
    lines = mq.query_profile_cup_team_member_competition_rows(sample_db, 100, 1, 2024)
    a = mq.assign_profile_cup_points_to_result_lines(sample_db, 100, 1, 2024, lines)
    assert a == [int(round(99.5))]
    lines101 = mq.query_profile_cup_team_member_competition_rows(sample_db, 101, 1, 2024)
    a101 = mq.assign_profile_cup_points_to_result_lines(sample_db, 101, 1, 2024, lines101)
    assert a101 == [150]
    pcr_lines = mq.query_profile_cup_results_lines_for_member(sample_db, 100, 1, 2024)
    assert mq.assign_profile_cup_points_to_result_lines(
        sample_db, 100, 1, 2024, pcr_lines
    ) == [None]


def test_profile_cup_results_lines_for_member(sample_db: Path) -> None:
    lines = mq.query_profile_cup_results_lines_for_member(sample_db, 100, 1, 2024)
    assert len(lines) == 1
    assert lines[0]["дистанция"] == "42 km"
    assert float(lines[0]["очков"]) == 99.5
    lines_p = mq.query_profile_cup_results_lines_for_member(sample_db, 101, 1, 2024)
    assert len(lines_p) == 1
    assert "Контрольный" in (lines_p[0].get("событие") or "")


def test_cup_team_score_rows_and_event_parser(sample_db: Path) -> None:
    rows = mq.query_cup_team_score_rows(sample_db, 1, 2024)
    assert len(rows) == 2
    by_team: dict[str, list[dict]] = {}
    for r in rows:
        by_team.setdefault(r["команда"], []).append(r)
    assert sum(float(x["очков"]) for x in by_team["Team B"]) == 150.0
    assert sum(float(x["очков"]) for x in by_team["Team A"]) == 99.5
    assert mq.aggregate_team_cup_points_top_five(by_team["Team A"]) == 100
    assert mq.aggregate_team_cup_points_top_five(by_team["Team B"]) == 150
    petr = next(r for r in rows if r["profile_id"] == 101)
    assert "Контрольный" in (petr.get("событие") or "")
    assert mq.parse_profile_cup_raw_event_title(petr.get("raw")) == "Контрольный забег (2024)"
    assert mq.query_cup_team_score_rows(sample_db, 2, 2024) == []


def test_aggregate_team_cup_points_top_five() -> None:
    rows = [{"profile_id": i, "очков": float(i * 10)} for i in range(1, 7)]
    assert mq.aggregate_team_cup_points_top_five(rows) == 200


def test_parse_profile_cup_raw_competition_id() -> None:
    assert mq.parse_profile_cup_raw_competition_id("") is None
    assert mq.parse_profile_cup_raw_competition_id('{"competition": {"id": 42}}') == 42
    assert mq.parse_profile_cup_raw_competition_id('{"competition_id": 7}') == 7


def test_map_profile_cup_points_by_competition_id(sample_db: Path) -> None:
    m = mq.map_profile_cup_points_by_competition_id(sample_db, 100, 1, 2024)
    assert m.get(1) == 99.5
    m2 = mq.map_profile_cup_points_by_competition_id(sample_db, 101, 1, 2024)
    assert m2.get(1) == 150.0


def test_norm_cup_match_title() -> None:
    assert mq.norm_cup_match_title("Забег (2024)") == "забег"
    assert mq.norm_cup_match_distance(" 21 km ") == "21 km"


def test_map_profile_cup_points_by_title_distance(sample_db: Path) -> None:
    m = mq.map_profile_cup_points_by_title_distance(sample_db, 101, 1, 2024)
    t = mq.norm_cup_match_title("Контрольный забег (2024)")
    d = mq.norm_cup_match_distance("21 km")
    assert abs(m[(t, d)] - 150.0) < 1e-6


def test_parse_profile_cup_raw_event_title() -> None:
    assert mq.parse_profile_cup_raw_event_title("") == ""
    j = '{"competition": {"title_short": "Забег X"}}'
    assert mq.parse_profile_cup_raw_event_title(j) == "Забег X"


def test_team_detail_and_cups(sample_db: Path) -> None:
    names = mq.query_team_names_for_select(sample_db, None, 100)
    assert "Team A" in names
    assert mq.query_team_names_for_select(sample_db, "team a", 50) == ["Team A"]
    st = mq.query_team_stats(sample_db, "Team A")
    assert st is not None
    assert st["participants"] == 1
    assert st["finishes"] == 2
    assert st["active_years_count"] == 2
    assert st["active_years_list"] == [2023, 2024]
    opts = mq.query_team_year_options_for_cups(sample_db, "Team A")
    assert 2024 in opts
    rows = mq.query_team_cup_points_for_year(sample_db, "Team A", 2024)
    assert len(rows) == 2
    cups_pts = {(r["cup"], r["points"]) for r in rows}
    assert ("Super Cup", 99.5) in cups_pts
    assert ("Mini Cup", 10.0) in cups_pts


def test_team_extended_queries(sample_db: Path) -> None:
    kpi = mq.query_team_kpi_extended(sample_db, "Team A")
    assert kpi is not None
    assert kpi["participants_distinct"] == 1
    assert kpi["starts_total"] == 3
    assert kpi["finishes_total"] == 2
    assert kpi["dnf_total"] == 1
    assert float(kpi["km_total"]) == 63.0
    assert kpi["active_years_list"] == [2023, 2024]

    kpi_2024 = mq.query_team_kpi_extended(sample_db, "Team A", year=2024)
    assert kpi_2024 is not None
    assert kpi_2024["starts_total"] == 1
    assert kpi_2024["finishes_total"] == 1

    roster = mq.query_team_roster_stats(sample_db, "Team A")
    assert len(roster) == 1
    assert roster[0]["athlete"] == "Testov Ivan"
    assert roster[0]["best_place_abs"] == 1

    events = mq.query_team_events_table(sample_db, "Team A", year=2024, event_search="marath")
    assert len(events) == 1
    assert events[0]["event_title"] == "Test Marathon"
    assert events[0]["best_athlete"] == "Testov Ivan"

    slices = mq.query_team_sport_distance_slices(sample_db, "Team A")
    assert slices["by_sport"][0]["sport"] == "run"
    assert slices["by_sport"][0]["starts"] == 3
    assert len(slices["by_distance"]) == 2

    geo = mq.query_team_geography(sample_db, "Team A")
    assert geo["cities"][0]["city"] == "Vologda"
    assert geo["regions"][0]["region"] == "VO"
    assert geo["countries"][0]["country"] == "Россия"

    trends = mq.query_team_yearly_trends(sample_db, "Team A")
    assert [r["year"] for r in trends] == [2023, 2024]
    assert int(next(r for r in trends if r["year"] == 2023)["dnf"]) == 1

    quality = mq.query_team_data_quality(sample_db, "Team A")
    q_map = {r["метрика"]: r["значение"] for r in quality}
    assert q_map["Стартов всего"] == 3
    assert q_map["Финишей"] == 2
    assert q_map["DNF"] == 1


def test_cups_for_obsh_header_filter(sample_db: Path) -> None:
    all_c = mq.query_cups_for_obsh_header_filter(sample_db, None, None)
    ids = {r["id"] for r in all_c}
    assert ids == {1, 2}
    r_2023 = mq.query_cups_for_obsh_header_filter(sample_db, [2023], None)
    assert {r["id"] for r in r_2023} == {2}
    r_2024 = mq.query_cups_for_obsh_header_filter(sample_db, [2024], None)
    assert {r["id"] for r in r_2024} == {1}


def test_general_stats_events_table(sample_db: Path) -> None:
    rows = mq.query_general_stats_events_table(sample_db, None, None, None, bar_year=None)
    assert len(rows) == 2
    t2024 = next(r for r in rows if r["год"] == 2024)
    assert "Test" in t2024["Событие"]
    assert t2024["Количество участников"] == 3
    rows24 = mq.query_general_stats_events_table(
        sample_db, None, None, None, bar_year=2024
    )
    assert len(rows24) == 1 and rows24[0]["год"] == 2024
    rows_f = mq.query_general_stats_events_table(
        sample_db, [2024], ["run"], [1], bar_year=None
    )
    assert len(rows_f) == 1
    assert rows_f[0]["год"] == 2024


def test_event_section_cards_and_tables(sample_db: Path) -> None:
    cards = mq.query_event_section_cards(sample_db, years=[2024], sports=["run"])
    assert cards["total_events"] == 1
    assert cards["total_participants"] == 2
    assert cards["teams_distinct"] == 2
    assert cards["regions_distinct"] >= 1
    assert cards["countries_distinct"] >= 1

    events = mq.query_event_section_events_table(sample_db, years=[2024], sports=["run"])
    assert len(events) == 1
    row = events[0]
    assert row["Год"] == 2024
    assert row["Количество участников"] == 3
    assert row["Количество команд"] == 2
    assert row["Регионов"] == 1
    assert row["Стран"] == 1

    records = mq.query_event_section_records_table(sample_db, years=[2024], sports=["run"])
    assert len(records) == 2
    first = records[0]
    assert first["Событие"] == "Test Marathon"
    assert first["Год"] == 2024
    assert first["Дистанция"] == "42 km"
    assert first["Время"] in {"00:58:00", "01:00:00"}


def test_event_section_records_hierarchy(sample_db: Path) -> None:
    rows = mq.query_event_section_records_hierarchy(
        sample_db, years=[2024], sports=["run"], top_n=5
    )
    assert len(rows) == 2
    for r in rows:
        assert r["Событие"] == "Test Marathon"
        assert r["Дистанция"] == "42 km"
        assert 1 <= int(r["Место"]) <= 5
        assert r["Пол"] == "Мужчины"
        assert r["Год"] == 2024
        assert "Test Marathon" in str(r["Этап"])
        assert str(r["Время"]).strip() != ""
    assert mq.query_event_section_records_hierarchy(
        sample_db, years=[2024], sports=["ski"], top_n=5
    ) == []


def test_event_records_top5_across_years_not_per_distance_id(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO competitions VALUES (3, 'Test Marathon', '2023-07-01', 2023, 'run');
        INSERT INTO competitions VALUES (4, 'Test Marathon', '2022-07-01', 2022, 'run');
        INSERT INTO distances VALUES (12, 3, '42 km', 42.0, 0);
        INSERT INTO distances VALUES (13, 4, '42 km', 42.0, 0);
        INSERT INTO profiles VALUES (102, 'A', 'Runner', '', 'm', 29, 1995, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (103, 'B', 'Runner', '', 'm', 29, 1995, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (104, 'C', 'Runner', '', 'm', 29, 1995, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (105, 'D', 'Runner', '', 'm', 29, 1995, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (106, 'E', 'Runner', '', 'm', 29, 1995, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO results VALUES (4, 3, 12, 102, 0, 3450.0, 'Team X', 1, 1, 1, 'M', '00:57:30', '{}');
        INSERT INTO results VALUES (5, 3, 12, 103, 0, 3460.0, 'Team X', 2, 2, 2, 'M', '00:57:40', '{}');
        INSERT INTO results VALUES (6, 3, 12, 104, 0, 3470.0, 'Team X', 3, 3, 3, 'M', '00:57:50', '{}');
        INSERT INTO results VALUES (7, 4, 13, 105, 0, 3480.0, 'Team X', 4, 4, 4, 'M', '00:58:00', '{}');
        INSERT INTO results VALUES (8, 4, 13, 106, 0, 3490.0, 'Team X', 5, 5, 5, 'M', '00:58:10', '{}');
        """
    )
    conn.commit()
    conn.close()

    rows = mq.query_event_section_records_hierarchy(
        sample_db, years=None, sports=["run"], top_n=5
    )
    men_42 = [
        r
        for r in rows
        if r["Событие"] == "Test Marathon"
        and r["Дистанция"] == "42 km"
        and r["Пол"] == "Мужчины"
    ]
    assert len(men_42) == 5


def test_event_records_use_distance_alias_dictionary(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO competitions VALUES (5, 'Series HM', '2022-05-01', 2022, 'run');
        INSERT INTO competitions VALUES (6, 'Series HM', '2023-05-01', 2023, 'run');
        INSERT INTO distances VALUES (20, 5, 'Полмарафон', 21.1, 0);
        INSERT INTO distances VALUES (21, 6, '21 км 97,5м', 21.0975, 0);
        INSERT INTO profiles VALUES (201, 'M1', 'HM', '', 'm', 25, 1999, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (202, 'M2', 'HM', '', 'm', 25, 1999, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (203, 'F1', 'HM', '', 'f', 25, 1999, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO profiles VALUES (204, 'F2', 'HM', '', 'f', 25, 1999, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO results VALUES (30, 5, 20, 201, 0, 3700.0, '', 1, 1, 1, 'M', '01:01:40', '{}');
        INSERT INTO results VALUES (31, 6, 21, 202, 0, 3650.0, '', 1, 1, 1, 'M', '01:00:50', '{}');
        INSERT INTO results VALUES (32, 5, 20, 203, 0, 4100.0, '', 1, 1, 1, 'F', '01:08:20', '{}');
        INSERT INTO results VALUES (33, 6, 21, 204, 0, 4050.0, '', 1, 1, 1, 'F', '01:07:30', '{}');
        """
    )
    conn.commit()
    conn.close()

    rows = mq.query_event_section_records_hierarchy(sample_db, years=None, sports=["run"], top_n=5)
    hm_rows = [r for r in rows if r["Событие"] == "Series HM"]
    assert len(hm_rows) == 4
    assert {r["Дистанция"] for r in hm_rows} == {"Полумарафон (21.1 км)"}
    assert {r["Пол"] for r in hm_rows} == {"Мужчины", "Женщины"}


def test_profile_analytics_queries(sample_db: Path) -> None:
    k_all = mq.query_profile_kpi_all_time(sample_db, 100)
    assert k_all["starts_total"] == 3
    assert k_all["finishes_total"] == 2
    assert k_all["dnf_total"] == 1
    assert float(k_all["km_total"]) == 63.0
    assert int(k_all["best_finish_time_sec"]) == 3600

    k_2024 = mq.query_profile_kpi_year(sample_db, 100, 2024)
    assert k_2024["starts_total"] == 1
    assert k_2024["finishes_total"] == 1
    assert k_2024["events_distinct"] == 1

    trends = mq.query_profile_yearly_trends(sample_db, 100)
    assert len(trends) == 2
    y2023 = next(r for r in trends if r["year"] == 2023)
    assert y2023["starts"] == 2
    assert y2023["dnf"] == 1

    ev_finish = mq.query_profile_events_table(
        sample_db, 100, years=[2023], include_dnf=False
    )
    assert len(ev_finish) == 1
    assert ev_finish[0]["статус"] == "finish"
    ev_all = mq.query_profile_events_table(sample_db, 100, years=[2023], include_dnf=True)
    assert len(ev_all) == 2

    pb_all = mq.query_profile_personal_bests(sample_db, 100, year=None)
    assert len(pb_all) >= 2
    pb_21 = next(x for x in pb_all if x["дистанция"] == "21 km")
    assert int(pb_21["время_сек"]) == 7200

    teams = mq.query_profile_team_summary(sample_db, 100)
    assert len(teams) == 1
    assert teams[0]["команда"] == "Team A"
    assert teams[0]["финишей"] == 2

    quality = mq.query_profile_data_quality(sample_db, 100)
    q_map = {r["метрика"]: r["значение"] for r in quality}
    assert q_map["Стартов всего"] == 3
    assert q_map["Финишей"] == 2
    assert q_map["DNF"] == 1


def test_calc_team_stage_base_points_rules() -> None:
    assert mq.calc_team_stage_base_points(1, 10.0, 1) == (600, 600)
    assert mq.calc_team_stage_base_points(1, 10.0, 2) == (598, 600)
    assert mq.calc_team_stage_base_points(1, 10.0, 7) == (589, 600)
    assert mq.calc_team_stage_base_points(1, 5.0, 1) == (598, 598)
    assert mq.calc_team_stage_base_points(1, 5.5, 6) == (589, 598)
    assert mq.calc_team_stage_base_points(7, 10.0, 1) == (602, 602)
    assert mq.calc_team_stage_base_points(7, 10.0, 3) == (599, 602)
    assert mq.calc_team_stage_base_points(7, 5.0, 1) == (598, 598)
    assert mq.calc_team_stage_base_points(7, 2.5, 1) == (0, 0)


def test_compute_team_scoring_for_cup_year(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO profiles VALUES (150, 'Old', 'Runner', '', 'm', 55, 1971, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO results VALUES (40, 1, 10, 150, 0, 3400.0, 'Team C', 1, 1, 1, 'M50', '00:56:40', '{}');
        """
    )
    conn.commit()
    conn.close()

    out = mq.compute_team_scoring_for_cup_year(
        sample_db, cup_id=1, year=2024, stage_map={1: 1}, rule_version="team_test_v1"
    )
    assert out["stage_rows"] >= 3
    teams = mq.query_team_scoring_team_totals(
        sample_db, cup_id=1, year=2024, rule_version="team_test_v1"
    )
    assert len(teams) >= 2
    members = mq.query_team_scoring_member_totals(
        sample_db, cup_id=1, year=2024, team_name="Team C", rule_version="team_test_v1"
    )
    assert len(members) == 1
    assert members[0]["очков_7из8"] == 600


def test_compute_team_scoring_uses_place_gender(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO profiles VALUES (160, 'Maria', 'Testova', '', 'f', 35, 1991, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO results VALUES (41, 1, 10, 160, 0, 3900.0, 'Team F', 10, 1, 1, 'F35', '01:05:00', '{}');
        """
    )
    conn.commit()
    conn.close()

    out = mq.compute_team_scoring_for_cup_year(
        sample_db, cup_id=1, year=2024, stage_map={1: 1}, rule_version="team_test_gender_v1"
    )
    assert out["stage_rows"] >= 1
    members = mq.query_team_scoring_member_totals(
        sample_db, cup_id=1, year=2024, team_name="Team F", rule_version="team_test_gender_v1"
    )
    assert len(members) == 1
    assert members[0]["очков_7из8"] == 600


def test_team_scoring_feature_gate() -> None:
    assert mq.is_team_scoring_enabled(54, 2026) is True
    assert mq.is_team_scoring_enabled(54, 2025) is False
    assert mq.is_team_scoring_enabled(1, 2026) is False


def test_team_championship_matrix_shape(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS team_scoring_stage_points (
            rule_version TEXT NOT NULL,
            cup_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            result_id INTEGER NOT NULL,
            competition_id INTEGER NOT NULL,
            distance_id INTEGER NOT NULL,
            stage_index INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            finish_time_sec REAL,
            distance_km REAL,
            place_for_score INTEGER,
            points_base INTEGER NOT NULL,
            points_bonus INTEGER NOT NULL,
            points_awarded INTEGER NOT NULL,
            points_for_team INTEGER NOT NULL DEFAULT 0,
            points_cap INTEGER NOT NULL,
            age INTEGER,
            gender TEXT,
            computed_at TEXT,
            PRIMARY KEY (rule_version, cup_id, year, result_id)
        );
        CREATE TABLE IF NOT EXISTS team_scoring_member_totals (
            rule_version TEXT NOT NULL,
            cup_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            points_best7 INTEGER NOT NULL,
            stages_json TEXT,
            computed_at TEXT,
            PRIMARY KEY (rule_version, cup_id, year, profile_id)
        );
        CREATE TABLE IF NOT EXISTS team_scoring_team_totals (
            rule_version TEXT NOT NULL,
            cup_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            points_top5 INTEGER NOT NULL,
            members_json TEXT,
            computed_at TEXT,
            PRIMARY KEY (rule_version, cup_id, year, team_name)
        );
        INSERT INTO team_scoring_team_totals
            (rule_version, cup_id, year, team_name, points_top5, members_json, computed_at)
        VALUES ('team_v1', 54, 2026, 'Team A', 1200, '[]', '2026-01-01T00:00:00Z');
        INSERT INTO team_scoring_stage_points
            (rule_version, cup_id, year, profile_id, result_id, competition_id, distance_id, stage_index,
             team_name, finish_time_sec, distance_km, place_for_score, points_base, points_bonus,
             points_awarded, points_for_team, points_cap, age, gender, computed_at)
        VALUES ('team_v1', 54, 2026, 100, 9001, 1, 10, 1, 'Team A', 3600, 10, 1, 600, 0, 600, 600, 600, 30, 'm', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    mat = mq.query_team_championship_matrix(
        sample_db, cup_id=54, year=2026, stage_map={1: 1}, rule_version="team_v1"
    )
    assert "rows" in mat and isinstance(mat["rows"], list)
    assert "stage_columns" in mat and isinstance(mat["stage_columns"], list)
    assert len(mat["rows"]) == 1
    first = mat["rows"][0]
    assert first["Команда"] == "Team A"
    assert first["Итого"] == 100


def test_interesting_facts_queries(sample_db: Path) -> None:
    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO competitions VALUES (7, 'Bike Stage', '2024-08-01', 2024, 'bike');
        INSERT INTO competitions VALUES (8, 'Ski Stage', '2024-12-01', 2024, 'ski');
        INSERT INTO distances VALUES (30, 7, '40 km', 40.0, 0);
        INSERT INTO distances VALUES (31, 8, '10 km', 10.0, 0);
        INSERT INTO results VALUES (50, 7, 30, 100, 0, 5000.0, 'Team A', 3, 2, 1, 'M40', '01:23:20', '{}');
        INSERT INTO results VALUES (51, 8, 31, 100, 0, 4200.0, 'Team A', 2, 1, 1, 'M40', '01:10:00', '{}');
        """
    )
    conn.commit()
    conn.close()

    loyal = mq.query_interesting_facts_loyal_participants(sample_db, year=None, sport=None, min_starts=1, limit=5)
    assert len(loyal) > 0
    assert loyal[0]["participant"] == "Testov Ivan"
    assert int(loyal[0]["active_years"]) >= 2

    finishers = mq.query_interesting_facts_finish_rate(sample_db, year=2024, sport="run", min_starts=1, limit=5)
    assert len(finishers) > 0
    assert finishers[0]["participant"] in {"Testov Ivan", "Secondov Petr"}
    assert float(finishers[0]["finish_rate_pct"]) >= 100.0

    universals = mq.query_interesting_facts_universal_participants(sample_db, year=2024, min_starts=1, limit=5)
    top_u = next(r for r in universals if r["participant"] == "Testov Ivan")
    assert int(top_u["sports_count"]) == 3

    km = mq.query_interesting_facts_km_leaders(sample_db, year=2024, sport=None, min_starts=1, limit=5)
    assert len(km) > 0
    assert km[0]["participant"] == "Testov Ivan"
    assert float(km[0]["km_total"]) >= 52.0

    freq = mq.query_interesting_facts_distance_frequency(sample_db, year=2024, sport="run", limit=10)
    assert len(freq) >= 1
    assert any(r["distance"] == "42 km" for r in freq)

    km_by_sport = mq.query_interesting_facts_km_by_sport(sample_db, year=2024)
    sport_map = {r["sport"]: float(r["km_total"] or 0) for r in km_by_sport}
    assert sport_map["run"] >= 84.0
    assert sport_map["bike"] >= 40.0
    assert sport_map["ski"] >= 10.0

    teams = mq.query_interesting_facts_team_longevity(sample_db, year=None, sport=None, min_starts=1, limit=5)
    assert len(teams) > 0
    assert teams[0]["team"] in {"Team A", "Team B"}

    geo = mq.query_interesting_facts_geography(sample_db, year=2024, sport="run", limit=5)
    assert "cities" in geo and "regions" in geo
    assert len(geo["cities"]) >= 1
    assert len(geo["regions"]) >= 1


def test_competitions_admin_queries_and_save(tmp_path: Path) -> None:
    db = tmp_path / "admin_comp.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE competitions (
            id INTEGER PRIMARY KEY,
            title TEXT,
            title_short TEXT,
            date TEXT,
            year INTEGER,
            sport TEXT,
            is_relay INTEGER,
            is_published INTEGER,
            page_url TEXT,
            raw TEXT
        );
        INSERT INTO competitions
        VALUES (99, 'Old Title', 'S', '2025-09-01', 2025, 'run', 0, 1, 'http://x', '{}');
        """
    )
    conn.commit()
    conn.close()

    years = mq.query_competition_years_admin(db)
    assert 2025 in years
    rows = mq.query_competitions_admin_rows(db, year=2025, limit=50)
    assert len(rows) == 1 and int(rows[0]["id"]) == 99

    errs = mq.save_competitions_admin_rows(
        db,
        [
            {
                "id": 99,
                "title": "New Title",
                "title_short": "S2",
                "date": "2025-09-02",
                "year": 2025,
                "sport": "run",
                "is_relay": 0,
                "is_published": 0,
                "page_url": "http://y",
            }
        ],
    )
    assert errs == []

    row2 = mq.query_competitions_admin_rows(db, year=None, limit=10)
    assert row2[0]["title"] == "New Title"
    assert int(row2[0]["is_published"]) == 0


def test_profile_event_series_rows_filters(sample_db: Path) -> None:
    rows = mq.query_profile_event_series_rows(sample_db, 100, years=[2024], sports=["run"])
    assert len(rows) >= 1
    assert rows[0]["Год"] == 2024
    assert rows[0]["вид"] == "run"
    assert "Событие" in rows[0]
    assert "Дистанция" in rows[0]
    assert "_series_short" in rows[0]

    rows_filtered = mq.query_profile_event_series_rows(
        sample_db,
        100,
        years=None,
        sports=None,
        series_shorts=["Spring Half"],
    )
    assert len(rows_filtered) >= 1
    assert all(r.get("_series_short") == "Spring Half" for r in rows_filtered)


def test_normalize_city_by_alias_rules_basic() -> None:
    rules = [
        {
            "alias": "спб",
            "canonical_key": "sankt_peterburg",
            "canonical_label": "Санкт-Петербург",
            "active": True,
        }
    ]
    key, label = mq.normalize_city_by_alias_rules("СПБ", rules=rules)
    assert key == "sankt_peterburg"
    assert label == "Санкт-Петербург"


def test_normalize_city_by_reference_csv(tmp_path: Path, monkeypatch) -> None:
    city_ref = tmp_path / "city.csv"
    city_ref.write_text(
        "city,region,geo_lat,geo_lon,population,capital_marker,fias_id\n"
        "Вологда,Вологодская,59.22,39.88,300000,2,fias-vologda\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CITY_REFERENCE_FILE", str(city_ref))
    monkeypatch.setenv("CITY_ALIASES_FILE", str(tmp_path / "missing_aliases.json"))
    key, label = mq.normalize_city_by_alias_rules("г Вологда", "Вологодская")
    assert key.startswith("cityref::")
    assert label == "Вологда"


def test_interesting_facts_geography_city_aliases(sample_db: Path, tmp_path: Path, monkeypatch) -> None:
    city_alias_file = tmp_path / "city_aliases.json"
    city_alias_file.write_text(
        """{
  "rules": [
    {"alias":"Vologda", "canonical_key":"vologda", "canonical_label":"Вологда", "active":true}
  ]
}""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CITY_ALIASES_FILE", str(city_alias_file))

    conn = sqlite3.connect(sample_db)
    conn.executescript(
        """
        INSERT INTO profiles VALUES (110, 'A', 'B', '', 'm', 30, 1994, 'Vologda',
            NULL, 'VO', 36, 'Россия', '', 0, 0, 0, 0, 0, 0, '{}');
        INSERT INTO results VALUES (60, 1, 10, 110, 0, 3900.0, 'Team A', 10, 9, 5, 'M40', '01:05:00', '{}');
        """
    )
    conn.commit()
    conn.close()

    geo = mq.query_interesting_facts_geography(sample_db, year=2024, sport="run", limit=20)
    city_names = [str(r.get("city")) for r in geo.get("cities", [])]
    assert "Вологда" in city_names


def test_vm_records_champions_cards_counts_by_series_distance_gender(tmp_path: Path) -> None:
    """Чемпионские карточки: тот же ключ, что hierarchy (серия×дистанция×пол)."""
    db = tmp_path / "rec_champ.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE competitions (
            id INTEGER NOT NULL PRIMARY KEY,
            title TEXT,
            title_short TEXT,
            date TEXT,
            year INTEGER,
            sport TEXT
        );
        CREATE TABLE distances (
            id INTEGER NOT NULL PRIMARY KEY,
            competition_id INTEGER,
            name TEXT,
            distance_km REAL,
            is_relay INTEGER DEFAULT 0
        );
        CREATE TABLE profiles (
            id INTEGER NOT NULL PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            second_name TEXT,
            gender TEXT,
            age INTEGER,
            birth_year INTEGER,
            city TEXT,
            city_id INTEGER,
            region TEXT,
            region_id INTEGER,
            country TEXT,
            club TEXT,
            stat_competitions INTEGER,
            stat_km INTEGER,
            stat_marathons INTEGER,
            stat_first INTEGER,
            stat_second INTEGER,
            stat_third INTEGER,
            raw TEXT
        );
        CREATE TABLE results (
            id INTEGER NOT NULL PRIMARY KEY,
            competition_id INTEGER,
            distance_id INTEGER,
            profile_id INTEGER,
            dnf INTEGER DEFAULT 0,
            finish_time_sec REAL,
            team TEXT,
            place_abs INTEGER,
            place_gender INTEGER,
            place_group INTEGER,
            group_name TEXT,
            finish_time TEXT,
            raw TEXT
        );
        INSERT INTO competitions VALUES (
            1, 'Winter Series Full', 'Winter Series',
            '2024-02-01', 2024, 'ski'
        );
        INSERT INTO distances VALUES (101, 1, '10 km freestyle', 10.0, 0);
        INSERT INTO distances VALUES (102, 1, '30 km classic', 30.0, 0);
        INSERT INTO profiles VALUES (
            1, 'A', 'One', '', 'm', 30, 1994,
            'CityA', NULL, NULL, NULL, NULL, '',
            0, 0, 0, 0, 0, 0, '{}'
        );
        INSERT INTO profiles VALUES (
            2, 'B', 'Two', '', 'm', 28, 1996,
            'CityB', NULL, NULL, NULL, NULL, '',
            0, 0, 0, 0, 0, 0, '{}'
        );
        INSERT INTO results VALUES (
            1, 1, 101, 1, 0, 2400.0, '', 1, 1, 1, 'OPEN', '00:40:00', '{}'
        );
        INSERT INTO results VALUES (
            2, 1, 102, 2, 0, 7800.0, '', 1, 1, 1, 'OPEN', '02:10:00', '{}'
        );
        INSERT INTO results VALUES (
            3, 1, 102, 1, 0, 7000.0, '', 2, 2, 2, 'OPEN', '02:40:00', '{}'
        );
        """
    )
    conn.commit()
    conn.close()
    out = mq.query_vm_records_champions_cards(db, years=None, sport="ski")
    m = out["males"]
    assert m["profile_id"] == 1
    assert int(m["records"]) == 2
    assert "One" in m["participant"] and "A" in m["participant"]
