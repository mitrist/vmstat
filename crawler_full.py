"""
Полный crawler vologdamarafon.ru
================================
Использует все найденные API v2 endpoint'ы.

Стратегия сбора:
  1. Получаем список всех соревнований (перебор ID)
  2. Для каждого соревнования — дистанции, группы, результаты, участники
  3. Получаем список всех кубков → дистанции, события, группы, результаты
  4. Профили участников (из results берём profile_id, качаем профиль + статистику)

Запуск:
    pip install requests
    python crawler_full.py                        # всё подряд
    python crawler_full.py --only competitions    # только события
    python crawler_full.py --only cups            # только кубки
    python crawler_full.py --only profiles        # только профили
    python crawler_full.py --comp-start 1 --comp-end 400
    python crawler_full.py --resume               # продолжить после прерывания
"""

import time
import json
import sqlite3
import logging
import argparse
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout
from urllib3.util.retry import Retry

# ── НАСТРОЙКИ ──────────────────────────────────────────────────────────────

BASE        = "https://vologdamarafon.ru/api/v2"
DB_PATH     = Path("marathon.db")

COMP_ID_START = 1
COMP_ID_END   = 400     # подбери по факту — у нас comp 298 уже есть

PROFILE_ID_START = 1
PROFILE_ID_END   = 18000

REQUEST_DELAY = 0.25    # сек между запросами

# (connect, read) — крупные списки результатов кубков не укладываются в 15s read
HTTP_TIMEOUT = (15, 60)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://vologdamarafon.ru/",
}

# ── СХЕМА БД ───────────────────────────────────────────────────────────────

SCHEMA = """
-- ═══════════════════════════════════════════════════
-- СОРЕВНОВАНИЯ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS competitions (
    id              INTEGER PRIMARY KEY,
    title           TEXT,
    title_short     TEXT,
    date            TEXT,
    year            INTEGER,
    sport           TEXT,        -- run / bike / ski / other
    is_relay        INTEGER,
    is_published    INTEGER,
    page_url        TEXT,
    raw             TEXT         -- полный JSON от API
);

CREATE TABLE IF NOT EXISTS competition_status (
    competition_id  INTEGER PRIMARY KEY REFERENCES competitions(id),
    status          TEXT,
    participants    INTEGER,
    finishers       INTEGER,
    dnf             INTEGER,
    raw             TEXT
);

CREATE TABLE IF NOT EXISTS competition_stats (
    competition_id  INTEGER PRIMARY KEY REFERENCES competitions(id),
    total_members   INTEGER,
    male            INTEGER,
    female          INTEGER,
    teams           INTEGER,
    regions         INTEGER,
    raw             TEXT
);

-- ═══════════════════════════════════════════════════
-- ДИСТАНЦИИ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS distances (
    id              INTEGER PRIMARY KEY,
    competition_id  INTEGER REFERENCES competitions(id),
    name            TEXT,
    distance_km     REAL,
    sport           TEXT,
    is_relay        INTEGER,
    max_participants INTEGER,
    raw             TEXT
);

-- ═══════════════════════════════════════════════════
-- ВОЗРАСТНЫЕ ГРУППЫ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS groups (
    id              INTEGER PRIMARY KEY,
    competition_id  INTEGER REFERENCES competitions(id),
    distance_id     INTEGER REFERENCES distances(id),
    name            TEXT,
    gender          TEXT,       -- m / f / mixed
    age_from        INTEGER,
    age_to          INTEGER,
    raw             TEXT
);

-- ═══════════════════════════════════════════════════
-- РЕЗУЛЬТАТЫ СОРЕВНОВАНИЙ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS results (
    id              INTEGER PRIMARY KEY,
    competition_id  INTEGER REFERENCES competitions(id),
    distance_id     INTEGER REFERENCES distances(id),
    profile_id      INTEGER,
    bib_number      INTEGER,
    place_abs       INTEGER,
    place_gender    INTEGER,
    place_group     INTEGER,
    group_id        INTEGER REFERENCES groups(id),
    group_name      TEXT,
    finish_time     TEXT,       -- "HH:MM:SS.ss"
    finish_time_sec REAL,       -- для сортировки и расчётов
    dnf             INTEGER,
    club            TEXT,
    team            TEXT,
    is_relay        INTEGER,
    relay_stage     INTEGER,
    certificate_url TEXT,
    raw             TEXT
);

-- ═══════════════════════════════════════════════════
-- КУБКИ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS cups (
    id              INTEGER PRIMARY KEY,
    title           TEXT,
    year            INTEGER,
    sport           TEXT,
    raw             TEXT
);

CREATE TABLE IF NOT EXISTS cup_distances (
    id              INTEGER PRIMARY KEY,
    cup_id          INTEGER REFERENCES cups(id),
    name            TEXT,
    distance_km     REAL,
    sport           TEXT,
    raw             TEXT
);

CREATE TABLE IF NOT EXISTS cup_competitions (
    cup_id          INTEGER REFERENCES cups(id),
    competition_id  INTEGER REFERENCES competitions(id),
    PRIMARY KEY (cup_id, competition_id)
);

CREATE TABLE IF NOT EXISTS cup_groups (
    id              INTEGER PRIMARY KEY,
    cup_id          INTEGER REFERENCES cups(id),
    distance_id     INTEGER REFERENCES cup_distances(id),
    name            TEXT,
    gender          TEXT,
    age_from        INTEGER,
    age_to          INTEGER,
    raw             TEXT
);

CREATE TABLE IF NOT EXISTS cup_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cup_id          INTEGER REFERENCES cups(id),
    distance_id     INTEGER REFERENCES cup_distances(id),
    profile_id      INTEGER,
    place_abs       INTEGER,
    place_gender    INTEGER,
    place_group     INTEGER,
    group_id        INTEGER,
    group_name      TEXT,
    total_points    REAL,
    total_time      TEXT,
    competitions_count INTEGER,
    raw             TEXT
);

-- ═══════════════════════════════════════════════════
-- ПРОФИЛИ УЧАСТНИКОВ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS profiles (
    id              INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    second_name     TEXT,
    gender          TEXT,
    age             INTEGER,
    birth_year      INTEGER,
    city            TEXT,
    city_id         INTEGER,
    region          TEXT,
    region_id       INTEGER,
    country         TEXT,
    club            TEXT,
    -- агрегаты из /statistics/
    stat_competitions INTEGER,
    stat_km           INTEGER,
    stat_marathons    INTEGER,
    stat_first        INTEGER,
    stat_second       INTEGER,
    stat_third        INTEGER,
    raw             TEXT
);

CREATE TABLE IF NOT EXISTS profile_cup_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      INTEGER REFERENCES profiles(id),
    year            INTEGER,
    cup_id          INTEGER REFERENCES cups(id),
    cup_title       TEXT,
    distance_id     INTEGER,
    distance_name   TEXT,
    place_abs       INTEGER,
    place_gender    INTEGER,
    place_group     INTEGER,
    group_name      TEXT,
    total_points    REAL,
    raw             TEXT
);

-- Расчётные очки кубка (правила вне API; cup_scoring.py, compute_cup_scoring.py)
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

-- ═══════════════════════════════════════════════════
-- СЛУЖЕБНЫЕ
-- ═══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS crawl_log (
    entity      TEXT,   -- competition / cup / profile
    entity_id   INTEGER,
    status      TEXT,   -- ok / not_found / error
    fetched_at  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (entity, entity_id)
);

-- индексы
CREATE INDEX IF NOT EXISTS idx_results_comp     ON results(competition_id);
CREATE INDEX IF NOT EXISTS idx_results_dist     ON results(distance_id);
CREATE INDEX IF NOT EXISTS idx_results_profile  ON results(profile_id);
CREATE INDEX IF NOT EXISTS idx_results_group    ON results(group_id);
CREATE INDEX IF NOT EXISTS idx_distances_comp   ON distances(competition_id);
CREATE INDEX IF NOT EXISTS idx_groups_dist      ON groups(distance_id);
CREATE INDEX IF NOT EXISTS idx_cup_results_cup  ON cup_results(cup_id);
CREATE INDEX IF NOT EXISTS idx_cup_results_prof ON cup_results(profile_id);
CREATE INDEX IF NOT EXISTS idx_profiles_city    ON profiles(city_id);
CREATE INDEX IF NOT EXISTS idx_profiles_gender  ON profiles(gender);
CREATE INDEX IF NOT EXISTS idx_comp_year        ON competitions(year);
CREATE INDEX IF NOT EXISTS idx_comp_sport       ON competitions(sport);
"""

# ── HTTP ───────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get(session: requests.Session, url: str) -> tuple[int, any]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return 200, r.json()
        return r.status_code, None
    except Timeout as e:
        log.warning("Таймаут HTTP при чтении ответа: %s — %s", url, e)
        return 0, None
    except Exception as e:
        log.debug(f"GET {url} → {e}")
        return 0, None


def sleep():
    time.sleep(REQUEST_DELAY)

# ── УТИЛИТЫ ────────────────────────────────────────────────────────────────

SPORT_MAP = {
    "ski":  ["лыж", "ski", "лыжн", "cross"],
    "bike": ["вело", "bike", "cycl", "велос"],
    "run":  ["бег", "run", "марафон", "marathon", "полумарафон", "забег", "спринт"],
}

def detect_sport(text: str) -> str:
    t = (text or "").lower()
    for sport, kw in SPORT_MAP.items():
        if any(w in t for w in kw):
            return sport
    return "other"


def time_to_sec(t: str) -> Optional[float]:
    """'HH:MM:SS.ss' → секунды"""
    if not t:
        return None
    try:
        parts = t.split(":")
        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        return None



def scalar(value, field: str = "") -> str:
    """
    Безопасно приводит любое значение к строке для SQLite.
    API иногда возвращает {"id":1,"name":"..."} вместо строки.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        name = (value.get("name") or value.get("title") or
                value.get("full_name") or value.get("short_name") or "")
        log.debug(f"  scalar({field}): dict->\'{name}\'  raw={value}")
        return str(name)
    if isinstance(value, list):
        log.debug(f"  scalar({field}): list->\'\'  raw={value}")
        return ""
    return str(value)


def _result_person(item: dict) -> dict:
    """Участник в строке результатов: API v2 отдаёт competitor, иногда profile."""
    for key in ("profile", "competitor"):
        v = item.get(key)
        if isinstance(v, dict) and v.get("id") is not None:
            return v
    return {}


def scalar_int(value):
    """Безопасно приводит к int или None."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        return value.get("id")
    try:
        return int(value)
    except Exception:
        return None


def crawled(conn, entity: str, eid: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM crawl_log WHERE entity=? AND entity_id=?", (entity, eid)
    ).fetchone() is not None


def log_crawl(conn, entity: str, eid: int, status: str):
    conn.execute("""
        INSERT OR REPLACE INTO crawl_log (entity, entity_id, status)
        VALUES (?, ?, ?)
    """, (entity, eid, status))
    conn.commit()

# ── СОРЕВНОВАНИЯ ───────────────────────────────────────────────────────────

def fetch_competition(conn, session, comp_id: int):
    if crawled(conn, "competition", comp_id):
        return

    # 1. Основные данные
    status, data = get(session, f"{BASE}/competitions/{comp_id}/")
    sleep()
    if status == 404:
        log_crawl(conn, "competition", comp_id, "not_found")
        return
    if status != 200 or not data:
        log_crawl(conn, "competition", comp_id, f"error_{status}")
        return

    title = data.get("title") or data.get("title_short") or ""
    sport = detect_sport(title)
    year  = None
    date  = data.get("date") or ""
    if date:
        try:
            year = int(date[:4])
        except Exception:
            pass

    conn.execute("""
        INSERT OR REPLACE INTO competitions
          (id, title, title_short, date, year, sport,
           is_relay, is_published, page_url, raw)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        comp_id,
        title,
        data.get("title_short", ""),
        date, year, sport,
        int(bool(data.get("is_relay", False))),
        int(bool(data.get("is_results_published", False))),
        data.get("page_url", ""),
        json.dumps(data, ensure_ascii=False),
    ))
    conn.commit()

    log.info(f"  competition {comp_id}: {title} ({year}) [{sport}]")

    # 2. Статус
    _, st = get(session, f"{BASE}/competitions/{comp_id}/status/")
    sleep()
    if st:
        conn.execute("""
            INSERT OR REPLACE INTO competition_status
              (competition_id, status, participants, finishers, dnf, raw)
            VALUES (?,?,?,?,?,?)
        """, (comp_id,
              st.get("status"),
              st.get("participants") or st.get("total"),
              st.get("finishers"),
              st.get("dnf"),
              json.dumps(st, ensure_ascii=False)))
        conn.commit()

    # 3. Статистика участия
    _, ms = get(session, f"{BASE}/competitions/{comp_id}/members/statistics/")
    sleep()
    if ms:
        conn.execute("""
            INSERT OR REPLACE INTO competition_stats
              (competition_id, total_members, male, female, teams, regions, raw)
            VALUES (?,?,?,?,?,?,?)
        """, (comp_id,
              ms.get("total") or ms.get("members") or ms.get("count"),
              ms.get("male") or ms.get("men"),
              ms.get("female") or ms.get("women"),
              ms.get("teams"),
              ms.get("regions"),
              json.dumps(ms, ensure_ascii=False)))
        conn.commit()

    # 4. Дистанции
    _, dists = get(session, f"{BASE}/competitions/{comp_id}/distances/")
    sleep()
    dist_ids = []
    if isinstance(dists, list):
        for d in dists:
            did = d.get("id")
            if not did:
                continue
            dist_ids.append(did)
            dkm = float(d.get("distance") or d.get("distance_km") or
                        d.get("km") or 0)
            conn.execute("""
                INSERT OR REPLACE INTO distances
                  (id, competition_id, name, distance_km, sport, is_relay, raw)
                VALUES (?,?,?,?,?,?,?)
            """, (did, comp_id,
                  d.get("name") or d.get("title") or "",
                  dkm, sport,
                  int(bool(d.get("is_relay", False))),
                  json.dumps(d, ensure_ascii=False)))
        conn.commit()
        log.info(f"    дистанций: {len(dist_ids)}")

    # 5. Группы и результаты по каждой дистанции
    for did in dist_ids:
        # Группы
        _, groups = get(session, f"{BASE}/competitions/{comp_id}/groups/?distance={did}")
        sleep()
        if isinstance(groups, list):
            for g in groups:
                gid = g.get("id")
                if not gid:
                    continue
                name = g.get("name") or ""
                gender = g.get("gender") or _guess_gender(name)
                conn.execute("""
                    INSERT OR REPLACE INTO groups
                      (id, competition_id, distance_id, name, gender,
                       age_from, age_to, raw)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (gid, comp_id, did, name, gender,
                      g.get("age_from") or g.get("min_age"),
                      g.get("age_to") or g.get("max_age"),
                      json.dumps(g, ensure_ascii=False)))
            conn.commit()

        # Результаты
        _, res = get(session, f"{BASE}/competitions/{comp_id}/results/?distance={did}")
        sleep()
        if isinstance(res, list):
            rows = []
            for item in res:
                rid = item.get("id")
                if not rid:
                    continue
                ft = item.get("total_time") or item.get("finish_time") or ""
                person = _result_person(item)
                group   = item.get("group") or {}
                rows.append((
                    rid, comp_id, did,
                    scalar_int(person.get("id") if person else item.get("profile_id")),
                    scalar_int(item.get("number") or item.get("bib")),
                    scalar_int(item.get("place")),
                    scalar_int(item.get("gender_place")),
                    scalar_int(item.get("group_place")),
                    scalar_int(group.get("id") if isinstance(group, dict) else item.get("group_id")),
                    scalar(group.get("name") if isinstance(group, dict) else item.get("group_name"), "group_name"),
                    scalar(ft, "finish_time"),
                    time_to_sec(ft),
                    int(bool(item.get("dnf", False))),
                    scalar(item.get("club"), "club"),
                    scalar(item.get("team"), "team"),
                    int(bool(item.get("is_relay", False))),
                    scalar_int(item.get("relay_stage")),
                    scalar(item.get("certificate_url"), "certificate_url"),
                    json.dumps(item, ensure_ascii=False),
                ))
            if rows:
                conn.executemany("""
                    INSERT OR REPLACE INTO results
                      (id, competition_id, distance_id, profile_id,
                       bib_number, place_abs, place_gender, place_group,
                       group_id, group_name, finish_time, finish_time_sec,
                       dnf, club, team, is_relay, relay_stage,
                       certificate_url, raw)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, rows)
                conn.commit()
            log.info(f"    dist {did}: {len(rows)} результатов")

    log_crawl(conn, "competition", comp_id, "ok")


def _guess_gender(name: str) -> str:
    n = name.lower()
    if any(w in n for w in ["муж", "мен", " м", "male", "men"]):
        return "m"
    if any(w in n for w in ["жен", "wom", "female", " ж"]):
        return "f"
    return ""

# ── КУБКИ ──────────────────────────────────────────────────────────────────

def fetch_all_cups(conn, session):
    log.info("=== Кубки ===")
    _, cups = get(session, f"{BASE}/cups/")
    sleep()
    if not isinstance(cups, list):
        log.warning("  Список кубков не получен")
        return

    log.info(f"  Найдено кубков: {len(cups)}")

    for cup in cups:
        cup_id = cup.get("id")
        if not cup_id:
            continue

        title = cup.get("title") or cup.get("name") or ""
        year  = cup.get("year")
        sport = detect_sport(title)

        conn.execute("""
            INSERT OR REPLACE INTO cups (id, title, year, sport, raw)
            VALUES (?,?,?,?,?)
        """, (cup_id, title, year, sport, json.dumps(cup, ensure_ascii=False)))
        conn.commit()
        log.info(f"  cup {cup_id}: {title}")

        if crawled(conn, "cup", cup_id):
            continue

        # Дистанции кубка
        _, cdists = get(session, f"{BASE}/cups/{cup_id}/distances/")
        sleep()
        dist_ids = []
        if isinstance(cdists, list):
            for d in cdists:
                did = d.get("id")
                if not did:
                    continue
                dist_ids.append(did)
                conn.execute("""
                    INSERT OR REPLACE INTO cup_distances
                      (id, cup_id, name, distance_km, sport, raw)
                    VALUES (?,?,?,?,?,?)
                """, (did, cup_id,
                      d.get("name") or "",
                      float(d.get("distance") or d.get("distance_km") or d.get("km") or 0),
                      sport,
                      json.dumps(d, ensure_ascii=False)))
            conn.commit()

        # События внутри кубка
        _, ccomps = get(session, f"{BASE}/cups/{cup_id}/competitions/")
        sleep()
        if isinstance(ccomps, list):
            for cc in ccomps:
                ccid = cc.get("id")
                if ccid:
                    conn.execute("""
                        INSERT OR IGNORE INTO cup_competitions (cup_id, competition_id)
                        VALUES (?,?)
                    """, (cup_id, ccid))
            conn.commit()

        # Группы и результаты по дистанциям кубка
        for did in dist_ids:
            _, cgroups = get(session, f"{BASE}/cups/{cup_id}/groups/?distance={did}")
            sleep()
            if isinstance(cgroups, list):
                for g in cgroups:
                    gid = g.get("id")
                    if gid:
                        conn.execute("""
                            INSERT OR REPLACE INTO cup_groups
                              (id, cup_id, distance_id, name, gender,
                               age_from, age_to, raw)
                            VALUES (?,?,?,?,?,?,?,?)
                        """, (gid, cup_id, did,
                              g.get("name") or "",
                              g.get("gender") or _guess_gender(g.get("name") or ""),
                              g.get("age_from"), g.get("age_to"),
                              json.dumps(g, ensure_ascii=False)))
                conn.commit()

            _, cres = get(session, f"{BASE}/cups/{cup_id}/results/?distance={did}")
            sleep()
            if isinstance(cres, list):
                rows = []
                for item in cres:
                    person = _result_person(item)
                    group   = item.get("group") or {}
                    rows.append((
                        cup_id, did,
                        scalar_int(person.get("id") if person else item.get("profile_id")),
                        scalar_int(item.get("place")),
                        scalar_int(item.get("gender_place")),
                        scalar_int(item.get("group_place")),
                        scalar_int(group.get("id") if isinstance(group, dict) else item.get("group_id")),
                        scalar(group.get("name") if isinstance(group, dict) else "", "group_name"),
                        item.get("total_points") or item.get("points"),
                        scalar(item.get("total_time") or item.get("time"), "total_time"),
                        scalar_int(item.get("competitions_count") or item.get("count")),
                        json.dumps(item, ensure_ascii=False),
                    ))
                if rows:
                    conn.executemany("""
                        INSERT INTO cup_results
                          (cup_id, distance_id, profile_id,
                           place_abs, place_gender, place_group,
                           group_id, group_name, total_points, total_time,
                           competitions_count, raw)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """, rows)
                    conn.commit()
                log.info(f"    cup {cup_id} dist {did}: {len(rows)} результатов")

        log_crawl(conn, "cup", cup_id, "ok")

# ── ПРОФИЛИ ────────────────────────────────────────────────────────────────

def fetch_profile(conn, session, pid: int):
    if crawled(conn, "profile", pid):
        return

    status, data = get(session, f"{BASE}/profile/{pid}/")
    sleep()
    if status == 404:
        log_crawl(conn, "profile", pid, "not_found")
        return
    if status != 200 or not data:
        log_crawl(conn, "profile", pid, f"error_{status}")
        return

    loc = data.get("location") or {}
    conn.execute("""
        INSERT OR REPLACE INTO profiles
          (id, first_name, last_name, second_name, gender, age, birth_year,
           city, city_id, region, region_id, country, club, raw)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        pid,
        data.get("first_name", ""),
        data.get("last_name", ""),
        data.get("second_name", ""),
        data.get("gender", ""),
        data.get("age"),
        data.get("year"),
        loc.get("city_name") or data.get("city", ""),
        loc.get("city_id"),
        loc.get("region_name", ""),
        loc.get("region_id"),
        loc.get("country_name", ""),
        data.get("club", "") or "",
        json.dumps(data, ensure_ascii=False),
    ))
    conn.commit()

    # Статистика
    _, stats = get(session, f"{BASE}/profile/{pid}/statistics/")
    sleep()
    if stats:
        conn.execute("""
            UPDATE profiles SET
                stat_competitions = ?,
                stat_km           = ?,
                stat_marathons    = ?,
                stat_first        = ?,
                stat_second       = ?,
                stat_third        = ?
            WHERE id = ?
        """, (
            stats.get("competitions"),
            stats.get("kilometers"),
            stats.get("marathons"),
            stats.get("first_places"),
            stats.get("second_places"),
            stats.get("third_places"),
            pid,
        ))
        conn.commit()

    # Годы с результатами
    _, years = get(session, f"{BASE}/profile/{pid}/competition-results/years/")
    sleep()
    years = years if isinstance(years, list) else []

    # Результаты по каждому году
    for year in years:
        _, yr = get(session, f"{BASE}/profile/{pid}/competition-results/?year={year}")
        sleep()
        # Результаты идут в таблицу results (уже есть из парсинга соревнований)
        # Здесь только проверяем что профиль участвовал — данные уже есть

    # Результаты кубков по годам
    for year in years:
        _, cr = get(session, f"{BASE}/profile/{pid}/cup-results/?year={year}")
        sleep()
        if isinstance(cr, list):
            rows = []
            for item in cr:
                cup   = item.get("cup") or {}
                dist  = item.get("distance") or {}
                group = item.get("group") or {}
                rows.append((
                    pid, year,
                    scalar_int(cup.get("id") if isinstance(cup, dict) else item.get("cup_id")),
                    scalar(cup.get("title") if isinstance(cup, dict) else "", "cup_title"),
                    scalar_int(dist.get("id") if isinstance(dist, dict) else item.get("distance_id")),
                    scalar(dist.get("name") if isinstance(dist, dict) else "", "dist_name"),
                    scalar_int(item.get("place")),
                    scalar_int(item.get("gender_place")),
                    scalar_int(item.get("group_place")),
                    scalar(group.get("name") if isinstance(group, dict) else "", "group_name"),
                    item.get("total_points") or item.get("points"),
                    json.dumps(item, ensure_ascii=False),
                ))
            if rows:
                conn.executemany("""
                    INSERT INTO profile_cup_results
                      (profile_id, year, cup_id, cup_title, distance_id,
                       distance_name, place_abs, place_gender, place_group,
                       group_name, total_points, raw)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, rows)
                conn.commit()

    name = f"{data.get('last_name','')} {data.get('first_name','')}".strip()
    log.info(f"  profile {pid}: {name} | {len(years)} лет активности")
    log_crawl(conn, "profile", pid, "ok")

# ── ВСПОМОГАТЕЛЬНО: профили из результатов ─────────────────────────────────

def fetch_profiles_from_results(conn, session):
    """Качаем профили всех участников, найденных в результатах."""
    log.info("=== Профили из результатов ===")
    pids = [r[0] for r in conn.execute("""
        SELECT DISTINCT profile_id FROM results
        WHERE profile_id IS NOT NULL
        UNION
        SELECT DISTINCT profile_id FROM cup_results
        WHERE profile_id IS NOT NULL
        ORDER BY profile_id
    """).fetchall()]
    log.info(f"  Уникальных участников: {len(pids)}")

    for i, pid in enumerate(pids):
        fetch_profile(conn, session, pid)
        if (i + 1) % 100 == 0:
            log.info(f"  [{i+1}/{len(pids)}]")

# ── ГЛАВНЫЙ ЦИКЛ ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["competitions", "cups", "profiles"],
                        help="Запустить только один этап")
    parser.add_argument("--comp-start", type=int, default=COMP_ID_START)
    parser.add_argument("--comp-end",   type=int, default=COMP_ID_END)
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--fresh", action="store_true",
                        help="Игнорировать crawl_log")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()

    session = make_session()

    run_comps    = args.only in (None, "competitions")
    run_cups     = args.only in (None, "cups")
    run_profiles = args.only in (None, "profiles")

    if args.fresh:
        conn.execute("DELETE FROM crawl_log")
        conn.commit()

    if run_comps:
        log.info(f"=== Соревнования (ID {args.comp_start}–{args.comp_end}) ===")
        for cid in range(args.comp_start, args.comp_end + 1):
            fetch_competition(conn, session, cid)

    if run_cups:
        fetch_all_cups(conn, session)

    if run_profiles:
        fetch_profiles_from_results(conn, session)

    conn.close()

    log.info("=" * 60)
    log.info(f"✅ Готово. База: {DB_PATH.resolve()}")
    _print_summary()


def _print_summary():
    conn = sqlite3.connect(DB_PATH)
    for label, sql in [
        ("Соревнований",  "SELECT COUNT(*) FROM competitions"),
        ("Дистанций",     "SELECT COUNT(*) FROM distances"),
        ("Результатов",   "SELECT COUNT(*) FROM results"),
        ("Кубков",        "SELECT COUNT(*) FROM cups"),
        ("Рез. кубков",   "SELECT COUNT(*) FROM cup_results"),
        ("Профилей",      "SELECT COUNT(*) FROM profiles"),
    ]:
        n = conn.execute(sql).fetchone()[0]
        log.info(f"  {label}: {n}")
    conn.close()


if __name__ == "__main__":
    main()
