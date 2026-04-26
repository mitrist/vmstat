"""
Crawler профилей vologdamarafon.ru — использует API v2.

Найденные endpoint'ы:
  GET /api/v2/profile/{id}/
  GET /api/v2/profile/{id}/statistics/
  GET /api/v2/profile/{id}/competition-results/years/
  GET /api/v2/profile/{id}/competition-results/?year=YYYY

Установка:
    pip install requests

Запуск:
    python crawler.py                        # ID 1..5000, 0.3 сек/профиль
    python crawler.py --start 1 --end 500    # только первые 500
    python crawler.py --workers 5            # 5 потоков (осторожно)
    python crawler.py --resume               # продолжить после прерывания

Результат:
    profiles.db   — SQLite база
    profiles.jsonl — построчный JSON (удобно для pandas / DuckDB)
"""

import time
import json
import sqlite3
import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── НАСТРОЙКИ ──────────────────────────────────────────────────────────────

BASE      = "https://vologdamarafon.ru/api/v2/profile"
ID_START  = 1
ID_END    = 18000
DELAY     = 0.3          # секунд между запросами (на поток)
WORKERS   = 3            # параллельных потоков
DB_PATH   = Path("profile.db")
JSONL_PATH = Path("profiles_m.jsonl")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://vologdamarafon.ru/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── МОДЕЛИ ─────────────────────────────────────────────────────────────────

@dataclass
class RaceResult:
    result_id:    int    = 0
    year:         int    = 0
    competition_id: int  = 0
    competition_title: str = ""
    competition_date:  str = ""
    distance_km:  float  = 0.0
    sport:        str    = ""   # run / bike / ski / other
    bib_number:   Optional[int] = None
    place_abs:    Optional[int] = None
    place_gender: Optional[int] = None
    place_group:  Optional[int] = None
    group_name:   str    = ""
    finish_time:  str    = ""
    dnf:          bool   = False
    club:         str    = ""
    is_relay:     bool   = False
    relay_stage:  Optional[int] = None
    certificate_url: str = ""


@dataclass
class ProfileStats:
    competitions:  int = 0
    kilometers:    int = 0
    marathons:     int = 0
    first_places:  int = 0
    second_places: int = 0
    third_places:  int = 0


@dataclass
class Profile:
    id:           int
    first_name:   str = ""
    last_name:    str = ""
    second_name:  str = ""   # отчество
    gender:       str = ""   # m / f
    age:          Optional[int] = None
    birth_year:   Optional[int] = None
    city:         str = ""
    city_id:      Optional[int] = None
    region:       str = ""
    region_id:    Optional[int] = None
    country:      str = ""
    club:         str = ""
    active_years: list = field(default_factory=list)
    stats:        Optional[ProfileStats] = None
    results:      list = field(default_factory=list)   # list[RaceResult]
    error:        str  = ""


# ── БД ─────────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS profiles (
            id          INTEGER PRIMARY KEY,
            first_name  TEXT,
            last_name   TEXT,
            second_name TEXT,
            gender      TEXT,
            age         INTEGER,
            birth_year  INTEGER,
            city        TEXT,
            city_id     INTEGER,
            region      TEXT,
            region_id   INTEGER,
            country     TEXT,
            club        TEXT,
            active_years TEXT,
            -- статистика
            stat_competitions INTEGER,
            stat_kilometers   INTEGER,
            stat_marathons    INTEGER,
            stat_first        INTEGER,
            stat_second       INTEGER,
            stat_third        INTEGER,
            --
            error       TEXT,
            fetched_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS results (
            result_id         INTEGER PRIMARY KEY,
            profile_id        INTEGER REFERENCES profiles(id),
            year              INTEGER,
            competition_id    INTEGER,
            competition_title TEXT,
            competition_date  TEXT,
            distance_km       REAL,
            sport             TEXT,
            bib_number        INTEGER,
            place_abs         INTEGER,
            place_gender      INTEGER,
            place_group       INTEGER,
            group_name        TEXT,
            finish_time       TEXT,
            dnf               INTEGER,
            club              TEXT,
            is_relay          INTEGER,
            relay_stage       INTEGER,
            certificate_url   TEXT
        );

        CREATE TABLE IF NOT EXISTS crawl_log (
            profile_id INTEGER PRIMARY KEY,
            status     TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_results_profile  ON results(profile_id);
        CREATE INDEX IF NOT EXISTS idx_results_comp     ON results(competition_id);
        CREATE INDEX IF NOT EXISTS idx_results_sport    ON results(sport);
        CREATE INDEX IF NOT EXISTS idx_results_year     ON results(year);
        CREATE INDEX IF NOT EXISTS idx_profiles_city    ON profiles(city_id);
        CREATE INDEX IF NOT EXISTS idx_profiles_region  ON profiles(region_id);
        CREATE INDEX IF NOT EXISTS idx_profiles_gender  ON profiles(gender);
    """)
    conn.commit()


def save_profile(conn: sqlite3.Connection, p: Profile):
    s = p.stats or ProfileStats()
    conn.execute("""
        INSERT OR REPLACE INTO profiles
          (id, first_name, last_name, second_name, gender, age, birth_year,
           city, city_id, region, region_id, country, club, active_years,
           stat_competitions, stat_kilometers, stat_marathons,
           stat_first, stat_second, stat_third, error)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        p.id, p.first_name, p.last_name, p.second_name, p.gender,
        p.age, p.birth_year, p.city, p.city_id, p.region, p.region_id,
        p.country, p.club, json.dumps(p.active_years),
        s.competitions, s.kilometers, s.marathons,
        s.first_places, s.second_places, s.third_places,
        p.error,
    ))

    if p.results:
        conn.executemany("""
            INSERT OR REPLACE INTO results
              (result_id, profile_id, year, competition_id, competition_title,
               competition_date, distance_km, sport, bib_number,
               place_abs, place_gender, place_group, group_name,
               finish_time, dnf, club, is_relay, relay_stage, certificate_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [(
            r.result_id, p.id, r.year, r.competition_id, r.competition_title,
            r.competition_date, r.distance_km, r.sport, r.bib_number,
            r.place_abs, r.place_gender, r.place_group, r.group_name,
            r.finish_time, int(r.dnf), r.club, int(r.is_relay),
            r.relay_stage, r.certificate_url,
        ) for r in p.results])

    status = "ok" if not p.error else ("not_found" if "404" in p.error else "error")
    conn.execute("""
        INSERT OR REPLACE INTO crawl_log (profile_id, status) VALUES (?, ?)
    """, (p.id, status))
    conn.commit()


def already_crawled(conn: sqlite3.Connection, pid: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM crawl_log WHERE profile_id=?", (pid,)
    ).fetchone() is not None


# ── ПАРСИНГ ОТВЕТОВ API ────────────────────────────────────────────────────

def detect_sport(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ["лыж", "ski", "cross", "лыжн"]):
        return "ski"
    if any(w in t for w in ["вело", "bike", "cycl", "велос"]):
        return "bike"
    if any(w in t for w in ["бег", "run", "марафон", "marathon",
                             "полумарафон", "забег", "спринт"]):
        return "run"
    return "other"


def parse_profile_data(pid: int, data: dict) -> Profile:
    p = Profile(id=pid)
    p.first_name  = data.get("first_name", "")
    p.last_name   = data.get("last_name", "")
    p.second_name = data.get("second_name", "")
    p.gender      = data.get("gender", "")
    p.age         = data.get("age")
    p.birth_year  = data.get("year")
    p.club        = data.get("club", "") or ""

    loc = data.get("location") or {}
    p.city      = loc.get("city_name", data.get("city", ""))
    p.city_id   = loc.get("city_id")
    p.region    = loc.get("region_name", "")
    p.region_id = loc.get("region_id")
    p.country   = loc.get("country_name", "")
    return p


def parse_results(pid: int, year: int, items: list) -> list[RaceResult]:
    results = []
    for item in items:
        r = RaceResult()
        r.result_id  = item.get("id", 0)
        r.year       = year
        r.place_abs  = item.get("place")
        r.place_gender = item.get("gender_place")
        r.place_group  = item.get("group_place")
        r.finish_time  = item.get("total_time", "")
        r.dnf          = bool(item.get("dnf", False))
        r.club         = item.get("club", "") or ""
        r.bib_number   = item.get("number")
        r.relay_stage  = item.get("relay_stage")
        r.certificate_url = item.get("certificate_url", "") or ""

        comp = item.get("competition") or {}
        r.competition_id    = comp.get("id", 0)
        r.competition_title = comp.get("title_short") or comp.get("title", "")
        r.competition_date  = comp.get("date", "")
        r.is_relay          = bool(comp.get("is_relay", False))
        r.sport             = detect_sport(r.competition_title)

        # Дистанция — из группы (если есть)
        group = item.get("group") or {}
        r.group_name  = group.get("name", "") or ""
        dist_raw = group.get("distance") or group.get("distance_km") or 0
        try:
            r.distance_km = float(str(dist_raw).replace(",", "."))
        except Exception:
            r.distance_km = 0.0

        results.append(r)
    return results


# ── HTTP СЕССИЯ ────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get_json(session: requests.Session, url: str) -> tuple[int, any]:
    """Возвращает (status_code, parsed_json | None)."""
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return 200, r.json()
        return r.status_code, None
    except Exception as e:
        log.debug(f"GET {url} -> {e}")
        return 0, None


# ── ОСНОВНАЯ ФУНКЦИЯ ОБХОДА ОДНОГО ПРОФИЛЯ ────────────────────────────────

def fetch_profile(session: requests.Session, pid: int) -> Profile:
    # 1. Данные профиля
    status, data = get_json(session, f"{BASE}/{pid}/")
    if status == 404:
        return Profile(id=pid, error="404 not_found")
    if status != 200 or not data:
        return Profile(id=pid, error=f"profile_fetch_error status={status}")

    p = parse_profile_data(pid, data)

    # 2. Статистика
    _, stats_data = get_json(session, f"{BASE}/{pid}/statistics/")
    if stats_data:
        p.stats = ProfileStats(
            competitions  = stats_data.get("competitions", 0),
            kilometers    = stats_data.get("kilometers", 0),
            marathons     = stats_data.get("marathons", 0),
            first_places  = stats_data.get("first_places", 0),
            second_places = stats_data.get("second_places", 0),
            third_places  = stats_data.get("third_places", 0),
        )

    # 3. Список лет с результатами
    _, years_data = get_json(session, f"{BASE}/{pid}/competition-results/years/")
    p.active_years = years_data if isinstance(years_data, list) else []

    # 4. Результаты по каждому году
    for year in p.active_years:
        _, results_data = get_json(
            session, f"{BASE}/{pid}/competition-results/?year={year}"
        )
        if isinstance(results_data, list):
            p.results.extend(parse_results(pid, year, results_data))
        time.sleep(0.05)   # небольшая пауза между запросами к одному профилю

    return p


# ── ГЛАВНЫЙ ЦИКЛ ──────────────────────────────────────────────────────────

def crawl(id_start: int, id_end: int, workers: int, delay: float, resume: bool):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    init_db(conn)

    # Определяем что осталось
    todo = [
        pid for pid in range(id_start, id_end + 1)
        if not (resume and already_crawled(conn, pid))
    ]

    log.info(f"Профилей к обработке: {len(todo)}  |  потоков: {workers}  |  пауза: {delay}с")

    counters = {"ok": 0, "not_found": 0, "error": 0, "total": 0}
    jsonl_file = JSONL_PATH.open("a", encoding="utf-8")

    def process_one(pid: int):
        session = make_session()
        p = fetch_profile(session, pid)
        return p

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_one, pid): pid for pid in todo}

        for future in as_completed(futures):
            p = future.result()
            counters["total"] += 1

            # Сохраняем в БД
            try:
                save_profile(conn, p)
            except Exception as e:
                log.error(f"DB save error id={p.id}: {e}")

            # Пишем в JSONL
            row = asdict(p)
            jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            jsonl_file.flush()

            # Счётчики
            if "404" in p.error:
                counters["not_found"] += 1
            elif p.error:
                counters["error"] += 1
            else:
                counters["ok"] += 1

            # Лог каждые 100 или при нахождении участника
            if counters["total"] % 100 == 0 or (p.first_name and not p.error):
                name = f"{p.last_name} {p.first_name}".strip() or "—"
                log.info(
                    f"[{counters['total']}/{len(todo)}] "
                    f"id={p.id} | {name} | "
                    f"{len(p.results)} стартов | "
                    f"ok={counters['ok']} skip={counters['not_found']} err={counters['error']}"
                )

            time.sleep(delay / workers)   # общий темп

    jsonl_file.close()
    conn.close()

    log.info("=" * 60)
    log.info(f"Готово: ok={counters['ok']}  "
             f"не найдено={counters['not_found']}  "
             f"ошибки={counters['error']}")
    log.info(f"База:  {DB_PATH.resolve()}")
    log.info(f"JSONL: {JSONL_PATH.resolve()}")


# ── CLI ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vologda Marathon API crawler v2")
    parser.add_argument("--start",   type=int,   default=ID_START, help="Начальный ID")
    parser.add_argument("--end",     type=int,   default=ID_END,   help="Конечный ID")
    parser.add_argument("--workers", type=int,   default=WORKERS,  help="Потоков")
    parser.add_argument("--delay",   type=float, default=DELAY,    help="Пауза (сек)")
    parser.add_argument("--resume",  action="store_true", default=True,
                        help="Пропускать уже обработанные ID (по умолчанию включено)")
    parser.add_argument("--fresh",   action="store_true",
                        help="Игнорировать crawl_log, обрабатывать всё заново")
    args = parser.parse_args()

    crawl(
        id_start = args.start,
        id_end   = args.end,
        workers  = args.workers,
        delay    = args.delay,
        resume   = not args.fresh,
    )
