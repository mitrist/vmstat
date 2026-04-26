"""
Дополнительный парсер справочника соревнований через API.

Запускать ПОСЛЕ того, как discover_refs.py нашёл нужные endpoint'ы.

Что делает:
  1. Берёт все competition_id из базы
  2. Дёргает API для каждого — получает полные данные события:
     название, дата, дистанции, группы, тип (бег/вело/лыжи)
  3. Обновляет ref_competitions и создаёт ref_distances

Запуск:
    python fetch_competition_details.py
    python fetch_competition_details.py --api-url "https://vologdamarafon.ru/api/v2/competition/{id}/"
"""

import sqlite3
import json
import time
import argparse
import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB_PATH = Path("profiles.db")

logging.basicConfig(level=logging.INFO, format="%(H:%M:%S)s %(message)s")
log = logging.getLogger(__name__)

# Кандидаты на API endpoint — проверяем все пока не найдём рабочий
API_CANDIDATES = [
    "https://vologdamarafon.ru/api/v2/competition/{id}/",
    "https://vologdamarafon.ru/api/v2/competitions/{id}/",
    "https://vologdamarafon.ru/api/v2/event/{id}/",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://vologdamarafon.ru/",
}

SPORT_KEYWORDS = {
    "ski":  ["лыж", "ski", "лыжн", "cross"],
    "bike": ["вело", "bike", "cycl", "велос"],
    "run":  ["бег", "run", "марафон", "marathon", "полумарафон", "забег", "спринт"],
}

def detect_sport(title: str) -> str:
    t = title.lower()
    for sport, keywords in SPORT_KEYWORDS.items():
        if any(w in t for w in keywords):
            return sport
    return "other"


def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def find_working_api(session, test_id: int) -> str | None:
    """Пробуем кандидатов, возвращаем рабочий шаблон."""
    for template in API_CANDIDATES:
        url = template.format(id=test_id)
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and ("id" in data or "title" in data):
                    log.info(f"✓ Рабочий API: {template}")
                    log.info(f"  Пример: {json.dumps(data, ensure_ascii=False)[:300]}")
                    return template
        except Exception:
            pass
    return None


def init_ref_distances(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ref_distances (
            distance_id     INTEGER PRIMARY KEY,
            competition_id  INTEGER REFERENCES ref_competitions(competition_id),
            name            TEXT,
            distance_km     REAL,
            sport           TEXT,
            is_relay        INTEGER DEFAULT 0
        );
    """)
    conn.commit()


def fetch_and_save(conn: sqlite3.Connection, session, api_template: str,
                   competition_id: int):
    url = api_template.format(id=competition_id)
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 404:
            return False
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"  competition {competition_id}: {e}")
        return False

    title = data.get("title") or data.get("name") or ""
    sport = detect_sport(title)

    # Обновляем ref_competitions
    conn.execute("""
        UPDATE ref_competitions SET
            competition_title = COALESCE(NULLIF(?, ''), competition_title),
            sport = ?
        WHERE competition_id = ?
    """, (title, sport, competition_id))

    # Если в ответе есть дистанции/группы — сохраняем в ref_distances
    distances = (data.get("distances") or data.get("groups") or
                 data.get("distance_groups") or [])
    for d in distances:
        dist_id  = d.get("id") or d.get("distance_id")
        dist_name = d.get("name") or d.get("title") or ""
        dist_km   = float(d.get("distance") or d.get("km") or d.get("distance_km") or 0)
        is_relay  = int(bool(d.get("is_relay", False)))

        if dist_id:
            conn.execute("""
                INSERT OR REPLACE INTO ref_distances
                  (distance_id, competition_id, name, distance_km, sport, is_relay)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dist_id, competition_id, dist_name, dist_km, sport, is_relay))

    conn.commit()
    log.info(f"  [{competition_id}] {title} | {sport} | {len(distances)} дистанций")
    return True


def main(api_template: str | None):
    conn = sqlite3.connect(DB_PATH)
    init_ref_distances(conn)
    session = make_session()

    # Все уникальные competition_id из базы
    comp_ids = [r[0] for r in conn.execute(
        "SELECT DISTINCT competition_id FROM results WHERE competition_id > 0 ORDER BY competition_id"
    ).fetchall()]
    log.info(f"Соревнований в базе: {len(comp_ids)}")

    # Если шаблон не передан — ищем автоматически
    if not api_template:
        log.info("Ищем рабочий API endpoint...")
        api_template = find_working_api(session, comp_ids[0] if comp_ids else 194)
        if not api_template:
            log.error("❌ Рабочий API не найден. Запусти discover_refs.py и передай URL через --api-url")
            conn.close()
            return

    log.info(f"Обрабатываем {len(comp_ids)} соревнований...")
    ok = 0
    for i, cid in enumerate(comp_ids):
        if fetch_and_save(conn, session, api_template, cid):
            ok += 1
        time.sleep(0.2)
        if (i + 1) % 20 == 0:
            log.info(f"  [{i+1}/{len(comp_ids)}] ok={ok}")

    conn.close()
    log.info(f"\n✅ Готово: обновлено {ok} из {len(comp_ids)} соревнований")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url",
        help='Шаблон URL, например: "https://vologdamarafon.ru/api/v2/competition/{id}/"')
    args = parser.parse_args()
    main(args.api_url)
