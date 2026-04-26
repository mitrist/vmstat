"""
ШАГ 2: Полный crawler всех профилей.

После запуска step1_discover_api.py:
1. Открой captured_requests.json
2. Найди URL, по которому приходят результаты участника
3. Вставь шаблон этого URL в API_TEMPLATE ниже

Пример: если для профиля 2139 найден запрос
    https://vologdamarafon.ru/api/v1/participant/2139/results/
то шаблон будет:
    API_TEMPLATE = "https://vologdamarafon.ru/api/v2/profile/{id}/"

Если отдельного API нет — используй режим PLAYWRIGHT_MODE = True,
тогда скрипт будет парсить через браузер (медленнее, но надёжнее).

Запуск:
    python step2_crawler.py

Результат: profiles.db (SQLite) + profiles.jsonl (построчный JSON)
"""

import time
import json
import sqlite3
import re
import argparse
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── НАСТРОЙКИ ────────────────────────────────────────────────────────────────

# Шаблон API (подставь после step1). {id} заменяется на ID профиля.
# Если API не нашёлся — оставь None и включи PLAYWRIGHT_MODE = True
API_TEMPLATE: Optional[str] = None

# Если True — парсим через Playwright (медленнее, зато работает без API)
PLAYWRIGHT_MODE: bool = False

BASE_URL = "https://vologdamarafon.ru/profile/{id}/"

# Диапазон ID для обхода
ID_START = 1
ID_END   = 5000

# Пауза между запросами (секунды). Будь вежлив с сервером!
DELAY = 0.5

# Число потоков (только для requests-режима, не для Playwright)
WORKERS = 3

# Куда сохранять
DB_PATH   = Path("profiles.db")
JSONL_PATH = Path("profiles.jsonl")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://vologdamarafon.ru/",
}

# ── МОДЕЛИ ДАННЫХ ─────────────────────────────────────────────────────────────

@dataclass
class RaceResult:
    event_name:  str = ""
    event_date:  str = ""
    distance_km: float = 0.0
    sport:       str = ""   # run / bike / ski
    finish_time: str = ""
    place_abs:   Optional[int] = None  # место в абсолютном зачёте
    place_cat:   Optional[int] = None  # место в категории
    category:    str = ""              # M35, W40 и т.п.
    team:        str = ""
    points:      Optional[float] = None  # очки Кубка (если есть)
    raw:         dict = field(default_factory=dict)


@dataclass
class Profile:
    id:       int
    name:     str = ""
    age:      Optional[int] = None
    gender:   str = ""   # M / F
    city:     str = ""
    club:     str = ""
    results:  list = field(default_factory=list)  # list[RaceResult]
    error:    str = ""   # если не удалось спарсить


# ── БАЗА ДАННЫХ ───────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS profiles (
            id       INTEGER PRIMARY KEY,
            name     TEXT,
            age      INTEGER,
            gender   TEXT,
            city     TEXT,
            club     TEXT,
            error    TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS results (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id   INTEGER REFERENCES profiles(id),
            event_name   TEXT,
            event_date   TEXT,
            distance_km  REAL,
            sport        TEXT,
            finish_time  TEXT,
            place_abs    INTEGER,
            place_cat    INTEGER,
            category     TEXT,
            team         TEXT,
            points       REAL,
            raw          TEXT
        );

        CREATE TABLE IF NOT EXISTS crawl_log (
            profile_id INTEGER PRIMARY KEY,
            status     TEXT,   -- ok / not_found / error
            fetched_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


def save_profile(conn: sqlite3.Connection, p: Profile):
    conn.execute("""
        INSERT OR REPLACE INTO profiles (id, name, age, gender, city, club, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (p.id, p.name, p.age, p.gender, p.city, p.club, p.error))

    if p.results:
        conn.executemany("""
            INSERT INTO results
              (profile_id, event_name, event_date, distance_km, sport,
               finish_time, place_abs, place_cat, category, team, points, raw)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            (p.id, r.event_name, r.event_date, r.distance_km, r.sport,
             r.finish_time, r.place_abs, r.place_cat, r.category,
             r.team, r.points, json.dumps(r.raw, ensure_ascii=False))
            for r in p.results
        ])

    status = "ok" if not p.error else "error"
    conn.execute("""
        INSERT OR REPLACE INTO crawl_log (profile_id, status) VALUES (?, ?)
    """, (p.id, status))
    conn.commit()


def already_crawled(conn: sqlite3.Connection, profile_id: int) -> bool:
    row = conn.execute(
        "SELECT status FROM crawl_log WHERE profile_id = ?", (profile_id,)
    ).fetchone()
    return row is not None


# ── ПАРСИНГ HTML (СТАТИЧЕСКАЯ ЧАСТЬ) ─────────────────────────────────────────

from bs4 import BeautifulSoup

def parse_static_html(html: str, profile_id: int) -> Profile:
    """Извлекаем ФИО, возраст, город из статического HTML."""
    soup = BeautifulSoup(html, "html.parser")
    p = Profile(id=profile_id)

    # Имя из <h1>
    h1 = soup.find("h1")
    if h1:
        p.name = h1.get_text(strip=True)

    # Пол — пробуем угадать по имени (отчество на -вна / -евна = женский)
    if p.name:
        parts = p.name.split()
        if len(parts) >= 3:
            patronymic = parts[2].lower()
            p.gender = "F" if patronymic.endswith(("вна", "евна", "овна")) else "M"

    # Возраст и город ищем рядом с лейблами
    text = soup.get_text(" ", strip=True)

    age_m = re.search(r"Возраст\s+(\d+)\s*лет", text)
    if age_m:
        p.age = int(age_m.group(1))

    city_m = re.search(r"Город\s+([^\n\r]+?)(?:Клуб|Команда|$)", text)
    if city_m:
        p.city = city_m.group(1).strip()

    club_m = re.search(r"(?:Клуб|Команда)\s+([^\n\r]+?)(?:Возраст|Город|$)", text)
    if club_m:
        p.club = club_m.group(1).strip()

    return p


# ── ПАРСИНГ РЕЗУЛЬТАТОВ ───────────────────────────────────────────────────────

def parse_results_from_json(data: dict | list) -> list[RaceResult]:
    """
    Разбираем JSON с результатами.
    Структура зависит от реального API — адаптируй под ответ из step1.
    """
    results = []

    # Пример: data = {"results": [...]}  или  data = [...]
    items = data if isinstance(data, list) else data.get("results", data.get("data", []))

    for item in items:
        r = RaceResult(raw=item)

        # Поля — угадываем по типичным именам, адаптируй под реальный API
        r.event_name  = item.get("event_name") or item.get("marafon") or item.get("event") or ""
        r.event_date  = item.get("date") or item.get("event_date") or ""
        r.finish_time = item.get("time") or item.get("finish_time") or item.get("result") or ""
        r.distance_km = float(item.get("distance", 0) or item.get("distance_km", 0) or 0)
        r.category    = item.get("category") or item.get("age_group") or ""
        r.team        = item.get("team") or item.get("club") or ""

        place = item.get("place") or item.get("position")
        if place:
            try:
                r.place_abs = int(str(place).split("/")[0])
            except Exception:
                pass

        # Определяем вид спорта по названию события или полю
        sport_raw = (item.get("sport") or item.get("type") or r.event_name).lower()
        if any(w in sport_raw for w in ["лыж", "ski", "cross"]):
            r.sport = "ski"
        elif any(w in sport_raw for w in ["вело", "bike", "cycl"]):
            r.sport = "bike"
        else:
            r.sport = "run"

        results.append(r)

    return results


def parse_results_from_html(html: str) -> list[RaceResult]:
    """
    Резервный вариант — ищем таблицу результатов прямо в HTML.
    Используется если API отдаёт HTML, а не JSON.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells:
                continue

            raw = dict(zip(headers, cells))
            r = RaceResult(raw=raw)

            for key, val in raw.items():
                if "событи" in key or "марафон" in key or "event" in key:
                    r.event_name = val
                elif "дата" in key or "date" in key:
                    r.event_date = val
                elif "время" in key or "time" in key or "результат" in key:
                    r.finish_time = val
                elif "км" in key or "дистан" in key or "distance" in key:
                    try:
                        r.distance_km = float(val.replace(",", "."))
                    except Exception:
                        pass
                elif "место" in key or "place" in key or "позиц" in key:
                    try:
                        r.place_abs = int(re.search(r"\d+", val).group())
                    except Exception:
                        pass
                elif "категор" in key or "group" in key:
                    r.category = val
                elif "команд" in key or "клуб" in key or "team" in key:
                    r.team = val

            # Вид спорта
            sport_raw = r.event_name.lower()
            if any(w in sport_raw for w in ["лыж", "ski"]):
                r.sport = "ski"
            elif any(w in sport_raw for w in ["вело", "bike"]):
                r.sport = "bike"
            else:
                r.sport = "run"

            results.append(r)

    return results


# ── REQUESTS-РЕЖИМ (быстрый, если есть API) ──────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_profile_requests(session: requests.Session, profile_id: int) -> Profile:
    url = BASE_URL.format(id=profile_id)
    resp = session.get(url, timeout=15)

    if resp.status_code == 404:
        p = Profile(id=profile_id, error="not_found")
        return p

    resp.raise_for_status()
    p = parse_static_html(resp.text, profile_id)

    if API_TEMPLATE:
        api_url = API_TEMPLATE.format(id=profile_id)
        try:
            api_resp = session.get(api_url, timeout=15)
            if api_resp.status_code == 200:
                try:
                    p.results = parse_results_from_json(api_resp.json())
                except Exception:
                    p.results = parse_results_from_html(api_resp.text)
        except Exception as e:
            p.error = f"api_error: {e}"
    else:
        # Ищем результаты прямо в HTML профиля
        p.results = parse_results_from_html(resp.text)

    return p


# ── PLAYWRIGHT-РЕЖИМ (медленный, но надёжный) ─────────────────────────────────

def fetch_profile_playwright(pw_browser, profile_id: int) -> Profile:
    url = BASE_URL.format(id=profile_id)
    page = pw_browser.new_page()

    captured_json = []

    def on_response(response):
        ct = response.headers.get("content-type", "")
        if "json" in ct and "vologdamarafon" in response.url:
            try:
                captured_json.append(response.json())
            except Exception:
                pass

    page.on("response", on_response)

    try:
        resp = page.goto(url, wait_until="networkidle", timeout=30000)
        if resp and resp.status == 404:
            page.close()
            return Profile(id=profile_id, error="not_found")

        # Ждём исчезновения лоадера
        try:
            page.wait_for_selector("img[src*='ajax-loader']", state="hidden", timeout=12000)
        except Exception:
            pass
        page.wait_for_timeout(2000)

        html = page.content()
        p = parse_static_html(html, profile_id)

        # Результаты из перехваченного JSON или HTML
        if captured_json:
            for data in captured_json:
                p.results.extend(parse_results_from_json(data))
        else:
            p.results = parse_results_from_html(html)

    except Exception as e:
        p = Profile(id=profile_id, error=str(e))

    page.close()
    return p


# ── ГЛАВНЫЙ ЦИКЛ ──────────────────────────────────────────────────────────────

def crawl():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Определяем какие ID ещё не обработаны
    todo = [
        pid for pid in range(ID_START, ID_END + 1)
        if not already_crawled(conn, pid)
    ]
    print(f"Всего к обработке: {len(todo)} профилей")
    print(f"Режим: {'Playwright' if PLAYWRIGHT_MODE else 'requests'}")
    print(f"API шаблон: {API_TEMPLATE or 'нет (парсим HTML)'}")
    print()

    stats = {"ok": 0, "not_found": 0, "error": 0}

    if PLAYWRIGHT_MODE:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            for i, pid in enumerate(todo):
                p = fetch_profile_playwright(browser, pid)
                _process(conn, p, stats, i, len(todo))
                time.sleep(DELAY)
            browser.close()
    else:
        session = make_session()
        for i, pid in enumerate(todo):
            try:
                p = fetch_profile_requests(session, pid)
            except Exception as e:
                p = Profile(id=pid, error=str(e))
            _process(conn, p, stats, i, len(todo))
            time.sleep(DELAY)

    conn.close()
    print(f"\n✅ Готово: ok={stats['ok']}, not_found={stats['not_found']}, error={stats['error']}")
    print(f"   База:  {DB_PATH}")
    print(f"   JSONL: {JSONL_PATH}")


def _process(conn, p: Profile, stats: dict, i: int, total: int):
    save_profile(conn, p)

    # Дописываем в JSONL
    with JSONL_PATH.open("a", encoding="utf-8") as f:
        row = asdict(p)
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

    status = p.error or "ok"
    stats[status if status in stats else "ok"] += 1
    if status == "not_found":
        stats["not_found"] += 1
        stats["ok"] -= 1
    elif status != "ok":
        stats["error"] += 1
        stats["ok"] -= 1

    # Прогресс каждые 50 профилей
    if i % 50 == 0 or i == total - 1:
        print(
            f"[{i+1}/{total}] id={p.id} | "
            f"{'✓' if not p.error else '✗'} {p.name or p.error} | "
            f"результатов: {len(p.results)} | "
            f"ok={stats['ok']} skip={stats['not_found']} err={stats['error']}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vologda Marathon profile crawler")
    parser.add_argument("--api", help="API URL template, e.g. https://.../{id}/results/")
    parser.add_argument("--playwright", action="store_true", help="Use Playwright mode")
    parser.add_argument("--start", type=int, default=ID_START)
    parser.add_argument("--end",   type=int, default=ID_END)
    parser.add_argument("--delay", type=float, default=DELAY)
    args = parser.parse_args()

    if args.api:
        API_TEMPLATE = args.api
    if args.playwright:
        PLAYWRIGHT_MODE = True
    ID_START = args.start
    ID_END   = args.end
    DELAY    = args.delay

    crawl()
