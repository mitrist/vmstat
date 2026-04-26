"""
Инкрементальная синхронизация marathon.db

Логика:
  1. СОРЕВНОВАНИЯ — берём max(id) из базы, проверяем новые ID вперёд
     + перепроверяем недавние события у которых результаты ещё не опубликованы
  2. КУБКИ — проверяем всегда (их мало, ~10–30 штук)
  3. ПРОФИЛИ — только те, у которых появились новые результаты

Запуск:
    python sync.py                   # стандартная синхронизация
    python sync.py --check-ahead 50  # смотреть 50 ID вперёд от максимального
    python sync.py --recheck-days 30 # перепроверять события младше 30 дней
    python sync.py --dry-run         # показать что будет обновлено, не трогать БД

Рекомендуемое расписание (cron):
    # Каждую пятницу вечером — события обычно в выходные
    0 20 * * 5 cd /path/to/project && python sync.py >> sync.log 2>&1
"""

import sqlite3
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout
from urllib3.util.retry import Retry

DB_PATH = Path("marathon.db")
BASE    = "https://vologdamarafon.ru/api/v2"

# (connect, read) — read 15s мало для крупных /cups/.../results/
HTTP_TIMEOUT = (15, 60)

# Сколько ID вперёд от максимального проверять на наличие новых событий
DEFAULT_CHECK_AHEAD = 30

# Перепроверять события, прошедшие не раньше чем N дней назад
# (результаты часто публикуют через 1–7 дней после старта)
DEFAULT_RECHECK_DAYS = 14

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

SPORT_MAP = {
    "ski":  ["лыж", "ski", "лыжн"],
    "bike": ["вело", "bike", "cycl"],
    "run":  ["бег", "run", "марафон", "marathon", "полумарафон", "забег"],
}

def detect_sport(text: str) -> str:
    t = (text or "").lower()
    for sport, kw in SPORT_MAP.items():
        if any(w in t for w in kw):
            return sport
    return "other"

def time_to_sec(t: str):
    if not t: return None
    try:
        parts = t.split(":")
        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def _api_str_field(v) -> str:
    """Поле для TEXT в SQLite: в JSON API иногда отдают объект (например team) вместо строки."""
    if v is None:
        return ""
    if isinstance(v, dict):
        s = v.get("name") or v.get("title")
        if s is not None and str(s).strip():
            return str(s)
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, (list, tuple, set)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

def make_session():
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

def get(session, url):
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
    time.sleep(0.25)


def _result_person(item: dict) -> dict:
    """Участник в строке результатов: API v2 отдаёт competitor, иногда profile."""
    for key in ("profile", "competitor"):
        v = item.get(key)
        if isinstance(v, dict) and v.get("id") is not None:
            return v
    return {}


def scalar(value, field: str = "") -> str:
    """Строка для SQLite; API может отдать dict/list вместо строки."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        name = (value.get("name") or value.get("title") or
                value.get("full_name") or value.get("short_name") or "")
        return str(name)
    if isinstance(value, list):
        return ""
    return str(value)


def scalar_int(value):
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


def _sync_profile_cup_results(conn, session, pid: int):
    """Таблица profile_cup_results — GET .../cup-results/?year= для каждого года активности."""
    _, years = get(session, f"{BASE}/profile/{pid}/competition-results/years/")
    sleep()
    years = years if isinstance(years, list) else []

    conn.execute("DELETE FROM profile_cup_results WHERE profile_id=?", (pid,))

    for year in years:
        _, cr = get(session, f"{BASE}/profile/{pid}/cup-results/?year={year}")
        sleep()
        if not isinstance(cr, list):
            continue
        rows = []
        for item in cr:
            cup = item.get("cup") or {}
            dist = item.get("distance") or {}
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


# ── ОПРЕДЕЛЯЕМ ЧТО НУЖНО ОБНОВИТЬ ─────────────────────────────────────────

class SyncPlan:
    """Что и почему нужно обновить."""

    def __init__(self, conn, check_ahead: int, recheck_days: int):
        self.conn = conn
        self.check_ahead  = check_ahead
        self.recheck_days = recheck_days

        self.new_comp_ids:     list[int] = []  # новые ID которых нет в базе
        self.recheck_comp_ids: list[int] = []  # старые события без опубл. результатов
        self.new_profile_ids:  list[int] = []  # профили из новых результатов

    def build(self, dry_run: bool = False):
        self._find_new_competitions()
        self._find_recheck_competitions()
        self._log_plan(dry_run)

    def _find_new_competitions(self):
        """ID выше максимального в базе — потенциально новые события."""
        row = self.conn.execute(
            "SELECT MAX(id) FROM competitions"
        ).fetchone()
        max_id = row[0] if row and row[0] else 0

        # Берём все known IDs из crawl_log чтобы не повторять 404
        known = set(r[0] for r in self.conn.execute(
            "SELECT entity_id FROM crawl_log WHERE entity='competition'"
        ).fetchall())

        for cid in range(max_id + 1, max_id + self.check_ahead + 1):
            if cid not in known:
                self.new_comp_ids.append(cid)

        log.info(f"  Новых competition ID для проверки: {len(self.new_comp_ids)} "
                 f"(диапазон {max_id+1}–{max_id+self.check_ahead})")

    def _find_recheck_competitions(self):
        """
        События которые:
        - уже в базе
        - дата старта в пределах recheck_days дней назад
        - результаты не опубликованы ИЛИ нет ни одного результата в таблице results
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.recheck_days))
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        rows = self.conn.execute("""
            SELECT c.id, c.title, c.date, c.is_published,
                   (SELECT COUNT(*) FROM results r WHERE r.competition_id = c.id) AS result_count
            FROM competitions c
            WHERE c.date >= ?
              AND (c.is_published = 0 OR result_count = 0)
            ORDER BY c.date DESC
        """, (cutoff_str,)).fetchall()

        self.recheck_comp_ids = [r[0] for r in rows]

        for r in rows:
            log.info(f"  Перепроверка: [{r[0]}] {r[1]} | дата: {r[2][:10]} | "
                     f"опубл: {'да' if r[3] else 'нет'} | рез-тов в базе: {r[4]}")

    def _log_plan(self, dry_run: bool):
        total = len(self.new_comp_ids) + len(self.recheck_comp_ids)
        mode = "DRY RUN — " if dry_run else ""
        log.info(f"\n{mode}План синхронизации:")
        log.info(f"  Новых событий для проверки: {len(self.new_comp_ids)}")
        log.info(f"  Событий для перепроверки:   {len(self.recheck_comp_ids)}")
        log.info(f"  Итого запросов ~{total * 6} (по ~6 на событие)")


# ── ОБНОВЛЕНИЕ ОДНОГО СОБЫТИЯ ──────────────────────────────────────────────

def sync_competition(conn, session, comp_id: int, is_recheck: bool = False) -> bool:
    """
    Возвращает True если событие нашлось и было обработано.
    """
    # Основные данные
    status, data = get(session, f"{BASE}/competitions/{comp_id}/")
    sleep()
    if status == 404:
        _log_crawl(conn, "competition", comp_id, "not_found")
        return False
    if status != 200 or not data:
        _log_crawl(conn, "competition", comp_id, f"error_{status}")
        return False

    title = data.get("title") or data.get("title_short") or ""
    sport = detect_sport(title)
    date  = data.get("date") or ""
    year  = int(date[:4]) if date else None
    is_published = int(bool(data.get("is_results_published", False)))

    action = "перепроверка" if is_recheck else "новое"
    log.info(f"  [{comp_id}] {action}: {title} ({date[:10]}) опубл={is_published}")

    conn.execute("""
        INSERT OR REPLACE INTO competitions
          (id, title, title_short, date, year, sport,
           is_relay, is_published, page_url, raw)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        comp_id, title, data.get("title_short",""), date, year, sport,
        int(bool(data.get("is_relay", False))), is_published,
        data.get("page_url",""), json.dumps(data, ensure_ascii=False),
    ))
    conn.commit()

    # Статус
    _, st = get(session, f"{BASE}/competitions/{comp_id}/status/")
    sleep()
    if st:
        conn.execute("""
            INSERT OR REPLACE INTO competition_status
              (competition_id, status, participants, finishers, dnf, raw)
            VALUES (?,?,?,?,?,?)
        """, (comp_id, st.get("status"),
              st.get("participants") or st.get("total"),
              st.get("finishers"), st.get("dnf"),
              json.dumps(st, ensure_ascii=False)))
        conn.commit()

    # Статистика участников
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
              ms.get("teams"), ms.get("regions"),
              json.dumps(ms, ensure_ascii=False)))
        conn.commit()

    # Если результаты не опубликованы — дистанции и результаты не тянем
    if not is_published:
        _log_crawl(conn, "competition", comp_id, "ok_no_results")
        log.info(f"    результаты ещё не опубликованы, пропускаем")
        return True

    # Дистанции
    _, dists = get(session, f"{BASE}/competitions/{comp_id}/distances/")
    sleep()
    dist_ids = []
    if isinstance(dists, list):
        for d in dists:
            did = d.get("id")
            if not did: continue
            dist_ids.append(did)
            dkm = float(d.get("distance") or d.get("distance_km") or d.get("km") or 0)
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

    new_profile_ids = set()

    for did in dist_ids:
        # Группы
        _, groups = get(session, f"{BASE}/competitions/{comp_id}/groups/?distance={did}")
        sleep()
        if isinstance(groups, list):
            for g in groups:
                gid = g.get("id")
                if not gid: continue
                name = g.get("name") or ""
                conn.execute("""
                    INSERT OR REPLACE INTO groups
                      (id, competition_id, distance_id, name, gender,
                       age_from, age_to, raw)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (gid, comp_id, did, name,
                      g.get("gender") or _guess_gender(name),
                      g.get("age_from") or g.get("min_age"),
                      g.get("age_to") or g.get("max_age"),
                      json.dumps(g, ensure_ascii=False)))
            conn.commit()

        # Результаты
        _, res = get(session, f"{BASE}/competitions/{comp_id}/results/?distance={did}")
        sleep()
        if isinstance(res, list):
            # При перепроверке — удаляем старые результаты дистанции перед вставкой
            if is_recheck:
                conn.execute(
                    "DELETE FROM results WHERE competition_id=? AND distance_id=?",
                    (comp_id, did)
                )

            rows = []
            for item in res:
                rid = item.get("id")
                if not rid: continue
                ft = item.get("total_time") or item.get("finish_time") or ""
                person = _result_person(item)
                group   = item.get("group") or {}
                pid = person.get("id") if person else item.get("profile_id")
                if pid:
                    new_profile_ids.add(pid)
                rows.append((
                    rid, comp_id, did, pid,
                    item.get("number") or item.get("bib"),
                    item.get("place"), item.get("gender_place"), item.get("group_place"),
                    group.get("id") if isinstance(group, dict) else item.get("group_id"),
                    group.get("name") if isinstance(group, dict) else "",
                    ft, time_to_sec(ft),
                    int(bool(item.get("dnf", False))),
                    _api_str_field(item.get("club")),
                    _api_str_field(item.get("team")),
                    int(bool(item.get("is_relay", False))),
                    item.get("relay_stage"),
                    item.get("certificate_url") or "",
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

    _log_crawl(conn, "competition", comp_id, "ok")
    return True, new_profile_ids


def _guess_gender(name: str) -> str:
    n = name.lower()
    if any(w in n for w in ["муж", " м", "male", "men"]): return "m"
    if any(w in n for w in ["жен", " ж", "female", "women"]): return "f"
    return ""


def _log_crawl(conn, entity, eid, status):
    conn.execute("""
        INSERT OR REPLACE INTO crawl_log (entity, entity_id, status)
        VALUES (?, ?, ?)
    """, (entity, eid, status))
    conn.commit()


# ── ОБНОВЛЕНИЕ КУБКОВ ─────────────────────────────────────────────────────

def sync_cups(conn, session):
    """Кубков мало — проверяем всегда полностью."""
    log.info("=== Синхронизация кубков ===")
    _, cups = get(session, f"{BASE}/cups/")
    sleep()
    if not isinstance(cups, list):
        log.warning("  Не удалось получить список кубков")
        return

    for cup in cups:
        cup_id = cup.get("id")
        if not cup_id: continue

        existing = conn.execute(
            "SELECT id FROM cups WHERE id=?", (cup_id,)
        ).fetchone()

        if existing and _cup_is_closed(conn, cup_id):
            log.info(f"  cup {cup_id}: уже закрыт, пропускаем")
            continue

        title = cup.get("title") or cup.get("name") or ""
        year  = cup.get("year")
        sport = detect_sport(title)
        conn.execute("""
            INSERT OR REPLACE INTO cups (id, title, year, sport, raw)
            VALUES (?,?,?,?,?)
        """, (cup_id, title, year, sport, json.dumps(cup, ensure_ascii=False)))
        conn.commit()
        log.info(f"  cup {cup_id}: {title} ({year}) — обновляем результаты")

        # Дистанции кубка
        _, cdists = get(session, f"{BASE}/cups/{cup_id}/distances/")
        sleep()
        dist_ids = []
        if isinstance(cdists, list):
            for d in cdists:
                did = d.get("id")
                if not did: continue
                dist_ids.append(did)
                conn.execute("""
                    INSERT OR REPLACE INTO cup_distances
                      (id, cup_id, name, distance_km, sport, raw)
                    VALUES (?,?,?,?,?,?)
                """, (did, cup_id, d.get("name") or "",
                      float(d.get("distance") or d.get("km") or 0),
                      sport, json.dumps(d, ensure_ascii=False)))
            conn.commit()

        # Результаты кубка (всегда перезаписываем — рейтинг меняется после каждого этапа)
        for did in dist_ids:
            conn.execute("DELETE FROM cup_results WHERE cup_id=? AND distance_id=?",
                        (cup_id, did))
            _, cres = get(session, f"{BASE}/cups/{cup_id}/results/?distance={did}")
            sleep()
            if isinstance(cres, list):
                rows = []
                for item in cres:
                    person = _result_person(item)
                    group   = item.get("group") or {}
                    rows.append((
                        cup_id, did,
                        person.get("id") if person else item.get("profile_id"),
                        item.get("place"), item.get("gender_place"), item.get("group_place"),
                        group.get("id") if isinstance(group, dict) else item.get("group_id"),
                        group.get("name") if isinstance(group, dict) else "",
                        item.get("total_points") or item.get("points"),
                        item.get("total_time") or item.get("time"),
                        item.get("competitions_count") or item.get("count"),
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


def _cup_is_closed(conn, cup_id: int) -> bool:
    """Кубок закрыт если год < текущий."""
    row = conn.execute("SELECT year FROM cups WHERE id=?", (cup_id,)).fetchone()
    if not row or not row[0]: return False
    return row[0] < datetime.now().year


# ── ОБНОВЛЕНИЕ ПРОФИЛЕЙ ───────────────────────────────────────────────────

def sync_profiles(conn, session, profile_ids: set[int]):
    """Обновляем только профили из переданного набора."""
    if not profile_ids:
        log.info("  Нет новых профилей для обновления")
        return

    # Фильтруем тех, кого уже качали недавно (за последние 7 дней)
    existing = set(r[0] for r in conn.execute("""
        SELECT entity_id FROM crawl_log
        WHERE entity='profile'
          AND status='ok'
          AND fetched_at >= datetime('now', '-7 days')
    """).fetchall())

    to_fetch = profile_ids - existing
    log.info(f"=== Профили: {len(to_fetch)} новых (из {len(profile_ids)} в результатах) ===")

    for i, pid in enumerate(sorted(to_fetch)):
        status, data = get(session, f"{BASE}/profile/{pid}/")
        sleep()
        if status == 404:
            _log_crawl(conn, "profile", pid, "not_found")
            continue
        if status != 200 or not data:
            _log_crawl(conn, "profile", pid, f"error_{status}")
            continue

        loc = data.get("location") or {}
        conn.execute("""
            INSERT OR REPLACE INTO profiles
              (id, first_name, last_name, second_name, gender, age, birth_year,
               city, city_id, region, region_id, country, club, raw)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            pid, data.get("first_name",""), data.get("last_name",""),
            data.get("second_name",""), data.get("gender",""), data.get("age"),
            data.get("year"),
            loc.get("city_name") or data.get("city",""),
            loc.get("city_id"), loc.get("region_name",""), loc.get("region_id"),
            loc.get("country_name",""), data.get("club","") or "",
            json.dumps(data, ensure_ascii=False),
        ))

        _, stats = get(session, f"{BASE}/profile/{pid}/statistics/")
        sleep()
        if stats:
            conn.execute("""
                UPDATE profiles SET
                    stat_competitions=?, stat_km=?, stat_marathons=?,
                    stat_first=?, stat_second=?, stat_third=?
                WHERE id=?
            """, (stats.get("competitions"), stats.get("kilometers"),
                  stats.get("marathons"), stats.get("first_places"),
                  stats.get("second_places"), stats.get("third_places"), pid))

        conn.commit()
        _sync_profile_cup_results(conn, session, pid)
        _log_crawl(conn, "profile", pid, "ok")

        if (i + 1) % 50 == 0:
            log.info(f"  [{i+1}/{len(to_fetch)}] профилей обновлено")


# ── ИТОГОВАЯ СВОДКА ───────────────────────────────────────────────────────

def print_sync_summary(conn, started_at: datetime):
    elapsed = (datetime.now() - started_at).seconds
    log.info("\n" + "="*60)
    log.info("Результат синхронизации:")
    for label, sql in [
        ("Соревнований в базе",  "SELECT COUNT(*) FROM competitions"),
        ("Результатов в базе",   "SELECT COUNT(*) FROM results"),
        ("Профилей в базе",      "SELECT COUNT(*) FROM profiles"),
        ("Рез. профилей в кубках", "SELECT COUNT(*) FROM profile_cup_results"),
        ("Кубков в базе",        "SELECT COUNT(*) FROM cups"),
    ]:
        n = conn.execute(sql).fetchone()[0]
        log.info(f"  {label}: {n}")

    # Что добавилось за последний запуск
    since = started_at.strftime("%Y-%m-%d %H:%M:%S")
    new_comps = conn.execute(
        "SELECT COUNT(*) FROM crawl_log WHERE entity='competition' AND status='ok' AND fetched_at>=?",
        (since,)
    ).fetchone()[0]
    new_profiles = conn.execute(
        "SELECT COUNT(*) FROM crawl_log WHERE entity='profile' AND status='ok' AND fetched_at>=?",
        (since,)
    ).fetchone()[0]
    log.info(f"  За эту синхронизацию: +{new_comps} событий, +{new_profiles} профилей")
    log.info(f"  Время: {elapsed} сек")


# ── MAIN ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Инкрементальная синхронизация marathon.db")
    parser.add_argument("--check-ahead",  type=int, default=DEFAULT_CHECK_AHEAD,
                        help="Сколько ID вперёд проверять (default: 30)")
    parser.add_argument("--recheck-days", type=int, default=DEFAULT_RECHECK_DAYS,
                        help="Перепроверять события младше N дней (default: 14)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Показать план без изменений в БД")
    parser.add_argument("--cups-only",    action="store_true")
    parser.add_argument("--comps-only",   action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        log.error(f"База {DB_PATH} не найдена. Сначала запусти crawler_full.py")
        return

    started_at = datetime.now()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS profile_cup_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            year INTEGER,
            cup_id INTEGER,
            cup_title TEXT,
            distance_id INTEGER,
            distance_name TEXT,
            place_abs INTEGER,
            place_gender INTEGER,
            place_group INTEGER,
            group_name TEXT,
            total_points REAL,
            raw TEXT
        )
    """)
    import cup_scoring

    cup_scoring.ensure_cup_scoring_tables(conn)
    conn.commit()
    session = make_session()

    # Строим план
    plan = SyncPlan(conn, args.check_ahead, args.recheck_days)
    plan.build(dry_run=args.dry_run)

    if args.dry_run:
        conn.close()
        return

    all_new_profiles: set[int] = set()

    # Синхронизируем соревнования
    if not args.cups_only:
        log.info("=== Новые события ===")
        for cid in plan.new_comp_ids:
            result = sync_competition(conn, session, cid, is_recheck=False)
            if isinstance(result, tuple) and result[0]:
                all_new_profiles.update(result[1])

        log.info("=== Перепроверка недавних событий ===")
        for cid in plan.recheck_comp_ids:
            result = sync_competition(conn, session, cid, is_recheck=True)
            if isinstance(result, tuple) and result[0]:
                all_new_profiles.update(result[1])

    # Кубки
    if not args.comps_only:
        sync_cups(conn, session)

    # Профили: id из новых результатов соревнований + все из cup_results
    # (участники только рейтинга кубков раньше не попадали в all_new_profiles)
    # Запускаем и при полном sync, и при --cups-only (не только при --comps-only)
    if not args.comps_only:
        cup_pids = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT profile_id FROM cup_results WHERE profile_id IS NOT NULL"
            ).fetchall()
        }
        all_new_profiles |= cup_pids
        sync_profiles(conn, session, all_new_profiles)

    print_sync_summary(conn, started_at)
    conn.close()


if __name__ == "__main__":
    main()
