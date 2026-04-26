"""
Аналитика по собранной базе marathon.db (CLI поверх marathon_queries).

Запуск:
    python analytics.py
    python analytics.py --section participants   # только раздел
    python analytics.py --comp-id 298            # по конкретному событию
    python analytics.py --profile-id 2139      # карточка участника
"""

from __future__ import annotations

import argparse
from pathlib import Path

import marathon_queries as mq

DB = Path(mq.DEFAULT_DB)


def section(t: str) -> None:
    print(f"\n{'═' * 64}\n  {t}\n{'═' * 64}")


def tbl(rows: list[dict], limit: int = 25) -> None:
    if not rows:
        print("  (нет данных)")
        return
    keys = list(rows[0].keys())
    ws = [max(max(len(str(r.get(k, "") or "")) for r in rows), len(k)) + 1 for k in keys]
    ws = [min(w, 40) for w in ws]
    hdr = " │ ".join(k.ljust(w) for k, w in zip(keys, ws))
    print("  " + hdr)
    print("  " + "─" * len(hdr))
    for row in rows[:limit]:
        print("  " + " │ ".join(str(row.get(k, "") or "")[:w].ljust(w) for k, w in zip(keys, ws)))
    if len(rows) > limit:
        print(f"  ... ещё {len(rows) - limit} строк")


def summary() -> None:
    section("Общая сводка базы")
    tbl(mq.query_summary_row(DB))

    section("События по годам и видам спорта")
    tbl(mq.query_events_by_year_sport(DB))


def participants() -> None:
    section("Топ-20 участников по числу стартов (абсолют)")
    tbl(mq.query_participants_top(DB, 20))

    section("Распределение по полу и возрасту")
    tbl(mq.query_gender_age_distribution(DB))

    section("Топ-15 городов по участникам")
    tbl(mq.query_cities_top(DB, 15))


def competitions_report() -> None:
    section("Топ-20 событий по числу участников")
    tbl(mq.query_competitions_top(DB, 20))

    section("Средние времена финиша по видам спорта и дистанциям")
    tbl(mq.query_avg_finish_by_sport_distance(DB, 30))

    section("DNF — события с наибольшим процентом сходов")
    tbl(mq.query_dnf_events_top(DB, 15))


def cups_report() -> None:
    section("Кубки")
    tbl(mq.query_cups_summary(DB))

    section("Лидеры кубков (топ-10 по очкам)")
    tbl(mq.query_cup_leaders(DB, 40))


def competition_card(comp_id: int) -> None:
    section(f"Карточка события #{comp_id}")

    tbl(mq.query_competition_header(DB, comp_id))

    print("\n  Дистанции:")
    tbl(mq.query_competition_distances(DB, comp_id))

    print("\n  Топ-10 абсолютного зачёта:")
    tbl(mq.query_competition_top10(DB, comp_id, 30))

    print("\n  Распределение по возрастным группам:")
    tbl(mq.query_competition_groups(DB, comp_id))


def profile_card(pid: int) -> None:
    section(f"Карточка участника #{pid}")

    p = mq.query_profile_row(DB, pid)
    if not p:
        print("  Профиль не найден")
        return
    print(f"  {p.get('last_name')} {p.get('first_name')} {p.get('second_name')}")
    print(f"  Пол: {p.get('gender')} | Возраст: {p.get('age')} | Год рождения: {p.get('birth_year')}")
    print(f"  Город: {p.get('city')}, {p.get('region')}, {p.get('country')}")
    print(f"  Клуб: {p.get('club') or '—'}")
    print(f"  Статистика: {p.get('stat_competitions')} соревнований, {p.get('stat_km')} км")
    print(f"  Места: 🥇{p.get('stat_first')}  🥈{p.get('stat_second')}  🥉{p.get('stat_third')}")

    print("\n  История стартов:")
    tbl(mq.query_profile_results_history(DB, pid))

    print("\n  Результаты в кубках:")
    tbl(mq.query_profile_cup_rows(DB, pid))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--section", choices=["summary", "participants", "competitions", "cups"])
    parser.add_argument("--comp-id", type=int)
    parser.add_argument("--profile-id", type=int)
    args = parser.parse_args()

    if not mq.db_exists(DB):
        print(f"База не найдена: {DB.resolve()}")
        raise SystemExit(1)

    if args.comp_id:
        competition_card(args.comp_id)
    elif args.profile_id:
        profile_card(args.profile_id)
    elif args.section == "participants":
        participants()
    elif args.section == "competitions":
        competitions_report()
    elif args.section == "cups":
        cups_report()
    elif args.section == "summary":
        summary()
    else:
        summary()
        participants()
        competitions_report()
        cups_report()
