# Системное сообщение: Проект аналитики марафонов Вологодской области

## Контекст проекта

Разрабатывается веб-сервис аналитики для организации, проводящей 20–25 спортивных событий в год (беговые, велосипедные, лыжные марафоны). Источник данных — сайт **vologdamarafon.ru**. Данные собираются через API v2 и хранятся локально в SQLite.

---

## Источник данных — API v2

Базовый URL: `https://vologdamarafon.ru/api/v2`

Все найденные endpoint'ы:

```
# Соревнования
GET /competitions/{id}/                          — данные события
GET /competitions/{id}/status/                   — статус (финишёров, DNF)
GET /competitions/{id}/members/statistics/       — участники: м/ж, команды, регионы
GET /competitions/{id}/distances/                — дистанции внутри события
GET /competitions/{id}/groups/?distance={id}     — возрастные группы дистанции
GET /competitions/{id}/results/?distance={id}    — результаты дистанции
GET /competitions/{id}/members/?distance={id}    — список участников дистанции

# Кубки
GET /cups/                                       — все кубки
GET /cups/{id}/distances/                        — дистанции кубка
GET /cups/{id}/competitions/                     — этапы (события) кубка
GET /cups/{id}/groups/?distance={id}             — возрастные группы
GET /cups/{id}/results/?distance={id}            — итоговый рейтинг кубка

# Профили участников
GET /profile/{id}/                               — данные профиля
GET /profile/{id}/statistics/                    — агрегаты (стартов, км, мест)
GET /profile/{id}/competition-results/years/     — годы с результатами
GET /profile/{id}/competition-results/?year=YYYY — результаты по году
GET /profile/{id}/cup-results/?year=YYYY         — результаты в кубках по году
```

Профили доступны по URL вида `https://vologdamarafon.ru/profile/2139/`. ID участников начинаются с 1, реальных участников ~2000–6000.

---

## Схема базы данных — `marathon.db` (SQLite, 14 таблиц)

### Соревнования
```sql
competitions          -- id, title, title_short, date, year, sport, is_relay, is_published, page_url, raw
competition_status    -- competition_id PK/FK, status, participants, finishers, dnf, raw
competition_stats     -- competition_id PK/FK, total_members, male, female, teams, regions, raw
distances             -- id, competition_id FK, name, distance_km, sport, is_relay, raw
groups                -- id, competition_id FK, distance_id FK, name, gender, age_from, age_to, raw
results               -- id, competition_id FK, distance_id FK, profile_id FK, bib_number,
                      --   place_abs, place_gender, place_group, group_id FK, group_name,
                      --   finish_time, finish_time_sec (сек, для сортировки), dnf,
                      --   club, team, is_relay, relay_stage, certificate_url, raw
```

### Кубки
```sql
cups                  -- id, title, year, sport, raw
cup_distances         -- id, cup_id FK, name, distance_km, sport, raw
cup_competitions      -- cup_id FK, competition_id FK  (таблица-мост)
cup_groups            -- id, cup_id FK, distance_id FK, name, gender, age_from, age_to, raw
cup_results           -- id, cup_id FK, distance_id FK, profile_id FK, place_abs, place_gender,
                      --   place_group, group_id, group_name, total_points, total_time,
                      --   competitions_count, raw
```

### Участники
```sql
profiles              -- id, first_name, last_name, second_name, gender (m/f), age, birth_year,
                      --   city, city_id, region, region_id, country, club,
                      --   stat_competitions, stat_km, stat_marathons, stat_first,
                      --   stat_second, stat_third, raw
profile_cup_results   -- id, profile_id FK, year, cup_id FK, cup_title, distance_id FK,
                      --   distance_name, place_abs, place_gender, place_group,
                      --   group_name, total_points, raw
```

### Служебная
```sql
crawl_log             -- entity (competition/cup/profile), entity_id, status (ok/not_found/error),
                      --   fetched_at
```

### Индексы
```sql
idx_results_comp, idx_results_dist, idx_results_profile, idx_results_group
idx_distances_comp, idx_groups_dist
idx_cup_results_cup, idx_cup_results_prof
idx_profiles_city, idx_profiles_gender
idx_comp_year, idx_comp_sport
```

---

## Файлы проекта

| Файл | Назначение |
|------|-----------|
| `crawler_full.py` | Полный первоначальный сбор: соревнования (перебор ID 1–400), кубки, профили |
| `sync.py` | Инкрементальная синхронизация: новые события, перепроверка недавних, кубки, новые профили |
| `marathon_queries.py` | Общий слой SQL к `marathon.db` для CLI и Streamlit |
| `analytics.py` | CLI-аналитика (вызывает `marathon_queries`) |
| `app.py` | Streamlit-дашборд MVP (Сезон / Событие / Участник / Команда / Кубки) |
| `import_profiles_csv.py` | Импорт `profiles.csv` в `marathon.db` |
| `fill_profile_cup_results.py` | Заполнение `profile_cup_results` через API |
| `explore.py` | Быстрый просмотр базы в терминале |
| `build_refs.py` | Справочники для legacy `profiles.db` / jsonl (дашборд MVP — только `marathon.db`) |

### Запуск первоначального сбора
```bash
pip install requests
python crawler_full.py                          # всё подряд
python crawler_full.py --only competitions      # только события
python crawler_full.py --only cups              # только кубки
python crawler_full.py --only profiles          # только профили
python crawler_full.py --comp-start 1 --comp-end 400
```

### Регулярная синхронизация (раз в неделю)
```bash
python sync.py                                  # стандартный запуск
python sync.py --dry-run                        # показать план без изменений
python sync.py --check-ahead 50                 # смотреть 50 ID вперёд
python sync.py --recheck-days 14                # перепроверять события младше 14 дней
python sync.py --cups-only                      # только кубки
```

### Аналитика и дашборд
```bash
python analytics.py                             # полная сводка
python analytics.py --comp-id 298              # карточка события
python analytics.py --profile-id 2139          # карточка участника
python analytics.py --section cups             # рейтинги кубков
streamlit run app.py                            # веб-MVP (`MARATHON_DB` или `.streamlit/secrets.toml`)
```

---

## Известные особенности API

- Поле `distance_km` в `/profile/{id}/competition-results/` всегда возвращает `0.0`. Реальный километраж берётся из `/competitions/{id}/distances/`.
- Поля `club` и `team` в результатах могут возвращаться как объект `{"id": 5, "name": "Клуб"}` вместо строки. В crawler_full.py это обрабатывается функциями `scalar()` и `scalar_int()`.
- Страница профиля (`/profile/{id}/`) рендерится через JavaScript — статические данные (имя, город) доступны из HTML, результаты — только через API.
- `is_results_published` — флаг публикации результатов. Пока `false`, дистанции и результаты не тянем.

---

## Вид спорта — определение по названию события

```python
SPORT_MAP = {
    "ski":  ["лыж", "ski", "лыжн", "cross"],
    "bike": ["вело", "bike", "cycl", "велос"],
    "run":  ["бег", "run", "марафон", "marathon", "полумарафон", "забег", "спринт"],
}
# Если ничего не совпало — "other"
```

---

## Логика инкрементальной синхронизации (`sync.py`)

1. **Новые соревнования** — берёт `MAX(id)` из таблицы `competitions`, проверяет следующие `--check-ahead` ID (по умолчанию 30).
2. **Перепроверка недавних** — ищет события с датой старта в пределах `--recheck-days` дней (по умолчанию 14) у которых `is_published=0` или нет ни одного результата в таблице `results`. При перепроверке старые результаты дистанции удаляются перед вставкой новых.
3. **Кубки** — перезаписываются всегда (рейтинг меняется после каждого этапа). Кубки с годом < текущего пропускаются.
4. **Профили** — подтягиваются только те `profile_id`, которые появились в новых результатах и не обновлялись последние 7 дней.

---

## Концепция аналитического дашборда

Четыре уровня анализа: **Сезон → Событие → Участник → Команда**

### Метрики сезона
- Событий проведено / запланировано
- Всего стартов (старт = один финиш на одной дистанции)
- Уникальных участников
- Суммарный километраж
- Команд-участниц, регионов
- Разбивка по видам (бег / вело / лыжи)
- Разбивка по полу и возрастным группам
- Активность по месяцам

### Карточка события
- Число финишировавших, DNF%
- Рекорд трассы (абсолют, по полу, по возрасту)
- Медианное время финиша
- Командный зачёт
- Сравнение с прошлыми годами

### Карточка участника
- Профиль: клуб, регион, возрастная группа
- Всего стартов, суммарный км
- Личные рекорды по дистанциям
- Рейтинговые очки в кубках
- Хронология выступлений
- Место в возрастной группе в сезоне

### Карточка команды
- Состав, суммарные очки
- Место в командном рейтинге
- Топ-участники
- История по сезонам

---

## Пример реальной записи участника (profile_id=4779)

```json
{
  "id": 4779, "first_name": "Татьяна", "last_name": "Новикова",
  "gender": "f", "age": 63, "birth_year": 1963,
  "city": "п.Майский", "city_id": 181,
  "region": "Вологодская Область", "region_id": 36,
  "active_years": [2022, 2020],
  "stats": {"competitions": 3, "kilometers": 27, "first_places": 0},
  "results": [
    {"competition_id": 194, "competition_title": "Вологодский марафон",
     "competition_date": "2022-03-08", "finish_time": "00:41:37.35",
     "place_abs": 24, "place_gender": 24, "place_group": 4, "dnf": false}
  ]
}
```

---

## Следующие шаги

**Сделано (MVP)**

- [x] Веб-дашборд поверх `marathon.db` — Streamlit [`app.py`](app.py), запросы в [`marathon_queries.py`](marathon_queries.py)

**Бэклог (после MVP)**

- [ ] Расширение дашборда: метрики сезона как в полной концепции, карточка команды (официальный зачёт)
- [ ] FastAPI + отдельный фронт (если понадобится вместо Streamlit)
- [ ] Система рейтинговых очков Кубка (своя формула поверх данных API)
- [ ] Геокарта участников по регионам
- [ ] Уведомления о новых событиях / результатах
- [ ] Экспорт отчётов в PDF/Excel
