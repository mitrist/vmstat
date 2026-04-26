# Vologda Marathon Crawler

Парсер и аналитика данных **vologdamarafon.ru**. Основной контур данных — **`marathon.db`** (см. [system_prompt.md](system_prompt.md)).

---

## Контур `marathon.db` (MVP)

### Установка

```bash
pip install -r requirements.txt
```

### Первичный сбор

```bash
python crawler_full.py                          # соревнования + кубки + профили из результатов
python crawler_full.py --only competitions      # только события (ID по умолчанию 1–400, см. скрипт)
python crawler_full.py --only cups
python crawler_full.py --only profiles
```

### Регулярное обновление

```bash
python sync.py
python sync.py --dry-run
python sync.py --check-ahead 50 --recheck-days 14
python sync.py --cups-only
```

### Дополнительно (по ситуации)

- Импорт профилей из CSV: `python import_profiles_csv.py --skip-not-found --crawl-log`
- Заполнение `profile_cup_results` из API: `python fill_profile_cup_results.py` (долго; есть `--only-missing`, диапазоны id)

### Чеклист данных

- В `results` / `cup_results` должны быть заполнены `profile_id` (после правок парсера — поле `competitor` в API).
- Карточка участника в UI использует `profiles` и при необходимости `profile_cup_results`.

### Аналитика и дашборд

```bash
python analytics.py                             # CLI-сводка
python analytics.py --profile-id 2139
streamlit run app.py                            # веб-MVP (переменная MARATHON_DB или secrets)
                                                # тема: .streamlit/config.toml + стиль vologdamarafon.ru в app.py
```

---

## Установка (шаги 1–3, legacy)

```bash
pip install -r requirements.txt
playwright install chromium
```

---

## Шаг 1 — Разведка (находим реальный API)

```bash
python step1_discover_api.py
```

Откроет браузер на профиле #2139, перехватит все сетевые запросы
и сохранит их в `captured_requests.json`.

**Что смотреть в captured_requests.json:**
Ищи запросы с `"content_type": "application/json"` и данными,
похожими на список результатов. URL этого запроса — твой API endpoint.

Пример находки:
```json
{
  "url": "https://vologdamarafon.ru/api/v1/participant/2139/results/",
  "data_preview": "[{\"event\": \"Стризневский марафон\", \"time\": \"1:42:30\", ...}]"
}
```

---

## Шаг 2 — Полный crawler

### Вариант А: нашли API endpoint (быстро, ~0.5 сек/профиль)

```bash
python step2_crawler.py --api "https://vologdamarafon.ru/api/v1/participant/{id}/results/"
```

### Вариант Б: API не нашли, парсим через браузер (медленно, ~5 сек/профиль)

```bash
python step2_crawler.py --playwright
```

### Дополнительные параметры

```bash
# Только первые 500 профилей
python step2_crawler.py --start 1 --end 500

# Более агрессивная скорость (осторожно — не перегружай сервер)
python step2_crawler.py --delay 0.3

# Комбинация
python step2_crawler.py --api "URL" --start 1 --end 5000 --delay 0.4
```

Скрипт **возобновляется** — если прервать и запустить снова,
уже обработанные профили пропускаются.

**Результат:**
- `profiles.db` — SQLite база данных
- `profiles.jsonl` — построчный JSON (удобно для pandas/duckdb)

---

## Шаг 3 — Просмотр данных

```bash
python step3_explore.py
```

Выводит: общую статистику, топ участников, распределение по видам спорта,
популярные события, пример профиля с результатами.

---

## Структура базы данных

```
profiles
  id, name, age, gender, city, club, error, fetched_at

results
  id, profile_id → profiles.id
  event_name, event_date, distance_km, sport (run/bike/ski)
  finish_time, place_abs, place_cat, category, team, points, raw

crawl_log
  profile_id, status (ok/not_found/error), fetched_at
```

---

## Адаптация парсера результатов

Если поля в JSON называются иначе — редактируй функцию
`parse_results_from_json()` в `step2_crawler.py`.
После step1 будет ясно, какие именно имена полей использует сайт.

---

## Оценка времени

| Режим | Скорость | 5000 профилей |
|---|---|---|
| requests + API | ~0.5 сек/профиль | ~40 минут |
| Playwright | ~5 сек/профиль | ~7 часов |

Рекомендую сначала всегда пробовать найти API через step1.
