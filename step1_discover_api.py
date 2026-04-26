"""
ШАГ 1: Разведка — находим реальный API endpoint.
Запускает Playwright на одном профиле, перехватывает все сетевые запросы
и показывает, откуда приходят данные (JSON).

Запуск:
    pip install playwright
    playwright install chromium
    python step1_discover_api.py
"""

from playwright.sync_api import sync_playwright
import json, re

PROFILE_ID = 2139
URL = f"https://vologdamarafon.ru/profile/{PROFILE_ID}/"


def discover(profile_id: int):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False — видим браузер
        page = browser.new_page()

        captured = []  # все интересные сетевые ответы

        def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            # Интересуют JSON и XHR/fetch запросы
            if "json" in ct or (
                "vologdamarafon" in url and url != URL
            ):
                try:
                    body = response.body()
                    try:
                        data = json.loads(body)
                    except Exception:
                        data = body.decode("utf-8", errors="replace")[:500]
                    captured.append({
                        "url": url,
                        "status": response.status,
                        "content_type": ct,
                        "data_preview": data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)[:600],
                    })
                except Exception:
                    pass

        page.on("response", on_response)

        print(f"Открываю {URL} ...")
        page.goto(URL, wait_until="networkidle", timeout=30000)

        # Ждём пока исчезнет ajax-loader (признак завершения загрузки)
        try:
            page.wait_for_selector("img[src*='ajax-loader']", state="hidden", timeout=15000)
        except Exception:
            pass

        # Дополнительная пауза на случай медленного AJAX
        page.wait_for_timeout(3000)

        # Снимаем статические данные из HTML
        static = {}
        try:
            static["name"] = page.locator("h1").first.text_content(timeout=3000).strip()
        except Exception:
            pass
        for label_text in ["Возраст", "Город", "Клуб", "Команда"]:
            try:
                # Ищем текст рядом с лейблом
                loc = page.get_by_text(label_text, exact=False).first
                parent = loc.locator("xpath=..").text_content(timeout=2000).strip()
                static[label_text.lower()] = parent
            except Exception:
                pass

        # Дамп всего HTML для ручного изучения
        html = page.content()
        with open("profile_debug.html", "w", encoding="utf-8") as f:
            f.write(html)

        browser.close()

    # Результат
    print("\n" + "=" * 60)
    print(f"Статические данные профиля {profile_id}:")
    print(json.dumps(static, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print(f"Перехвачено сетевых запросов: {len(captured)}")
    for i, req in enumerate(captured):
        print(f"\n--- Запрос {i+1} ---")
        print(f"  URL:    {req['url']}")
        print(f"  Status: {req['status']}")
        print(f"  CT:     {req['content_type']}")
        print(f"  Data:   {req['data_preview'][:300]}")

    # Сохраняем все перехваченные запросы
    with open("captured_requests.json", "w", encoding="utf-8") as f:
        json.dump(captured, f, ensure_ascii=False, indent=2)
    print("\n✅ Все перехваченные запросы сохранены в captured_requests.json")
    print("✅ Полный HTML страницы сохранён в profile_debug.html")
    print("\nСледующий шаг: изучи captured_requests.json и найди URL с результатами участника.")
    print("Затем запускай step2_crawler.py (подставив найденный API URL).")

    return captured


if __name__ == "__main__":
    discover(PROFILE_ID)
