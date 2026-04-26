"""
Разведка API для справочников: соревнования и города.

Заходит на страницы событий и перехватывает все API-запросы,
чтобы найти endpoint'ы для competition и city/location.

Запуск:
    pip install playwright
    playwright install chromium
    python discover_refs.py
"""

from playwright.sync_api import sync_playwright
import json

# Известные competition_id из данных
COMPETITION_IDS = [194, 115, 127, 298]

# Известные city_id из данных
CITY_IDS = [181, 212]

BASE = "https://vologdamarafon.ru"


def intercept(page, label: str) -> list:
    captured = []

    def on_response(response):
        url = response.url
        ct = response.headers.get("content-type", "")
        if "json" in ct and "vologdamarafon.ru" in url:
            try:
                data = response.json()
                captured.append({
                    "label": label,
                    "url": url,
                    "data": data,
                })
                print(f"  ✓ JSON: {url}")
                print(f"    {json.dumps(data, ensure_ascii=False)[:300]}")
            except Exception:
                pass

    page.on("response", on_response)
    return captured


def probe_competition(browser, comp_id: int) -> list:
    """Пробуем разные URL-паттерны для соревнования."""
    page = browser.new_page()
    all_captured = []

    print(f"\n{'='*60}")
    print(f"Пробуем competition_id={comp_id}")

    # Пробуем прямой API endpoint
    candidates = [
        f"{BASE}/api/v2/competition/{comp_id}/",
        f"{BASE}/api/v2/competitions/{comp_id}/",
        f"{BASE}/api/v2/event/{comp_id}/",
        f"{BASE}/api/v2/race/{comp_id}/",
    ]

    captured = intercept(page, f"competition_{comp_id}")

    # Открываем страницу результатов события (там точно будет AJAX)
    # Используем competition_date и slug из данных (нам нужно найти URL страницы)
    # Попробуем напрямую API
    for url in candidates:
        try:
            resp = page.request.get(url)
            if resp.status == 200:
                try:
                    data = resp.json()
                    print(f"  ✓ DIRECT API: {url}")
                    print(f"    {json.dumps(data, ensure_ascii=False)[:400]}")
                    all_captured.append({"url": url, "data": data})
                except Exception:
                    print(f"  ~ Not JSON: {url}")
            else:
                print(f"  ✗ {resp.status}: {url}")
        except Exception as e:
            print(f"  ✗ Error {url}: {e}")

    page.close()
    return all_captured + captured


def probe_location(browser) -> list:
    """Ищем API для городов и регионов."""
    page = browser.new_page()
    all_captured = []

    print(f"\n{'='*60}")
    print("Пробуем location/city API")

    candidates = [
        f"{BASE}/api/v2/location/",
        f"{BASE}/api/v2/locations/",
        f"{BASE}/api/v2/cities/",
        f"{BASE}/api/v2/city/",
        f"{BASE}/api/v2/regions/",
        f"{BASE}/api/v2/city/181/",
        f"{BASE}/api/v2/location/city/181/",
        f"{BASE}/api/v2/location/?type=city",
        f"{BASE}/api/v2/location/?city_id=181",
    ]

    for url in candidates:
        try:
            resp = page.request.get(url)
            if resp.status == 200:
                try:
                    data = resp.json()
                    print(f"  ✓ DIRECT: {url}")
                    print(f"    {json.dumps(data, ensure_ascii=False)[:400]}")
                    all_captured.append({"url": url, "data": data})
                except Exception:
                    print(f"  ~ Not JSON: {url}")
            else:
                print(f"  ✗ {resp.status}: {url}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    page.close()
    return all_captured


def probe_competition_page(browser, comp_id: int) -> list:
    """
    Открываем страницу списка событий и страницу результатов —
    перехватываем все JSON-запросы.
    """
    page = browser.new_page()
    captured = intercept(page, f"comp_page_{comp_id}")

    print(f"\n{'='*60}")
    print(f"Открываем список событий (перехват AJAX)...")

    # Список всех событий — там может быть API со всеми competition
    page.goto(f"{BASE}/marafon/", wait_until="networkidle", timeout=20000)
    try:
        page.wait_for_selector("img[src*='ajax-loader']", state="hidden", timeout=10000)
    except Exception:
        pass
    page.wait_for_timeout(2000)

    print(f"Открываем страницу кубков...")
    page.goto(f"{BASE}/kubki/", wait_until="networkidle", timeout=20000)
    page.wait_for_timeout(2000)

    page.close()
    return captured


def probe_results_page(browser) -> list:
    """
    Открываем страницу результатов конкретного события,
    ищем API с дистанциями и group_id.
    """
    page = browser.new_page()
    captured = intercept(page, "results_page")

    print(f"\n{'='*60}")
    print("Открываем страницу результатов события (Стризневский 2026)...")

    url = ("https://vologdamarafon.ru/marafon/"
           "%D1%81%D1%82%D1%80%D0%B8%D0%B7%D0%BD%D0%B5%D0%B2%D1%81%D0%BA%D0%B8%D0%B9-%D0%BC%D0%B0%D1%80%D0%B0%D1%84%D0%BE%D0%BD/"
           "%D1%80%D0%B5%D0%B7%D1%83%D0%BB%D1%8C%D1%82%D0%B0%D1%82%D1%8B/2026/?distance=794")
    page.goto(url, wait_until="networkidle", timeout=30000)
    try:
        page.wait_for_selector("img[src*='ajax-loader']", state="hidden", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(3000)

    page.close()
    return captured


def main():
    all_found = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)

        # 1. Прямые API-запросы к competition
        for cid in COMPETITION_IDS[:2]:
            found = probe_competition(browser, cid)
            if found:
                all_found[f"competition_{cid}"] = found

        # 2. Location/city API
        loc_found = probe_location(browser)
        if loc_found:
            all_found["location"] = loc_found

        # 3. Страница событий (перехват AJAX)
        comp_page = probe_competition_page(browser, 298)
        if comp_page:
            all_found["competition_page_ajax"] = comp_page

        # 4. Страница результатов события
        results_page = probe_results_page(browser)
        if results_page:
            all_found["results_page_ajax"] = results_page

        browser.close()

    # Сохраняем всё найденное
    with open("discovered_refs.json", "w", encoding="utf-8") as f:
        json.dump(all_found, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print("Итог:")
    for key, items in all_found.items():
        print(f"  {key}: {len(items)} endpoint(ов)")
        for item in items:
            url = item.get("url", "?")
            print(f"    {url}")

    print(f"\n✅ Всё сохранено в discovered_refs.json")
    print("Изучи файл и найди:")
    print("  1. URL для получения данных соревнования по ID")
    print("  2. URL для получения списка всех городов/регионов")
    print("  3. URL для получения дистанций/групп внутри соревнования")


if __name__ == "__main__":
    main()
