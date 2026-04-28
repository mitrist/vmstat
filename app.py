"""
MVP-дашборд marathon.db (Streamlit).

Палитра: фон #FFFFFF, основной текст светло-чёрный #1a1a1a, приглушённый #767676, ссылки #93BDDD.

Запуск из корня проекта:
  pip install -r requirements.txt
  streamlit run app.py

Путь к БД: переменная MARATHON_DB или поле [marathon] path в .streamlit/secrets.toml
"""

from __future__ import annotations

import calendar
import datetime
import json
from urllib.parse import urljoin
from typing import Any
import html
import os
from pathlib import Path

import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import re
import streamlit as st
import streamlit.components.v1 as components

import marathon_queries as mq

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", str(mq.DEFAULT_DB)))
APP_DIR = Path(__file__).resolve().parent
SIDEBAR_LOGO = APP_DIR / "assets" / "vologdamarafon.png"
FAVICON_LOCAL = APP_DIR / "favicon-32x32.png"
FAVICON_ASSET = APP_DIR / "assets" / "vologdamarafon.png"
REGION_CENTERS_FILE = APP_DIR / "config" / "region_centers.json"
RUSSIA_REGIONS_GEOJSON_FILE = APP_DIR / "config" / "russia_regions.geojson"
FAVICON_ATTACHED = Path(
    r"C:\Users\Pavlov DA\.cursor\projects\c-Projects-vm-stat\assets\c__Projects_vm_stat_favicon-32x32.png"
)

# Основные цвета сервиса
VM_LINK = "#93BDDD"
VM_TEXT = "#1a1a1a"
VM_PAGE_BG = "#ffffff"
VM_CARD_BORDER = "#dddddd"
VM_MUTED = "#767676"
# Акценты UI (границы, графики, плашки)
VM_ACCENT = "#93BDDD"
VM_BLUE = "#93BDDD"
# Подзаголовок к гистограммам на «Общая статистика» (курсив под названием)
OBSH_BAR_TITLE_NOTE = "Линия тренда - скользящее среднее за 3 года"

RECORDS_VM_CARD_SPORTS: tuple[tuple[str, str], ...] = (
    ("run", "Бег"),
    ("ski", "Лыжи"),
    ("bike", "Вело"),
    ("trail_run", "Трэйл"),
)

PAGE_ALIASES: dict[str, str] = {
    "Общая статистика": "general",
    "Интересные факты": "facts",
    "География ВМ": "geo",
    "Календарь событий": "upcoming",
    "События": "event",
    "Рекорды ВМ": "records",
    "Участники": "participant",
    "Команды": "team",
    "Кубки": "cups",
}

SECTION_SUBMENUS: dict[str, list[tuple[str, str]]] = {
    "Общая статистика": [
        ("general-kpi", "Показатели"),
        ("general-charts", "Графики"),
        ("general-events", "События"),
    ],
    "Интересные факты": [
        ("facts-day", "Факт дня"),
        ("facts-collection", "Подборка фактов"),
        ("facts-charts", "Графики"),
    ],
    "География ВМ": [
        ("geo-map", "Карта"),
        ("geo-tables", "Таблицы"),
    ],
    "События": [
        ("event-list", "События"),
        ("event-series", "Серии событий"),
        ("event-detail", "Детали события"),
    ],
    "Календарь событий": [
        ("upcoming-cal", "Календарь"),
    ],
    "Рекорды ВМ": [
        ("records-vm", "Рекорды ВМ"),
    ],
    "Участники": [
        ("participant-search", "Поиск"),
        ("participant-kpi", "KPI"),
        ("participant-tabs", "Вкладки"),
    ],
    "Команды": [
        ("team-kpi", "KPI"),
        ("team-tabs", "Вкладки"),
    ],
    "Кубки": [
        ("cups-list", "Кубки за год"),
        ("cups-results", "Результаты"),
    ],
}

# Внутренний ключ сессии: панель администратора открывается только по секретному ?page=<slug>.
ADMIN_PANEL_PAGE = "__vm_secret_admin__"
CHART_STYLE_EXPERIMENT_KEY = "chart_style_experiment"

YANDEX_METRICA_SNIPPET = """
<!-- Yandex.Metrika counter -->
<script type="text/javascript">
    (function(m,e,t,r,i,k,a){
        m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
        m[i].l=1*new Date();
        for (var j = 0; j < document.scripts.length; j++) {if (document.scripts[j].src === r) { return; }}
        k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)
    })(window, document,'script','https://mc.yandex.ru/metrika/tag.js?id=108802154', 'ym');

    ym(108802154, 'init', {ssr:true, webvisor:true, clickmap:true, ecommerce:"dataLayer", referrer: document.referrer, url: location.href, accurateTrackBounce:true, trackLinks:true});
</script>
<noscript><div><img src="https://mc.yandex.ru/watch/108802154" style="position:absolute; left:-9999px;" alt="" /></div></noscript>
<!-- /Yandex.Metrika counter -->
""".strip()

YANDEX_METRICA_PARENT_INJECTOR = """
<script>
(function () {
  var COUNTER_ID = 108802154;
  var SCRIPT_SRC = "https://mc.yandex.ru/metrika/tag.js?id=" + COUNTER_ID;
  var d = (window.parent && window.parent.document) ? window.parent.document : document;
  var w = (window.parent && window.parent.window) ? window.parent.window : window;
  if (!d || !w) return;
  if (w.__vmMetricaInjected) return;
  w.__vmMetricaInjected = true;

  if (!w.ym) {
    w.ym = function () { (w.ym.a = w.ym.a || []).push(arguments); };
    w.ym.l = 1 * new Date();
  }

  var scripts = d.getElementsByTagName("script");
  for (var i = 0; i < scripts.length; i++) {
    if (scripts[i].src === SCRIPT_SRC) {
      w.ym(COUNTER_ID, "init", {
        ssr: true,
        webvisor: true,
        clickmap: true,
        ecommerce: "dataLayer",
        referrer: d.referrer,
        url: w.location.href,
        accurateTrackBounce: true,
        trackLinks: true
      });
      return;
    }
  }

  var s = d.createElement("script");
  s.async = true;
  s.src = SCRIPT_SRC;
  var first = scripts[0];
  if (first && first.parentNode) {
    first.parentNode.insertBefore(s, first);
  } else if (d.head) {
    d.head.appendChild(s);
  } else if (d.body) {
    d.body.appendChild(s);
  }

  w.ym(COUNTER_ID, "init", {
    ssr: true,
    webvisor: true,
    clickmap: true,
    ecommerce: "dataLayer",
    referrer: d.referrer,
    url: w.location.href,
    accurateTrackBounce: true,
    trackLinks: true
  });
})();
</script>
""".strip()

COUNTRY_TO_ISO3: dict[str, str] = {
    "россия": "RUS",
    "российская федерация": "RUS",
    "russia": "RUS",
    "беларусь": "BLR",
    "belarus": "BLR",
    "казахстан": "KAZ",
    "kazakhstan": "KAZ",
    "украина": "UKR",
    "ukraine": "UKR",
    "кыргызстан": "KGZ",
    "киргизия": "KGZ",
    "kyrgyzstan": "KGZ",
    "узбекистан": "UZB",
    "uzbekistan": "UZB",
    "армения": "ARM",
    "armenia": "ARM",
    "азербайджан": "AZE",
    "azerbaijan": "AZE",
    "грузия": "GEO",
    "georgia": "GEO",
    "латвия": "LVA",
    "latvia": "LVA",
    "литва": "LTU",
    "lithuania": "LTU",
    "эстония": "EST",
    "estonia": "EST",
    "германия": "DEU",
    "germany": "DEU",
    "польша": "POL",
    "poland": "POL",
    "сша": "USA",
    "соединенные штаты": "USA",
    "united states": "USA",
    "канада": "CAN",
    "canada": "CAN",
}

ISO3_TO_CENTER: dict[str, tuple[float, float]] = {
    "RUS": (61.5240, 105.3188),
    "BLR": (53.7098, 27.9534),
    "KAZ": (48.0196, 66.9237),
    "UKR": (48.3794, 31.1656),
    "KGZ": (41.2044, 74.7661),
    "UZB": (41.3775, 64.5853),
    "ARM": (40.0691, 45.0382),
    "AZE": (40.1431, 47.5769),
    "GEO": (42.3154, 43.3569),
    "LVA": (56.8796, 24.6032),
    "LTU": (55.1694, 23.8813),
    "EST": (58.5953, 25.0136),
    "DEU": (51.1657, 10.4515),
    "POL": (51.9194, 19.1451),
    "USA": (39.8283, -98.5795),
    "CAN": (56.1304, -106.3468),
}

_RUSSIA_REGION_CENTER_CACHE: dict[str, tuple[float, float]] | None = None
_RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE: dict[str, str] | None = None
_RUSSIA_REGIONS_GEOJSON_CACHE: dict[str, Any] | None = None


def _country_to_iso3(country: str | None) -> str | None:
    if not country:
        return None
    key = " ".join(str(country).strip().casefold().replace("ё", "е").split())
    return COUNTRY_TO_ISO3.get(key)


def _norm_geo_token(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().casefold().replace("ё", "е").split())


def _load_region_centers_map() -> dict[str, tuple[float, float]]:
    global _RUSSIA_REGION_CENTER_CACHE
    if _RUSSIA_REGION_CENTER_CACHE is not None:
        return _RUSSIA_REGION_CENTER_CACHE

    out: dict[str, tuple[float, float]] = {}
    try:
        payload = json.loads(REGION_CENTERS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _RUSSIA_REGION_CENTER_CACHE = out
        return out
    rows = payload.get("regions") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        _RUSSIA_REGION_CENTER_CACHE = out
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _norm_geo_token(str(row.get("name") or ""))
        lat = row.get("lat")
        lon = row.get("lon")
        try:
            if not name:
                continue
            center = (float(lat), float(lon))
        except (TypeError, ValueError):
            continue
        out[name] = center
        aliases = row.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                ak = _norm_geo_token(str(a or ""))
                if ak:
                    out[ak] = center

    _RUSSIA_REGION_CENTER_CACHE = out
    return out


def _load_region_alias_to_canonical_map() -> dict[str, str]:
    global _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE
    if _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE is not None:
        return _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE

    out: dict[str, str] = {}
    try:
        payload = json.loads(REGION_CENTERS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE = out
        return out
    rows = payload.get("regions") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE = out
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        canonical = _norm_geo_token(str(row.get("name") or ""))
        if not canonical:
            continue
        out[canonical] = canonical
        aliases = row.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                ak = _norm_geo_token(str(a or ""))
                if ak:
                    out[ak] = canonical
    _RUSSIA_REGION_ALIAS_TO_CANONICAL_CACHE = out
    return out


def _region_to_canonical(region: str | None) -> str | None:
    key = _norm_geo_token(region)
    if not key:
        return None
    return _load_region_alias_to_canonical_map().get(key)


def _load_russia_regions_geojson_prepared() -> dict[str, Any] | None:
    global _RUSSIA_REGIONS_GEOJSON_CACHE
    if _RUSSIA_REGIONS_GEOJSON_CACHE is not None:
        return _RUSSIA_REGIONS_GEOJSON_CACHE
    try:
        geo = json.loads(RUSSIA_REGIONS_GEOJSON_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    feats = geo.get("features")
    if not isinstance(feats, list):
        return None
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties")
        if not isinstance(props, dict):
            props = {}
            f["properties"] = props
        props["norm_name"] = _norm_geo_token(props.get("name"))
    _RUSSIA_REGIONS_GEOJSON_CACHE = geo
    return geo


def _region_to_center(region: str | None) -> tuple[float, float] | None:
    key = _norm_geo_token(region)
    if not key:
        return None
    return _load_region_centers_map().get(key)


def _st_plotly_chart_supports_on_select() -> bool:
    import inspect

    return "on_select" in inspect.signature(st.plotly_chart).parameters


def _obsh_parse_year_from_plotly_event(event: object) -> int | None:
    """Ось X гистограммы «по годам» — взять год из selection.points (st.plotly_chart on_select)."""
    if event is None:
        return None
    sel: Any = getattr(event, "selection", None)
    if sel is None and isinstance(event, dict):
        sel = event.get("selection")
    if not sel:
        return None
    points: Any = (
        sel.get("points")
        if isinstance(sel, dict)
        else getattr(sel, "points", None)
    )
    if not points:
        return None
    p0 = points[0]
    if isinstance(p0, dict):
        x = p0.get("x", p0.get("label"))
    else:
        x = getattr(p0, "x", None)
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            return int(x)
        s = str(x).strip()
        if s.lstrip("-").isdigit():
            return int(s)
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _facts_prepare_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "km_total" in df.columns:
        df["km_total"] = pd.to_numeric(df["km_total"], errors="coerce").fillna(0.0).round(1)
    if "profile_id" in df.columns:
        pid_num = pd.to_numeric(df["profile_id"], errors="coerce")
        df["profile_id"] = pid_num.map(
            lambda x: f"https://vm-stat.ru/?page=participant&pid={int(x)}" if pd.notna(x) else ""
        )
    rename_map = {
        "participant": "Участник",
        "active_years": "Активных лет",
        "starts": "Стартовал всего",
        "finishes": "Финишировал всего",
        "km_total": "Пройдено километров",
        "finish_rate_pct": "Процент финишей",
        "sports_count": "Видов спорта",
        "sports_list": "Виды спорта",
        "distance": "Дистанция",
        "distance_km": "Км дистанции",
        "participants": "Участников",
        "team": "Команда",
        "first_year": "Первый год",
        "last_year": "Последний год",
        "active_span_years": "Активный период (лет)",
        "city": "Город",
        "region": "Регион",
        "country": "Страна",
        "sport": "Вид спорта",
        "profile_id": "profile_id",
    }
    return df.rename(columns=rename_map)


def _facts_table(rows: list[dict[str, Any]], key: str) -> None:
    df = _facts_prepare_df(rows)
    if df.empty:
        st.caption("Нет данных для таблицы.")
        return
    cfg: dict[str, Any] = {}
    if "profile_id" in df.columns:
        cfg["profile_id"] = st.column_config.LinkColumn(
            "profile_id",
            help="Открыть страницу участника",
            display_text=r".*pid=(\d+)",
        )
    st.dataframe(df, use_container_width=True, hide_index=True, key=key, column_config=cfg or None)


def db_path() -> Path:
    try:
        if hasattr(st, "secrets") and st.secrets and "marathon" in st.secrets:
            p = st.secrets["marathon"].get("path")
            if p:
                return Path(p)
    except Exception:
        pass
    return DEFAULT_DB


def page_icon_path() -> str | None:
    """Путь к favicon; сначала локальный файл проекта, затем файл из вложения."""
    for p in (FAVICON_LOCAL, FAVICON_ASSET, FAVICON_ATTACHED):
        try:
            if p.is_file():
                return str(p)
        except OSError:
            continue
    return None


def inject_yandex_metrica() -> None:
    """
    Вставляет код Метрики максимально рано в рендере страницы.
    Добавляет скрипт в parent.document (верхний DOM), чтобы счётчик корректно
    определялся внешними валидаторами.
    """
    components.html(YANDEX_METRICA_PARENT_INJECTOR, height=0)
    # Fallback для окружений, где доступ к parent.document ограничен.
    if hasattr(st, "html"):
        st.html(YANDEX_METRICA_SNIPPET)


def inject_vm_styles() -> None:
    """CSS в духе navbar-vm + контент результатов."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        html, body, [class*="css"] {{
            font-family: "Inter", "Segoe UI", system-ui, sans-serif !important;
        }}
        [data-testid="stAppViewContainer"] {{
            background-color: {VM_PAGE_BG} !important;
        }}
        [data-testid="stAppViewContainer"] > .main {{
            background-color: {VM_PAGE_BG};
        }}
        .main, .block-container, .stMarkdown, .stText {{
            color: {VM_TEXT} !important;
        }}
        .main a, .main a:visited,
        [data-testid="stAppViewContainer"] a, [data-testid="stAppViewContainer"] a:visited {{
            color: {VM_LINK} !important;
        }}
        .main a:hover,
        [data-testid="stAppViewContainer"] a:hover {{
            color: #7aacc4 !important;
        }}
        [data-testid="stHeader"] {{
            background-color: {VM_PAGE_BG};
            background-image: none;
            border-bottom: 1px solid {VM_CARD_BORDER};
        }}
        [data-testid="stHeader"] [data-testid="stDecoration"] {{
            background-image: linear-gradient(90deg, {VM_ACCENT}, #b0d4ea);
        }}
        section[data-testid="stSidebar"] > div:first-child {{
            background: #e6edf7;
            color: #1d3f72;
        }}
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] p {{
            color: #1d3f72 !important;
        }}
        section[data-testid="stSidebar"] a, section[data-testid="stSidebar"] a:visited {{
            color: #1b4f9c !important;
        }}
        section[data-testid="stSidebar"] .stMarkdown {{
            color: #2e5f9f !important;
        }}
        section[data-testid="stSidebar"] hr {{
            border-color: #b8cbe2;
        }}
        /* Более контрастная рамка селекторов/мультиселектов */
        div[data-baseweb="select"] > div {{
            border: 1px solid #8faac8 !important;
            box-shadow: 0 0 0 1px rgba(143, 170, 200, 0.18);
        }}
        div[data-baseweb="select"] > div:hover {{
            border-color: #6d90b6 !important;
        }}
        div[data-baseweb="select"] input::placeholder {{
            color: #3f6690 !important;
            opacity: 1;
        }}
        /* Сайдбар: навигация — только текст, без кружков/иконок (см. .vm-sidebar-text-nav) */
        .vm-sidebar-text-nav {{
            font-size: 0.95rem;
            line-height: 1.45;
        }}
        .vm-sidebar-text-nav p {{
            margin: 0.4rem 0;
            padding: 0;
        }}
        .vm-sidebar-text-nav .vm-sidebar-here {{
            font-weight: 600;
            color: #143f7a;
        }}
        .vm-sidebar-text-nav a {{
            color: #1b4f9c;
            text-decoration: none;
            font-weight: 400;
        }}
        .vm-sidebar-text-nav a:hover {{
            text-decoration: underline;
            color: #0d3a7a;
        }}
        .vm-brand-bar {{
            background: {VM_PAGE_BG};
            color: {VM_TEXT};
            padding: 14px 4px 14px 4px;
            margin: -1rem -4rem 1.25rem -4rem;
            border-bottom: 4px solid {VM_ACCENT};
            font-weight: 700;
            letter-spacing: 0.14em;
            font-size: 0.95rem;
        }}
        .vm-brand-bar span.sub {{
            font-weight: 400;
            letter-spacing: normal;
            opacity: 0.88;
            margin-left: 0.75rem;
            font-size: 0.85rem;
        }}
        h1, h2, h3 {{
            color: {VM_TEXT} !important;
            font-weight: 600 !important;
        }}
        h2 {{
            border-left: 4px solid {VM_ACCENT};
            padding-left: 0.65rem !important;
            margin-top: 1.25rem !important;
        }}
        h3 {{
            border-left: 3px solid {VM_ACCENT};
            padding-left: 0.5rem !important;
            opacity: 0.95;
        }}
        [data-testid="stMetricValue"] {{
            color: {VM_TEXT} !important;
        }}
        [data-testid="stMetricLabel"] {{
            color: {VM_MUTED} !important;
        }}
        div[data-testid="stExpander"] details summary span {{
            color: {VM_TEXT};
        }}
        [data-testid="stDataFrame"] {{
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 2px;
        }}
        .stAlert {{
            border-radius: 2px;
        }}
        /* Фильтры-плашки: выбранная плашка синяя, остальные с синим текстом */
        div[data-testid="stPills"] [data-testid="stBaseButton-primary"] {{
            background-color: {VM_BLUE} !important;
            border-color: {VM_BLUE} !important;
        }}
        div[data-testid="stPills"] [data-testid="stBaseButton-primary"] p {{
            color: #ffffff !important;
        }}
        div[data-testid="stPills"] [data-testid="stBaseButton-secondary"] {{
            background-color: transparent !important;
            border: 1px solid transparent !important;
        }}
        div[data-testid="stPills"] [data-testid="stBaseButton-secondary"] p {{
            color: {VM_BLUE} !important;
            font-weight: 500;
        }}
        /* Segmented control (тоже формат плашек) */
        div[data-testid="stSegmentedControl"] [data-baseweb="button-group"] button[aria-pressed="true"] {{
            background-color: {VM_BLUE} !important;
            border-color: {VM_BLUE} !important;
            color: #ffffff !important;
        }}
        div[data-testid="stSegmentedControl"] [data-baseweb="button-group"] button[aria-pressed="false"] {{
            color: {VM_BLUE} !important;
        }}
        /* Вкладки в стиле Chrome (st.tabs): полоска, активная с белым фоном) */
        div[data-testid="stTabs"] {{
            margin-top: 0.35rem;
        }}
        div[data-testid="stTabs"] [data-baseweb="tab-list"] {{
            display: flex;
            flex-direction: row;
            gap: 0;
            background: #dfe1e5;
            padding: 0 0 0 0;
            border-radius: 8px 8px 0 0;
            border: 1px solid #d0d0d0;
            border-bottom: none;
        }}
        div[data-testid="stTabs"] [data-baseweb="tab"] {{
            background: #e8eaed !important;
            color: {VM_MUTED} !important;
            border: none;
            border-radius: 7px 7px 0 0;
            margin: 4px 0 0 4px;
            padding: 8px 18px 9px 18px !important;
            font-size: 0.92rem;
            font-weight: 500;
            min-height: 0;
        }}
        div[data-testid="stTabs"] [data-baseweb="tab"]:hover {{
            color: {VM_TEXT} !important;
            background: #ecedef !important;
        }}
        div[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {{
            background: #fff !important;
            color: {VM_TEXT} !important;
            box-shadow: 0 -1px 0 0 #fff, 0 0 0 0 #000;
            position: relative;
            z-index: 2;
            border: 1px solid #d0d0d0;
            border-bottom: 1px solid #fff !important;
            margin-bottom: -1px;
        }}
        /* Раздел «Кубки»: радио выбора кубка в main — строки-фильтры (без влияния на sidebar) */
        [data-testid="stAppViewContainer"] .main [data-testid="stRadio"] [data-baseweb="radio"] > div,
        [data-testid="stAppViewContainer"] .main [data-testid="stRadio"] [role="radiogroup"] {{
            flex-direction: column !important;
            align-items: stretch !important;
            gap: 0;
            width: 100%;
        }}
        [data-testid="stAppViewContainer"] .main [data-testid="stRadio"] [role="radiogroup"] > label {{
            display: block !important;
            width: 100%;
            max-width: 100%;
            box-sizing: border-box;
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 4px;
            padding: 10px 12px 10px 2rem;
            margin: 0 0 6px 0;
            background: #f8f8f8;
        }}
        [data-testid="stAppViewContainer"] .main [data-testid="stRadio"] [role="radiogroup"] > label:has([aria-checked="true"]) {{
            background: #e8f2f9 !important;
            border-color: {VM_ACCENT} !important;
        }}
        /* Кубки — командное: иерархия team → member → события (HTML details) */
        .vm-cup-tree {{
            font-size: 0.92rem;
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 8px;
            overflow: hidden;
            background: #fff;
            margin: 0.5rem 0 0 0;
        }}
        .vm-cup-tree .vm-cup-head {{
            display: grid;
            grid-template-columns: 56px minmax(0,1fr) 80px 76px;
            gap: 8px;
            align-items: center;
            padding: 10px 14px;
            background: #e8f2f9;
            color: {VM_TEXT};
            font-weight: 600;
        }}
        .vm-cup-tree details.vm-cup-team {{
            border-bottom: 1px solid {VM_CARD_BORDER};
        }}
        .vm-cup-tree details.vm-cup-team:last-child {{
            border-bottom: none;
        }}
        .vm-cup-tree details.vm-cup-team > summary {{
            list-style: none;
            cursor: pointer;
            display: grid;
            grid-template-columns: 56px minmax(0,1fr) 80px 76px;
            gap: 8px;
            align-items: center;
            padding: 10px 14px;
            background: #fafafa;
            font-weight: 500;
        }}
        .vm-cup-tree details.vm-cup-team > summary::-webkit-details-marker {{ display: none; }}
        .vm-cup-tree details.vm-cup-team > summary::marker {{
            content: "";
        }}
        .vm-cup-tree .vm-cup-rank-cell {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-variant-numeric: tabular-nums;
        }}
        .vm-cup-tree details.vm-cup-team:not([open]) > summary .vm-cup-caret-t::before {{
            content: "▸";
            color: {VM_ACCENT};
            font-weight: 700;
        }}
        .vm-cup-tree details.vm-cup-team[open] > summary .vm-cup-caret-t::before {{
            content: "▾";
            color: {VM_ACCENT};
            font-weight: 700;
        }}
        .vm-cup-tree details.vm-cup-team > summary:hover {{
            background: #f0f0f2;
        }}
        .vm-cup-tree .vm-cup-team-body {{
            padding: 8px 10px 12px 18px;
            background: #f5f5f6;
            border-top: 1px solid #e4e4e6;
        }}
        .vm-cup-tree details.vm-cup-member {{
            margin-bottom: 8px;
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 6px;
            background: #fff;
            overflow: hidden;
        }}
        .vm-cup-tree details.vm-cup-member:last-child {{ margin-bottom: 0; }}
        .vm-cup-tree details.vm-cup-member > summary {{
            list-style: none;
            cursor: pointer;
            display: grid;
            grid-template-columns: minmax(0,1fr) 56px 44px auto;
            gap: 8px;
            align-items: center;
            padding: 8px 12px;
            font-size: 0.9rem;
        }}
        .vm-cup-tree details.vm-cup-member > summary::-webkit-details-marker {{ display: none; }}
        .vm-cup-tree details.vm-cup-member > summary::marker {{
            content: "";
        }}
        .vm-cup-tree .vm-cup-name-cell {{
            display: flex;
            align-items: center;
            gap: 6px;
            min-width: 0;
        }}
        .vm-cup-tree details.vm-cup-member:not([open]) > summary .vm-cup-caret-m::before {{
            content: "▸";
            color: {VM_ACCENT};
            font-weight: 700;
            flex-shrink: 0;
        }}
        .vm-cup-tree details.vm-cup-member[open] > summary .vm-cup-caret-m::before {{
            content: "▾";
            color: {VM_ACCENT};
            font-weight: 700;
            flex-shrink: 0;
        }}
        .vm-cup-tree details.vm-cup-member > summary:hover {{
            background: #fafafa;
        }}
        .vm-cup-tree .vm-cup-badge {{
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            padding: 3px 8px;
            border-radius: 4px;
            background: #d4edda;
            color: #155724;
            white-space: nowrap;
        }}
        .vm-cup-tree .vm-cup-badge-muted {{
            background: #e9ecef;
            color: #495057;
        }}
        .vm-cup-tree .vm-cup-share {{
            font-size: 0.82rem;
            color: {VM_MUTED};
            text-align: right;
            font-variant-numeric: tabular-nums;
            white-space: nowrap;
        }}
        .vm-cup-tree table.vm-cup-ev {{
            width: calc(100% - 16px);
            margin: 0 8px 10px 8px;
            border-collapse: collapse;
            font-size: 0.84rem;
        }}
        .vm-cup-tree table.vm-cup-ev th,
        .vm-cup-tree table.vm-cup-ev td {{
            border: 1px solid #e2e2e4;
            padding: 6px 8px;
            text-align: left;
            vertical-align: top;
        }}
        .vm-cup-tree table.vm-cup-ev th {{
            background: #ececed;
            font-weight: 600;
            color: {VM_TEXT};
        }}
        .vm-cup-tree table.vm-cup-ev td:nth-child(3),
        .vm-cup-tree table.vm-cup-ev td:nth-child(4) {{
            white-space: nowrap;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
def require_db(path: Path) -> bool:
    if not path.is_file():
        st.error(
            f"Файл базы не найден: `{path.resolve()}`. Соберите данные: `python crawler_full.py` или укажите MARATHON_DB."
        )
        return False
    return True


def _sidebar_read_nav_i_from_url() -> int | None:
    """Параметр ?i= — индекс пункта меню (0..n), без Streamlit-виджетов."""
    try:
        q = st.query_params
        if "i" not in q:
            return None
        raw = q.get("i")
        if raw is None:
            return None
        s = raw[0] if isinstance(raw, (list, tuple)) and len(raw) else str(raw)
        i = int(s)
    except (ValueError, TypeError):
        return None
    return i


def _sidebar_read_page_from_url() -> str | None:
    """Стабильный параметр ?page=<alias> для прямой навигации без зависимости от индекса."""
    try:
        q = st.query_params
        if "page" not in q:
            return None
        raw = q.get("page")
        if raw is None:
            return None
        s = raw[0] if isinstance(raw, (list, tuple)) and len(raw) else str(raw)
        page_raw = str(s).strip()
        slug = _resolve_admin_route_slug()
        if slug and page_raw == slug.strip():
            return ADMIN_PANEL_PAGE
        key = page_raw.casefold()
    except Exception:
        return None
    for title, alias in PAGE_ALIASES.items():
        if key == alias:
            return title
    if key == "interesting-facts":
        return "Интересные факты"
    return None


def _sidebar_read_section_from_url() -> str | None:
    """Параметр ?section=<anchor-id> для скролла к нужному блоку."""
    try:
        q = st.query_params
        if "section" not in q:
            return None
        raw = q.get("section")
        if raw is None:
            return None
        s = raw[0] if isinstance(raw, (list, tuple)) and len(raw) else str(raw)
        section = str(s).strip()
    except Exception:
        return None
    return section if re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,60}", section) else None


def _participant_id_from_url() -> int | None:
    """Читает pid из URL-параметров: ?pid=123."""
    try:
        q = st.query_params
        if "pid" not in q:
            return None
        raw = q.get("pid")
        if raw is None:
            return None
        s = raw[0] if isinstance(raw, (list, tuple)) and len(raw) else str(raw)
        return int(str(s).strip())
    except (TypeError, ValueError):
        return None


def _section_anchor(section_id: str) -> None:
    st.markdown(f'<div id="{html.escape(section_id)}"></div>', unsafe_allow_html=True)


def _scroll_to_section_once(current_page: str) -> None:
    section = _sidebar_read_section_from_url()
    if not section:
        return
    key = f"{current_page}:{section}"
    if st.session_state.get("_last_section_scroll_key") == key:
        return
    st.session_state["_last_section_scroll_key"] = key
    script = f"""
    <script>
    (function() {{
      const targetId = {section!r};
      const tryScroll = () => {{
        const docs = [window.document, window.parent?.document].filter(Boolean);
        for (const d of docs) {{
          const el = d.getElementById(targetId);
          if (el) {{
            el.scrollIntoView({{behavior: 'auto', block: 'start'}});
            return true;
          }}
        }}
        return false;
      }};
      if (!tryScroll()) {{
        setTimeout(tryScroll, 120);
        setTimeout(tryScroll, 300);
        setTimeout(tryScroll, 700);
      }}
    }})();
    </script>
    """
    if hasattr(st, "html"):
        st.html(script)
    else:
        components.html(script, height=0)


def render_sidebar_text_nav(pages: tuple[str, ...], current: str) -> None:
    """Рендерит только основные пункты меню в сайдбаре."""
    icons = {
        "Общая статистика": "📊",
        "География ВМ": "🗺️",
        "Календарь событий": "📅",
        "События": "🏁",
        "Рекорды ВМ": "🥇",
        "Участники": "👤",
        "Команды": "🛡️",
        "Кубки": "🏆",
        "Интересные факты": "✨",
    }
    parts: list[str] = ['<div class="vm-sidebar-text-nav">']
    for title in pages:
        icon = icons.get(title, "•")
        esc = html.escape(f"{icon} {title}")
        alias = PAGE_ALIASES.get(title, "general")
        base_link = f"?page={alias}"
        if title == current:
            parts.append(f'<p class="vm-sidebar-here">{esc}</p>')
        else:
            parts.append(f'<p><a href="{base_link}" target="_self">{esc}</a></p>')
    parts.append("</div>")
    st.sidebar.markdown("".join(parts), unsafe_allow_html=True)


def sidebar_stat_card(label: str, value: int | str) -> None:
    """Компактная карточка для боковой панели (события, участники, команды)."""
    lab = html.escape(label)
    v = html.escape(str(value))
    st.sidebar.markdown(
        f"""
        <div style="
            background: #ffffff;
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 6px;
            padding: 10px 10px 12px 10px;
            margin: 0 0 8px 0;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        ">
            <div style="font-size:0.7rem;color:{VM_MUTED};font-weight:500;line-height:1.3;">{lab}</div>
            <div style="font-size:1.25rem;font-weight:700;color:{VM_TEXT};margin-top:6px;">{v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _layout_year_bar_with_labels_and_ma3(
    fig: go.Figure, df: pd.DataFrame, y_column: str, chart_title: str
) -> None:
    """Подписи над столбцами, сглаживание — скользящее среднее за 3 года (по центру), hover только Y."""
    if df.empty or len(df) < 1:
        return
    ds = df.sort_values("year").reset_index(drop=True)
    yv = ds[y_column].astype(float)
    ymax = float(yv.max()) if len(yv) else 0.0
    pad = ymax * 0.2 + (4 if ymax < 30 else max(2.0, ymax * 0.06))

    title_with_note = (
        f"{chart_title}<br>"
        f'<span style="color:{VM_MUTED}; font-size:0.78em;"><i>{OBSH_BAR_TITLE_NOTE}</i></span>'
    )

    fig.update_traces(
        texttemplate="%{y:.0f}",
        textposition="outside",
        cliponaxis=False,
        hovertemplate="%{y:.0f}<extra></extra>",
    )
    title_layout: dict[str, Any] = dict(
        title=dict(
            text=title_with_note,
            x=0.0,
            xanchor="left",
        ),
    )
    if ymax > 0:
        title_layout["yaxis"] = dict(range=[0, ymax + pad])
    fig.update_layout(**title_layout)

    if len(ds) >= 2:
        ma3 = yv.rolling(window=3, center=True, min_periods=1).mean()
        fig.add_trace(
            go.Scatter(
                x=ds["year"],
                y=ma3,
                mode="lines",
                line=dict(color=VM_MUTED, width=2, dash="dash"),
                name="Ср. 3 г.",
                showlegend=False,
                hovertemplate="%{y:.0f}<extra></extra>",
            )
        )


def metric_plaque(label: str, value: int | str) -> None:
    """Карточка-метрика на светлой плашке (как фон гистограмм Plotly)."""
    lab = html.escape(label)
    v = html.escape(str(value))
    st.markdown(
        f"""
        <div style="
            background: #ffffff;
            border: 1px solid {VM_CARD_BORDER};
            border-radius: 6px;
            padding: 14px 12px;
            margin: 2px 0 8px 0;
            min-height: 88px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
        ">
            <div style="
                font-size: 0.78rem;
                color: {VM_MUTED};
                font-weight: 500;
                letter-spacing: 0.03em;
                line-height: 1.3;
            ">{lab}</div>
            <div style="
                font-size: 1.85rem;
                font-weight: 700;
                color: {VM_TEXT};
                line-height: 1.2;
                margin-top: 8px;
            ">{v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_general_statistics() -> None:
    _section_anchor("general-kpi")
    st.header("Сервис аналитики мероприятий ВологдаМарафон")
    path = db_path()
    if not require_db(path):
        return

    years_all = mq.query_distinct_years(path)
    sports_all = mq.query_distinct_sports(path)

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Год</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — учитываются все годы.</p>",
        unsafe_allow_html=True,
    )
    if years_all:
        sy = st.pills(
            "Год",
            options=years_all,
            selection_mode="multi",
            default=[],
            key="obsh_p_y",
            label_visibility="collapsed",
        )
        yf = list(sy) if sy else None
    else:
        yf = None

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Вид спорта</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — все виды.</p>",
        unsafe_allow_html=True,
    )
    if sports_all:
        ss = st.pills(
            "Вид спорта",
            options=sports_all,
            selection_mode="multi",
            default=[],
            key="obsh_p_s",
            label_visibility="collapsed",
        )
        sf = list(ss) if ss else None
    else:
        sf = None

    cups_rows = mq.query_cups_for_obsh_header_filter(path, yf, sf)
    cup_labels: dict[int, str] = {}
    for r in cups_rows:
        cid = int(r["id"])
        title = str(r.get("title") or "").strip() or f"#{cid}"
        yv = r.get("year")
        try:
            ytxt = str(int(yv)) if yv is not None else ""
        except (TypeError, ValueError):
            ytxt = ""
        cup_labels[cid] = f"{title} ({ytxt})" if ytxt else title
    cup_ids_opts = list(cup_labels.keys())
    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Кубок</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Список зависит от выбранных года и вида спорта. Пусто — все кубки.</p>",
        unsafe_allow_html=True,
    )
    if cup_ids_opts:
        sel_cup_ids = st.multiselect(
            "Кубок",
            options=cup_ids_opts,
            format_func=lambda i: cup_labels.get(int(i), str(i)),
            default=[],
            placeholder="Выбрать Кубок",
            key="obsh_cup_ms",
        )
    else:
        st.caption("Нет кубков в БД, подходящих под выбранные год и вид спорта (если кубок привязан к соревнованиям).")
        sel_cup_ids = []
    cf = sel_cup_ids if sel_cup_ids else None

    cards = mq.query_general_stats_cards(path, yf, sf, cf)
    st.markdown("##### Показатели")
    m1, m2, m3, m4, m5 = st.columns(5, gap="small")
    with m1:
        metric_plaque("Всего событий", cards.get("total_events", 0))
    with m2:
        metric_plaque("Всего участников", cards.get("total_participants", 0))
    with m3:
        metric_plaque("Регионов (уник.)", cards.get("regions_distinct", 0))
    with m4:
        metric_plaque("Команд (уник.)", cards.get("teams_distinct", 0))
    with m5:
        metric_plaque("Стран (уник.)", cards.get("countries_distinct", 0))

    _section_anchor("general-charts")
    st.subheader("Графики")
    g1, g2 = st.columns(2)
    with g1:
        dfe = pd.DataFrame(mq.query_chart_events_by_year(path, yf, sf, cf))
        if dfe.empty:
            st.caption("Нет данных для гистограммы событий по годам.")
        else:
            dfe2 = dfe.copy().sort_values("year")
            dfe2["Год"] = dfe2["year"].astype(int).astype(str)
            dfe2["Событий"] = pd.to_numeric(dfe2["events"], errors="coerce").fillna(0).astype(int)
            dfe2["Тренд"] = (
                pd.to_numeric(dfe2["Событий"], errors="coerce")
                .rolling(window=3, min_periods=1)
                .mean()
                .round(2)
            )
            st.caption("Количество событий по годам")
            chart_bar = (
                alt.Chart(dfe2)
                .mark_bar()
                .encode(
                    x=alt.X("Год:N", title="Год"),
                    y=alt.Y("Событий:Q", title="Событий"),
                    tooltip=[
                        alt.Tooltip("Год:N", title="Год"),
                        alt.Tooltip("Событий:Q", title="Событий"),
                    ],
                )
            )
            chart_trend = (
                alt.Chart(dfe2)
                .mark_line(color="#1f4e79", strokeWidth=2.5, point=True)
                .encode(
                    x=alt.X("Год:N", title="Год"),
                    y=alt.Y("Тренд:Q", title="Событий"),
                    tooltip=[
                        alt.Tooltip("Год:N", title="Год"),
                        alt.Tooltip("Тренд:Q", title="Тренд (MA-3)"),
                    ],
                )
            )
            labels = chart_bar.mark_text(dy=-8).encode(text="Событий:Q")
            st.altair_chart(
                (chart_bar + chart_trend + labels).properties(height=360),
                use_container_width=True,
            )

    with g2:
        dfp = pd.DataFrame(mq.query_chart_unique_participants_by_year(path, yf, sf, cf))
        if dfp.empty:
            st.caption("Нет данных для гистограммы участников по годам.")
        else:
            dfp2 = dfp.copy().sort_values("year")
            dfp2["Год"] = dfp2["year"].astype(int).astype(str)
            dfp2["Участников"] = pd.to_numeric(dfp2["participants"], errors="coerce").fillna(0).astype(int)
            dfp2["Тренд"] = (
                pd.to_numeric(dfp2["Участников"], errors="coerce")
                .rolling(window=3, min_periods=1)
                .mean()
                .round(2)
            )
            st.caption("Уникальных участников по годам")
            chart_bar = (
                alt.Chart(dfp2)
                .mark_bar()
                .encode(
                    x=alt.X("Год:N", title="Год"),
                    y=alt.Y("Участников:Q", title="Участников"),
                    tooltip=[
                        alt.Tooltip("Год:N", title="Год"),
                        alt.Tooltip("Участников:Q", title="Участников"),
                    ],
                )
            )
            chart_trend = (
                alt.Chart(dfp2)
                .mark_line(color="#1f4e79", strokeWidth=2.5, point=True)
                .encode(
                    x=alt.X("Год:N", title="Год"),
                    y=alt.Y("Тренд:Q", title="Участников"),
                    tooltip=[
                        alt.Tooltip("Год:N", title="Год"),
                        alt.Tooltip("Тренд:Q", title="Тренд (MA-3)"),
                    ],
                )
            )
            labels = chart_bar.mark_text(dy=-8).encode(text="Участников:Q")
            st.altair_chart(
                (chart_bar + chart_trend + labels).properties(height=360),
                use_container_width=True,
            )

    _section_anchor("general-events")
    st.subheader("События")
    if "obsh_bar_drill" not in st.session_state:
        st.session_state["obsh_bar_drill"] = None
    bar_drill: int | None = st.session_state.get("obsh_bar_drill")
    d1, d2 = st.columns([1, 4])
    with d1:
        if st.button("Сброс уточнения по графику", help="Снять фильтр года, заданный кликом по столбцу", key="obsh_drill_clear"):
            st.session_state["obsh_bar_drill"] = None
            st.rerun()
    with d2:
        if bar_drill is not None:
            st.caption(f"Таблица уточнена по **{bar_drill}** г. (клик по гистограмме «событий по годам»).")
    ev_rows = mq.query_general_stats_events_table(
        path, yf, sf, cf, bar_year=bar_drill
    )
    if not ev_rows:
        st.caption("Нет строк для выбранных фильтров.")
    else:
        st.dataframe(
            pd.DataFrame(ev_rows),
            use_container_width=True,
            hide_index=True,
        )

    p1, p2 = st.columns(2)
    with p1:
        dfs = pd.DataFrame(mq.query_chart_events_by_sport(path, yf, sf, cf))
        if dfs.empty:
            st.caption("Нет данных для диаграммы по видам спорта.")
        else:
            dfs2 = dfs.rename(columns={"sport": "Вид спорта", "n": "События"})
            st.caption("События по видам спорта")
            pie_sport = (
                alt.Chart(dfs2)
                .transform_joinaggregate(total_events="sum(События)")
                .transform_calculate(pct="datum.События / datum.total_events")
                .mark_arc()
                .encode(
                    theta=alt.Theta("События:Q", title="События"),
                    color=alt.Color("Вид спорта:N", title="Вид спорта"),
                    tooltip=[
                        alt.Tooltip("Вид спорта:N", title="Вид спорта"),
                        alt.Tooltip("События:Q", title="События"),
                        alt.Tooltip("pct:Q", title="Доля", format=".1%"),
                    ],
                )
            )
            labels = pie_sport.mark_text(radius=128).encode(text=alt.Text("pct:Q", format=".1%"))
            st.altair_chart((pie_sport + labels).properties(height=360), use_container_width=True)

    with p2:
        dfg = pd.DataFrame(mq.query_chart_participants_by_gender(path, yf, sf, cf))
        if dfg.empty:
            st.caption("Нет данных для диаграммы по полу.")
        else:
            map_g = {"m": "муж.", "f": "жен.", "не указан": "не указан"}
            dfg["gender_label"] = dfg["gender"].map(lambda g: map_g.get(str(g).lower(), str(g)))
            st.caption("Участники по полу (уникальные профили)")
            pie_gender = dfg.rename(columns={"gender_label": "Пол", "n": "Участников"})[["Пол", "Участников"]]
            c_gender = (
                alt.Chart(pie_gender)
                .transform_joinaggregate(total_participants="sum(Участников)")
                .transform_calculate(pct="datum.Участников / datum.total_participants")
                .mark_arc()
                .encode(
                    theta=alt.Theta("Участников:Q", title="Участников"),
                    color=alt.Color("Пол:N", title="Пол"),
                    tooltip=[
                        alt.Tooltip("Пол:N", title="Пол"),
                        alt.Tooltip("Участников:Q", title="Участников"),
                        alt.Tooltip("pct:Q", title="Доля", format=".1%"),
                    ],
                )
            )
            c_text = c_gender.mark_text(radius=128).encode(text=alt.Text("pct:Q", format=".1%"))
            st.altair_chart((c_gender + c_text).properties(height=360), use_container_width=True)


def page_interesting_facts() -> None:
    _section_anchor("facts-day")
    st.header("Интересные факты")
    path = db_path()
    if not require_db(path):
        return

    sports_all = mq.query_distinct_sports(path)
    sport_pick = st.pills(
        "Вид спорта",
        options=["Все"] + sports_all,
        selection_mode="single",
        default="Все",
        key="facts_sport_filter_pills",
    )
    min_starts = 1
    year_val = None
    sport_val = None if sport_pick == "Все" else str(sport_pick)

    loyal = mq.query_interesting_facts_loyal_participants(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    finishers = mq.query_interesting_facts_finish_rate(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    km_leaders = mq.query_interesting_facts_km_leaders(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    universals = mq.query_interesting_facts_universal_participants(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    distances = mq.query_interesting_facts_distance_frequency(path, year=year_val, sport=sport_val, limit=100)
    teams = mq.query_interesting_facts_team_longevity(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    geo = mq.query_interesting_facts_geography(path, year=year_val, sport=sport_val, limit=100)

    st.subheader("Факт дня")
    if loyal:
        top = loyal[0]
        st.caption("Как считается: лидер по активным годам, затем по числу стартов.")
        metric_plaque(
            "Самый преданный участник",
            f"{top.get('participant', '—')} · {int(top.get('active_years') or 0)} лет участия в кубках Вологда Марафон",
        )
    else:
        st.caption("Нет данных для выбранных фильтров.")

    _section_anchor("facts-collection")
    st.subheader("Подборка фактов")
    f1, f2 = st.columns(2)
    with f1:
        st.markdown("**Самые преданные участники**")
        st.caption("Топ по активным годам участия.")
        _facts_table(loyal, key="facts_table_loyal")
    with f2:
        st.markdown("**Железные финишеры**")
        st.caption("Топ по проценту финишей.")
        _facts_table(finishers, key="facts_table_finishers")

    f3, f4 = st.columns(2)
    with f3:
        st.markdown("**Лидеры по километражу**")
        st.caption("Суммарный километраж по финишам.")
        _facts_table(km_leaders, key="facts_table_km")
    with f4:
        st.markdown("**Универсалы по видам спорта**")
        st.caption("Чем больше покрытие видов спорта, тем выше место.")
        _facts_table(universals, key="facts_table_universals")

    f5, f6 = st.columns(2)
    with f5:
        st.markdown("**Топ дистанций**")
        st.caption("Самые частые дистанции по числу стартов.")
        _facts_table(distances, key="facts_table_distances")
    with f6:
        st.markdown("**Команды-долгожители**")
        st.caption("Команды с самым длинным активным периодом.")
        _facts_table(teams, key="facts_table_teams")

    _section_anchor("facts-charts")
    st.subheader("Графики")
    dc = pd.DataFrame(geo.get("cities") or [])
    if dc.empty:
        st.caption("Нет данных по географии.")
    else:
        city_chart = dc.head(20).rename(columns={"city": "Город", "participants": "Участников"})
        st.caption("Топ городов по участникам")
        c = (
            alt.Chart(city_chart)
            .mark_bar()
            .encode(
                x=alt.X("Участников:Q", title="Участников"),
                y=alt.Y("Город:N", sort="-x", title="Город"),
            )
        )
        t = c.mark_text(align="left", baseline="middle", dx=4).encode(text="Участников:Q")
        st.altair_chart((c + t).properties(height=500), use_container_width=True)

def page_vm_geography() -> None:
    st.header("География ВМ")
    st.info("Страница в разработке")


def page_vm_records() -> None:
    _section_anchor("records-vm")
    st.header("Рекорды ВМ")
    path = db_path()
    if not require_db(path):
        return

    years_all = mq.query_distinct_years(path)
    sports_all = mq.query_distinct_sports(path)

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Год</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — учитываются все годы.</p>",
        unsafe_allow_html=True,
    )
    if years_all:
        sy = st.pills(
            "Год",
            options=years_all,
            selection_mode="multi",
            default=[],
            key="records_vm_pills_years",
            label_visibility="collapsed",
        )
        years_filter = list(sy) if sy else None
    else:
        years_filter = None

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Вид спорта</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — учитываются все виды спорта.</p>",
        unsafe_allow_html=True,
    )
    if sports_all:
        ss = st.pills(
            "Вид спорта",
            options=sports_all,
            selection_mode="multi",
            default=[],
            key="records_vm_pills_sports",
            label_visibility="collapsed",
        )
        sports_filter = list(ss) if ss else None
    else:
        sports_filter = None

    st.subheader("Лидеры по числу действующих рекордов")
    for code, label in RECORDS_VM_CARD_SPORTS:
        hdr = f"{label} ({code})"
        st.markdown(f"##### {hdr}")
        ch = mq.query_vm_records_champions_cards(path, years_filter, code)
        col_m, col_f = st.columns(2)
        male = ch.get("males") or {}
        female = ch.get("females") or {}
        with col_m:
            st.markdown(f'<p style="color:{VM_MUTED};font-size:0.88rem;margin:0 0 6px 0;">Мужские рекорды</p>', unsafe_allow_html=True)
            nm = html.escape(str(male.get("participant") or "").strip())
            ct = html.escape(str(male.get("city") or "").strip())
            rc = male.get("records")
            try:
                nrec = int(rc) if rc is not None else 0
            except (TypeError, ValueError):
                nrec = 0
            if nm and nrec > 0:
                st.markdown(
                    f"<p style='margin:0 0 4px 0;'><strong>{nm}</strong></p>"
                    f"<p style='margin:0 0 2px 0;color:{VM_MUTED};font-size:0.92rem;'>{ct}</p>"
                    f"<p style='margin:0;'>Действующих рекордов: <strong>{nrec}</strong></p>",
                    unsafe_allow_html=True,
                )
            else:
                st.caption("Нет данных для расчёта.")
        with col_f:
            st.markdown(f'<p style="color:{VM_MUTED};font-size:0.88rem;margin:0 0 6px 0;">Женские рекорды</p>', unsafe_allow_html=True)
            nm = html.escape(str(female.get("participant") or "").strip())
            ct = html.escape(str(female.get("city") or "").strip())
            rc = female.get("records")
            try:
                nrec_f = int(rc) if rc is not None else 0
            except (TypeError, ValueError):
                nrec_f = 0
            if nm and nrec_f > 0:
                st.markdown(
                    f"<p style='margin:0 0 4px 0;'><strong>{nm}</strong></p>"
                    f"<p style='margin:0 0 2px 0;color:{VM_MUTED};font-size:0.92rem;'>{ct}</p>"
                    f"<p style='margin:0;'>Действующих рекордов: <strong>{nrec_f}</strong></p>",
                    unsafe_allow_html=True,
                )
            else:
                st.caption("Нет данных для расчёта.")

    st.subheader("Рекорды ВМ")
    rec_rows = mq.query_event_section_records_hierarchy(
        path, years_filter, sports_filter, top_n=5
    )
    if not rec_rows:
        st.caption("Нет данных для построения рекордов по выбранным фильтрам.")
    else:
        st.caption("Иерархия: **событие → дистанция → топ-5 мужчин и топ-5 женщин (по всем годам)**.")
        frag = _event_records_hierarchy_html(rec_rows)
        if hasattr(st, "html"):
            st.html(frag)
        else:
            st.markdown(frag, unsafe_allow_html=True)


_MONTH_NAMES_RU: tuple[str, ...] = (
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
)


def sport_calendar_icon(code: str) -> str:
    c = str(code or "").strip().lower()
    if c in {"run", "trail_run"}:
        return "🏃"
    if c == "bike":
        return "🚴"
    if c == "ski":
        return "🎿"
    return "📍"


def _truncate_cal_title(title: str, max_len: int = 48) -> str:
    t = (title or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max(0, max_len - 1)] + "…"


def _shift_calendar_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """Сдвигает (год, месяц) на delta месяцев; месяцы 1–12."""
    m = int(month) + int(delta)
    y = int(year)
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return y, m


def upcoming_public_event_url(page_url: str | None) -> str | None:
    """https://vologdamarafon.ru/ плюс относительный путь из колонки page_url."""
    base = "https://vologdamarafon.ru/"
    p = str(page_url or "").strip()
    if not p:
        return None
    low = p.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return p
    path = p.lstrip("/")
    return urljoin(base, path)


def _calendar_event_line_html(ev: dict[str, Any]) -> str:
    icon = sport_calendar_icon(str(ev.get("sport") or ""))
    ttl = html.escape(_truncate_cal_title(str(ev.get("title") or "")))
    dest = upcoming_public_event_url(ev.get("page_url"))
    if dest:
        ttl_block = (
            f'<a class="vm-upcoming-ev-link" href="{html.escape(dest, quote=True)}" '
            f'target="_blank" rel="noopener noreferrer">{ttl}</a>'
        )
    else:
        ttl_block = f'<span class="vm-upcoming-ev-text">{ttl}</span>'
    return f'<span class="vm-upcoming-ev">{icon} {ttl_block}</span>'


def upcoming_calendar_table_html(
    year: int,
    month: int,
    by_day_past: dict[int, list[dict[str, Any]]],
    by_day_future: dict[int, list[dict[str, Any]]],
) -> str:
    """HTML-таблица календаря (неделя с понедельника): прошедшие (голубой) и будущие (зелёный)."""
    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    calendar.setfirstweekday(calendar.MONDAY)
    weeks = calendar.monthcalendar(int(year), int(month))
    th = "".join(f'<th scope="col">{html.escape(w)}</th>' for w in wd)
    tbody_rows: list[str] = []
    for wk in weeks:
        cells: list[str] = []
        for d in wk:
            if d == 0:
                cells.append(f'<td class="vm-cal-pad"></td>')
                continue
            di = int(d)
            lines_past = [_calendar_event_line_html(ev) for ev in by_day_past.get(di, [])]
            lines_future = [_calendar_event_line_html(ev) for ev in by_day_future.get(di, [])]
            inner = "".join(lines_past + lines_future)
            has_p = bool(lines_past)
            has_u = bool(lines_future)
            if has_p and has_u:
                td_cls = "vm-cal-mixed"
            elif has_p:
                td_cls = "vm-cal-has-past"
            elif has_u:
                td_cls = "vm-cal-has-events"
            else:
                td_cls = ""
            td_open = f'<td class="{td_cls}">' if td_cls else "<td>"
            cells.append(f'{td_open}<div class="cal-day-num">{di}</div>{inner}</td>')
        tbody_rows.append("<tr>" + "".join(cells) + "</tr>")
    border = "#546e7a"
    head_bg = "#37474f"
    return f"""
<style>
.vm-upcoming-cal-wrap {{ width:100%; max-width:100%; box-sizing:border-box; }}
.vm-upcoming-cal {{ width:100%; border-collapse:collapse; table-layout:fixed; font-size:13px; color:#263238; }}
.vm-upcoming-cal th {{ padding:11px 6px; border:1px solid {border}; background:{head_bg}; color:#eceff1; font-weight:600; }}
.vm-upcoming-cal td {{ border:1px solid #90a4ae; vertical-align:top; padding:12px; min-height:158px; word-wrap:break-word; background:#f5f5f5; }}
.vm-upcoming-cal td.vm-cal-pad {{ background:#eceff1; border-color:#b0bec5; }}
.vm-upcoming-cal td.vm-cal-has-past {{ background:#e3f2fd; border-color:#42a5f5; }}
.vm-upcoming-cal td.vm-cal-has-events {{ background:#dcedc8; border-color:#7cb342; }}
.vm-upcoming-cal td.vm-cal-mixed {{ background:linear-gradient(180deg,#e3f2fd 0%,#e3f2fd 48%, #dcedc8 52%, #dcedc8 100%); border-color:#558ed5; }}
.vm-upcoming-cal .cal-day-num {{ font-weight:700; color:#263238; margin-bottom:6px; font-size:13px; }}
.vm-upcoming-ev {{ display:block; margin-top:6px; font-size:12px; line-height:1.4; }}
.vm-upcoming-ev-text {{ color:#37474f; }}
.vm-upcoming-ev-link {{ color:#1565c0; font-weight:500; text-decoration:none; border-bottom:1px solid rgba(21,101,192,0.35); }}
.vm-upcoming-ev-link:hover {{ color:#0d47a1; border-bottom-color:#0d47a1; }}
</style>
<div class="vm-upcoming-cal-wrap">
<table class="vm-upcoming-cal" role="grid" aria-label="Календарь событий">
<thead><tr>{th}</tr></thead>
<tbody>{"".join(tbody_rows)}</tbody>
</table>
</div>
""".strip()


def page_upcoming_events() -> None:
    _section_anchor("upcoming-cal")
    st.header("Календарь событий")
    path = db_path()
    if not require_db(path):
        return
    today = datetime.date.today()
    ss_y = "vm_upcoming_cal_y"
    ss_m = "vm_upcoming_cal_m"
    pill_key = "upcoming_month_pills"
    if ss_y not in st.session_state:
        st.session_state[ss_y] = today.year
        st.session_state[ss_m] = today.month

    cy = int(st.session_state[ss_y])
    cm = max(1, min(12, int(st.session_state[ss_m])))
    counts = mq.query_competition_calendar_month_counts_year(path, cy)
    month_labels = [f"{_MONTH_NAMES_RU[i]} - {counts[i]}" for i in range(12)]

    if pill_key in st.session_state and st.session_state[pill_key] not in month_labels:
        st.session_state[pill_key] = month_labels[cm - 1]

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Месяц</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f'Год календаря: <strong>{cy}</strong>.</p>',
        unsafe_allow_html=True,
    )

    col_l, col_mid, col_r = st.columns([0.7, 20, 0.7])
    with col_l:
        if st.button(
            "<",
            key="vm_up_cal_prev_month",
            help="Предыдущий месяц",
            use_container_width=True,
        ):
            ny, nm = _shift_calendar_month(cy, cm, -1)
            nc = mq.query_competition_calendar_month_counts_year(path, ny)
            st.session_state[ss_y] = ny
            st.session_state[ss_m] = nm
            st.session_state[pill_key] = f"{_MONTH_NAMES_RU[nm - 1]} - {nc[nm - 1]}"
            st.rerun()

    with col_r:
        if st.button(
            ">",
            key="vm_up_cal_next_month",
            help="Следующий месяц",
            use_container_width=True,
        ):
            ny, nm = _shift_calendar_month(cy, cm, 1)
            nc = mq.query_competition_calendar_month_counts_year(path, ny)
            st.session_state[ss_y] = ny
            st.session_state[ss_m] = nm
            st.session_state[pill_key] = f"{_MONTH_NAMES_RU[nm - 1]} - {nc[nm - 1]}"
            st.rerun()

    with col_mid:
        picked_month = st.pills(
            "Месяц",
            options=month_labels,
            selection_mode="single",
            default=month_labels[cm - 1],
            key=pill_key,
            label_visibility="collapsed",
        )

    if picked_month is not None:
        try:
            m_sel = month_labels.index(str(picked_month)) + 1
        except ValueError:
            m_sel = cm
    else:
        m_sel = cm

    if m_sel != st.session_state[ss_m]:
        st.session_state[ss_m] = m_sel

    cy = int(st.session_state[ss_y])
    m_sel = max(1, min(12, int(st.session_state[ss_m])))

    past_rows, fut_rows = mq.query_competitions_calendar_month_events(
        path, cy, m_sel, today=today
    )
    by_day_past: dict[int, list[dict[str, Any]]] = {}
    by_day_future: dict[int, list[dict[str, Any]]] = {}
    for r in past_rows:
        d = int(r.get("day_of_month") or 0)
        if d <= 0:
            continue
        by_day_past.setdefault(d, []).append(r)
    for r in fut_rows:
        d = int(r.get("day_of_month") or 0)
        if d <= 0:
            continue
        by_day_future.setdefault(d, []).append(r)

    st.caption(
        "События из **competitions** с разобранной **date** в выбранном месяце: "
        f"**{cy}** · {_MONTH_NAMES_RU[m_sel - 1]}. "
        f"Сегодня ({today.isoformat()}): прошедшие — голубой фон ячейки, "
        "предстоящие и сегодня — зелёный."
    )
    if not past_rows and not fut_rows:
        st.info(
            "В этом месяце нет событий с заполненной датой в базе "
            "(проверьте поле **date** в **competitions**)."
        )
    frag = upcoming_calendar_table_html(cy, m_sel, by_day_past, by_day_future)
    st.markdown(frag, unsafe_allow_html=True)


def page_event() -> None:
    st.header("События")
    path = db_path()
    if not require_db(path):
        return
    years_all = mq.query_distinct_years(path)
    sports_all = mq.query_distinct_sports(path)

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Год</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — учитываются все годы.</p>",
        unsafe_allow_html=True,
    )
    if years_all:
        sy = st.pills(
            "Год",
            options=years_all,
            selection_mode="multi",
            default=[],
            key="event_pills_years",
            label_visibility="collapsed",
        )
        years_filter = list(sy) if sy else None
    else:
        years_filter = None

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Вид спорта</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 6px 0;">'
        f"Ничего не выбрано — учитываются все виды спорта.</p>",
        unsafe_allow_html=True,
    )
    if sports_all:
        ss = st.pills(
            "Вид спорта",
            options=sports_all,
            selection_mode="multi",
            default=[],
            key="event_pills_sports",
            label_visibility="collapsed",
        )
        sports_filter = list(ss) if ss else None
    else:
        sports_filter = None

    cards = mq.query_event_section_cards(path, years_filter, sports_filter)
    st.markdown("##### Показатели")
    c1, c2, c3, c4, c5 = st.columns(5, gap="small")
    with c1:
        metric_plaque("Всего событий", cards.get("total_events", 0))
    with c2:
        metric_plaque("Уникальных участников", cards.get("total_participants", 0))
    with c3:
        metric_plaque("Команд (уник.)", cards.get("teams_distinct", 0))
    with c4:
        metric_plaque("Регионов (уник.)", cards.get("regions_distinct", 0))
    with c5:
        metric_plaque("Стран (уник.)", cards.get("countries_distinct", 0))

    _section_anchor("event-series")
    st.subheader("Серии событий")
    series_rank = mq.query_event_series_title_short_ranking(path, years_filter, sports_filter)
    if not series_rank:
        st.caption(
            "Нет серий с повторными выпусками по **title_short** в текущих фильтрах, "
            "либо в таблице **competitions** нет колонки **title_short**."
        )
    else:
        sdf = pd.DataFrame(series_rank)[
            [
                "series_title",
                "editions",
                "years_csv",
                "sports_csv",
                "participants_sum",
                "teams_sum",
            ]
        ].rename(
            columns={
                "series_title": "Серия",
                "editions": "Проведено раз",
                "years_csv": "Года проведения",
                "sports_csv": "Вид спорта",
                "participants_sum": "Количество участников",
                "teams_sum": "Количество команд",
            }
        )
        st.dataframe(sdf, use_container_width=True, hide_index=True)

    _section_anchor("event-list")
    st.subheader("События")
    event_rows = mq.query_event_section_events_table(path, years_filter, sports_filter)
    if not event_rows:
        st.caption("Нет строк для выбранных фильтров.")
    else:
        st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)

    _section_anchor("event-detail")
    st.subheader("Детали выбранного события")
    years_for_detail = years_filter if years_filter else years_all
    if not years_for_detail:
        st.caption("Нет годов для детализации.")
        return

    comps: list[dict[str, Any]] = []
    for yy in years_for_detail:
        rows = mq.query_competitions_for_year(path, int(yy))
        if sports_filter:
            sports_set = {str(x) for x in sports_filter}
            rows = [c for c in rows if str(c.get("вид") or "") in sports_set]
        comps.extend(rows)
    if not comps:
        st.info("Нет событий для выбранных фильтров.")
        return

    # Плашки выбора: одна плашка = одно событие.
    comp_map: dict[str, int] = {}
    comp_labels: list[str] = []
    for c in comps:
        cid = int(c.get("id") or 0)
        if cid <= 0:
            continue
        title = str(c.get("событие") or "").strip() or f"id {cid}"
        year_val = str(c.get("дата") or "")[:4]
        label = f"{title} ({year_val})" if year_val else title
        # Гарантия уникальности label для pills.
        if label in comp_map:
            label = f"{label} · #{cid}"
        comp_map[label] = cid
        comp_labels.append(label)
    if not comp_labels:
        st.info("Нет событий для выбранных фильтров.")
        return

    picked_comp = st.pills(
        "Событие (детали)",
        options=comp_labels,
        selection_mode="single",
        default=comp_labels[0],
        key="ev_competition_detail_pills",
        label_visibility="collapsed",
    )
    picked_label = str(picked_comp or comp_labels[0])
    comp_id = int(comp_map[picked_label])

    st.markdown("##### Сводка")
    st.dataframe(
        pd.DataFrame(mq.query_competition_header(path, comp_id)),
        use_container_width=True,
        hide_index=True,
    )
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("##### Дистанции")
        st.dataframe(
            pd.DataFrame(mq.query_competition_distances(path, comp_id)),
            use_container_width=True,
            hide_index=True,
        )
    with d2:
        st.markdown("##### Группы")
        st.dataframe(
            pd.DataFrame(mq.query_competition_groups(path, comp_id)),
            use_container_width=True,
            hide_index=True,
        )
    st.markdown("##### Топ-10 зачёта (фрагмент)")
    st.dataframe(
        pd.DataFrame(mq.query_competition_top10(path, comp_id)),
        use_container_width=True,
        hide_index=True,
    )


def _stat_int(val: object) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def _is_admin_user() -> bool:
    """Админ-режим включается только локально через env VMSTAT_ADMIN_UI=1."""
    return str(os.environ.get("VMSTAT_ADMIN_UI", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _resolve_admin_route_slug() -> str | None:
    """
    Секретный сегмент URL: ?page=<slug> открывает панель администратора без пункта в меню.
    Задаётся VMSTAT_ADMIN_ROUTE или [admin] route_slug в .streamlit/secrets.toml.
    """
    env_sl = str(os.environ.get("VMSTAT_ADMIN_ROUTE", "")).strip()
    if env_sl:
        return env_sl
    try:
        adm: Any = st.secrets.get("admin", {}) if hasattr(st, "secrets") else {}
        if isinstance(adm, dict):
            rs = str(adm.get("route_slug") or "").strip()
            if rs:
                return rs
    except Exception:
        return None
    return None


def _chart_style_experiment_default() -> bool:
    raw_env = str(os.environ.get("VMSTAT_CHART_STYLE_EXPERIMENT", "")).strip().lower()
    if raw_env in {"1", "true", "yes", "on"}:
        return True
    if raw_env in {"0", "false", "no", "off"}:
        return False
    try:
        adm: Any = st.secrets.get("admin", {}) if hasattr(st, "secrets") else {}
        if isinstance(adm, dict):
            raw = str(adm.get("chart_style_experiment", "")).strip().lower()
            if raw in {"1", "true", "yes", "on"}:
                return True
            if raw in {"0", "false", "no", "off"}:
                return False
    except Exception:
        pass
    return False


def _is_chart_style_experiment_enabled() -> bool:
    if CHART_STYLE_EXPERIMENT_KEY not in st.session_state:
        st.session_state[CHART_STYLE_EXPERIMENT_KEY] = _chart_style_experiment_default()
    return bool(st.session_state.get(CHART_STYLE_EXPERIMENT_KEY, False))


def _apply_plotly_style_flag() -> None:
    if _is_chart_style_experiment_enabled():
        pio.templates["vm_experiment"] = go.layout.Template(
            layout=go.Layout(
                font=dict(family="Inter, Segoe UI, sans-serif", color=VM_TEXT, size=13),
                colorway=[VM_BLUE, "#1f4e79", "#69a7d6", "#4c78a8", "#7f8ea3"],
                paper_bgcolor="#ffffff",
                plot_bgcolor="#ffffff",
                hoverlabel=dict(
                    bgcolor="#ffffff",
                    bordercolor="#d7e3ee",
                    font=dict(color=VM_TEXT, size=12),
                ),
                xaxis=dict(showgrid=True, gridcolor="#edf2f7", zeroline=False),
                yaxis=dict(showgrid=True, gridcolor="#edf2f7", zeroline=False),
            )
        )
        pio.templates.default = "vm_experiment"
    else:
        pio.templates.default = "plotly_white"


def _cup_detail_age_group_options(rows: list[dict]) -> list[str]:
    labels = {mq.parse_profile_cup_raw_age_group_label(r.get("raw")) for r in rows}
    return sorted(x for x in labels if x)


def _filter_cup_detail_rows(
    rows: list[dict],
    surname_needle: str,
    gender_mode: str,
    age_group: str,
) -> list[dict]:
    """Фильтр таблицы результатов кубка: фамилия (подстрока), пол, метка группы из raw."""
    needle = surname_needle.strip().casefold()
    out: list[dict] = []
    for r in rows:
        ln = (r.get("last_name") or "").strip().casefold()
        if needle and needle not in ln:
            continue
        g = (r.get("gender") or "").strip().lower()
        if gender_mode == "Мужчины" and g != "m":
            continue
        if gender_mode == "Женщины" and g != "f":
            continue
        # «Абсолютный зачёт» — без отбора по полу профиля
        lbl = mq.parse_profile_cup_raw_age_group_label(r.get("raw"))
        if age_group != "Все" and lbl != age_group:
            continue
        out.append(r)
    return out


def _cup_team_aggregate_points(rows: list[dict]) -> list[tuple[str, int, list[dict]]]:
    """Группировка по команде; сумма в зачёт = сумма очков пяти лучших участников (из profile_cup_results)."""
    from collections import defaultdict

    by_team: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        t = (r.get("команда") or "").strip()
        if not t:
            continue
        by_team[t].append(r)
    out: list[tuple[str, int, list[dict]]] = []
    for team, members in by_team.items():
        total = mq.aggregate_team_cup_points_top_five(members)
        out.append((team, total, members))
    out.sort(key=lambda x: -x[1])
    return out


def _team_participants_ranked_with_rows(
    members: list[dict],
) -> list[tuple[int, str, int, list[dict]]]:
    """Участники команды: (profile_id, имя, сумма очков int, строки profile_cup для кубка), по убыванию суммы."""
    from collections import defaultdict

    by_pid: dict[int, list[dict]] = defaultdict(list)
    for r in members:
        by_pid[int(r["profile_id"])].append(r)
    out: list[tuple[int, str, int, list[dict]]] = []
    for pid, rows in by_pid.items():
        name = (rows[0].get("участник") or "").strip() or f"id {pid}"
        tot = 0
        for x in rows:
            try:
                tot += int(round(float(x.get("очков") or 0)))
            except (TypeError, ValueError):
                pass
        out.append((pid, name, tot, rows))
    out.sort(key=lambda x: -x[2])
    return out


def _esc_html(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _cup_row_points_int(ov: Any) -> int | None:
    try:
        return int(round(float(ov))) if ov is not None else None
    except (TypeError, ValueError):
        return None


def _cup_resolve_team_event_points(
    line: dict[str, Any],
    pts_by_cid: dict[int, float],
    pts_by_td: dict[tuple[str, str], float],
) -> int | None:
    """
    Очки строки: только из **profile_cup_results** — по competition_id в raw / карте,
    иначе по паре (нормализованное название из raw, дистанция). Без подстановки одной
    суммы на все финиши (SQL distance-fallback отключён).
    """
    cid = line.get("competition_id")
    if cid is not None:
        try:
            ic = int(cid)
            if ic in pts_by_cid:
                return int(round(pts_by_cid[ic]))
        except (TypeError, ValueError):
            pass
    cid2 = mq.parse_profile_cup_raw_competition_id(line.get("raw"))
    if cid2 is not None and cid2 in pts_by_cid:
        return int(round(pts_by_cid[cid2]))
    ev = (line.get("событие") or "").strip()
    dst = (line.get("дистанция") or "").strip()
    t = mq.norm_cup_match_title(ev)
    d = mq.norm_cup_match_distance(dst)
    if (t, d) in pts_by_td:
        return int(round(pts_by_td[(t, d)]))
    return None


def _cup_team_hierarchy_html(
    teams: list[tuple[str, int, list[dict]]],
    db_path: Path,
    cup_id: int,
    year: int,
) -> str:
    """Иерархия «команда → участник → все финиши в соревнованиях кубка» (по строке на results)."""
    leader_pts = int(teams[0][1]) if teams else 0
    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>Место</span><span>Команда</span><span>Очки</span><span>Отставание</span></div>
"""
    ]
    for rank, (team, total_pts, members) in enumerate(teams, start=1):
        ranked = _team_participants_ranked_with_rows(members)
        team_pool = 0
        for _x, _y, ptot_sum, _z in ranked:
            try:
                team_pool += int(ptot_sum)
            except (TypeError, ValueError):
                pass
        gap_v = max(0, leader_pts - int(total_pts))
        gap_html = _esc_html(gap_v)
        team_e = _esc_html(team)
        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>'
            f"{_esc_html(rank)}</span>"
            f"<span>{team_e}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(total_pts)}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{gap_html}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        for pos, (_pid, pname, ptot, rows_p) in enumerate(ranked, start=1):
            in_top = pos <= 5
            badge = (
                '<span class="vm-cup-badge">в зачёт</span>'
                if in_top
                else '<span class="vm-cup-badge vm-cup-badge-muted">вне топ‑5</span>'
            )
            share_pct = 0
            if team_pool > 0:
                try:
                    share_pct = int(round(100 * int(ptot) / float(team_pool)))
                except (TypeError, ValueError, ZeroDivisionError):
                    share_pct = 0
            share_html = (
                f'<span class="vm-cup-share">{_esc_html(share_pct)}%</span>'
                if team_pool > 0
                else '<span class="vm-cup-share">—</span>'
            )
            pts_by_cid = mq.map_profile_cup_points_by_competition_id(
                db_path, _pid, cup_id, year
            )
            pts_by_td = mq.map_profile_cup_points_by_title_distance(
                db_path, _pid, cup_id, year
            )
            parts.append(
                "<details class='vm-cup-member'><summary>"
                f'<span class="vm-cup-name-cell"><span class="vm-cup-caret-m" aria-hidden="true"></span>'
                f"{_esc_html(pname)}</span>"
                f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(ptot)}</span>'
                f"{share_html}"
                f"{badge}</summary>"
            )
            parts.append("<table class='vm-cup-ev'><thead><tr>")
            for h in (
                "Событие",
                "Дистанция",
                "Место абсолют",
                "Очки",
                "Время",
            ):
                parts.append(f"<th>{_esc_html(h)}</th>")
            parts.append("</tr></thead><tbody>")
            lines = mq.query_profile_cup_team_member_competition_rows(
                db_path, _pid, cup_id, year
            )
            if not lines:
                lines = mq.query_profile_cup_results_lines_for_member(
                    db_path, _pid, cup_id, year
                )
            if not lines:
                for r in rows_p:
                    lines.append(
                        {
                            "событие": mq.parse_profile_cup_raw_event_title(r.get("raw")),
                            "дистанция": (r.get("дистанция") or "").strip(),
                            "место_абс": r.get("место_абс"),
                            "очков": r.get("очков"),
                            "время": "",
                            "raw": r.get("raw"),
                            "competition_id": mq.parse_profile_cup_raw_competition_id(
                                r.get("raw")
                            ),
                        }
                    )
            assign_pts = (
                mq.assign_profile_cup_points_to_result_lines(
                    db_path, _pid, cup_id, year, lines
                )
                if lines and all("competition_id" in ln for ln in lines)
                else None
            )
            for i, line in enumerate(lines):
                el = dict(line)
                if el.get("competition_id") is None:
                    cix = mq.parse_profile_cup_raw_competition_id(el.get("raw"))
                    if cix is not None:
                        el["competition_id"] = cix
                ev = (el.get("событие") or "").strip()
                if not ev:
                    ev = mq.parse_profile_cup_raw_event_title(el.get("raw"))
                if not (ev and str(ev).strip()):
                    ev = "—"
                tm = (el.get("время") or "").strip()
                if not tm:
                    tm = mq.parse_profile_cup_raw_finish_time(el.get("raw"))
                if not (tm and str(tm).strip()):
                    tm = "—"
                pts = _cup_row_points_int(el.get("очков"))
                if (
                    pts is None
                    and assign_pts is not None
                    and i < len(assign_pts)
                    and assign_pts[i] is not None
                ):
                    pts = assign_pts[i]
                if pts is None:
                    pts = _cup_resolve_team_event_points(el, pts_by_cid, pts_by_td)
                pts_s = str(pts) if pts is not None else "—"
                place = el.get("место_абс")
                place_s = str(place) if place is not None and str(place).strip() else "—"
                dist = ((el.get("дистанция") or "").strip() or "—")
                parts.append(
                    "<tr>"
                    f"<td>{_esc_html(ev)}</td>"
                    f"<td>{_esc_html(dist)}</td>"
                    f"<td>{_esc_html(place_s)}</td>"
                    f"<td>{_esc_html(pts_s)}</td>"
                    f"<td>{_esc_html(tm)}</td>"
                    "</tr>"
                )
            parts.append("</tbody></table></details>")
        parts.append("</div></details>")
    parts.append("</div>")
    return "".join(parts)


def _event_records_hierarchy_html(rows: list[dict[str, Any]]) -> str:
    """Иерархия для раздела «Рекорды события»: событие -> дистанция -> топ-5 м/ж."""
    from collections import defaultdict

    by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        event_name = str(r.get("Событие") or "—").strip() or "—"
        by_event[event_name].append(r)

    events_sorted = sorted(by_event.items(), key=lambda x: x[0].casefold())
    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>#</span><span>Событие</span><span>Дистанций</span><span>Результатов</span></div>
"""
    ]
    for event_rank, (event_name, event_rows) in enumerate(events_sorted, start=1):
        by_dist: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in event_rows:
            dist_name = str(r.get("Дистанция") or "—").strip() or "—"
            by_dist[dist_name].append(r)
        dist_items = sorted(by_dist.items(), key=lambda x: x[0].casefold())

        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>{_esc_html(event_rank)}</span>'
            f"<span>{_esc_html(event_name)}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(len(dist_items))}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(len(event_rows))}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        for dist_name, dist_rows in dist_items:
            parts.append(
                "<details class='vm-cup-member'><summary>"
                f'<span class="vm-cup-name-cell"><span class="vm-cup-caret-m" aria-hidden="true"></span>{_esc_html(dist_name)}</span>'
                f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(len(dist_rows))}</span>'
                "<span></span>"
                '<span class="vm-cup-badge">Топ 5 M/F</span>'
                "</summary>"
            )
            parts.append("<table class='vm-cup-ev'><thead><tr>")
            for h in ("Пол", "Место", "Год", "Этап", "Участник", "Время"):
                parts.append(f"<th>{_esc_html(h)}</th>")
            parts.append("</tr></thead><tbody>")
            for row in dist_rows:
                place_raw = row.get("Место")
                place_s = str(place_raw) if place_raw is not None else "—"
                if place_s == "1":
                    place_s = "👑 1"
                parts.append(
                    "<tr>"
                    f"<td>{_esc_html(row.get('Пол'))}</td>"
                    f"<td>{_esc_html(place_s)}</td>"
                    f"<td>{_esc_html(row.get('Год'))}</td>"
                    f"<td>{_esc_html(row.get('Этап'))}</td>"
                    f"<td>{_esc_html(row.get('Участник'))}</td>"
                    f"<td>{_esc_html(row.get('Время'))}</td>"
                    "</tr>"
                )
            parts.append("</tbody></table></details>")
        parts.append("</div></details>")
    parts.append("</div>")
    return "".join(parts)


def _team_scoring_hierarchy_html(
    teams_rows: list[dict[str, Any]],
    members_rows: list[dict[str, Any]],
    stage_rows: list[dict[str, Any]],
) -> str:
    """Иерархия командного зачёта: команда -> участник -> этапы (расчётные очки)."""
    from collections import defaultdict

    by_team_members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in members_rows:
        team = str(r.get("команда") or "").strip()
        if not team:
            continue
        by_team_members[team].append(r)

    by_team_member_stages: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for r in stage_rows:
        team = str(r.get("команда") or "").strip()
        pid = r.get("profile_id")
        try:
            pid_i = int(pid)
        except (TypeError, ValueError):
            continue
        by_team_member_stages[(team, pid_i)].append(r)

    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>Место</span><span>Команда</span><span>Очки</span><span>Участников</span></div>
"""
    ]
    for rank, team_row in enumerate(teams_rows, start=1):
        team = str(team_row.get("команда") or "").strip()
        team_pts = int(team_row.get("очков") or 0)
        mems = by_team_members.get(team, [])
        mems_sorted = sorted(mems, key=lambda x: int(x.get("очков_7из8") or 0), reverse=True)
        team_points_all_members = int(
            sum(int(x.get("очков_7из8") or 0) for x in mems_sorted)
        )
        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>{_esc_html(rank)}</span>'
            f"<span>{_esc_html(team)}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(team_pts)}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(len(mems_sorted))}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        for i, m in enumerate(mems_sorted, start=1):
            pid = int(m.get("profile_id") or 0)
            name = str(m.get("участник") or f"id {pid}")
            pts_best7 = int(m.get("очков_7из8") or 0)
            share = (
                round((100.0 * pts_best7 / team_points_all_members), 1)
                if team_points_all_members > 0
                else 0.0
            )
            in_counted = i <= 5
            badge = (
                '<span class="vm-cup-badge">в зачёт</span>'
                if in_counted
                else '<span class="vm-cup-badge vm-cup-badge-muted">вне топ‑5</span>'
            )
            row_style = "background:#eaf7ea;" if in_counted else ""
            parts.append(
                f"<details class='vm-cup-member' style='{row_style}'><summary>"
                f'<span class="vm-cup-name-cell"><span class="vm-cup-caret-m" aria-hidden="true"></span>{_esc_html(name)}</span>'
                f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(pts_best7)}</span>'
                f'<span class="vm-cup-share">{_esc_html(share)}%</span>'
                f"{badge}</summary>"
            )
            stg = by_team_member_stages.get((team, pid), [])
            stg_sorted = sorted(stg, key=lambda x: int(x.get("этап") or 0))
            parts.append("<table class='vm-cup-ev'><thead><tr>")
            for h in ("Этап", "Событие", "Место, абсолют", "Очки (база)", "Бонус", "Очки (зачёт)", "Комментарий"):
                parts.append(f"<th>{_esc_html(h)}</th>")
            parts.append("</tr></thead><tbody>")
            for s in stg_sorted:
                parts.append(
                    "<tr>"
                    f"<td>{_esc_html(s.get('этап'))}</td>"
                    f"<td>{_esc_html(s.get('событие'))}</td>"
                    f"<td>{_esc_html(s.get('место_абс'))}</td>"
                    f"<td>{_esc_html(s.get('очков_база'))}</td>"
                    f"<td>{_esc_html(s.get('очков_бонус'))}</td>"
                    f"<td>{_esc_html(s.get('очков_в_командный'))}</td>"
                    f"<td>{_esc_html(s.get('комментарий'))}</td>"
                    "</tr>"
                )
            parts.append("</tbody></table></details>")
        parts.append("</div></details>")
    parts.append("</div>")
    return "".join(parts)


def page_participant() -> None:
    _section_anchor("participant-search")
    st.header("Участники")
    path = db_path()
    if not require_db(path):
        return

    st.caption("Быстрый выбор: начните вводить фамилию/имя/id — подсказки появятся в этом же поле.")
    ac_rows = mq.query_profile_autocomplete_options(path, limit=20000)
    ac_labels: list[str] = []
    ac_map: dict[str, int] = {}
    for r in ac_rows:
        rid = int(r.get("id") or 0)
        if rid <= 0:
            continue
        label = (
            f"{(r.get('last_name') or '').strip()} {(r.get('first_name') or '').strip()} · "
            f"{(r.get('city') or '').strip() or '—'} · id {rid}"
        )
        ac_labels.append(label)
        ac_map[label] = rid
    picked_label = st.selectbox(
        "ФИО или id",
        options=ac_labels,
        index=None,
        placeholder="Начните вводить фамилию, имя или id...",
        key="part_single_autocomplete",
    )
    picked_pid: int | None = ac_map.get(str(picked_label)) if picked_label else None

    if "participant_recent_ids" not in st.session_state:
        st.session_state["participant_recent_ids"] = []
    recent_ids_raw = st.session_state.get("participant_recent_ids", [])
    recent_ids = [int(x) for x in recent_ids_raw if isinstance(x, (int, float, str)) and str(x).strip().isdigit()]
    recent_ids = list(dict.fromkeys(recent_ids))[:10]
    recent_pick: int | None = None
    if recent_ids:
        with st.expander("Недавние участники", expanded=False):
            recent_rows: list[dict] = []
            for rid in recent_ids[:10]:
                pr = mq.query_profile_row(path, int(rid))
                if not pr:
                    continue
                fn = (pr.get("first_name") or "").strip()
                ln = (pr.get("last_name") or "").strip()
                recent_rows.append(
                    {
                        "id": int(rid),
                        "Участник": f"{ln} {fn}".strip() or f"id {rid}",
                        "Город": (pr.get("city") or "").strip(),
                    }
                )
            if recent_rows:
                opt_ids = [int(r["id"]) for r in recent_rows]
                recent_pick = st.radio(
                    "Открыть из недавних",
                    options=opt_ids,
                    format_func=lambda i: next(
                        (f"{x['Участник']} · id {i}" for x in recent_rows if int(x["id"]) == int(i)),
                        f"id {i}",
                    ),
                    index=0,
                    key="participant_recent_pick",
                )
            else:
                st.caption("Список недавних пока пуст.")

    active_pid: int | None = picked_pid if picked_pid is not None else recent_pick
    if active_pid is None:
        active_pid = _participant_id_from_url()

    if active_pid is None:
        st.caption("Введите запрос в поле выше или выберите участника из недавних.")
        return

    hist = [int(active_pid)] + [int(x) for x in recent_ids if int(x) != int(active_pid)]
    st.session_state["participant_recent_ids"] = hist[:10]

    show_participant_dashboard(path, active_pid)


def show_participant_dashboard(path: Path, pid: int) -> None:
    p = mq.query_profile_row(path, pid)
    if not p:
        st.warning(f"Профиль #{pid} не найден.")
        return

    title = f"{(p.get('last_name') or '').strip()} {(p.get('first_name') or '').strip()}".strip() or "Участник"
    st.subheader(f"{title} · id {pid}")
    profile_url = f"https://vologdamarafon.ru/profile/{int(pid)}/"
    st.markdown(
        f'<p style="margin:0 0 10px 0;">'
        f'<a href="{html.escape(profile_url)}" target="_blank" rel="noopener">Профиль на vologdamarafon.ru</a>'
        f"</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.95rem;margin:0 0 12px 0;">'
        f"Пол: <b>{html.escape(str(p.get('gender') or '—'))}</b> · "
        f"Возраст: <b>{html.escape(str(p.get('age') if p.get('age') is not None else '—'))}</b> · "
        f"Город: <b>{html.escape(str(p.get('city') or '—'))}</b> · "
        f"Клуб: <b>{html.escape(str(p.get('club') or '—'))}</b></p>",
        unsafe_allow_html=True,
    )

    raw_years = mq.parse_profile_active_years(p.get("raw"))
    ay_text = ", ".join(str(y) for y in raw_years) if raw_years else "—"
    db_years = mq.query_profile_participation_years(path, pid)
    year_opts = sorted(set(db_years) | set(raw_years), reverse=True)
    cy = datetime.date.today().year
    if not year_opts:
        year_opts = [cy]
    default_y = cy if cy in year_opts else year_opts[0]

    sk = f"participant_year_filter_{pid}"
    current_years = st.session_state.get(sk)
    if not isinstance(current_years, list):
        current_years = [default_y]
    current_years = [int(y) for y in current_years if int(y) in year_opts]
    st.session_state[sk] = current_years

    picked_years = st.multiselect(
        "Год",
        options=year_opts,
        default=st.session_state[sk],
        key=f"{sk}_multiselect",
        help="Можно выбрать несколько лет (Ctrl) или оставить пусто — тогда используются все годы.",
    )
    years_for_query: list[int] | None = [int(y) for y in picked_years] if picked_years else None

    # Общий фильтр по видам спорта в разделе «Участник» (исключая service).
    all_events_for_sports = mq.query_profile_events_table(path, pid, years=None, include_dnf=True, limit=5000)
    sport_opts = sorted(
        {
            str(r.get("вид") or "").strip()
            for r in all_events_for_sports
            if str(r.get("вид") or "").strip()
            and str(r.get("вид") or "").strip().casefold() != "service"
        }
    )
    sport_pill_opts = ["Все"] + sport_opts
    sport_key = f"participant_sport_filter_{pid}"
    if sport_key not in st.session_state or st.session_state[sport_key] not in sport_pill_opts:
        st.session_state[sport_key] = "Все"
    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:14px 0 6px 0;">Вид спорта</p>',
        unsafe_allow_html=True,
    )
    st.pills(
        "Вид спорта",
        options=sport_pill_opts,
        selection_mode="single",
        key=sport_key,
        label_visibility="collapsed",
    )
    sport_pick = str(st.session_state.get(sport_key, "Все"))
    sports_for_query: list[str] | None = sport_opts if sport_pick == "Все" else [sport_pick]

    def _filter_participant_df_by_sport(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "вид" not in df.columns:
            return df
        ds = df.copy()
        ds["вид"] = ds["вид"].fillna("").astype(str).str.strip()
        ds = ds[ds["вид"].str.casefold() != "service"]
        if sports_for_query:
            ds = ds[ds["вид"].isin(sports_for_query)]
        return ds

    kpi_rows = mq.query_profile_events_table(
        path,
        pid,
        years=years_for_query,
        sports=sports_for_query,
        include_dnf=True,
        limit=5000,
    )
    kpi_df = _filter_participant_df_by_sport(pd.DataFrame(kpi_rows))
    active_years_count = (
        int(kpi_df["год"].nunique()) if (not kpi_df.empty and "год" in kpi_df.columns) else 0
    )
    starts_total = int(len(kpi_df))
    finishes_total = (
        int((kpi_df.get("статус", pd.Series(dtype="object")) == "finish").sum()) if not kpi_df.empty else 0
    )
    dnf_total = (
        int((kpi_df.get("статус", pd.Series(dtype="object")) == "dnf").sum()) if not kpi_df.empty else 0
    )
    events_distinct = int(kpi_df["событие"].nunique()) if (not kpi_df.empty and "событие" in kpi_df.columns) else 0
    distances_distinct = (
        int(kpi_df.loc[kpi_df["статус"] == "finish", "дистанция"].replace("", pd.NA).dropna().nunique())
        if (not kpi_df.empty and {"статус", "дистанция"}.issubset(kpi_df.columns))
        else 0
    )
    km_total = (
        float(pd.to_numeric(kpi_df.loc[kpi_df["статус"] == "finish", "км"], errors="coerce").fillna(0).sum())
        if (not kpi_df.empty and {"статус", "км"}.issubset(kpi_df.columns))
        else 0.0
    )
    stat_first_profile = (
        int((pd.to_numeric(kpi_df.get("место_абс"), errors="coerce") == 1).sum()) if not kpi_df.empty else 0
    )
    stat_second_profile = (
        int((pd.to_numeric(kpi_df.get("место_абс"), errors="coerce") == 2).sum()) if not kpi_df.empty else 0
    )
    stat_third_profile = (
        int((pd.to_numeric(kpi_df.get("место_абс"), errors="coerce") == 3).sum()) if not kpi_df.empty else 0
    )
    _section_anchor("participant-kpi")
    st.markdown("##### KPI участника")
    k1, k2, k3, k4, k5 = st.columns(5, gap="small")
    with k1:
        metric_plaque("Старты", starts_total)
    with k2:
        metric_plaque("Финиши", finishes_total)
    with k3:
        metric_plaque("Активных лет", active_years_count)
    with k4:
        metric_plaque("Км", int(round(km_total)))
    with k5:
        metric_plaque("DNF", dnf_total)
    k6, k7, k8, k9, k10 = st.columns(5, gap="small")
    with k6:
        metric_plaque("Событий", events_distinct)
    with k7:
        metric_plaque("Дистанций (уник.)", distances_distinct)
    with k8:
        metric_plaque("Первых мест", stat_first_profile)
    with k9:
        metric_plaque("Вторых мест", stat_second_profile)
    with k10:
        metric_plaque("Третьих мест", stat_third_profile)
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:6px 0 10px 0;">'
        f"Годы активности (raw): <b>{html.escape(ay_text)}</b></p>",
        unsafe_allow_html=True,
    )

    tr = pd.DataFrame(mq.query_profile_yearly_trends(path, pid))
    if not tr.empty:
        g1, g2 = st.columns(2)
        with g1:
            f_st = px.bar(tr, x="year", y="starts", labels={"year": "Год", "starts": "Стартов"})
            f_st.update_traces(
                marker_color=VM_ACCENT,
                texttemplate="%{y:.0f}",
                textposition="outside",
                cliponaxis=False,
            )
            f_st.update_layout(plot_bgcolor="white", paper_bgcolor="white", font=dict(color=VM_TEXT), xaxis_type="category")
            st.plotly_chart(f_st, use_container_width=True)
        with g2:
            sport_rows = mq.query_profile_events_table(
                path, pid, years=None, sports=sports_for_query, include_dnf=True, limit=5000
            )
            sport_df = pd.DataFrame(sport_rows)
            sport_df = _filter_participant_df_by_sport(sport_df)
            if sport_df.empty:
                st.caption("Нет данных по видам спорта.")
            else:
                pie_df = (
                    sport_df.assign(вид=sport_df["вид"].fillna("").astype(str).str.strip())
                    .assign(вид=lambda d: d["вид"].replace("", "не указан"))
                    .groupby("вид", as_index=False)
                    .size()
                    .rename(columns={"size": "стартов"})
                )
                fig_sport = (
                    alt.Chart(pie_df)
                    .transform_joinaggregate(total_starts="sum(стартов)")
                    .transform_calculate(pct="datum.стартов / datum.total_starts")
                    .mark_arc()
                    .encode(
                        theta=alt.Theta("стартов:Q", title="Стартов"),
                        color=alt.Color("вид:N", title="Вид спорта"),
                        tooltip=[
                            alt.Tooltip("вид:N", title="Вид спорта"),
                            alt.Tooltip("стартов:Q", title="Стартов"),
                            alt.Tooltip("pct:Q", title="Доля", format=".1%"),
                        ],
                    )
                )
                labels = fig_sport.mark_text(radius=128).encode(text=alt.Text("pct:Q", format=".1%"))
                st.caption("Количество стартов по видам спорта")
                st.altair_chart((fig_sport + labels).properties(height=280), use_container_width=True)

    _section_anchor("participant-tabs")
    tabs = ["События", "Рекорды", "Кубки", "Команды", "Серия событий"]
    if _is_admin_user():
        tabs.append("Качество данных")
    tt = st.tabs(tabs)
    tab_ev = tt[0]
    tab_rec = tt[1]
    tab_cup = tt[2]
    tab_team = tt[3]
    tab_series = tt[4]
    tab_quality = tt[5] if len(tt) > 5 else None

    with tab_ev:
        all_rows = mq.query_profile_events_table(
            path,
            pid,
            years=None,
            sports=sports_for_query,
            include_dnf=True,
        )
        all_df = pd.DataFrame(all_rows)
        all_df = _filter_participant_df_by_sport(all_df)
        if all_df.empty:
            st.caption("Нет стартов по участнику.")
            return
        dist_opts = sorted(
            {
                str(x).strip()
                for x in all_df.get("дистанция", pd.Series(dtype="object")).tolist()
                if str(x).strip()
            }
        )
        f1, f2, f3 = st.columns(3)
        with f1:
            st.caption(f"Фильтр вида спорта: **{sport_pick}**")
        with f2:
            dd = st.multiselect("Дистанция", options=dist_opts, default=[], key=f"part_dists_{pid}")
        with f3:
            include_dnf = st.checkbox("Показывать DNF", value=True, key=f"part_inc_dnf_{pid}")
        ev_df = pd.DataFrame(
            mq.query_profile_events_table(
                path,
                pid,
                years=years_for_query,
                sports=sports_for_query,
                distance_labels=dd or None,
                include_dnf=include_dnf,
            )
        )
        ev_df = _filter_participant_df_by_sport(ev_df)
        if ev_df.empty:
            st.caption("Нет стартов с выбранными фильтрами.")
        else:
            st.dataframe(ev_df, use_container_width=True, hide_index=True)

    with tab_rec:
        pb_all = pd.DataFrame(mq.query_profile_personal_bests(path, pid, year=None))
        pb_year = pb_all.copy()
        if years_for_query and "год" in pb_year.columns:
            pb_year = pb_year[pb_year["год"].isin(years_for_query)]
        pb_all = _filter_participant_df_by_sport(pb_all)
        pb_year = _filter_participant_df_by_sport(pb_year)
        rec_sport_options = ["Все"]
        if not pb_all.empty and "вид" in pb_all.columns:
            rec_sports = sorted(
                {
                    str(x).strip()
                    for x in pb_all["вид"].tolist()
                    if str(x).strip() and str(x).strip().casefold() != "service"
                }
            )
            rec_sport_options.extend(rec_sports)
        rec_sport_pick = st.selectbox(
            "Вид спорта",
            options=rec_sport_options,
            index=0,
            key=f"part_rec_sport_{pid}",
        )
        if rec_sport_pick != "Все":
            if "вид" in pb_all.columns:
                pb_all = pb_all[pb_all["вид"] == rec_sport_pick]
            if "вид" in pb_year.columns:
                pb_year = pb_year[pb_year["вид"] == rec_sport_pick]
        st.markdown("##### Личные рекорды (PB, все годы)")
        if pb_all.empty:
            st.caption("Нет корректных финишей для расчёта PB.")
        else:
            st.dataframe(pb_all, use_container_width=True, hide_index=True)
        st.markdown("##### Лучшие результаты по выбранным годам (SB)")
        if pb_year.empty:
            st.caption("Нет корректных финишей по выбранным годам.")
        else:
            st.dataframe(pb_year, use_container_width=True, hide_index=True)

    with tab_cup:
        cup_rows: list[dict[str, Any]] = []
        years_for_cups = years_for_query if years_for_query else year_opts
        for yy in years_for_cups:
            cup_rows.extend(mq.query_profile_cup_rows_for_year(path, pid, int(yy)))
        cup_df = pd.DataFrame(cup_rows)
        cup_df = _filter_participant_df_by_sport(cup_df)
        if cup_df.empty:
            st.caption(
                "Нет строк в **profile_cup_results** по выбранным годам. "
                "При необходимости: `python fill_profile_cup_results.py`."
            )
        else:
            st.dataframe(cup_df, use_container_width=True, hide_index=True)
    with tab_team:
        team_df = pd.DataFrame(mq.query_profile_team_summary(path, pid))
        team_df = _filter_participant_df_by_sport(team_df)
        if team_df.empty:
            st.caption("Нет финишей с непустой командой.")
        else:
            st.dataframe(team_df, use_container_width=True, hide_index=True)

    with tab_series:
        series_rows = mq.query_profile_event_series_rows(
            path,
            pid,
            years=years_for_query,
            sports=sports_for_query,
            include_dnf=True,
            limit=5000,
        )
        series_df = pd.DataFrame(series_rows)
        if series_df.empty:
            st.caption("Нет данных для таблицы «Серии событий» по выбранным фильтрам.")
        else:
            series_opts = sorted(
                {
                    str(x).strip()
                    for x in series_df.get("_series_short", pd.Series(dtype="object")).tolist()
                    if str(x).strip()
                }
            )
            sport_opts_series = sorted(
                {
                    str(x).strip()
                    for x in series_df.get("вид", pd.Series(dtype="object")).tolist()
                    if str(x).strip() and str(x).strip().casefold() != "service"
                }
            )
            fs1, fs2 = st.columns(2)
            with fs1:
                pick_series = st.selectbox(
                    "Серия событий",
                    options=["Все"] + series_opts,
                    index=0,
                    key=f"part_series_short_{pid}",
                )
            with fs2:
                pick_sport_series = st.selectbox(
                    "Вид спорта",
                    options=["Все"] + sport_opts_series,
                    index=0,
                    key=f"part_series_sport_{pid}",
                )
            show_df = series_df.copy()
            if pick_series != "Все":
                show_df = show_df[show_df["_series_short"] == pick_series]
            if pick_sport_series != "Все":
                show_df = show_df[show_df["вид"] == pick_sport_series]
            show_df = show_df.drop(columns=["_series_short"], errors="ignore")
            show_cols = [
                "Год",
                "Событие",
                "вид",
                "Дистанция",
                "время",
                "место_абс",
                "место_пол",
                "команда",
                "статус",
            ]
            show_cols = [c for c in show_cols if c in show_df.columns]
            st.markdown("##### Серии событий")
            st.dataframe(show_df[show_cols], use_container_width=True, hide_index=True)
    if tab_quality is not None:
        with tab_quality:
            if not _is_admin_user():
                st.warning("Недостаточно прав для просмотра диагностики качества.")
                return
            qdf = pd.DataFrame(mq.query_profile_data_quality(path, pid))
            st.dataframe(qdf, use_container_width=True, hide_index=True)


def page_team() -> None:
    st.header("Команды")
    st.caption(
        "Команда задаётся полем **team** в результатах соревнований. "
        "Страница показывает KPI, состав, события, срезы, географию, тренды и кубковые блоки."
    )
    path = db_path()
    if not require_db(path):
        return

    st.caption("Быстрый выбор: начните вводить название команды — подсказки появятся в этом же поле.")
    names = mq.query_team_names_for_select(path, None, limit=5000)
    if not names:
        st.warning("В базе нет команд с непустым полем team в финишах (dnf=0).")
        return

    team = st.selectbox(
        "Команда",
        options=names,
        index=None,
        placeholder="Начните вводить название команды...",
        key="team_pick",
    )
    if not team:
        st.caption("Выберите команду в поле выше.")
        return

    base_stats = mq.query_team_stats(path, team)
    if not base_stats:
        st.error("Нет стартов для этой команды в выбранных данных.")
        return

    year_options = sorted(set(base_stats.get("active_years_list") or []), reverse=True)
    year_filter = st.selectbox(
        "Год",
        options=["Все"] + year_options,
        index=0,
        key=f"team_page_year_{team}",
    )
    year_value = None if year_filter == "Все" else int(year_filter)

    _section_anchor("team-kpi")
    kpi = mq.query_team_kpi_extended(path, team, year=year_value) or {}
    st.subheader("KPI")
    k1, k2, k3, k4 = st.columns(4, gap="small")
    with k1:
        metric_plaque("Участников (уник.)", int(kpi.get("participants_distinct") or 0))
    with k2:
        metric_plaque("Стартов", int(kpi.get("starts_total") or 0))
    with k3:
        metric_plaque("Финишей / DNF", f"{int(kpi.get('finishes_total') or 0)} / {int(kpi.get('dnf_total') or 0)}")
    with k4:
        metric_plaque("Километраж", f"{float(kpi.get('km_total') or 0.0):.1f} км")

    years_str = ", ".join(str(y) for y in (kpi.get("active_years_list") or []))
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.9rem;margin:4px 0 16px 0;">'
        f"<b>Годы участия в событиях:</b> {years_str or '—'}</p>",
        unsafe_allow_html=True,
    )

    _section_anchor("team-tabs")
    tab_labels = [
        "Состав",
        "События",
        "Дистанции и спорт",
        "География",
        "Тренды",
        "Кубки (legacy)",
        "Кубки (расчёт team_scoring)",
        "Командное первенство",
    ]
    show_quality_tab = _is_admin_user()
    if show_quality_tab:
        tab_labels.append("Диагностика")
    tabs = st.tabs(tab_labels)
    tab_roster, tab_events, tab_sport, tab_geo, tab_trends, tab_cups_legacy, tab_cups_calc, tab_champ = tabs[:8]
    tab_quality = tabs[8] if show_quality_tab else None

    with tab_roster:
        roster = pd.DataFrame(mq.query_team_roster_stats(path, team, year=year_value))
        if roster.empty:
            st.caption("Нет данных состава для выбранного фильтра.")
        else:
            name_filter = st.text_input(
                "Поиск участника",
                key=f"team_roster_filter_{team}",
                placeholder="Фамилия или имя",
            ).strip().casefold()
            if name_filter:
                roster = roster[
                    roster["athlete"].fillna("").astype(str).str.casefold().str.contains(name_filter)
                ]
            roster = roster.rename(
                columns={
                    "athlete": "Участник",
                    "gender": "Пол",
                    "age": "Возраст",
                    "city": "Город",
                    "starts_total": "Старты",
                    "finishes_total": "Финиши",
                    "dnf_total": "DNF",
                    "km_total": "Км",
                    "best_place_abs": "Лучшее место (абс.)",
                }
            )
            st.dataframe(roster, use_container_width=True, hide_index=True)

    with tab_events:
        event_search = st.text_input(
            "Поиск события",
            key=f"team_events_filter_{team}",
            placeholder="Подстрока в названии события",
        )
        events_df = pd.DataFrame(
            mq.query_team_events_table(path, team, year=year_value, event_search=event_search)
        )
        if events_df.empty:
            st.caption("События не найдены.")
        else:
            events_df = events_df.rename(
                columns={
                    "event_title": "Событие",
                    "event_date": "Дата",
                    "year": "Год",
                    "sport": "Вид",
                    "team_participants": "Участников команды",
                    "team_finishes": "Финишей",
                    "best_place_abs": "Лучшее место",
                    "best_athlete": "Лучший участник",
                }
            )
            st.dataframe(events_df, use_container_width=True, hide_index=True)

    with tab_sport:
        slices = mq.query_team_sport_distance_slices(path, team, year=year_value)
        by_sport = pd.DataFrame(slices.get("by_sport") or [])
        by_distance = pd.DataFrame(slices.get("by_distance") or [])
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**По видам спорта**")
            st.dataframe(by_sport, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**По дистанциям**")
            st.dataframe(by_distance, use_container_width=True, hide_index=True)

    with tab_geo:
        geo = mq.query_team_geography(path, team, year=year_value)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Города**")
            st.dataframe(pd.DataFrame(geo.get("cities") or []), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**Регионы**")
            st.dataframe(pd.DataFrame(geo.get("regions") or []), use_container_width=True, hide_index=True)
        with c3:
            st.markdown("**Страны**")
            st.dataframe(pd.DataFrame(geo.get("countries") or []), use_container_width=True, hide_index=True)

    with tab_trends:
        trends_df = pd.DataFrame(mq.query_team_yearly_trends(path, team))
        if trends_df.empty:
            st.caption("Нет годовой динамики.")
        else:
            st.dataframe(trends_df, use_container_width=True, hide_index=True)
            fig = px.line(
                trends_df,
                x="year",
                y=["starts", "finishes", "km_total"],
                markers=True,
                title="Динамика стартов, финишей и километража",
            )
            st.plotly_chart(fig, use_container_width=True)

    year_opts = mq.query_team_year_options_for_cups(path, team)
    cy = datetime.date.today().year
    if not year_opts:
        year_opts = [cy]
    default_year = cy if cy in year_opts else year_opts[0]
    cup_year = st.selectbox(
        "Год для кубковых блоков",
        options=year_opts,
        index=year_opts.index(default_year) if default_year in year_opts else 0,
        key=f"team_cup_year_{team}",
    )

    with tab_cups_legacy:
        cup_rows = mq.query_team_cup_points_for_year(path, team, int(cup_year))
        if not cup_rows:
            st.info(f"За **{cup_year}** нет строк в profile_cup_results для участников этой команды.")
        else:
            df = pd.DataFrame(cup_rows).rename(
                columns={
                    "athlete": "Участник",
                    "cup": "Кубок",
                    "distance": "Дистанция",
                    "place": "Место",
                    "cup_group": "Группа",
                    "points": "Очки",
                }
            )
            if "profile_id" in df.columns:
                df = df.drop(columns=["profile_id"], errors="ignore")
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tab_cups_calc:
        cups_for_year = mq.query_profile_cup_summaries_for_year(path, int(cup_year))
        if not cups_for_year:
            st.info("За выбранный год нет кубков.")
        else:
            cup_label_map = {f"{int(r['id'])} · {str(r.get('кубок') or '—')}": int(r["id"]) for r in cups_for_year}
            selected_label = st.selectbox(
                "Кубок (расчётный блок)",
                options=list(cup_label_map.keys()),
                key=f"team_calc_cup_pick_{team}_{cup_year}",
            )
            cup_id = cup_label_map[selected_label]
            if mq.is_team_scoring_enabled(int(cup_id), int(cup_year)):
                stage_map = mq.load_stage_index_map()
                rec = mq.compute_team_scoring_for_cup_year(
                    path, cup_id=cup_id, year=int(cup_year), stage_map=stage_map, rule_version="team_v1"
                )
                st.caption(
                    f"Пересчёт выполнен: этапов={rec.get('stage_rows', 0)}, "
                    f"участников={rec.get('member_rows', 0)}, команд={rec.get('team_rows', 0)}."
                )
                team_tot = mq.query_team_scoring_team_totals(path, cup_id, int(cup_year), rule_version="team_v1")
                if team_tot:
                    members = mq.query_team_scoring_member_totals(
                        path, cup_id, int(cup_year), team_name=team, rule_version="team_v1"
                    )
                    stages = mq.query_team_scoring_stage_points(
                        path, cup_id, int(cup_year), team_name=team, rule_version="team_v1"
                    )
                    st.markdown("**Итоги команды в расчётном контуре**")
                    st.dataframe(pd.DataFrame([r for r in team_tot if r.get("Команда") == team]), use_container_width=True, hide_index=True)
                    st.markdown("**Участники команды**")
                    st.dataframe(pd.DataFrame(members), use_container_width=True, hide_index=True)
                    st.markdown("**Поэтапные очки**")
                    st.dataframe(pd.DataFrame(stages), use_container_width=True, hide_index=True)
                else:
                    st.info("Нет расчётных строк для выбранных параметров.")
            else:
                st.info("Расчёт team_scoring доступен только для cup_id=54 и year=2026. Для остальных случаев используйте legacy-вкладку.")

    with tab_champ:
        cups_for_year = mq.query_profile_cup_summaries_for_year(path, int(cup_year))
        if not cups_for_year:
            st.info("За выбранный год нет кубков.")
        else:
            cup_label_map = {f"{int(r['id'])} · {str(r.get('кубок') or '—')}": int(r["id"]) for r in cups_for_year}
            selected_label = st.selectbox(
                "Кубок (матрица)",
                options=list(cup_label_map.keys()),
                key=f"team_champ_cup_pick_{team}_{cup_year}",
            )
            cup_id = cup_label_map[selected_label]
            if mq.is_team_scoring_enabled(int(cup_id), int(cup_year)):
                stage_map = mq.load_stage_index_map()
                mq.compute_team_scoring_for_cup_year(
                    path, cup_id=cup_id, year=int(cup_year), stage_map=stage_map, rule_version="team_v1"
                )
                mat = mq.query_team_championship_matrix(
                    path, cup_id=cup_id, year=int(cup_year), stage_map=stage_map, rule_version="team_v1"
                )
                rows = mat.get("rows") or []
                if not rows:
                    st.info("Нет данных для матрицы командного первенства.")
                else:
                    dfm = pd.DataFrame(rows)
                    base_cols = ["Место", "Команда"]
                    stage_cols = [str(x.get("label")) for x in (mat.get("stage_columns") or [])]
                    show_cols = [c for c in base_cols + stage_cols + ["Итого"] if c in dfm.columns]
                    st.dataframe(dfm[show_cols], use_container_width=True, hide_index=True)
            else:
                st.info("Матрица командного первенства доступна только для cup_id=54 и year=2026.")

    if show_quality_tab and tab_quality is not None:
        with tab_quality:
            qdf = pd.DataFrame(mq.query_team_data_quality(path, team, year=year_value))
            st.dataframe(qdf, use_container_width=True, hide_index=True)


def page_cups() -> None:
    _section_anchor("cups-list")
    st.header("Кубки")
    path = db_path()
    if not require_db(path):
        return

    st.caption(
        "Сводка кубков за год — **profile_cup_results**. Личное первенство — те же строки + **cup_results**. "
        "Командное первенство — сумма очков **пяти лучших участников** (из **profile_cup_results.total_points**, "
        "команда из **results** по **cup_competitions**)."
    )

    year_opts = mq.query_profile_cup_result_years(path)
    cy = datetime.date.today().year
    if not year_opts:
        st.info("В **profile_cup_results** нет ни одной строки — нечего показывать по годам.")
        return

    default_y = cy if cy in year_opts else year_opts[0]
    yk = "cups_page_filter_year"
    if yk not in st.session_state or st.session_state[yk] not in year_opts:
        st.session_state[yk] = default_y

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:0 0 6px 0;">Год</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 8px 0;">'
        f"По умолчанию — текущий календарный год, если есть данные.</p>",
        unsafe_allow_html=True,
    )
    st.pills(
        "Год",
        options=year_opts,
        selection_mode="single",
        key=yk,
        label_visibility="collapsed",
    )
    year = int(st.session_state[yk])

    summaries = mq.query_profile_cup_summaries_for_year(path, year)
    st.subheader(f"Кубки за {year} год")
    if not summaries:
        st.warning(f"За **{year}** нет строк в profile_cup_results.")
        return

    st.caption("Выберите кубок в таблице — результаты ниже обновятся.")
    df_cups = pd.DataFrame(
        [
            {
                "id": int(r["id"]),
                "Наименование кубка": str(r.get("кубок") or "—"),
                "Количество участников": int(r.get("участников") or 0),
            }
            for r in summaries
        ]
    )
    cup_pick_ev = st.dataframe(
        df_cups[["Наименование кубка", "Количество участников"]],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=f"cups_pick_df_{year}",
        height=min(340, 74 + len(df_cups) * 36),
    )
    selected_row_idx: int | None = None
    if cup_pick_ev.selection.rows:
        selected_row_idx = int(cup_pick_ev.selection.rows[0])
    if selected_row_idx is None:
        selected_row_idx = 0
    cup_id = int(df_cups.iloc[selected_row_idx]["id"])
    cup_title = str(df_cups.iloc[selected_row_idx]["Наименование кубка"] or f"#{cup_id}")

    _section_anchor("cups-results")
    st.subheader(f"Результаты: {cup_title} · {year}")
    tab_ind, tab_team, tab_team_champ = st.tabs(
        ["Личное первенство", "Командный зачёт", "Командное первенство"]
    )

    with tab_ind:
        detail = mq.query_profile_cup_detail_for_year_cup(path, year, cup_id)
        if not detail:
            st.caption("Нет строк для выбранного кубка в **profile_cup_results**.")
        else:
            age_opts = _cup_detail_age_group_options(detail)
            f1, f2, f3 = st.columns(3)
            with f1:
                sn = st.text_input(
                    "Поиск по фамилии",
                    key=f"cups_res_sn_{year}_{cup_id}",
                    placeholder="Например: Столяров",
                )
            with f2:
                gmode = st.selectbox(
                    "Пол / зачёт",
                    options=["Мужчины", "Женщины", "Абсолютный зачёт"],
                    index=2,
                    key=f"cups_res_g_{year}_{cup_id}",
                    help="Мужчины и женщины — по полю gender в профиле. Абсолютный зачёт — все участники.",
                )
            with f3:
                age_sel = st.selectbox(
                    "Возрастная группа",
                    options=["Все"] + age_opts,
                    key=f"cups_res_age_{year}_{cup_id}",
                    help="Берётся из JSON profile_cup_results.raw → group (name или age_from–age_to).",
                )
            filtered = _filter_cup_detail_rows(detail, sn, gmode, age_sel)
            st.caption(
                f"Показано строк: **{len(filtered)}** из {len(detail)}."
            )
            hide = (
                "last_name",
                "gender",
                "raw",
                "pcr_place_abs",
                "cup_place_abs",
                "cup_place_gender",
                "cup_place_group",
            )
            view: list[dict] = []
            for row in filtered:
                place = mq.cup_detail_resolve_display_place(row, gmode, age_sel)
                pub = {k: v for k, v in row.items() if k not in hide}
                pub["место"] = place
                ov = pub.get("очков")
                try:
                    pub["очки"] = int(round(float(ov))) if ov is not None else None
                except (TypeError, ValueError):
                    pub["очки"] = None
                pub.pop("очков", None)
                view.append(pub)
            st.dataframe(pd.DataFrame(view), use_container_width=True, hide_index=True)

    with tab_team:
        if int(cup_id) == 54 and int(year) == 2026:
            st.caption(
                "Логика расчёта: очки по месту в абсолюте в разрезе пола зависят от этапа и дистанции. "
                "Этапы 1–6: 7+ км (600/598/596..., с 7 места шаг -1), 5–6 км (598/596/..., с 6 места шаг -1). "
                "Этапы 7–8: 10–21 км (602/600/599..., далее шаг -1), 5 км (598/596/..., с 6 места шаг -1), "
                "на 2–3 км очки не начисляются. Для 50+ добавляется +15 очков в командный зачёт, "
                "но итог за этап не выше очков 1 места. Участнику в зачёт идёт 7 из 8 лучших этапов, "
                "команде — сумма 5 лучших участников."
            )
            stage_map = mq.load_stage_index_map()
            rec = mq.compute_team_scoring_for_cup_year(
                path, cup_id=cup_id, year=year, stage_map=stage_map, rule_version="team_v1"
            )
            st.caption(
                f"Пересчёт выполнен: этапов={rec.get('stage_rows', 0)}, "
                f"участников={rec.get('member_rows', 0)}, команд={rec.get('team_rows', 0)}."
            )
            team_tot = mq.query_team_scoring_team_totals(path, cup_id, year, rule_version="team_v1")
            if not team_tot:
                st.info("Нет расчётных строк командного зачёта для выбранного кубка и года.")
            else:
                all_members = mq.query_team_scoring_member_totals(
                    path, cup_id, year, team_name=None, rule_version="team_v1"
                )
                all_stages = mq.query_team_scoring_stage_points(
                    path, cup_id, year, team_name=None, rule_version="team_v1"
                )
                with st.spinner("Загрузка…"):
                    frag = _team_scoring_hierarchy_html(team_tot, all_members, all_stages)
                if hasattr(st, "html"):
                    st.html(frag)
                else:
                    st.markdown(frag, unsafe_allow_html=True)
        else:
            score_rows = mq.query_cup_team_score_rows(path, cup_id, year)
            if not score_rows:
                st.info(
                    "Нет данных: для участников с очками в **profile_cup_results** за этот кубок и год "
                    "не найдено ни одного финиша в соревнованиях кубка (**cup_competitions**) "
                    "с непустым **results.team**."
                )
            else:
                tneedle = st.text_input(
                    "Показать команды (подстрока в названии)",
                    key=f"cups_team_filter_legacy_{year}_{cup_id}",
                    placeholder="Пусто — все команды",
                )
                needle = tneedle.strip().casefold()
                if needle:
                    score_rows = [
                        r
                        for r in score_rows
                        if needle in (r.get("команда") or "").strip().casefold()
                    ]
                if not score_rows:
                    st.caption("После фильтра команд не осталось.")
                else:
                    st.caption(
                        "Сумма в командный зачёт = сумма **очков пяти лучших участников** "
                        "(очки из **profile_cup_results.total_points**, по каждому этапу — целое после округления). "
                        "Таблица: **место · команда · очки**; под участником — **все его финиши** в соревнованиях этого кубка за год "
                        "(**results** + **cup_competitions**)."
                    )
                    with st.spinner("Загрузка…"):
                        frag = _cup_team_hierarchy_html(
                            _cup_team_aggregate_points(score_rows), path, cup_id, year
                        )
                    if hasattr(st, "html"):
                        st.html(frag)
                    else:
                        st.markdown(frag, unsafe_allow_html=True)

    with tab_team_champ:
        stage_map = mq.load_stage_index_map()
        if not stage_map:
            st.info("Не найдена карта этапов (.cursor/etapy.yaml).")
        else:
            if int(cup_id) == 54 and int(year) == 2026:
                mq.compute_team_scoring_for_cup_year(
                    path, cup_id=cup_id, year=year, stage_map=stage_map, rule_version="team_v1"
                )
                mat = mq.query_team_championship_matrix(
                    path, cup_id=cup_id, year=year, stage_map=stage_map, rule_version="team_v1"
                )
                rows = mat.get("rows") or []
                if not rows:
                    st.info("Нет данных для матрицы командного первенства.")
                else:
                    dfm = pd.DataFrame(rows)
                    base_cols = ["Место", "Команда"]
                    stage_cols = [str(x.get("label")) for x in (mat.get("stage_columns") or [])]
                    show_cols = base_cols + stage_cols + ["Итого"]
                    show_cols = [c for c in show_cols if c in dfm.columns]
                    st.dataframe(dfm[show_cols], use_container_width=True, hide_index=True)
            else:
                st.info("Матрица командного первенства доступна для Кубка беговых марафонов 2026 (id 54).")


def page_admin() -> None:
    slug = _resolve_admin_route_slug()
    if not slug:
        st.error(
            "Панель администратора недоступна: не задан секретный путь. Укажите переменную окружения "
            "**VMSTAT_ADMIN_ROUTE** или ключ **[admin]** **route_slug** в `.streamlit/secrets.toml`."
        )
        return
    path = DEFAULT_DB
    if not mq.db_exists(path):
        st.warning(f"База недоступна по пути `{path}`. Установите MARATHON_DB или соберите marathon.db.")

    st.header("Панель администратора")
    base_pub = "?page=%s" % PAGE_ALIASES["Общая статистика"]
    st.markdown(
        f"<p>Секретная страница (нет в меню). Обратно на сайт: "
        f'<a href="{html.escape(base_pub)}">общая статистика</a>.</p>',
        unsafe_allow_html=True,
    )

    st.subheader("Экспериментальные стили графиков")
    st.caption("Включает альтернативное оформление Plotly-графиков на всём сайте для текущей сессии.")
    cur_chart_flag = _is_chart_style_experiment_enabled()
    new_chart_flag = st.toggle(
        "Экспериментальный стиль графиков",
        value=cur_chart_flag,
        key="admin_chart_style_toggle",
    )
    if bool(new_chart_flag) != cur_chart_flag:
        st.session_state[CHART_STYLE_EXPERIMENT_KEY] = bool(new_chart_flag)
        _apply_plotly_style_flag()
        st.success("Режим стилей графиков обновлён.")
        st.rerun()

    _section_anchor("admin-aliases")
    st.subheader("Справочник алиасов дистанций")
    st.caption(
        "Используется в разделе «Событие» для нормализации дистанций "
        "в блоке «Рекорды события» (группировка топов по канонической дистанции)."
    )
    src = mq.distance_aliases_file_path()
    st.code(str(src), language=None)

    current_rows = mq.load_distance_alias_rules()
    if not current_rows:
        current_rows = mq.default_distance_alias_rules()
    df = pd.DataFrame(current_rows)
    for col in ("alias", "canonical_key", "canonical_label", "active"):
        if col not in df.columns:
            df[col] = "" if col != "active" else True
    df = df[["alias", "canonical_key", "canonical_label", "active"]]

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="admin_distance_aliases_editor",
        column_config={
            "alias": st.column_config.TextColumn("Alias (как в исходных данных)"),
            "canonical_key": st.column_config.TextColumn("Канонический key"),
            "canonical_label": st.column_config.TextColumn("Название в UI"),
            "active": st.column_config.CheckboxColumn("Активно"),
        },
    )
    c1, c2 = st.columns([1, 3])
    with c1:
        save_clicked = st.button("Сохранить справочник", key="admin_save_distance_aliases")
    with c2:
        st.caption(
            "Пустые строки будут отброшены. Один alias не может быть привязан к разным canonical_key."
        )
    if save_clicked:
        rows = edited.to_dict(orient="records")
        errs = mq.save_distance_alias_rules(rows)
        if errs:
            for e in errs:
                st.error(e)
        else:
            st.success("Справочник сохранён.")
            st.rerun()

    _section_anchor("admin-city-aliases")
    st.subheader("Справочник алиасов городов")
    st.caption(
        "Используется для нормализации написаний городов в географических блоках "
        "(Интересные факты, Команда, Общая статистика)."
    )
    src_city = mq.city_aliases_file_path()
    st.code(str(src_city), language=None)

    city_rows = mq.load_city_alias_rules()
    if not city_rows:
        city_rows = mq.default_city_alias_rules()
    df_city = pd.DataFrame(city_rows)
    for col in ("alias", "canonical_key", "canonical_label", "active"):
        if col not in df_city.columns:
            df_city[col] = "" if col != "active" else True
    df_city = df_city[["alias", "canonical_key", "canonical_label", "active"]]

    edited_city = st.data_editor(
        df_city,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="admin_city_aliases_editor",
        column_config={
            "alias": st.column_config.TextColumn("Alias (как в исходных данных)"),
            "canonical_key": st.column_config.TextColumn("Канонический key"),
            "canonical_label": st.column_config.TextColumn("Название города в UI"),
            "active": st.column_config.CheckboxColumn("Активно"),
        },
    )
    c1_city, c2_city = st.columns([1, 3])
    with c1_city:
        save_city_clicked = st.button("Сохранить справочник городов", key="admin_save_city_aliases")
    with c2_city:
        st.caption("Один alias не может указывать на разные canonical_key.")
    if save_city_clicked:
        rows = edited_city.to_dict(orient="records")
        errs = mq.save_city_alias_rules(rows)
        if errs:
            for e in errs:
                st.error(e)
        else:
            st.success("Справочник городов сохранён.")
            st.rerun()

    _section_anchor("admin-competitions")
    st.subheader("Таблица competitions")
    st.caption(
        "Редактирование метаданных событий (название, дата, год, вид спорта, признаки). "
        "Колонку **raw** можно менять только скриптами синхронизации."
    )
    if require_db(path):
        years_admin = mq.query_competition_years_admin(path)
        cnt_all = mq.query_competitions_admin_count(path, None)
        year_labels = ["Все годы"] + [str(y) for y in years_admin]

        picked = st.selectbox(
            "Фильтр по году соревнования",
            options=year_labels,
            key="admin_comp_year_select",
        )
        year_val: int | None = None if picked == "Все годы" else int(picked)
        total = mq.query_competitions_admin_count(path, year_val)
        lim_cap = 800
        caption_tail = ""
        if total > lim_cap:
            caption_tail = (
                f"В базе **{total}** строк при выбранном фильтре — показываются первые **{lim_cap}**. "
                "Уточните год или редактируйте порциями."
            )
        else:
            caption_tail = f"Строк к показу: **{total}**."
        if cnt_all != total:
            caption_tail += f" Всего событий в базе: **{cnt_all}**."
        st.caption(caption_tail)

        rows_admin = mq.query_competitions_admin_rows(path, year=year_val, limit=lim_cap)
        df_comp = pd.DataFrame(rows_admin)

        if not df_comp.empty:
            if "year" in df_comp.columns:
                df_comp["year"] = pd.to_numeric(df_comp["year"], errors="coerce").astype(
                    "Int64"
                )
            for ic in ("is_relay", "is_published"):
                if ic in df_comp.columns:
                    df_comp[ic] = (
                        pd.to_numeric(df_comp[ic], errors="coerce").fillna(0).astype(int)
                    )

            cc: dict[str, Any] = {}
            if "id" in df_comp.columns:
                cc["id"] = st.column_config.NumberColumn("ID", disabled=True, format="%d")
            if "title" in df_comp.columns:
                cc["title"] = st.column_config.TextColumn("Название")
            if "title_short" in df_comp.columns:
                cc["title_short"] = st.column_config.TextColumn("Кратко")
            if "date" in df_comp.columns:
                cc["date"] = st.column_config.TextColumn("Дата")
            if "year" in df_comp.columns:
                cc["year"] = st.column_config.NumberColumn(
                    "Год", min_value=1980, max_value=2100, step=1
                )
            if "sport" in df_comp.columns:
                cc["sport"] = st.column_config.TextColumn("Спорт")
            if "is_relay" in df_comp.columns:
                cc["is_relay"] = st.column_config.NumberColumn(
                    "Эстафета", min_value=0, max_value=1, step=1
                )
            if "is_published" in df_comp.columns:
                cc["is_published"] = st.column_config.NumberColumn(
                    "Опубликовано", min_value=0, max_value=1, step=1
                )
            if "page_url" in df_comp.columns:
                cc["page_url"] = st.column_config.TextColumn("Страница события")

            edited_comp = st.data_editor(
                df_comp,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="admin_competitions_editor",
                column_config=cc,
            )

            saved_c = st.button(
                "Сохранить изменения competitions",
                key="admin_save_competitions",
                type="primary",
            )
            if saved_c:
                recs = edited_comp.to_dict(orient="records")
                errs = mq.save_competitions_admin_rows(path, recs)
                if errs:
                    for er in errs[:40]:
                        st.warning(er)
                    if len(errs) > 40:
                        st.warning(f"… ещё {len(errs) - 40} ошибок.")
                else:
                    st.success(f"Обновлено строк: **{len(recs)}**.")
                    st.rerun()
        else:
            st.info("Нет строк в **competitions** по выбранному фильтру.")


def main() -> None:
    icon = page_icon_path()
    st.set_page_config(
        page_title="VM-Stat",
        page_icon=icon if icon else None,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _apply_plotly_style_flag()
    inject_yandex_metrica()
    inject_vm_styles()

    pages: list[str] = [
        "Общая статистика",
        "Интересные факты",
        "География ВМ",
        "События",
        "Календарь событий",
        "Рекорды ВМ",
        "Участники",
        "Команды",
        "Кубки",
    ]
    PAGES: tuple[str, ...] = tuple(pages)
    if SIDEBAR_LOGO.is_file():
        st.sidebar.image(str(SIDEBAR_LOGO), use_container_width=True)
    page_from_url = _sidebar_read_page_from_url()
    if page_from_url == ADMIN_PANEL_PAGE:
        st.session_state["nav_page"] = ADMIN_PANEL_PAGE
    elif page_from_url in PAGES:
        st.session_state["nav_page"] = str(page_from_url)
    else:
        i_from_url = _sidebar_read_nav_i_from_url()
        if i_from_url is not None and 0 <= i_from_url < len(PAGES):
            st.session_state["nav_page"] = PAGES[i_from_url]
    nav_now = st.session_state.get("nav_page")
    if nav_now != ADMIN_PANEL_PAGE and nav_now not in PAGES:
        st.session_state["nav_page"] = PAGES[0]
    page: str = str(st.session_state["nav_page"])

    sidebar_here = page if page in PAGES else ""
    st.sidebar.divider()
    render_sidebar_text_nav(PAGES, sidebar_here)

    if page == ADMIN_PANEL_PAGE:
        st.sidebar.info("Открыта секретная страница администратора.")

    st.sidebar.divider()
    st.sidebar.markdown(
        f'<p style="font-size:11px;color:{VM_MUTED};">Официальный сайт: '
        f'<a href="https://vologdamarafon.ru" target="_blank" rel="noopener" '
        f'style="color:{VM_LINK};">vologdamarafon.ru</a></p>',
        unsafe_allow_html=True,
    )

    if page == "Общая статистика":
        page_general_statistics()
    elif page == "Интересные факты":
        page_interesting_facts()
    elif page == "География ВМ":
        page_vm_geography()
    elif page == "События":
        page_event()
    elif page == "Календарь событий":
        page_upcoming_events()
    elif page == "Рекорды ВМ":
        page_vm_records()
    elif page == "Участники":
        page_participant()
    elif page == "Команды":
        page_team()
    elif page == ADMIN_PANEL_PAGE:
        page_admin()
    else:
        page_cups()
    _scroll_to_section_once(page)


if __name__ == "__main__":
    main()
