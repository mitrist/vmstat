"""
MVP-дашборд marathon.db (Streamlit).

Палитра: фон #FFFFFF, основной текст светло-чёрный #1a1a1a, приглушённый #767676, ссылки #93BDDD.

Запуск из корня проекта:
  pip install -r requirements.txt
  streamlit run app.py

Путь к БД: переменная MARATHON_DB или поле [marathon] path в .streamlit/secrets.toml.
Ключ Яндекс.Карт (страница «География ВМ»): [yandex_maps] api_key или YANDEX_MAPS_API_KEY.
"""

from __future__ import annotations

import base64
import calendar
import datetime
import math
import json
from collections.abc import Mapping
from copy import deepcopy
from io import BytesIO
import tomllib
from urllib.parse import quote, urljoin
from typing import Any
import html
import os
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import re
import streamlit as st
import streamlit.components.v1 as components
from streamlit.errors import StreamlitSecretNotFoundError

import marathon_queries as mq

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", str(mq.DEFAULT_DB)))
APP_DIR = Path(__file__).resolve().parent
SIDEBAR_LOGO = APP_DIR / "assets" / "vologdamarafon.png"
FAVICON_LOCAL = APP_DIR / "favicon-32x32.png"
FAVICON_ASSET = APP_DIR / "assets" / "vologdamarafon.png"
REGION_CENTERS_FILE = APP_DIR / "config" / "region_centers.json"
RUSSIA_REGIONS_GEOJSON_FILE = APP_DIR / "config" / "russia_regions.geojson"
WORLD_COUNTRIES_GEOJSON_FILE = APP_DIR / "config" / "countries_ne110m.geojson"
VOLOGDA_DISTRICTS_GEOJSON_FILE = APP_DIR / "config" / "vologda_districts.geojson"
FAVICON_ATTACHED = Path(
    r"C:\Users\Pavlov DA\.cursor\projects\c-Projects-vm-stat\assets\c__Projects_vm_stat_favicon-32x32.png"
)
# Иконка Excel для кнопки выгрузки (Кубки → Командный зачёт)
EXCEL_EXPORT_ICON_PNG = APP_DIR / "85-855276_excel-icon-microsoft-excel-logo-transparent.png"

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
    "Админ панель": "admin",
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
        ("yamap-tables", "Таблицы"),
        ("yamap-cities", "Города"),
        ("yamap-regions", "Регионы и страны"),
        ("yamap-vo-districts", "Районы Вологодской области"),
        ("yamap-vo-by-event", "Участники по событиям"),
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
        ("cups-stage-rating", "Рейтинг по этапам"),
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
_WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE: tuple[dict[str, Any], dict[str, str]] | None = None
_VOLOGDA_DISTRICTS_GEOJSON_CACHE: dict[str, Any] | None = None


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


def _load_vologda_districts_geojson() -> dict[str, Any] | None:
    """Полигоны муниципальных районов ВО (OSM), файл `config/vologda_districts.geojson`."""
    global _VOLOGDA_DISTRICTS_GEOJSON_CACHE
    if _VOLOGDA_DISTRICTS_GEOJSON_CACHE is not None:
        return _VOLOGDA_DISTRICTS_GEOJSON_CACHE
    try:
        geo = json.loads(VOLOGDA_DISTRICTS_GEOJSON_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    feats = geo.get("features")
    if not isinstance(feats, list):
        return None
    _VOLOGDA_DISTRICTS_GEOJSON_CACHE = geo
    return geo


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


def _regions_stats_to_norm_participants(reg_rows: Any) -> dict[str, int]:
    """Сводка строк «регион → участников» в ключ geojson.properties.norm_name (канонический алиас или норм. название)."""
    counts: dict[str, int] = {}
    if not isinstance(reg_rows, list):
        return counts
    for r in reg_rows:
        if not isinstance(r, dict):
            continue
        lbl = str(r.get("region") or "").strip()
        try:
            p = int(r.get("participants") or 0)
        except (TypeError, ValueError):
            continue
        if p <= 0 or not lbl:
            continue
        key = _region_to_canonical(lbl) or _norm_geo_token(lbl)
        if key:
            counts[key] = counts.get(key, 0) + p
    return counts


def _natural_earth_admin0_iso(props: dict[str, Any]) -> str | None:
    """ISO 3166-1 alpha-3 для полигона Natural Earth (ADM0 или валидный ISO_A3)."""
    # Редко ISO_A3 = -99; тогда можно взять ADM0_A3 (NOR для Норвегии и островов и т.д.).
    iso_a = props.get("ISO_A3") if isinstance(props.get("ISO_A3"), str) else ""
    iso_a_s = iso_a.strip().upper()
    if len(iso_a_s) == 3 and iso_a_s != "-99":
        return iso_a_s
    adm = props.get("ADM0_A3") if isinstance(props.get("ADM0_A3"), str) else ""
    adm_s = adm.strip().upper()
    if len(adm_s) == 3 and adm_s != "-99":
        return adm_s
    return None


def _load_world_countries_geojson_and_iso_alias_index(
) -> tuple[dict[str, Any] | None, dict[str, str]]:
    """GeoJSON стран (Natural Earth) + составной индекс норм. название → ISO3 (ручной словарь + имена полигонов)."""
    global _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE
    base_ix: dict[str, str] = dict(COUNTRY_TO_ISO3)
    if _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE is not None:
        geo0, ix0 = _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE
        return geo0, ix0
    ix = dict(base_ix)
    geo: dict[str, Any] | None = None
    try:
        raw = WORLD_COUNTRIES_GEOJSON_FILE.read_text(encoding="utf-8")
        cand = json.loads(raw)
        if isinstance(cand, dict) and isinstance(cand.get("features"), list):
            geo = cand
    except (OSError, json.JSONDecodeError):
        geo = None
    if geo is None:
        _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE = (None, base_ix)
        return None, base_ix

    feats = geo.get("features")
    if not isinstance(feats, list):
        _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE = (None, base_ix)
        return None, base_ix

    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties")
        if not isinstance(props, dict):
            props = {}
            f["properties"] = props
        iso_fc = _natural_earth_admin0_iso(props)
        props["iso_match"] = iso_fc or ""
        if not iso_fc:
            continue
        for nk in (
            props.get("NAME"),
            props.get("ADMIN"),
            props.get("NAME_EN"),
            props.get("NAME_RU"),
        ):
            if not isinstance(nk, str):
                continue
            kn = _norm_geo_token(nk)
            if kn and kn not in ix:
                ix[kn] = iso_fc

    _WORLD_COUNTRIES_GEO_AND_ISO_INDEX_CACHE = (geo, ix)
    return geo, ix


def _country_ui_to_iso3(country_ui: str | None) -> str | None:
    """Сопоставить подпись страны из аналитики с ISO3 (без РФ на слое «страны» отдельно отсекаем)."""
    nk = _norm_geo_token(country_ui)
    if not nk:
        return None
    _, alias_ix = _load_world_countries_geojson_and_iso_alias_index()
    return alias_ix.get(nk) or _country_to_iso3(country_ui)


def _foreign_countries_iso_participants(cc_rows: Any) -> dict[str, int]:
    """Участники по странам, ISO3, без России (РФ подсвечивается только областями)."""
    out: dict[str, int] = {}
    if not isinstance(cc_rows, list):
        return out
    for r in cc_rows:
        if not isinstance(r, dict):
            continue
        lbl = str(r.get("country") or "").strip()
        try:
            p = int(r.get("participants") or 0)
        except (TypeError, ValueError):
            continue
        if p <= 0 or not lbl:
            continue
        iso = _country_ui_to_iso3(lbl)
        if not iso or iso == "RUS":
            continue
        out[iso] = out.get(iso, 0) + p
    return out


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
    if "norm_distance_km_total" in df.columns:
        df["norm_distance_km_total"] = (
            pd.to_numeric(df["norm_distance_km_total"], errors="coerce").fillna(0.0).round(1)
        )
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
        "norm_distance_km_total": "Пройдено км",
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
        "streak_current": "Текущая серия (подряд)",
        "streak_longest": "Макс. серия",
    }
    out = df.rename(columns=rename_map)
    if "norm_distance_km_total" in df.columns and "km_total" in df.columns:
        col_fix: dict[str, str] = {}
        if "Пройдено километров" in out.columns:
            col_fix["Пройдено километров"] = "Км из БД (сумма)"
        if "Пройдено км" in out.columns:
            col_fix["Пройдено км"] = "Пройдено км (норма)"
        if col_fix:
            out = out.rename(columns=col_fix)
    return out


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


def _resolve_yandex_maps_api_key() -> str | None:
    """Ключ JS API Яндекс.Карт: [yandex_maps] api_key в secrets или YANDEX_MAPS_API_KEY."""
    try:
        if hasattr(st, "secrets") and st.secrets:
            try:
                yx = st.secrets["yandex_maps"]
            except Exception:
                yx = None
            # Вложенные секции TOML в Streamlit могут быть не dict — читаем как mapping-подобный объект.
            if yx is not None:
                for kk in ("api_key", "apikey", "API_KEY"):
                    v = None
                    try:
                        v = yx[kk]
                    except Exception:
                        if hasattr(yx, "get"):
                            try:
                                v = yx.get(kk)
                            except Exception:
                                v = None
                    if v:
                        s = str(v).strip()
                        if s:
                            return s
    except Exception:
        pass
    ev = os.environ.get("YANDEX_MAPS_API_KEY", "").strip()
    return ev or None


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
        /* Кубки — иерархии в HTML details (командный зачёт: team → событие → участник) */
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
        /* Командный зачёт: команда → событие → участник */
        .vm-cup-tree details.vm-cup-event {{
            margin-bottom: 10px;
            border: 1px solid #cfd6de;
            border-radius: 6px;
            background: #fff;
            overflow: hidden;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }}
        .vm-cup-tree details.vm-cup-event:last-child {{ margin-bottom: 0; }}
        .vm-cup-tree details.vm-cup-event > summary {{
            list-style: none;
            cursor: pointer;
            display: grid;
            grid-template-columns: 32px minmax(0,1fr) 88px 72px;
            gap: 8px;
            align-items: center;
            padding: 9px 12px 9px 8px;
            background: linear-gradient(180deg, #f8fafc 0%, #eef3f9 100%);
            font-weight: 500;
            font-size: 0.91rem;
            border-bottom: 1px solid transparent;
        }}
        .vm-cup-tree details.vm-cup-event[open] > summary {{
            border-bottom-color: #e2e6eb;
        }}
        .vm-cup-tree details.vm-cup-event > summary::-webkit-details-marker {{ display: none; }}
        .vm-cup-tree details.vm-cup-event > summary::marker {{
            content: "";
        }}
        .vm-cup-tree details.vm-cup-event:not([open]) > summary .vm-cup-caret-e::before {{
            content: "▸";
            color: {VM_ACCENT};
            font-weight: 700;
        }}
        .vm-cup-tree details.vm-cup-event[open] > summary .vm-cup-caret-e::before {{
            content: "▾";
            color: {VM_ACCENT};
            font-weight: 700;
        }}
        .vm-cup-tree details.vm-cup-event > summary:hover {{
            background: linear-gradient(180deg, #f3f7fb 0%, #e6edf5 100%);
        }}
        .vm-cup-tree .vm-cup-event-body {{
            padding: 8px 10px 10px 12px;
            background: #fafbfc;
            border-top: 1px solid #e8eaed;
        }}
        .vm-cup-tree details.vm-cup-ts-participant {{
            margin-bottom: 8px;
            border: 1px solid #e0e3e8;
            border-radius: 5px;
            overflow: hidden;
            background: #fff;
        }}
        .vm-cup-tree details.vm-cup-ts-participant:last-child {{
            margin-bottom: 0;
        }}
        .vm-cup-tree details.vm-cup-ts-participant > summary {{
            list-style: none;
            cursor: pointer;
            display: grid;
            grid-template-columns: minmax(0,1fr) 76px auto;
            gap: 8px;
            align-items: center;
            padding: 8px 12px;
            font-size: 0.88rem;
        }}
        .vm-cup-tree details.vm-cup-ts-participant.vm-cup-ts-top5 > summary {{
            background: linear-gradient(90deg, #d4efda 0%, #dbf0e3 40%, #e8f5eb 100%) !important;
            border-left: 4px solid #28a745;
            font-weight: 500;
        }}
        .vm-cup-tree details.vm-cup-ts-participant:not(.vm-cup-ts-top5) > summary {{
            background: #fff;
            border-left: 4px solid #dee2e6;
        }}
        .vm-cup-tree details.vm-cup-ts-participant > summary::-webkit-details-marker {{ display: none; }}
        .vm-cup-tree details.vm-cup-ts-participant > summary::marker {{
            content: "";
        }}
        .vm-cup-tree details.vm-cup-ts-participant:not([open]) > summary .vm-cup-caret-m::before {{
            content: "▸";
            color: {VM_ACCENT};
            font-weight: 700;
            flex-shrink: 0;
        }}
        .vm-cup-tree details.vm-cup-ts-participant[open] > summary .vm-cup-caret-m::before {{
            content: "▾";
            color: {VM_ACCENT};
            font-weight: 700;
            flex-shrink: 0;
        }}
        .vm-cup-tree details.vm-cup-ts-participant > summary:hover {{
            filter: brightness(0.985);
        }}
        .vm-cup-tree table.vm-cup-ts {{
            width: calc(100% - 12px);
            margin: 0 6px 8px 6px;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 0.83rem;
            border-radius: 4px;
            overflow: hidden;
            border: 1px solid #dce0e5;
        }}
        .vm-cup-tree table.vm-cup-ts th {{
            background: #e9edf2;
            color: #2c3e50;
            font-weight: 600;
            text-align: left;
            padding: 7px 10px;
            border-bottom: 1px solid #dce0e5;
        }}
        .vm-cup-tree table.vm-cup-ts td {{
            padding: 7px 10px;
            border-bottom: 1px solid #eef0f2;
            vertical-align: top;
        }}
        .vm-cup-tree table.vm-cup-ts tbody tr {{
            background: #fff;
        }}
        .vm-cup-tree details.vm-cup-ts-participant.vm-cup-ts-top5 table.vm-cup-ts tbody tr {{
            background: #f3faf5;
        }}
        .vm-cup-tree table.vm-cup-ts td:nth-last-child(-n+2),
        .vm-cup-tree table.vm-cup-ts td:nth-child(2),
        .vm-cup-tree table.vm-cup-ts td:nth-child(3) {{
            font-variant-numeric: tabular-nums;
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
        if slug and page_raw.casefold() == str(slug).strip().casefold():
            return ADMIN_PANEL_PAGE
        key = page_raw.casefold()
    except Exception:
        return None
    for title, alias in PAGE_ALIASES.items():
        if key == alias:
            return title
    if key == "yandex-maps":
        return "География ВМ"
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
    """Карточка-метрика с единым стилем/высотой."""
    lab = html.escape(label)
    v = html.escape(str(value))
    st.markdown(
        f"""
        <div style="
            background: #eef7ff;
            border: 1px solid #d8e9fb;
            border-radius: 10px;
            padding: 12px 12px;
            margin: 2px 0 8px 0;
            min-height: 100px;
            box-shadow: 0 3px 10px rgba(22, 76, 126, 0.12);
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        ">
            <div style="
                font-size: 0.78rem;
                color: #3f668f;
                font-weight: 600;
                letter-spacing: 0.01em;
                line-height: 1.3;
            ">{lab}</div>
            <div style="
                font-size: 1.05rem;
                font-weight: 700;
                color: {VM_TEXT};
                line-height: 1.35;
                margin-top: 6px;
                word-break: break-word;
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
    km_leaders_m = mq.query_interesting_facts_km_leaders_by_gender(
        path, year=year_val, sport=sport_val, gender_code="m", min_starts=int(min_starts), limit=10
    )
    km_leaders_f = mq.query_interesting_facts_km_leaders_by_gender(
        path, year=year_val, sport=sport_val, gender_code="f", min_starts=int(min_starts), limit=10
    )
    universals = mq.query_interesting_facts_universal_participants(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    distances = mq.query_interesting_facts_distance_frequency(path, year=year_val, sport=sport_val, limit=100)
    teams = mq.query_interesting_facts_team_longevity(
        path, year=year_val, sport=sport_val, min_starts=int(min_starts), limit=100
    )
    geo = mq.query_interesting_facts_geography(path, year=year_val, sport=sport_val, limit=100)
    starts_avg = mq.query_interesting_facts_starts_per_participant(path, year=year_val, sport=sport_val)
    starts_avg_year = mq.query_interesting_facts_starts_per_participant_per_year(
        path, year=year_val, sport=sport_val
    )
    longest_series_cards = mq.query_interesting_facts_longest_series_by_sport(
        path, year=year_val, sport=sport_val
    )
    record_leaders_cards = mq.query_interesting_facts_record_leaders_by_sport(
        path, year=year_val, sport=sport_val
    )
    abs_wins_top = mq.query_interesting_facts_abs_wins_top10(
        path, year=year_val, sport=sport_val
    )
    cup_streaks = mq.query_interesting_facts_cup_stage_finish_streaks(
        path, year=year_val, sport=sport_val, min_longest_streak=1, limit=100
    )

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

    def _facts_block_title(text: str) -> None:
        st.markdown(
            f"""
            <div style="
                border-left: 4px solid {VM_ACCENT};
                background: #e4effa;
                border-radius: 8px;
                padding: 8px 12px;
                margin: 10px 0 10px 0;
                color: {VM_TEXT};
                font-weight: 700;
            ">{html.escape(text)}</div>
            """,
            unsafe_allow_html=True,
        )

    def _avg(starts_n: int, participants_n: int) -> float:
        return (float(starts_n) / float(participants_n)) if int(participants_n) > 0 else 0.0

    with st.container(border=True):
        _facts_block_title("Средние старты на участника")
        a1, a2, a3 = st.columns(3)
        with a1:
            metric_plaque(
                "Среднее количество стартов на одного участника",
                f"{_avg(int(starts_avg.get('starts_total') or 0), int(starts_avg.get('participants_total') or 0)):.2f}",
            )
        with a2:
            metric_plaque(
                "Среднее стартов на одного участника (мужчины)",
                f"{_avg(int(starts_avg.get('starts_male') or 0), int(starts_avg.get('participants_male') or 0)):.2f}",
            )
        with a3:
            metric_plaque(
                "Среднее стартов на одного участника (женщины)",
                f"{_avg(int(starts_avg.get('starts_female') or 0), int(starts_avg.get('participants_female') or 0)):.2f}",
            )

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        _facts_block_title("Средние старты на участника в год")
        b1, b2, b3 = st.columns(3)
        with b1:
            metric_plaque(
                "Среднее количество стартов на одного участника в год",
                f"{_avg(float(starts_avg_year.get('avg_starts_total') or 0.0), float(starts_avg_year.get('avg_participants_total') or 0.0)):.2f}",
            )
        with b2:
            metric_plaque(
                "Среднее стартов на одного участника в год (мужчины)",
                f"{_avg(float(starts_avg_year.get('avg_starts_male') or 0.0), float(starts_avg_year.get('avg_participants_male') or 0.0)):.2f}",
            )
        with b3:
            metric_plaque(
                "Среднее стартов на одного участника в год (женщины)",
                f"{_avg(float(starts_avg_year.get('avg_starts_female') or 0.0), float(starts_avg_year.get('avg_participants_female') or 0.0)):.2f}",
            )

    if longest_series_cards:
        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            _facts_block_title("Самая длинная серия событий по видам спорта")
            sport_labels = {
                "run": "бег",
                "trail_run": "трейл",
                "bike": "вело",
                "ski": "лыжи",
                "service": "сервис",
                "other": "другое",
            }
            for i in range(0, len(longest_series_cards), 3):
                row_cards = longest_series_cards[i : i + 3]
                cols = st.columns(len(row_cards))
                for col, card in zip(cols, row_cards):
                    code = str(card.get("sport") or "").strip()
                    icon = sport_calendar_icon(code)
                    sport_name = sport_labels.get(code, code or "unknown")
                    with col:
                        metric_plaque(
                            f"{icon} Самая длинная серия ({sport_name})",
                            f"{card.get('series_title', '—')} · {int(card.get('editions') or 0)} событий",
                        )

    if record_leaders_cards:
        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            _facts_block_title("Лидеры по числу действующих рекордов (по видам спорта)")
            sport_labels = {
                "run": "бег",
                "trail_run": "трейл",
                "bike": "вело",
                "ski": "лыжи",
                "service": "сервис",
                "other": "другое",
            }
            for i in range(0, len(record_leaders_cards), 3):
                row_cards = record_leaders_cards[i : i + 3]
                cols = st.columns(len(row_cards))
                for col, card in zip(cols, row_cards):
                    code = str(card.get("sport") or "").strip()
                    icon = sport_calendar_icon(code)
                    sport_name = sport_labels.get(code, code or "unknown")
                    male = f"{card.get('male_participant', '—')} ({int(card.get('male_records') or 0)})"
                    female = f"{card.get('female_participant', '—')} ({int(card.get('female_records') or 0)})"
                    with col:
                        metric_plaque(
                            f"{icon} Действующие рекорды ({sport_name})",
                            f"М: {male} | Ж: {female}",
                        )

    if abs_wins_top:
        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            _facts_block_title("Лидеры по количеству побед в абсолюте (Топ-10)")
            c_m, c_f = st.columns(2)
            with c_m:
                st.markdown("**Мужчины**")
                for idx, row in enumerate(abs_wins_top.get("males") or [], start=1):
                    metric_plaque(
                        f"#{idx} {row.get('participant', '—')}",
                        f"{int(row.get('wins') or 0)} побед",
                    )
            with c_f:
                st.markdown("**Женщины**")
                for idx, row in enumerate(abs_wins_top.get("females") or [], start=1):
                    metric_plaque(
                        f"#{idx} {row.get('participant', '—')}",
                        f"{int(row.get('wins') or 0)} побед",
                    )

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
        st.markdown("**Топ-10 по пройденному расстоянию (мужчины)**")
        st.caption("Сумма км по нормализованному справочнику событие×дистанция (финиши); столбец «Км из БД» — из таблицы distances.")
        _facts_table(km_leaders_m, key="facts_table_km_male")
    with f4:
        st.markdown("**Топ-10 по пройденному расстоянию (женщины)**")
        st.caption("Те же правила, что и для мужчин.")
        _facts_table(km_leaders_f, key="facts_table_km_female")

    with st.container():
        st.markdown("**Универсалы по видам спорта**")
        st.caption("Чем больше покрытие видов спорта, тем выше место.")
        _facts_table(universals, key="facts_table_universals")

    with st.container(border=True):
        st.markdown("**Серии финишей на этапах кубков**")
        st.caption(
            "Ряд событий: все соревнования из cup_competitions (без привязки к конкретному кубку), "
            "порядок по дате соревнования. Серия подряд — только финиши (DNF разрывает). "
            "«Текущая серия» — среди этапов **не позже сегодняшней даты**, от самого последнего по календарю назад "
            "считаем подряд идущие финиши до первого этапа без финиша; будущие заезды не обнуляют серию; "
            "«Макс. серия» — самый длинный такой же непрерывный участок по **всему** ряду (с учётом фильтров)."
        )
        _facts_table(cup_streaks, key="facts_table_cup_stage_streaks")

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


def _scatter_mapbox_viewport_geo(df_pts: pd.DataFrame) -> tuple[dict[str, float], float]:
    """Центр и уровень zoom для карты городов по bbox точек (слой OSM через Mapbox raster)."""
    lat = df_pts["lat"].astype(float)
    lon = df_pts["lon"].astype(float)
    lat_min, lat_max = float(lat.min()), float(lat.max())
    lon_min, lon_max = float(lon.min()), float(lon.max())
    lat_mid = (lat_min + lat_max) / 2.0
    lon_mid = (lon_min + lon_max) / 2.0
    lat_gap = max(lat_max - lat_min, 0.08)
    lon_gap = max(lon_max - lon_min, 0.08)
    lat_pad = max(0.35, lat_gap * 0.4)
    lon_pad = max(0.35, lon_gap * 0.4)
    cos_mid = float(max(0.12, np.cos(np.radians(lat_mid))))
    lat_span = lat_gap + 2 * lat_pad
    lon_span = (lon_gap + 2 * lon_pad) / cos_mid
    span = max(lat_span, lon_span, 0.3)
    zoom = float(np.clip(9.3 - np.log2(span * 4.0 + 0.12), 3.2, 12.0))
    return dict(lat=lat_mid, lon=lon_mid), zoom


# Хороплет «Регионы и страны»: узкий разброс (низ — явный бледно‑зелёный, не серо‑белый)
_GEO_CHORO_FILL_COLORSCALE: list[list[float | str]] = [
    [0.0, "rgb(210, 237, 220)"],
    [0.3, "rgb(200, 230, 213)"],
    [0.6, "rgb(186, 220, 201)"],
    [0.88, "rgb(170, 210, 187)"],
    [1.0, "rgb(155, 200, 175)"],
]

_GEO_CHORO_COLORBAR: dict[str, Any] = {
    "title": {"text": "Участников"},
    "tickformat": ".0f",
    "len": 0.58,
    "thickness": 11,
    "outlinewidth": 0,
    "bgcolor": "rgba(255, 255, 255, 0.92)",
    "bordercolor": "#e0e0e0",
    "borderwidth": 1,
}

# Вся зелёная градиентная заливка «вытягивается» между 100 … 500 участников (ось ln(1+n))
_GEO_GREEN_BRACKET_MIN_P = 100
_GEO_GREEN_BRACKET_MAX_P = 500
_GEO_GREEN_BRACKET_MIN_LN = float(np.log1p(_GEO_GREEN_BRACKET_MIN_P))
_GEO_GREEN_BRACKET_MAX_LN = float(np.log1p(_GEO_GREEN_BRACKET_MAX_P))
# Сжатие по оси палитры: нет «чисто белого» — даже 2–3 участника дают бледно‑зелёный тон
_GEO_GREEN_VISUAL_FLOOR_T = 0.10
_GEO_GREEN_VISUAL_SPAN_T = 0.86


def _geo_choro_green_t_bracket_line(n: int) -> float:
    """Доля 0–1 между порогами 100–500 по ln(1+n) (до визуального сжатия)."""
    lo = _GEO_GREEN_BRACKET_MIN_LN
    hi = _GEO_GREEN_BRACKET_MAX_LN
    d = hi - lo
    if d <= 0:
        return 0.0
    ln_z = float(np.log1p(max(0, int(n))))
    t_lin = (ln_z - lo) / d
    return float(np.clip(t_lin, 0.0, 1.0))


def _geo_choro_green_t_visual(bracket_unit: float) -> float:
    """Сжимаем палитру: узкий разброс + пол гарантированно «бледно‑зелёный» даже для 2–3 участников."""
    u = float(np.clip(bracket_unit, 0.0, 1.0))
    return float(
        _GEO_GREEN_VISUAL_FLOOR_T + _GEO_GREEN_VISUAL_SPAN_T * u,
    )


def _geo_choro_green_z_bracket_100_500(counts: list[int]) -> list[float]:
    """Комбинация порога ln(между 100–500 участников) + сжатый интервал палитры (z для Plotly)."""
    out: list[float] = []
    for z in counts:
        tl = _geo_choro_green_t_bracket_line(int(z))
        out.append(_geo_choro_green_t_visual(tl))
    return out


def _geo_choro_green_bracket_t_tickvals_text(
) -> tuple[list[float], list[str]]:
    """Подписи colorbar как число участников; позиции — та же нормализация, что для заливки (100–500, ln)."""
    lo = _GEO_GREEN_BRACKET_MIN_LN
    hi = _GEO_GREEN_BRACKET_MAX_LN
    d = hi - lo
    if d <= 0:
        return [0.0, 1.0], ["100", "500"]

    def _t_visual_for_nlab(nlab: int) -> float:
        return _geo_choro_green_t_visual(_geo_choro_green_t_bracket_line(nlab))

    v_end_lo = _t_visual_for_nlab(_GEO_GREEN_BRACKET_MIN_P)
    v_end_hi = _t_visual_for_nlab(_GEO_GREEN_BRACKET_MAX_P)

    pairs: list[tuple[float, str]] = [
        (v_end_lo, f"\u2264{_GEO_GREEN_BRACKET_MIN_P}"),
        (v_end_hi, f"\u2265{_GEO_GREEN_BRACKET_MAX_P}"),
    ]
    for nl in (
        120,
        150,
        180,
        220,
        260,
        300,
        340,
        380,
        420,
        460,
        498,
    ):
        if nl <= _GEO_GREEN_BRACKET_MIN_P or nl >= _GEO_GREEN_BRACKET_MAX_P:
            continue
        tv = _t_visual_for_nlab(nl)
        if v_end_lo + 1e-3 < tv < v_end_hi - 1e-3:
            pairs.append((tv, str(nl)))

    pairs.sort(key=lambda p: (p[0], p[1]))
    out_v: list[float] = []
    out_t: list[str] = []
    seen_round: set[float] = set()
    for tv, lb in pairs:
        rk = round(tv, 4)
        if rk in seen_round:
            continue
        seen_round.add(rk)
        out_v.append(tv)
        out_t.append(lb)
    return out_v, out_t


def _geo_choro_green_colorbar_bundle_bracket_100_500() -> dict[str, Any]:
    tv, tt = _geo_choro_green_bracket_t_tickvals_text()
    cb = dict(_GEO_CHORO_COLORBAR)
    cb.pop("tickformat", None)
    cb.update(
        {
            "title": {
                "text": "Участников<br>(100–500 ln(1+n))",
                "font": {"size": 12},
            },
            "tickmode": "array",
            "tickvals": tv,
            "ticktext": tt,
        }
    )
    return cb


# Вологодская область — одна бледно-голубая заливка, вне общей шкалы участников по РФ (кроме ВО — зелёный градиент)
_VO_RF_BLUE_COLORSCALE: list[list[float | str]] = [
    [0.0, "rgb(214, 235, 250)"],
    [1.0, "rgb(214, 235, 250)"],
]


def _geo_rf_norm_is_vologda_oblast(norm_name: str | None) -> bool:
    """Совпадение с регионом GeoJSON после нормализации (имя полигона Вологодской области)."""
    if not norm_name:
        return False
    return norm_name == _norm_geo_token("Вологодская область")


# Заливка ВО на Яндекс-слое (как _VO_RF_BLUE_COLORSCALE)
_VO_RF_BLUE_FILL_HEX = "#d6ebfa"
# Обводка голубых точек городов-центров ВО (Вологда, Череповец) на карте участников
_VO_CAPITAL_CITY_DOT_STROKE_HEX = "#2980b9"


def _geo_city_capital_vo_blue_marker(city_display: str | None) -> bool:
    """
    Два крупнейших центра области: канонические UI-имена «Вологда», «Череповец»
    (в т.ч. с приставкой «г.» в сыром виде — снимает _norm_city_token).
    Исключаются из суммирования по полигонам районов и рисуются голубым, как заливка ВО на хороплете.
    """
    nk = mq._norm_city_token(str(city_display or "").strip())
    if not nk:
        return False
    return nk == mq._norm_city_token("Вологда") or nk == mq._norm_city_token("Череповец")


def _enrich_map_points_vo_blue_capital(pts: list[Any]) -> None:
    """Помечает точки vo_blue для слоя Яндекса и единообразной логики карт."""
    for p in pts:
        if not isinstance(p, dict):
            continue
        if _geo_city_capital_vo_blue_marker(p.get("city")):
            p["vo_blue"] = True


def _parse_rgb_triplet(s: str) -> tuple[int, int, int]:
    m = re.match(
        r"^\s*rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*$",
        str(s).strip(),
        re.I,
    )
    if not m:
        raise ValueError(f"not rgb(): {s!r}")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def _interpolate_rgb_colorscale(
    stops: list[tuple[float, tuple[int, int, int]]],
    t: float,
) -> tuple[int, int, int]:
    """t в [0,1], stops отсортированы по первому элементу пар (t_boundary, rgb)."""
    if not stops:
        return (200, 220, 200)
    t = float(np.clip(t, 0.0, 1.0))
    if t <= stops[0][0]:
        return stops[0][1]
    if t >= stops[-1][0]:
        return stops[-1][1]
    for i in range(len(stops) - 1):
        t0, c0 = stops[i]
        t1, c1 = stops[i + 1]
        if t0 <= t <= t1 and t1 > t0:
            u = (t - t0) / (t1 - t0)
            rr = int(round(c0[0] * (1 - u) + c1[0] * u))
            gg = int(round(c0[1] * (1 - u) + c1[1] * u))
            bb = int(round(c0[2] * (1 - u) + c1[2] * u))
            return rr, gg, bb
    return stops[-1][1]


def _geo_choro_green_fill_hex_for_participants(n: int) -> str:
    """Тот же визуальный градиент, что хороплет Plotly по числу участников."""
    tl = _geo_choro_green_t_bracket_line(int(n))
    u = _geo_choro_green_t_visual(tl)
    parsed: list[tuple[float, tuple[int, int, int]]] = []
    for a, col in _GEO_CHORO_FILL_COLORSCALE:
        parsed.append((float(a), _parse_rgb_triplet(str(col))))
    rr, gg, bb = _interpolate_rgb_colorscale(parsed, u)
    rr = max(0, min(255, rr))
    gg = max(0, min(255, gg))
    bb = max(0, min(255, bb))
    return f"#{rr:02x}{gg:02x}{bb:02x}"


def _yandex_choropleth_feature_collection(geo_vm: Mapping[str, Any]) -> dict[str, Any]:
    """GeoJSON FeatureCollection со стилями в properties (хороплет регионов для Яндекс.Карт)."""
    features: list[dict[str, Any]] = []
    reg_rows = geo_vm.get("regions") or []
    cc_rows = geo_vm.get("countries") or []
    part_by_norm = _regions_stats_to_norm_participants(reg_rows)
    part_iso_foreign = _foreign_countries_iso_participants(cc_rows)

    geo_rf = _load_russia_regions_geojson_prepared()
    feats_rf_list = geo_rf.get("features") if isinstance(geo_rf, dict) else None
    if feats_rf_list and isinstance(feats_rf_list, list) and part_by_norm:
        for f in feats_rf_list:
            if not isinstance(f, dict):
                continue
            props_rf = f.get("properties")
            nn = ""
            dn = ""
            if isinstance(props_rf, dict):
                nn = str(props_rf.get("norm_name") or "")
                dn = str(props_rf.get("name") or nn)
            try:
                pv = int(part_by_norm.get(nn, 0)) if nn else 0
            except (TypeError, ValueError):
                pv = 0
            if pv <= 0:
                continue
            geom = f.get("geometry")
            if not isinstance(geom, dict):
                continue
            is_vo = _geo_rf_norm_is_vologda_oblast(nn)
            fill_hex = _VO_RF_BLUE_FILL_HEX if is_vo else _geo_choro_green_fill_hex_for_participants(pv)
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "fill": fill_hex,
                        "fillOpacity": 0.74,
                        "stroke": "#6b8574",
                        "strokeWidth": 1,
                        "hintTitle": dn,
                        "participants": pv,
                    },
                    "geometry": deepcopy(geom),
                }
            )

    geo_world, _ix = _load_world_countries_geojson_and_iso_alias_index()
    if isinstance(geo_world, dict):
        feats_w_raw = geo_world.get("features")
        if isinstance(feats_w_raw, list):
            iso_label_rf: dict[str, str] = {}
            iso_feature: dict[str, dict[str, Any]] = {}
            for f_w in feats_w_raw:
                if not isinstance(f_w, dict):
                    continue
                p_w = f_w.get("properties")
                if not isinstance(p_w, dict):
                    continue
                iso_m = str(p_w.get("iso_match") or "").strip()
                if not iso_m or iso_m in iso_label_rf:
                    continue
                lab = (
                    p_w.get("NAME_RU")
                    or p_w.get("NAME_EN")
                    or p_w.get("ADMIN")
                    or p_w.get("NAME")
                    or iso_m
                )
                iso_label_rf[iso_m] = str(lab)
                iso_feature[iso_m] = f_w

            iso_on_map = set(iso_label_rf.keys())
            for iso, pv_raw in sorted(part_iso_foreign.items()):
                iso_k = str(iso).strip()
                try:
                    pv = int(pv_raw)
                except (TypeError, ValueError):
                    continue
                if pv <= 0 or iso_k not in iso_on_map:
                    continue
                f_geo = iso_feature.get(iso_k)
                if not f_geo:
                    continue
                geom = f_geo.get("geometry")
                if not isinstance(geom, dict):
                    continue
                fill_hex = _geo_choro_green_fill_hex_for_participants(pv)
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "fill": fill_hex,
                            "fillOpacity": 0.74,
                            "stroke": "#6b8574",
                            "strokeWidth": 1,
                            "hintTitle": iso_label_rf.get(iso_k) or iso_k,
                            "participants": pv,
                        },
                        "geometry": deepcopy(geom),
                    }
                )

    return {"type": "FeatureCollection", "features": features}


def _vo_approx_bbox_contains(lon: float, lat: float) -> bool:
    """Грубый прямоугольник по Вологодской области (отсекает чужие города без тяжёлого расчёта)."""
    return 34.85 <= lon <= 48.95 and 58.72 <= lat <= 61.55


def _point_in_lonlat_ring(ring: Any, lon: float, lat: float) -> bool:
    """Замкнутое кольцо GeoJSON [lon,lat] — даже‑нечётное число пересечений луча (ray casting)."""
    if not isinstance(ring, list) or len(ring) < 3:
        return False
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        pi = ring[i]
        pj = ring[j]
        if not isinstance(pi, (list, tuple)) or not isinstance(pj, (list, tuple)) or len(pi) < 2 or len(pj) < 2:
            j = i
            continue
        xi = float(pi[0])
        yi = float(pi[1])
        xj = float(pj[0])
        yj = float(pj[1])
        denom = (yj - yi) + 1e-21
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / denom + xi:
            inside = not inside
        j = i
    return inside


def _point_in_polygon_rings(coords: Any, lon: float, lat: float) -> bool:
    """GeoJSON Polygon: coordinates = [ exterior, hole1, ... ]."""
    if not isinstance(coords, list) or not coords:
        return False
    exterior = coords[0]
    if not isinstance(exterior, list) or not _point_in_lonlat_ring(exterior, lon, lat):
        return False
    for hole_idx in range(1, len(coords)):
        hi = coords[hole_idx]
        if isinstance(hi, list) and _point_in_lonlat_ring(hi, lon, lat):
            return False
    return True


def _point_in_geojson_geometry(geom: Mapping[str, Any], lon: float, lat: float) -> bool:
    gt = str(geom.get("type") or "")
    coo = geom.get("coordinates")
    if gt == "Polygon" and isinstance(coo, list):
        return _point_in_polygon_rings(coo, lon, lat)
    if gt == "MultiPolygon" and isinstance(coo, list):
        for poly in coo:
            if isinstance(poly, list) and _point_in_polygon_rings(poly, lon, lat):
                return True
        return False
    return False


def _vo_district_counts_from_map_points_pip(
    feats_in: list[dict[str, Any]],
    map_points: list[Any],
    rayon_resolve_index: dict[str, tuple[str, str]] | None = None,
) -> dict[str, int]:
    """Сумма участников по городам (`map_points`), попавшая в полигон района ВО."""
    idx = rayon_resolve_index or {}
    feats_index: list[tuple[str, str, dict[str, Any]]] = []
    for f in feats_in:
        if not isinstance(f, dict):
            continue
        geom = f.get("geometry")
        if not isinstance(geom, dict):
            continue
        pr = f.get("properties")
        if not isinstance(pr, dict):
            pr = {}
        poly_name = mq._clean_geo_label(str(pr.get("district") or ""), fallback="")
        if not poly_name:
            continue
        nk = mq.resolve_vologda_rayon_with_index(poly_name, idx)[0]
        if not nk:
            continue
        feats_index.append((nk, poly_name, geom))

    out: dict[str, int] = {}
    if not feats_index:
        return out

    for pt in map_points:
        if not isinstance(pt, dict):
            continue
        try:
            la = float(pt.get("lat"))
            lo = float(pt.get("lon"))
        except (TypeError, ValueError):
            continue
        try:
            pw = int(pt.get("participants") or 0)
        except (TypeError, ValueError):
            pw = 0
        if pw <= 0 or not _vo_approx_bbox_contains(lo, la):
            continue
        if _geo_city_capital_vo_blue_marker(pt.get("city")):
            continue
        for nk, _pname, gdict in feats_index:
            try:
                if _point_in_geojson_geometry(gdict, lo, la):
                    out[nk] = out.get(nk, 0) + pw
                    break
            except (TypeError, ValueError):
                continue
    return out


def _yandex_vo_district_feature_collection(
    geo_vm: Mapping[str, Any],
    db_path: Path | str,
) -> dict[str, Any]:
    """Районы Вологодской области: полигоны из config + участники (PIP по map_points или vologda_districts)."""
    geo = _load_vologda_districts_geojson()
    idx = mq.load_vologda_rayon_resolve_index(db_path) if mq.db_exists(db_path) else {}
    if not geo:
        return {"type": "FeatureCollection", "features": []}
    feats_in = geo.get("features")
    if not isinstance(feats_in, list):
        return {"type": "FeatureCollection", "features": []}

    counts_text: dict[str, int] = {}
    label_for: dict[str, str] = {}
    for r in geo_vm.get("vologda_districts") or []:
        if not isinstance(r, dict):
            continue
        d = mq._clean_geo_label(str(r.get("district") or ""), fallback="")
        if not d:
            continue
        k_agg, d_lab = mq.resolve_vologda_rayon_with_index(d, idx)
        if not k_agg:
            continue
        try:
            p = int(r.get("participants") or 0)
        except (TypeError, ValueError):
            p = 0
        counts_text[k_agg] = counts_text.get(k_agg, 0) + p
        label_for[k_agg] = d_lab

    mpts_raw = geo_vm.get("map_points") or []
    map_points_list = [x for x in mpts_raw if isinstance(x, dict)]

    counts_pip = _vo_district_counts_from_map_points_pip(
        [x for x in feats_in if isinstance(x, dict)],
        map_points_list,
        rayon_resolve_index=idx,
    )
    # Таблица «районы ВО» (агрегат по полю района в справочниках) — основной источник чисел для карты;
    # PIP по центрам городов только дополняет районы без строки в таблице (нет совпадения alias/район или 0 в данных).
    for ff in feats_in:
        if not isinstance(ff, dict):
            continue
        pr_ff = ff.get("properties")
        if not isinstance(pr_ff, dict):
            continue
        pn_ff = mq._clean_geo_label(str(pr_ff.get("district") or ""), fallback="")
        if pn_ff:
            kn_ff, pn_ui = mq.resolve_vologda_rayon_with_index(pn_ff, idx)
            if kn_ff:
                label_for.setdefault(kn_ff, pn_ui)

    poly_nk_set: set[str] = set()
    for pf in feats_in:
        if not isinstance(pf, dict):
            continue
        prp = pf.get("properties")
        if not isinstance(prp, dict):
            prp = {}
        pnm = mq._clean_geo_label(str(prp.get("district") or ""), fallback="")
        if pnm:
            knp, _ = mq.resolve_vologda_rayon_with_index(pnm, idx)
            if knp:
                poly_nk_set.add(knp)

    merge_keys = set(counts_text) | set(counts_pip) | poly_nk_set
    counts: dict[str, int] = {}
    for nk in merge_keys:
        ct = int(counts_text.get(nk, 0))
        cp = int(counts_pip.get(nk, 0))
        counts[nk] = ct if ct > 0 else cp

    features: list[dict[str, Any]] = []
    for f in feats_in:
        if not isinstance(f, dict):
            continue
        pr0 = f.get("properties")
        if not isinstance(pr0, dict):
            pr0 = {}
        poly_name = mq._clean_geo_label(str(pr0.get("district") or ""), fallback="")
        if not poly_name:
            continue
        nk, poly_ui = mq.resolve_vologda_rayon_with_index(poly_name, idx)
        pv = int(counts.get(nk, 0))
        title = label_for.get(nk) or poly_ui
        geom = f.get("geometry")
        if not isinstance(geom, dict):
            continue
        fill_hex = _geo_choro_green_fill_hex_for_participants(max(0, pv))
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "fill": fill_hex,
                    "fillOpacity": 0.74,
                    "stroke": "#6b8574",
                    "strokeWidth": 1,
                    "hintTitle": title,
                    "participants": pv,
                },
                "geometry": deepcopy(geom),
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _render_geo_vm_data_tables(geo_vm: Mapping[str, Any], *, widget_key_prefix: str) -> None:
    """Таблицы и диаграмма по районам ВО (раздел «География ВМ»)."""
    cities_all = geo_vm.get("cities") or []
    _section_anchor("yamap-tables")
    st.subheader("Топ городов")
    st.caption("Первые **30** городов по числу уникальных участников (учитывается нормализация городов из справочника). Строку можно отфильтровать подстрокой.")
    needle = st.text_input(
        "Фильтр по названию города",
        key=f"{widget_key_prefix}_city_needle",
        placeholder="Например: Вологда",
        label_visibility="collapsed",
    )
    nneedle = needle.strip().casefold()
    if cities_all:
        if nneedle:
            cities_filt = [
                r
                for r in cities_all
                if nneedle in str(r.get("city") or "").strip().casefold()
            ]
        else:
            cities_filt = cities_all
        df_top_c = pd.DataFrame(cities_filt[:30])
        if df_top_c.empty:
            st.caption("После фильтра не осталось городов.")
        else:
            rename_top = {"city": "Город", "participants": "Участников", "starts": "Стартов"}
            for old, new in (
                ("participants_m", "Участников М"),
                ("participants_f", "Участников Ж"),
                ("starts_m", "Стартов М"),
                ("starts_f", "Стартов Ж"),
            ):
                rename_top[old] = new
            df_top_c = df_top_c.rename(columns=rename_top)
            show_tc = [
                c
                for c in (
                    "Город",
                    "Участников",
                    "Участников М",
                    "Участников Ж",
                    "Стартов",
                    "Стартов М",
                    "Стартов Ж",
                )
                if c in df_top_c.columns
            ]
            st.dataframe(
                df_top_c[show_tc],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Участников": st.column_config.NumberColumn(format="%d"),
                    "Участников М": st.column_config.NumberColumn(format="%d"),
                    "Участников Ж": st.column_config.NumberColumn(format="%d"),
                    "Стартов": st.column_config.NumberColumn(format="%d"),
                    "Стартов М": st.column_config.NumberColumn(format="%d"),
                    "Стартов Ж": st.column_config.NumberColumn(format="%d"),
                },
            )
    else:
        st.caption("Нет данных для выбранных фильтров.")

    st.subheader("Количество участников по регионам")
    reg_rows = geo_vm.get("regions") or []
    if reg_rows:
        dfr = pd.DataFrame(reg_rows)
        dfr = dfr.rename(
            columns={
                "region": "Регион",
                "participants": "Участников",
                "starts": "Стартов",
                "participants_m": "Участников М",
                "participants_f": "Участников Ж",
                "starts_m": "Стартов М",
                "starts_f": "Стартов Ж",
            }
        )
        show_r = [
            c
            for c in (
                "Регион",
                "Участников",
                "Участников М",
                "Участников Ж",
                "Стартов",
                "Стартов М",
                "Стартов Ж",
            )
            if c in dfr.columns
        ]
        st.dataframe(
            dfr[show_r],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Участников": st.column_config.NumberColumn(format="%d"),
                "Участников М": st.column_config.NumberColumn(format="%d"),
                "Участников Ж": st.column_config.NumberColumn(format="%d"),
                "Стартов": st.column_config.NumberColumn(format="%d"),
                "Стартов М": st.column_config.NumberColumn(format="%d"),
                "Стартов Ж": st.column_config.NumberColumn(format="%d"),
            },
        )
    else:
        st.caption("Нет данных для выбранных фильтров.")

    st.subheader("Участники по странам")
    cc_rows = geo_vm.get("countries") or []
    if cc_rows:
        dfc = pd.DataFrame(cc_rows)
        dfc = dfc.rename(
            columns={
                "country": "Страна",
                "participants": "Участников",
                "starts": "Стартов",
                "participants_m": "Участников М",
                "participants_f": "Участников Ж",
                "starts_m": "Стартов М",
                "starts_f": "Стартов Ж",
            }
        )
        show_c = [
            c
            for c in (
                "Страна",
                "Участников",
                "Участников М",
                "Участников Ж",
                "Стартов",
                "Стартов М",
                "Стартов Ж",
            )
            if c in dfc.columns
        ]
        st.dataframe(
            dfc[show_c],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Участников": st.column_config.NumberColumn(format="%d"),
                "Участников М": st.column_config.NumberColumn(format="%d"),
                "Участников Ж": st.column_config.NumberColumn(format="%d"),
                "Стартов": st.column_config.NumberColumn(format="%d"),
                "Стартов М": st.column_config.NumberColumn(format="%d"),
                "Стартов Ж": st.column_config.NumberColumn(format="%d"),
            },
        )
    else:
        st.caption("Нет данных для выбранных фильтров.")

    st.subheader("Количество участников по районам Вологодской области")
    vo_rows = geo_vm.get("vologda_districts") or []
    if vo_rows:
        dfd = pd.DataFrame(vo_rows).rename(
            columns={
                "district": "Район",
                "participants": "Участников",
                "starts": "Стартов",
                "participants_m": "Участников М",
                "participants_f": "Участников Ж",
                "starts_m": "Стартов М",
                "starts_f": "Стартов Ж",
            }
        )
        pie_df = dfd[["Район", "Участников"]].copy()
        pie_df = pie_df[pie_df["Участников"] > 0]
        if not pie_df.empty:
            total_vo = int(pie_df["Участников"].sum())
            share_df = dfd[dfd["Участников"] > 0].copy()
            share_df["Доля района"] = (
                (100.0 * share_df["Участников"] / total_vo) if total_vo > 0 else 0.0
            )
            share_df = share_df.sort_values(by="Участников", ascending=False)

            fig_vo = px.pie(
                pie_df,
                names="Район",
                values="Участников",
                title="Доля района в общем количестве участников Вологодской области",
            )
            fig_vo.update_traces(textposition="inside", textinfo="percent+label")
            fig_vo.update_layout(margin=dict(l=0, r=0, t=60, b=0))

            tbl_cols = ["Район", "Доля района"]
            for extras in ("Участников М", "Участников Ж"):
                if extras in share_df.columns:
                    tbl_cols.append(extras)

            col_pie, col_tbl = st.columns(2)
            with col_pie:
                st.plotly_chart(fig_vo, use_container_width=True)
            with col_tbl:
                st.dataframe(
                    share_df[tbl_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Доля района": st.column_config.NumberColumn("Доля района", format="%.1f%%"),
                        "Участников М": st.column_config.NumberColumn(format="%d"),
                        "Участников Ж": st.column_config.NumberColumn(format="%d"),
                    },
                )
    else:
        st.caption("Нет данных по районам Вологодской области для выбранных фильтров.")


def _yandex_maps_cities_html(
    api_key: str,
    points: list[dict[str, Any]],
    *,
    start_lat: float,
    start_lon: float,
    start_zoom: int,
) -> str:
    """Полная HTML-страница со встроенной картой API 2.1 для iframe components.html."""
    qk = quote(str(api_key).strip(), safe="")
    api_url = f"https://api-maps.yandex.ru/2.1/?apikey={qk}&lang=ru_RU"
    src_esc = html.escape(api_url, quote=True)
    pts_raw = json.dumps(points, ensure_ascii=False)
    pts_safe = pts_raw.replace("</script>", "<\\/script>")
    zm = max(3, min(15, int(start_zoom)))
    # JS без f-string, чтобы не экранировать фигурные скобки
    lat_j = repr(float(start_lat))
    lon_j = repr(float(start_lon))
    zm_j = str(zm)
    script_body = """ymaps.ready(function () {
  var map = new ymaps.Map('vm-yandex-map-root', {
    center: [""" + lat_j + ", " + lon_j + """],
    zoom: """ + zm_j + """,
    controls: ['zoomControl', 'fullscreenControl', 'typeSelector']
  });
  var el = document.getElementById('vm-yamap-points-json');
  var points = [];
  try { points = JSON.parse(el ? el.textContent : '[]'); } catch (e2) { points = []; }
  var col = new ymaps.GeoObjectCollection();
  function escHtml(s) {
    return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  var CityDotRed = null;
  var CityDotBlueVo = null;
  try {
    CityDotRed = ymaps.templateLayoutFactory.createClass(
      '<div style="width:9px;height:9px;background:#c62828;border-radius:50%;' +
        'border:1px solid #fff;box-sizing:border-box;cursor:pointer;' +
        'box-shadow:0 1px 2px rgba(0,0,0,.35);transform:translate(-50%,-50%);"></div>'
    );
    CityDotBlueVo = ymaps.templateLayoutFactory.createClass(
      '<div style="width:9px;height:9px;background:#d6ebfa;border-radius:50%;' +
        'border:1px solid #2980b9;box-sizing:border-box;cursor:pointer;' +
        'box-shadow:0 1px 2px rgba(0,0,0,.35);transform:translate(-50%,-50%);"></div>'
    );
  } catch (eDot) {
    CityDotRed = null;
    CityDotBlueVo = null;
  }
  for (var i = 0; i < points.length; i++) {
    var p = points[i];
    var voBlue = p.vo_blue === true || p.vo_blue === 1;
    var layoutCls = voBlue ? CityDotBlueVo : CityDotRed;
    var pmOpts = layoutCls ? {
      iconLayout: layoutCls,
      iconOffset: [0, 0],
      iconShape: { type: 'Circle', coordinates: [0, 0], radius: 10 }
    } : (voBlue
      ? { preset: 'islands#circleDotIcon', iconColor: '#2980b9' }
      : { preset: 'islands#redCircleDotIcon', iconColor: '#c62828' });
    var pm = new ymaps.Placemark([p.lat, p.lon], {
      balloonContentHeader: escHtml(p.city),
      balloonContentBody: 'Участников: ' + String(p.participants),
      hintContent: escHtml(p.city) + ' — ' + String(p.participants)
    }, pmOpts);
    col.add(pm);
  }
  map.geoObjects.add(col);
  if (points.length > 0) {
    var b = col.getBounds();
    if (b) {
      var pr = map.setBounds(b, { checkZoomRange: true, zoomMargin: 52 });
      if (pr && typeof pr.catch === 'function') {
        pr.catch(function () {
          map.setCenter([""" + lat_j + ", " + lon_j + """], """ + zm_j + """);
        });
      }
    }
  }
});
"""
    return (
        '<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8"/>'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>'
        "<style>"
        "html,body{margin:0;padding:0;height:100%;width:100%;}"
        "#vm-yandex-map-root{width:100%;height:698px;min-height:560px;}"
        "</style>"
        f'<script src="{src_esc}" type="text/javascript"></script>'
        "</head><body>"
        '<div id="vm-yandex-map-root"></div>'
        f'<script type="application/json" id="vm-yamap-points-json">{pts_safe}</script>'
        f'<script type="text/javascript">{script_body}</script>'
        "</body></html>"
    )


def _yandex_maps_choropleth_html(
    api_key: str,
    feature_collection: Mapping[str, Any],
    *,
    start_lat: float,
    start_lon: float,
    start_zoom: int,
    map_dom_id: str = "vm-yandex-map-choro",
    json_script_id: str = "vm-yamap-choro-json",
) -> str:
    """Карта API 2.1: полигоны регионов РФ и зарубежных стран по тому же смыслу, что Choroplethmapbox."""
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,72}", map_dom_id):
        map_dom_id = "vm-yandex-map-choro"
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,72}", json_script_id):
        json_script_id = "vm-yamap-choro-json"
    qk = quote(str(api_key).strip(), safe="")
    api_url = f"https://api-maps.yandex.ru/2.1/?apikey={qk}&lang=ru_RU"
    src_esc = html.escape(api_url, quote=True)
    fc_any: Any = dict(feature_collection) if not isinstance(feature_collection, dict) else feature_collection
    fc_raw = json.dumps(fc_any, ensure_ascii=False)
    fc_safe = fc_raw.replace("</script>", "<\\/script>")
    zm = max(2, min(12, int(start_zoom)))
    lat_j = repr(float(start_lat))
    lon_j = repr(float(start_lon))
    zm_j = str(zm)
    mid_js = json.dumps(map_dom_id)
    jid_js = json.dumps(json_script_id)
    css_map = (
        "html,body{margin:0;padding:0;height:100%;width:100%;}#"
        + map_dom_id
        + "{width:100%;height:698px;min-height:560px;}"
    )
    script_body = """ymaps.ready(function () {
  var map = new ymaps.Map(""" + mid_js + """, {
    center: [""" + lat_j + ", " + lon_j + """],
    zoom: """ + zm_j + """,
    controls: ['zoomControl', 'fullscreenControl', 'typeSelector']
  });
  var el = document.getElementById(""" + jid_js + """);
  var fc = { type: 'FeatureCollection', features: [] };
  try { fc = JSON.parse(el ? el.textContent : '{}'); } catch (e0) { fc = { type: 'FeatureCollection', features: [] }; }
  function escHtml(s) {
    return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function addPolygonRings(ringsLonLat, pr) {
    var yandexRings = ringsLonLat.map(function (ring) {
      return ring.map(function (c) { return [c[1], c[0]]; });
    });
    map.geoObjects.add(new ymaps.Polygon(yandexRings, {
      balloonContentHeader: escHtml(pr.hintTitle),
      balloonContentBody: 'Участников: ' + String(pr.participants),
      hintContent: escHtml(pr.hintTitle) + ' — ' + String(pr.participants)
    }, {
      fillColor: pr.fill,
      fillOpacity: pr.fillOpacity != null ? pr.fillOpacity : 0.74,
      strokeColor: pr.stroke || '#6b8574',
      strokeWidth: pr.strokeWidth != null ? pr.strokeWidth : 1
    }));
  }
  function ringCentroidLonLat(ring) {
    if (!ring || ring.length < 3) return null;
    var n = ring.length;
    var end = n;
    var aa = ring[0], zz = ring[n - 1];
    if (aa && zz && aa.length > 1 && zz.length > 1 &&
        Math.abs(Number(aa[0]) - Number(zz[0])) < 1e-10 &&
        Math.abs(Number(aa[1]) - Number(zz[1])) < 1e-10) {
      end = n - 1;
    }
    var sx = 0, sy = 0, kk = 0;
    var ij = 0;
    for (ij = 0; ij < end; ij++) {
      var c = ring[ij];
      if (!c || c.length < 2) continue;
      sx += Number(c[0]); sy += Number(c[1]); kk++;
    }
    if (!kk) return null;
    return [sx / kk, sy / kk];
  }
  function ringBBoxMetric(ring) {
    if (!ring || ring.length < 2) return 0;
    var mnX = Infinity, mnY = Infinity, mxX = -Infinity, mxY = -Infinity;
    var ij = 0;
    for (ij = 0; ij < ring.length; ij++) {
      var c = ring[ij];
      if (!c || c.length < 2) continue;
      var x = Number(c[0]), y = Number(c[1]);
      if (x < mnX) mnX = x; if (x > mxX) mxX = x;
      if (y < mnY) mnY = y; if (y > mxY) mxY = y;
    }
    if (!isFinite(mxX) || !isFinite(mxY)) return 0;
    return Math.abs((mxX - mnX) * (mxY - mnY));
  }
  function geometryCentroidLonLat(g) {
    if (!g) return null;
    if (g.type === 'Polygon' && g.coordinates && g.coordinates[0]) {
      return ringCentroidLonLat(g.coordinates[0]);
    }
    if (g.type === 'MultiPolygon' && g.coordinates && g.coordinates.length) {
      var best = null;
      var bestM = -1;
      var pj = 0;
      for (pj = 0; pj < g.coordinates.length; pj++) {
        var part = g.coordinates[pj];
        if (!part || !part[0]) continue;
        var ext = part[0];
        var m = ringBBoxMetric(ext);
        if (m > bestM) {
          bestM = m;
          best = ringCentroidLonLat(ext);
        }
      }
      return best;
    }
    return null;
  }
  var feats = fc.features || [];
  var fi = 0;
  for (fi = 0; fi < feats.length; fi++) {
    var f = feats[fi];
    if (!f || !f.geometry || !f.properties) { continue; }
    var g = f.geometry;
    var pr = f.properties;
    if (!pr.fill) { continue; }
    if (g.type === 'Polygon') {
      addPolygonRings(g.coordinates, pr);
    } else if (g.type === 'MultiPolygon') {
      var mp = g.coordinates;
      var pj = 0;
      for (pj = 0; pj < mp.length; pj++) {
        addPolygonRings(mp[pj], pr);
      }
    }
  }
  var ChoroValueLabel = null;
  try {
    ChoroValueLabel = ymaps.templateLayoutFactory.createClass(
      '<div style="font:700 12px/1.1 system-ui,-apple-system,Segoe UI,sans-serif;' +
        'color:#14422a;text-align:center;pointer-events:none;white-space:nowrap;' +
        'text-shadow:1px 0 0 #fff,-1px 0 0 #fff,0 1px 0 #fff,0 -1px 0 #fff,' +
        '0 0 3px rgba(255,255,255,.9);">$[properties.iconContent]</div>'
    );
  } catch (eLbl) {
    ChoroValueLabel = null;
  }
  if (ChoroValueLabel) { for (fi = 0; fi < feats.length; fi++) {
    var f2 = feats[fi];
    if (!f2 || !f2.geometry || !f2.properties) { continue; }
    var g2 = f2.geometry;
    var pr2 = f2.properties;
    if (!pr2.fill) { continue; }
    var nv = Number(pr2.participants);
    if (!isFinite(nv) || nv <= 0) { continue; }
    var cll = geometryCentroidLonLat(g2);
    if (!cll) { continue; }
    map.geoObjects.add(new ymaps.Placemark([cll[1], cll[0]], {
      iconContent: String(Math.round(nv))
    }, {
      iconLayout: ChoroValueLabel,
      iconOffset: [0, 0],
      hasBalloon: false,
      hasHint: false,
      zIndex: 900
    }));
  } }
  if (feats.length > 0) {
    var b = map.geoObjects.getBounds();
    if (b) {
      var prb = map.setBounds(b, { checkZoomRange: true, zoomMargin: 52 });
      if (prb && typeof prb.catch === 'function') {
        prb.catch(function () {
          map.setCenter([""" + lat_j + ", " + lon_j + """], """ + zm_j + """);
        });
      }
    }
  }
});
"""
    return (
        '<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8"/>'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>'
        f"<style>{css_map}</style>"
        f'<script src="{src_esc}" type="text/javascript"></script>'
        "</head><body>"
        f'<div id="{map_dom_id}"></div>'
        f'<script type="application/json" id="{json_script_id}">{fc_safe}</script>'
        f'<script type="text/javascript">{script_body}</script>'
        "</body></html>"
    )


def page_vm_geography() -> None:
    st.header("География ВМ")
    path = db_path()
    if not require_db(path):
        return

    years_all = mq.query_distinct_years(path)
    sports_all = mq.query_distinct_sports(path)

    year_options = ["Все"] + [str(y) for y in years_all]
    year_pick = (
        st.pills(
            "Год",
            options=year_options,
            selection_mode="single",
            default="Все",
            key="yamap_pills_year",
        )
        if year_options
        else "Все"
    )
    year_val: int | None = None if year_pick == "Все" else int(year_pick)

    sport_options = ["Все"] + sports_all
    sport_pick = (
        st.pills(
            "Вид спорта",
            options=sport_options,
            selection_mode="single",
            default="Все",
            key="yamap_pills_sport",
        )
        if sport_options
        else "Все"
    )
    sport_val: str | None = None if sport_pick == "Все" else str(sport_pick).strip()

    with st.spinner("Загрузка данных…"):
        geo_vm = mq.query_vm_geography_page(path, year=year_val, sport=sport_val)

    _render_geo_vm_data_tables(geo_vm, widget_key_prefix="yamap_geo")

    api_key = _resolve_yandex_maps_api_key()
    if api_key:
        pts: list[dict[str, Any]] = list(geo_vm.get("map_points") or [])
        cities_any = geo_vm.get("cities") or []

        default_lat = 59.218
        default_lon = 39.884
        default_zoom = 6
        start_lat = default_lat
        start_lon = default_lon
        start_zoom_i = default_zoom
        if pts:
            cen, zf = _scatter_mapbox_viewport_geo(pd.DataFrame(pts))
            start_lat = float(cen["lat"])
            start_lon = float(cen["lon"])
            start_zoom_i = int(np.clip(round(zf), 3, 12))

        _section_anchor("yamap-cities")
        st.subheader("Города участников")
        st.caption(
            "**Вологда** и **Череповец** — голубые маркеры (как заливка ВО на карте регионов); в суммировании участников "
            "**по полигону района** по точке города они не участвуют (см. карту районов ниже). Остальные города — красные точки."
        )
        if cities_any and not pts:
            st.caption(
                "Не удалось поставить точки на карту — для канонических названий городов нет координат в справочнике."
            )
        elif not cities_any:
            st.caption("Нет городов для выбранных фильтров.")
    
        _enrich_map_points_vo_blue_capital(pts)
        html_map = _yandex_maps_cities_html(
            api_key,
            pts,
            start_lat=start_lat,
            start_lon=start_lon,
            start_zoom=start_zoom_i,
        )
        components.html(html_map, height=720, scrolling=False)
    
        _section_anchor("yamap-regions")
        st.subheader("Регионы и страны")
    
        geo_rf_chk = _load_russia_regions_geojson_prepared()
        geo_world_chk, _wix = _load_world_countries_geojson_and_iso_alias_index()
        if geo_rf_chk is None:
            st.caption(
                "Не загружается файл контурной карты регионов России (`config/russia_regions.geojson`)."
            )
        if geo_world_chk is None:
            st.caption(
                "Не загружается файл границ стран (`config/countries_ne110m.geojson`). "
                "Файл GeoJSON стран — см. подсказки в этом блоке или в документации репозитория (`config/`)."
            )
    
        fc_choro: dict[str, Any] = _yandex_choropleth_feature_collection(geo_vm)
        reg_rows_vm = geo_vm.get("regions") or []
        cc_rows_vm = geo_vm.get("countries") or []
        part_by_norm_vm = _regions_stats_to_norm_participants(reg_rows_vm)
        part_iso_vm = _foreign_countries_iso_participants(cc_rows_vm)
        chor_feats = fc_choro.get("features") if isinstance(fc_choro, dict) else []
        if not isinstance(chor_feats, list):
            chor_feats = []
    
        if chor_feats:
            ch_lat = start_lat if pts else 58.55
            ch_lon = start_lon if pts else 43.82
            ch_zoom = int(np.clip(round(start_zoom_i if pts else 3.95), 2, 11))
            html_choro = _yandex_maps_choropleth_html(
                api_key,
                fc_choro,
                start_lat=ch_lat,
                start_lon=ch_lon,
                start_zoom=ch_zoom,
                map_dom_id="yamap-choro-rf",
                json_script_id="yamap-choro-rf-data",
            )
            components.html(html_choro, height=720, scrolling=False)
        else:
            has_layer_file = geo_rf_chk is not None or geo_world_chk is not None
            has_stats = bool(part_by_norm_vm or part_iso_vm)
            if has_layer_file and has_stats:
                if geo_rf_chk is not None and part_by_norm_vm:
                    rf_feats_rf = geo_rf_chk.get("features") if isinstance(geo_rf_chk, dict) else None
                    rf_match = False
                    if feats_rf_list := (rf_feats_rf if isinstance(rf_feats_rf, list) else None):
                        for f_try in feats_rf_list:
                            if not isinstance(f_try, dict):
                                continue
                            pr_try = f_try.get("properties")
                            nn_try = ""
                            if isinstance(pr_try, dict):
                                nn_try = str(pr_try.get("norm_name") or "")
                            pv_try = part_by_norm_vm.get(nn_try, 0) if nn_try else 0
                            if pv_try > 0:
                                rf_match = True
                                break
                    if not rf_match:
                        st.caption(
                            "Не удалось сопоставить названия регионов из данных с полигонами карты России "
                            "(алиасы см. в `config/region_centers.json`)."
                        )
                if geo_world_chk is not None and part_iso_vm:
                    st.caption(
                        "Не удалось сопоставить страны из данных с контурной картой — дополните словарь "
                        "`COUNTRY_TO_ISO3` в `app.py` или проверьте написание как в Natural Earth."
                    )
            elif has_layer_file and not has_stats:
                st.caption(
                    "Нет строк по регионам и странам с числом участников больше нуля для выбранных фильтров."
                )
    
        _section_anchor("yamap-vo-districts")
        st.subheader("Районы Вологодской области")
        vo_geo_file = VOLOGDA_DISTRICTS_GEOJSON_FILE.is_file()
        if not vo_geo_file:
            st.caption(
                "Нет локального файла **config/vologda_districts.geojson** — добавьте GeoJSON районов ВО "
                "(пример источника: [Russia_geojson_OSM](https://github.com/timurkanaz/Russia_geojson_OSM), "
                "каталог **Regions / SZFO**, файл с названием Вологодской области и суффиксом Vologda region) или свой слой "
                "с полем **`district`** в свойствах каждого полигона."
            )
        else:
            fc_vo = _yandex_vo_district_feature_collection(geo_vm, path)
            vo_feats = fc_vo.get("features") if isinstance(fc_vo, dict) else []
            if isinstance(vo_feats, list) and vo_feats:
                html_vo = _yandex_maps_choropleth_html(
                    api_key,
                    fc_vo,
                    start_lat=float(default_lat),
                    start_lon=float(default_lon),
                    start_zoom=max(6, min(10, default_zoom)),
                    map_dom_id="yamap-choro-vo",
                    json_script_id="yamap-choro-vo-data",
                )
                components.html(html_vo, height=720, scrolling=False)
                st.caption(
                    "Контуры районов: данные **© OpenStreetMap** ([ODbL](https://www.openstreetmap.org/copyright))."
                )
            else:
                st.caption(
                    "Файл **vologda_districts.geojson** загружен, но в нём нет полигонов с полем `district`."
                )
    
        st.caption("Картографические данные © Яндекс")
    else:
        st.warning(
            "Чтобы показать карты Яндекса, задайте ключ API: блок **`[yandex_maps]`** с полем **`api_key`** "
            "в `.streamlit/secrets.toml` или переменную окружения **YANDEX_MAPS_API_KEY**."
        )
        st.code(
            "[yandex_maps]\napi_key = \"ваш_ключ\"\n\n# или: export YANDEX_MAPS_API_KEY=...\n",
            language="toml",
        )

    _section_anchor("yamap-vo-by-event")
    st.subheader("Вологодская область: участники по событиям")
    y_show = html.escape(str(year_pick))
    sp_show = html.escape(str(sport_pick))
    st.markdown(
        f"<p style=\"margin:0 0 6px 0;color:{VM_TEXT};font-size:0.9rem\">"
        f"Выбрано (плашки в начале страницы): год <strong>{y_show}</strong>, вид спорта <strong>{sp_show}</strong></p>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Выделите строку **события** в таблице — ниже список **уникальных участников из Вологодской области** по району "
        "(город и район из справочника, как в блоке «Районы ВО» выше)."
    )
    ev_pick_rows = mq.query_competitions_for_geography_event_picker(path, year=year_val, sport=sport_val)
    if not ev_pick_rows:
        yr_s = str(year_val) if year_val is not None else "все годы"
        sp_s = str(sport_val) if sport_val else "все виды"
        st.info(f"Нет соревнований с результатами для выбранных фильтров (**{yr_s}**, **{sp_s}**).")
    else:
        df_ev = pd.DataFrame(
            [
                {
                    "competition_id": int(r["competition_id"]),
                    "Дата": str(r.get("date_raw") or "—"),
                    "Событие": str(r.get("event_label") or "—"),
                    "Вид спорта": str(r.get("sport") or "—"),
                    "Участников": int(r.get("participants") or 0),
                }
                for r in ev_pick_rows
            ]
        )
        display_cols = ["Дата", "Событие", "Вид спорта", "Участников"]
        st.caption("Выделите строку события — таблица по районам обновится.")
        ev_key = f"geo_vo_event_rows_{year_val}_{sport_val or 'all'}"
        ev_ht = min(420, 74 + len(df_ev) * 36)
        ev_sel = st.dataframe(
            df_ev[display_cols],
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=ev_key,
            height=ev_ht,
        )
        ev_idx: int | None = None
        if ev_sel.selection.rows:
            ev_idx = int(ev_sel.selection.rows[0])
        if ev_idx is None:
            ev_idx = 0
        sel_cid = int(df_ev.iloc[ev_idx]["competition_id"])
        sel_title = str(df_ev.iloc[ev_idx]["Событие"] or f"#{sel_cid}")
        sel_date = str(df_ev.iloc[ev_idx]["Дата"] or "—")

        st.markdown(
            f'<p style="color:{VM_TEXT};font-weight:600;margin:12px 0 6px 0;">'
            f"{html.escape(sel_date)} · {html.escape(sel_title)}</p>",
            unsafe_allow_html=True,
        )
        br = mq.query_vm_vologda_rayons_for_competition(path, sel_cid)
        if not br:
            st.caption(
                "Нет участников с профилем **Вологодской области** по этому событию (или не удалось сопоставить город с районом)."
            )
        else:
            df_br = pd.DataFrame(br)
            df_show = df_br.rename(
                columns={
                    "district": "Район",
                    "participants_m": "Участников М",
                    "participants_f": "Участников Ж",
                    "participants": "Участников всего",
                }
            )
            st.dataframe(
                df_show[["Район", "Участников М", "Участников Ж", "Участников всего"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Участников М": st.column_config.NumberColumn(format="%d"),
                    "Участников Ж": st.column_config.NumberColumn(format="%d"),
                    "Участников всего": st.column_config.NumberColumn(format="%d"),
                },
            )


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
        st.caption(
            "Иерархия: **событие → дистанция → топ-5 мужчин и топ-5 женщин (по всем годам)**. "
            "Для бега и трэйла колонка **Темп** — мин:сек на км; километраж берётся из справочника алиасов дистанций."
        )
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


def _admin_route_slug_from_nested(adm: Any) -> str | None:
    """[admin].route_slug из dict / Mapping / объекта Streamlit Secrets."""
    if adm is None:
        return None
    rs_any: Any
    if isinstance(adm, dict):
        rs_any = adm.get("route_slug")
    elif isinstance(adm, Mapping):
        rs_any = adm.get("route_slug")
    else:
        rs_any = getattr(adm, "route_slug", None)
    rs = str(rs_any or "").strip()
    return rs or None


def _read_route_slug_local_secrets_files() -> str | None:
    """Если st.secrets не нашёл файл (cwd, локальный деплой). Обходим Streamlit-секреты."""
    roots: list[Path] = [APP_DIR.resolve(), Path.cwd().resolve()]
    tried: set[str] = set()
    paths: list[Path] = []
    for root in roots:
        cur = root
        for _ in range(8):
            candidate = (cur / ".streamlit" / "secrets.toml").resolve()
            sid = str(candidate)
            if sid not in tried:
                tried.add(sid)
                paths.append(candidate)
            parent = cur.parent
            if parent == cur:
                break
            cur = parent
    for secrets_path in paths:
        if not secrets_path.is_file():
            continue
        try:
            with secrets_path.open("rb") as fp:
                data = tomllib.load(fp)
            rs = _admin_route_slug_from_nested(data.get("admin"))
            if rs:
                return rs
        except (OSError, TypeError, UnicodeDecodeError, tomllib.TOMLDecodeError):
            continue
    return None


def _resolve_admin_route_slug() -> str | None:
    """
    Секретный сегмент URL: ?page=<slug> открывает панель администратора без пункта в меню.
    Задаётся VMSTAT_ADMIN_ROUTE или [admin] route_slug в .streamlit/secrets.toml.
    """
    env_sl = str(os.environ.get("VMSTAT_ADMIN_ROUTE", "")).strip()
    if env_sl:
        return env_sl

    if hasattr(st, "secrets"):
        try:
            rs = _admin_route_slug_from_nested(st.secrets.get("admin"))
            if rs:
                return rs
        except StreamlitSecretNotFoundError:
            pass
        except Exception:
            pass

    return _read_route_slug_local_secrets_files()


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


def _dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Данные") -> bytes:
    buf = BytesIO()
    safe_name = sheet_name[:31]
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_name)
    buf.seek(0)
    return buf.getvalue()


def _dataframes_to_excel_bytes(sheets: list[tuple[str, pd.DataFrame]]) -> bytes:
    """Несколько листов Excel; имя листа ≤ 31 символа."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets:
            safe = sheet_name[:31]
            df.to_excel(writer, index=False, sheet_name=safe)
    buf.seek(0)
    return buf.getvalue()


_MAX_EXCEL_BYTES_FOR_DATA_URL = 1_500_000


def _html_excel_download_anchor_png(
    file_bytes: bytes,
    file_name: str,
    icon_path: Path,
    mime: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
) -> str:
    """Ссылка с PNG: скачивание через data: URL (умеренный размер файла)."""
    img_b64 = base64.standard_b64encode(icon_path.read_bytes()).decode("ascii")
    xlsx_b64 = base64.standard_b64encode(file_bytes).decode("ascii")
    fname_attr = html.escape(file_name, quote=True)
    return (
        f'<a href="data:{mime};base64,{xlsx_b64}" download="{fname_attr}" '
        'title="Выгрузить в Excel" style="display:inline-block;line-height:0;">'
        '<img src="data:image/png;base64,'
        + img_b64
        + '" width="44" style="height:auto;display:block;border:0;" '
        'alt="" /></a>'
    )


def _excel_download_button_png_or_fallback(
    file_bytes: bytes,
    file_name: str,
    *,
    streamlit_key: str,
) -> None:
    """Выгрузка Excel: кликабельное лого PNG или встроенная кнопка при большом файле / без картинки."""
    if (
        EXCEL_EXPORT_ICON_PNG.is_file()
        and len(file_bytes) <= _MAX_EXCEL_BYTES_FOR_DATA_URL
    ):
        components.html(
            _html_excel_download_anchor_png(file_bytes, file_name, EXCEL_EXPORT_ICON_PNG),
            height=54,
            width=56,
        )
        return
    st.download_button(
        label="",
        icon=":material/table_chart:",
        help="Выгрузить в Excel",
        data=file_bytes,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=streamlit_key,
        type="tertiary",
        width="content",
    )


def _team_scoring_stage_rows_mark_event_top5(stage_rows: list[dict[str, Any]]) -> None:
    """Помечает строки, вошедшие в топ‑5 результатов команды на этом competition_id (в зачёт события)."""
    from collections import defaultdict

    by_team_ev: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for r in stage_rows:
        tn = str(r.get("команда") or "").strip()
        cid = r.get("competition_id")
        if not tn or cid is None:
            continue
        by_team_ev[(tn, int(cid))].append(r)
    for lst in by_team_ev.values():
        lst.sort(
            key=lambda x: (
                -int(x.get("очков_в_командный") or 0),
                int(x.get("profile_id") or 0),
            )
        )
        top_pids = {int(x.get("profile_id") or 0) for x in lst[:5]}
        for r in lst:
            pid = int(r.get("profile_id") or 0)
            r["в_топ5_события"] = "да" if pid in top_pids else ""


def _cup_team_flat_excel_team_scoring_v1(
    cup_title: str,
    cup_id: int,
    year: int,
    team_totals: list[dict[str, Any]],
    member_totals: list[dict[str, Any]],
) -> pd.DataFrame:
    """Одна строка на связку команда × участник (расчёт team_scoring)."""
    from collections import defaultdict

    ordered_teams = sorted(
        team_totals,
        key=lambda r: (-int(r.get("очков") or 0), str(r.get("команда") or "")),
    )
    team_place_by_name: dict[str, int] = {}
    team_pts_by_name: dict[str, int] = {}
    for i, row in enumerate(ordered_teams, start=1):
        tn = str(row.get("команда") or "").strip()
        if not tn:
            continue
        team_place_by_name[tn] = i
        team_pts_by_name[tn] = int(row.get("очков") or 0)

    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in member_totals:
        t = str(m.get("команда") or "").strip()
        by_team[t].append(m)

    out_rows: list[dict[str, Any]] = []
    for team_name, members in sorted(by_team.items(), key=lambda x: (-team_pts_by_name.get(x[0], 0), x[0])):
        members_sorted = sorted(
            members,
            key=lambda r: (-float(r.get("очков_7из8") or 0.0), str(r.get("участник") or "")),
        )
        tpl = team_place_by_name.get(team_name)
        tpts = team_pts_by_name.get(team_name)
        for rk, mr in enumerate(members_sorted, start=1):
            scj = mr.get("stages_counted_json")
            if scj is not None and not isinstance(scj, str):
                try:
                    scj = json.dumps(scj, ensure_ascii=False)
                except TypeError:
                    scj = str(scj)
            out_rows.append(
                {
                    "Год": year,
                    "Кубок (id)": cup_id,
                    "Наименование кубка": cup_title,
                    "Место команды": tpl,
                    "Команда": team_name,
                    "Очки команды (сумма по событиям)": tpts,
                    "Место участника в команде": rk,
                    "В топ‑5 хотя бы одного события": "Да"
                    if int(mr.get("очков_7из8") or 0) > 0
                    else "Нет",
                    "profile_id": mr.get("profile_id"),
                    "Участник": mr.get("участник"),
                    "Очки в командный (топ‑5 на событиях)": mr.get("очков_7из8"),
                    "Сумма очков по всем событиям (все результаты)": mr.get(
                        "очков_всего"
                    ),
                    "Этапы (JSON)": scj if scj is not None else "",
                }
            )
    return pd.DataFrame(out_rows)


def _cup_team_stage_rows_excel_team_scoring_v1(
    cup_title: str,
    cup_id: int,
    year: int,
    team_totals: list[dict[str, Any]],
    stage_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Одна строка на участника × этап: название события и комментарий про бонус 50+
    (как во вкладке / в team_scoring_stage_points).
    """
    ordered_teams = sorted(
        team_totals,
        key=lambda r: (-int(r.get("очков") or 0), str(r.get("команда") or "")),
    )
    team_place_by_name: dict[str, int] = {}
    for i, row in enumerate(ordered_teams, start=1):
        tn = str(row.get("команда") or "").strip()
        if not tn:
            continue
        team_place_by_name[tn] = i

    def _stage_sort_key(sr: dict[str, Any]) -> tuple[str, str, int]:
        st = int(sr.get("этап") or 0)
        tm = str(sr.get("команда") or "")
        ua = str(sr.get("участник") or "")
        return (tm, ua, st)

    stage_wrk = [dict(x) for x in stage_rows]
    _team_scoring_stage_rows_mark_event_top5(stage_wrk)

    out_rows: list[dict[str, Any]] = []
    for sr in sorted(stage_wrk, key=_stage_sort_key):
        tn = str(sr.get("команда") or "").strip()
        bonus_txt = str(sr.get("комментарий") or "").strip()
        ev = sr.get("событие")
        out_rows.append(
            {
                "Год": year,
                "Кубок (id)": cup_id,
                "Наименование кубка": cup_title,
                "Место команды": team_place_by_name.get(tn),
                "Команда": tn or None,
                "profile_id": sr.get("profile_id"),
                "Участник": sr.get("участник"),
                "Этап": sr.get("этап"),
                "Наименование события": ev if ev is not None and str(ev).strip() != "" else "",
                "competition_id": sr.get("competition_id"),
                "Место абсолют": sr.get("место_абс"),
                "Очки базовые": sr.get("очков_база"),
                "Очки бонус": sr.get("очков_бонус"),
                "Очков в командный зачёт": sr.get("очков_в_командный"),
                "В топ‑5 этого события": sr.get("в_топ5_события") or "",
                "Комментарий (бонус 50+)": bonus_txt,
                "Дистанция": sr.get("дистанция"),
                "Время": sr.get("время"),
            }
        )
    if not out_rows:
        return pd.DataFrame(
            columns=[
                "Год",
                "Кубок (id)",
                "Наименование кубка",
                "Место команды",
                "Команда",
                "profile_id",
                "Участник",
                "Этап",
                "Наименование события",
                "competition_id",
                "Место абсолют",
                "Очки базовые",
                "Очки бонус",
                "Очков в командный зачёт",
                "В топ‑5 этого события",
                "Комментарий (бонус 50+)",
                "Дистанция",
                "Время",
            ]
        )
    return pd.DataFrame(out_rows)


def _cup_team_flat_excel_legacy(
    cup_title: str,
    cup_id: int,
    year: int,
    teams_agg: list[tuple[str, int, list[dict]]],
) -> pd.DataFrame:
    """Одна строка на связку команда × участник (старая схема по profile_cup_results)."""
    rows_out: list[dict[str, Any]] = []
    for team_place, (team, team_total_pts, members) in enumerate(teams_agg, start=1):
        ranked = _team_participants_ranked_with_rows(members)
        for rk, (_pid, pname, ptot, _rows_p) in enumerate(ranked, start=1):
            rows_out.append(
                {
                    "Год": year,
                    "Кубок (id)": cup_id,
                    "Наименование кубка": cup_title,
                    "Место команды": team_place,
                    "Команда": team,
                    "Очки команды (топ‑5, итог)": team_total_pts,
                    "Место участника в команде": rk,
                    "В топ‑5 участников команды": "Да" if rk <= 5 else "Нет",
                    "profile_id": _pid,
                    "Участник": pname,
                    "Сумма очков участника (по строкам кубка)": ptot,
                }
            )
    return pd.DataFrame(rows_out)


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


def _individual_finish_lines_points_table(
    db_path: Path,
    profile_id: int,
    cup_id: int,
    year: int,
    lines: list[dict[str, Any]],
    stage_ix_map: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """По финишам (или запасному списку) — строки этап/дистанция/событие/место/очки и сумма для сводки."""
    enriched: list[dict[str, Any]] = []
    for ln in lines:
        el = dict(ln)
        if el.get("competition_id") is None:
            cix = mq.parse_profile_cup_raw_competition_id(el.get("raw"))
            if cix is not None:
                el["competition_id"] = cix
        enriched.append(el)
    pts_by_cid = mq.map_profile_cup_points_by_competition_id(
        db_path, profile_id, cup_id, year
    )
    pts_by_td = mq.map_profile_cup_points_by_title_distance(
        db_path, profile_id, cup_id, year
    )
    assign_pts = (
        mq.assign_profile_cup_points_to_result_lines(
            db_path, profile_id, cup_id, year, enriched
        )
        if enriched and all(el.get("competition_id") is not None for el in enriched)
        else None
    )
    ix = stage_ix_map if stage_ix_map is not None else mq.load_stage_index_map()
    inner: list[dict[str, Any]] = []
    total_pts = 0
    for i, line in enumerate(enriched):
        el = dict(line)
        ev = (el.get("событие") or "").strip()
        if not ev:
            ev = mq.parse_profile_cup_raw_event_title(el.get("raw"))
        if not ev:
            ev = "—"
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
        place = el.get("место_абс")
        place_s = str(place).strip() if place is not None and str(place).strip() != "" else "—"
        dist = (el.get("дистанция") or "").strip() or "—"
        stage_disp = "—"
        cid_ev = el.get("competition_id")
        if cid_ev is not None and ix:
            try:
                cix_ev = int(cid_ev)
                if cix_ev in ix:
                    stage_disp = str(ix[cix_ev])
            except (TypeError, ValueError):
                pass
        pts_disp = str(pts) if pts is not None else "—"
        ftime = str(el.get("время") or "").strip() or "—"
        inner.append(
            {
                "этап": stage_disp,
                "дистанция": dist,
                "событие": ev,
                "место_абс": place_s,
                "время": ftime,
                "очки": pts_disp,
                "_pts_int": int(pts) if pts is not None else None,
            }
        )
        if pts is not None:
            total_pts += int(pts)
    return inner, total_pts


def _cup_individual_championship_hierarchy_html(
    db_path: Path,
    cup_id: int,
    year: int,
    filtered_detail_rows: list[dict[str, Any]],
) -> str:
    """Личное первенство: участник → сумма очков; разбивка по всем финишам в событиях кубка (results), без привязки к команде."""
    from collections import defaultdict

    pcr_by_pid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    names: dict[int, str] = {}
    for row in filtered_detail_rows:
        pid_raw = row.get("profile_id")
        if pid_raw is None:
            continue
        try:
            pid = int(pid_raw)
        except (TypeError, ValueError):
            continue
        nm = str(row.get("участник") or "").strip()
        if nm:
            names[pid] = nm
        pcr_by_pid[pid].append(row)

    st_ix = mq.load_stage_index_map()
    agg: list[tuple[int, str, int, list[dict[str, Any]]]] = []
    for pid, p_rows in pcr_by_pid.items():
        name = names.get(pid) or (str(p_rows[0].get("участник") or "").strip() if p_rows else "") or f"id {pid}"

        finish_lines = mq.query_profile_cup_finishes_for_participant(db_path, pid, cup_id, year)
        if finish_lines:
            inner_raw, total = _individual_finish_lines_points_table(
                db_path, pid, cup_id, year, finish_lines, stage_ix_map=st_ix
            )
        else:
            pcr_member = mq.query_profile_cup_results_lines_for_member(db_path, pid, cup_id, year)
            if not pcr_member and p_rows:
                pcr_member = []
                for r in p_rows:
                    cid = mq.parse_profile_cup_raw_competition_id(r.get("raw"))
                    pcr_member.append(
                        {
                            "событие": (
                                str(r.get("событие") or "").strip()
                                or mq.parse_profile_cup_raw_event_title(r.get("raw"))
                                or "—"
                            ),
                            "дистанция": str(r.get("дистанция") or "").strip(),
                            "место_абс": (
                                r.get("cup_place_abs")
                                if r.get("cup_place_abs") is not None
                                else r.get("pcr_place_abs")
                            ),
                            "очков": r.get("очков"),
                            "время": "",
                            "raw": r.get("raw"),
                            "competition_id": cid,
                        }
                    )
            inner_raw, total = (
                _individual_finish_lines_points_table(
                    db_path, pid, cup_id, year, pcr_member, stage_ix_map=st_ix
                )
                if pcr_member
                else ([], 0)
            )
        inner_disp = [{k: v for k, v in ir.items() if k != "_pts_int"} for ir in inner_raw]
        agg.append((pid, name, total, inner_disp))
    agg.sort(key=lambda x: (-x[2], x[1].casefold()))
    leader_pts = int(agg[0][2]) if agg else 0
    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>Место</span><span>Участник</span><span>Очки</span><span>Отставание</span></div>
"""
    ]
    for rank, (_pid, name, total_pts, inner_rows) in enumerate(agg, start=1):
        gap_v = max(0, leader_pts - int(total_pts))
        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>'
            f"{_esc_html(rank)}</span>"
            f"<span>{_esc_html(name)}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(total_pts)}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(gap_v)}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        parts.append("<table class='vm-cup-ev'><thead><tr>")
        for h in ("Этап", "Дистанция", "Событие", "Место абсолют", "Время", "Очки"):
            parts.append(f"<th>{_esc_html(h)}</th>")
        parts.append("</tr></thead><tbody>")
        for ir in inner_rows:
            parts.append(
                "<tr>"
                f"<td>{_esc_html(ir.get('этап'))}</td>"
                f"<td>{_esc_html(ir.get('дистанция'))}</td>"
                f"<td>{_esc_html(ir.get('событие'))}</td>"
                f"<td>{_esc_html(ir.get('место_абс'))}</td>"
                f"<td>{_esc_html(ir.get('время'))}</td>"
                f"<td>{_esc_html(ir.get('очки'))}</td>"
                "</tr>"
            )
        parts.append("</tbody></table></div></details>")
    parts.append("</div>")
    return "".join(parts)


def _cup_individual_base_scoring_hierarchy_html(
    db_path: Path,
    cup_id: int,
    year: int,
    filtered_detail_rows: list[dict[str, Any]],
    display_limit: int,
) -> tuple[str, int, int]:
    """
    Личное первенство: сумма базовых очков (как в командном зачёте, без бонуса 50+),
    сортировка по убыванию. display_limit — максимум участников в разметке.
    Возвращает (html, показано, всего по фильтру).
    """
    from collections import defaultdict

    pcr_by_pid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    names: dict[int, str] = {}
    for row in filtered_detail_rows:
        pid_raw = row.get("profile_id")
        if pid_raw is None:
            continue
        try:
            pid = int(pid_raw)
        except (TypeError, ValueError):
            continue
        nm = str(row.get("участник") or "").strip()
        if nm:
            names[pid] = nm
        pcr_by_pid[pid].append(row)

    by_pid_stages = mq.compute_individual_cup_base_rows_by_participant(
        db_path, cup_id, year
    )
    agg: list[tuple[int, str, int, list[dict[str, Any]]]] = []
    for pid, p_rows in pcr_by_pid.items():
        name = (
            names.get(pid)
            or (str(p_rows[0].get("участник") or "").strip() if p_rows else "")
            or f"id {pid}"
        )
        rows = by_pid_stages.get(pid, [])
        total = int(sum(int(r.get("очков_база") or 0) for r in rows))
        inner_disp: list[dict[str, Any]] = []
        for r in rows:
            pa = r.get("место_абс")
            try:
                pai = int(pa) if pa is not None else 0
            except (TypeError, ValueError):
                pai = 0
            place_s = str(pai) if pai > 0 else "—"
            inner_disp.append(
                {
                    "этап": str(r.get("этап") or "—"),
                    "дистанция": str(r.get("дистанция") or "—"),
                    "событие": str(r.get("событие") or "—"),
                    "место_абс": place_s,
                    "время": str(r.get("время") or "—").strip() or "—",
                    "очки": str(int(r.get("очков_база") or 0)),
                }
            )
        agg.append((pid, name, total, inner_disp))

    agg.sort(key=lambda x: (-x[2], x[1].casefold()))
    total_filtered = len(agg)
    lim = max(0, int(display_limit))
    agg_slice = agg[:lim] if lim else agg

    if not agg_slice:
        empty_msg = (
            "<p>Нет участников по фильтру.</p>"
            if total_filtered == 0
            else "<p>Нет данных для отображения.</p>"
        )
        return (empty_msg, 0, total_filtered)

    leader_pts = int(agg_slice[0][2])
    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>Место</span><span>Участник</span><span>Очки</span><span>Отставание</span></div>
"""
    ]
    for rank, (_pid, name, total_pts, inner_rows) in enumerate(agg_slice, start=1):
        gap_v = max(0, leader_pts - int(total_pts))
        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>'
            f"{_esc_html(rank)}</span>"
            f"<span>{_esc_html(name)}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(total_pts)}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(gap_v)}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        parts.append("<table class='vm-cup-ev'><thead><tr>")
        for h in (
            "Этап",
            "Дистанция",
            "Событие",
            "Место, абсолют",
            "Время",
            "Очки (база)",
        ):
            parts.append(f"<th>{_esc_html(h)}</th>")
        parts.append("</tr></thead><tbody>")
        for ir in inner_rows:
            parts.append(
                "<tr>"
                f"<td>{_esc_html(ir.get('этап'))}</td>"
                f"<td>{_esc_html(ir.get('дистанция'))}</td>"
                f"<td>{_esc_html(ir.get('событие'))}</td>"
                f"<td>{_esc_html(ir.get('место_абс'))}</td>"
                f"<td>{_esc_html(ir.get('время'))}</td>"
                f"<td>{_esc_html(ir.get('очки'))}</td>"
                "</tr>"
            )
        if not inner_rows:
            parts.append(
                "<tr><td colspan=\"6\">"
                "<em>Нет зачётных этапов по правилам базовых очков</em>"
                "</td></tr>"
            )
        parts.append("</tbody></table></div></details>")
    parts.append("</div>")
    return ("".join(parts), len(agg_slice), total_filtered)


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
            for h in ("Пол", "Место", "Год", "Этап", "Участник", "Время", "Темп"):
                parts.append(f"<th>{_esc_html(h)}</th>")
            parts.append("</tr></thead><tbody>")
            for row in dist_rows:
                place_raw = row.get("Место")
                place_s = str(place_raw) if place_raw is not None else "—"
                if place_s == "1":
                    place_s = "👑 1"
                tempo = row.get("Темп") if row.get("Темп") is not None else "—"
                parts.append(
                    "<tr>"
                    f"<td>{_esc_html(row.get('Пол'))}</td>"
                    f"<td>{_esc_html(place_s)}</td>"
                    f"<td>{_esc_html(row.get('Год'))}</td>"
                    f"<td>{_esc_html(row.get('Этап'))}</td>"
                    f"<td>{_esc_html(row.get('Участник'))}</td>"
                    f"<td>{_esc_html(row.get('Время'))}</td>"
                    f"<td>{_esc_html(tempo)}</td>"
                    "</tr>"
                )
            parts.append("</tbody></table></details>")
        parts.append("</div></details>")
    parts.append("</div>")
    return "".join(parts)


def _team_scoring_hierarchy_html(
    teams_rows: list[dict[str, Any]],
    _members_rows: list[dict[str, Any]],
    stage_rows: list[dict[str, Any]],
) -> str:
    """Иерархия командного зачёта: команда → событие → участник (детализация очков)."""
    from collections import defaultdict

    stage_rows = [dict(x) for x in stage_rows]
    _team_scoring_stage_rows_mark_event_top5(stage_rows)

    by_team_comp: dict[str, dict[int, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for r in stage_rows:
        team = str(r.get("команда") or "").strip()
        if not team:
            continue
        cid = r.get("competition_id")
        if cid is None:
            continue
        try:
            ci = int(cid)
        except (TypeError, ValueError):
            continue
        by_team_comp[team][ci].append(r)

    parts: list[str] = [
        """<div class="vm-cup-tree" role="tree">
<div class="vm-cup-head"><span>Место</span><span>Команда</span><span>Очки</span><span>Событий</span></div>
"""
    ]
    for rank, team_row in enumerate(teams_rows, start=1):
        team = str(team_row.get("команда") or "").strip()
        team_pts = int(team_row.get("очков") or 0)
        ev_map = by_team_comp.get(team, {})
        n_events = len(ev_map)
        parts.append(
            f'<details class="vm-cup-team"><summary>'
            f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-t" aria-hidden="true"></span>{_esc_html(rank)}</span>'
            f"<span>{_esc_html(team)}</span>"
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(team_pts)}</span>'
            f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(n_events)}</span>'
            f"</summary><div class='vm-cup-team-body'>"
        )
        if not ev_map:
            parts.append("<p style='margin:0;padding:4px 0;color:#666;font-size:0.88rem;'>Нет строк этапов.</p>")
            parts.append("</div></details>")
            continue

        event_items: list[tuple[int, int, str, list[dict[str, Any]], int, int]] = []
        for cid, lst in ev_map.items():
            mn_st = min(int(x.get("этап") or 0) for x in lst)
            title = str(lst[0].get("событие") or "").strip() or f"Событие #{cid}"
            sorted_for_top5 = sorted(
                lst,
                key=lambda x: (
                    -int(x.get("очков_в_командный") or 0),
                    int(x.get("profile_id") or 0),
                ),
            )
            top5_sum = sum(
                int(x.get("очков_в_командный") or 0) for x in sorted_for_top5[:5]
            )
            event_items.append((mn_st, cid, title, lst, top5_sum, len(lst)))
        event_items.sort(key=lambda x: (x[0], x[1], x[2].casefold()))

        for _mn_st, cid, title, lst, evt_top5_sum, n_part in event_items:
            sorted_participants = sorted(
                lst,
                key=lambda x: (
                    -int(x.get("очков_в_командный") or 0),
                    int(x.get("profile_id") or 0),
                ),
            )
            parts.append(
                "<details class='vm-cup-event'><summary>"
                f'<span class="vm-cup-rank-cell"><span class="vm-cup-caret-e" aria-hidden="true"></span></span>'
                f"<span>{_esc_html(title)}</span>"
                f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(evt_top5_sum)}</span>'
                f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(n_part)}</span>'
                "</summary>"
                "<div class='vm-cup-event-body'>"
            )
            for i, s in enumerate(sorted_participants):
                pid = int(s.get("profile_id") or 0)
                name = str(s.get("участник") or f"id {pid}")
                pts_z = int(s.get("очков_в_командный") or 0)
                is_top5 = i < 5
                p_cls = (
                    "vm-cup-ts-participant vm-cup-ts-top5"
                    if is_top5
                    else "vm-cup-ts-participant vm-cup-ts-flat"
                )
                badge = (
                    '<span class="vm-cup-badge">топ‑5 события</span>'
                    if is_top5
                    else '<span class="vm-cup-badge vm-cup-badge-muted">вне топ‑5</span>'
                )
                parts.append(
                    f"<details class='{p_cls}'><summary>"
                    f'<span class="vm-cup-name-cell"><span class="vm-cup-caret-m" aria-hidden="true"></span>'
                    f"{_esc_html(name)}</span>"
                    f'<span style="text-align:right;font-variant-numeric:tabular-nums;">{_esc_html(pts_z)}</span>'
                    f"{badge}</summary>"
                )
                parts.append("<table class='vm-cup-ts'><thead><tr>")
                for h in (
                    "Этап",
                    "Место, абсолют",
                    "Очки (база)",
                    "Бонус",
                    "Очки (зачёт)",
                    "Комментарий",
                    "Дистанция",
                    "Время",
                ):
                    parts.append(f"<th>{_esc_html(h)}</th>")
                parts.append("</tr></thead><tbody>")
                parts.append(
                    "<tr>"
                    f"<td>{_esc_html(s.get('этап'))}</td>"
                    f"<td>{_esc_html(s.get('место_абс'))}</td>"
                    f"<td>{_esc_html(s.get('очков_база'))}</td>"
                    f"<td>{_esc_html(s.get('очков_бонус'))}</td>"
                    f"<td>{_esc_html(s.get('очков_в_командный'))}</td>"
                    f"<td>{_esc_html(s.get('комментарий'))}</td>"
                    f"<td>{_esc_html(s.get('дистанция'))}</td>"
                    f"<td>{_esc_html(s.get('время'))}</td>"
                    "</tr>"
                )
                parts.append("</tbody></table></details>")
            parts.append("</div></details>")
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
    norm_km_total = mq.query_profile_norm_km_total(
        path, pid, years=years_for_query, sports=sports_for_query
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
    km_row = st.columns(5, gap="small")
    with km_row[0]:
        metric_plaque("Общий километраж", int(round(norm_km_total)))
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:4px 0 8px 0;">'
        f"«Общий километраж» — сумма по справочнику <b>norm_distances</b> (файл <code>бд/norm_distances.csv</code> "
        f"при первом запуске); сопоставление по <b>competition_id</b> и названию дистанции. "
        f"Карточка «Км» — километраж из таблицы <b>distances</b> в БД.</p>"
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 10px 0;">'
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
    tab_ind, tab_team, tab_team_champ, tab_stage_rating = st.tabs(
        [
            "Личное первенство",
            "Командный зачёт",
            "Командное первенство",
            "Рейтинг по этапам",
        ]
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
            n_participants = len(
                {
                    int(r["profile_id"])
                    for r in filtered
                    if r.get("profile_id") is not None
                    and str(r.get("profile_id")).strip() != ""
                }
            )
            lim_key = f"cups_indiv_limit_{year}_{cup_id}"
            if mq.is_team_scoring_enabled(cup_id, year):
                if lim_key not in st.session_state:
                    st.session_state[lim_key] = 100
                lim = int(st.session_state[lim_key])
                st.caption(
                    f"Участников в списке: **{n_participants}** (есть хотя бы одна строка **profile_cup_results** под фильтры; всего строк PCR: "
                    f"**{len(filtered)}** из **{len(detail)}**). **Очки** — **базовые**, как колонка **Очки (база)** на вкладке «Командный зачёт» "
                    "(этап, дистанция, место: **место в зачёте пола**, если есть, иначе **место абсолют**), **без** бонуса +15 за группу 50+ и без усечения по потолку 1-го места. "
                    "На каждый этап — один лучший финиш участника по величине базовых очков. Сортировка по **убыванию** суммы базовых очков."
                )
                with st.spinner("Загрузка…"):
                    frag_ind, n_shown, n_total = _cup_individual_base_scoring_hierarchy_html(
                        path, cup_id, year, filtered, lim
                    )
                if hasattr(st, "html"):
                    st.html(frag_ind)
                else:
                    st.markdown(frag_ind, unsafe_allow_html=True)
                if n_total > 0:
                    if n_shown < n_total:
                        st.caption(
                            f"Отображено **{n_shown}** участников из **{n_total}** (по фильтру)."
                        )
                        if st.button(
                            "Загрузить еще 50 участников",
                            key=f"cups_ind_more_{year}_{cup_id}",
                            use_container_width=True,
                        ):
                            st.session_state[lim_key] = lim + 50
                            st.rerun()
                    else:
                        st.caption(f"Показаны все **{n_total}** участников по фильтру.")
            else:
                st.caption(
                    f"Участников в списке: **{n_participants}** (есть хотя бы одна строка **profile_cup_results** под фильтры; всего строк PCR: "
                    f"**{len(filtered)}** из **{len(detail)}**). **Очки** в строке участника — сумма по **непустым** очкам во вложенной таблице. "
                    f"При раскрытии строки показываются **все финиши этого участника** в событиях кубка за год (**results** через **cup_competitions**) "
                    f"или, если финишей нет, строки PCR; столбцы: **Этап** (дистанция), **Событие**, **Место абсолют**, **Очки** — разрешение очков как на вкладке «Командный зачёт»."
                )
                with st.spinner("Загрузка…"):
                    frag_ind = _cup_individual_championship_hierarchy_html(
                        path, cup_id, year, filtered
                    )
                if hasattr(st, "html"):
                    st.html(frag_ind)
                else:
                    st.markdown(frag_ind, unsafe_allow_html=True)

    with tab_team:
        if int(cup_id) == 54 and int(year) == 2026:
            st.caption(
                "Логика расчёта: очки по месту в абсолюте в разрезе пола зависят от этапа и дистанции. "
                "Этапы 1–6: 7+ км (600/598/596..., с 7 места шаг -1), 5–6 км (598/596/..., с 6 места шаг -1). "
                "Этапы 7–8: 10–21 км (602/600/599..., далее шаг -1), 5 км (598/596/..., с 6 места шаг -1), "
                "на 2–3 км очки не начисляются. Для 50+ добавляется +15 очков в зачёт результата, "
                "но итог за результат не выше очков 1 места. По **каждому событию** (соревнованию) в счёт команды "
                "идут **пять лучших результатов** участников этой команды на этом событии; **итог команды** — "
                "сумма таких взносов по всем учитываемым событиям года."
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
                _, _team_dl = st.columns([4, 1])
                with _team_dl:
                    _df_team = _cup_team_flat_excel_team_scoring_v1(
                        cup_title, cup_id, year, team_tot, all_members
                    )
                    _df_stages = _cup_team_stage_rows_excel_team_scoring_v1(
                        cup_title, cup_id, year, team_tot, all_stages
                    )
                    _excel_download_button_png_or_fallback(
                        _dataframes_to_excel_bytes(
                            [
                                ("Участники", _df_team),
                                ("По этапам", _df_stages),
                            ]
                        ),
                        f"komandnyy_zachyot_{cup_id}_{year}.xlsx",
                        streamlit_key=f"cups_team_xlsx_v1_{year}_{cup_id}",
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
                    teams_agg = _cup_team_aggregate_points(score_rows)
                    st.caption(
                        "Сумма в командный зачёт = сумма **очков пяти лучших участников** "
                        "(очки из **profile_cup_results.total_points**, по каждому этапу — целое после округления). "
                        "Таблица: **место · команда · очки**; под участником — **все его финиши** в соревнованиях этого кубка за год "
                        "(**results** + **cup_competitions**)."
                    )
                    _, _team_dl_legacy = st.columns([4, 1])
                    with _team_dl_legacy:
                        _df_legacy = _cup_team_flat_excel_legacy(
                            cup_title, cup_id, year, teams_agg
                        )
                        _excel_download_button_png_or_fallback(
                            _dataframe_to_excel_bytes(_df_legacy, "Командный зачёт"),
                            f"komandnyy_zachyot_{cup_id}_{year}.xlsx",
                            streamlit_key=f"cups_team_xlsx_legacy_{year}_{cup_id}",
                        )
                    with st.spinner("Загрузка…"):
                        frag = _cup_team_hierarchy_html(
                            teams_agg, path, cup_id, year
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

    with tab_stage_rating:
        _section_anchor("cups-stage-rating")
        stages = mq.query_cup_team_stage_events_ordered(path, cup_id, year)
        st.caption(
            "Подвкладки соответствуют **названию (title)** соревнования этапа. "
            "**Очки** — сумма **пяти лучших** вкладов участников команды на этапе "
            "(тот же пересчёт, что вкладка «Командный зачёт», при доступности данных). "
            "**Отставание от лидера** — сколько очков команда набрала меньше победителя этапа."
        )

        boards: dict[int, list[dict[str, Any]]] = {}
        if stages and mq.is_team_scoring_enabled(int(cup_id), int(year)):
            stage_map_rating = mq.load_stage_index_map()
            if not stage_map_rating:
                st.warning("Не загружена карта этапов (.cursor/etapy.yaml); пересчёт командной таблицы невозможен.")
            else:
                mq.compute_team_scoring_for_cup_year(
                    path,
                    cup_id=int(cup_id),
                    year=int(year),
                    stage_map=stage_map_rating,
                    rule_version="team_v1",
                )
                cid_order = [int(s["competition_id"]) for s in stages]
                boards = mq.query_team_scoring_leaderboards_by_competition(
                    path,
                    int(cup_id),
                    int(year),
                    cid_order,
                    rule_version="team_v1",
                    top_k=5,
                )
        elif stages:
            st.info(
                "Таблицы с очками и отставанием доступны для кубков с пересчитанным командным зачётом "
                "в контуре **team_scoring** (сейчас **cup_id = 54**, **2026** год). Выберите другой кубок "
                "или вкладку «Командный зачёт», чтобы видеть свой формат данных."
            )

        if not stages:
            st.warning("Этапов кубка в базе за этот год не найдено — подвкладки не созданы.")
        else:
            _cnt_titles: dict[str, int] = {}
            _tab_labels: list[str] = []
            for _s in stages:
                _tid = str(_s.get("title") or "").strip()
                if not _tid:
                    _tid = f'Событие {int(_s["competition_id"])}'
                _cnt_titles[_tid] = _cnt_titles.get(_tid, 0) + 1
                _n = _cnt_titles[_tid]
                _lbl = (
                    _tid if _n == 1 else f'{_tid} · id {int(_s["competition_id"])}'
                )
                if len(_lbl) > 96:
                    _lbl = _lbl[:93].rstrip() + "..."
                _tab_labels.append(_lbl)

            stage_rating_subtabs = st.tabs(_tab_labels)
            for _s_tab, nav_sub in zip(stages, stage_rating_subtabs):
                _cid_ev = int(_s_tab["competition_id"])
                with nav_sub:
                    rows_lb = list(boards.get(_cid_ev) or [])
                    if not rows_lb:
                        if mq.is_team_scoring_enabled(int(cup_id), int(year)):
                            st.caption("На этом этапе нет зачётных строк в **team_scoring_stage_points**.")
                        pd_empty = pd.DataFrame(
                            columns=[
                                "Место",
                                "Команда",
                                "Очки",
                                "Отставание от лидера",
                            ]
                        )
                        st.dataframe(pd_empty, use_container_width=True, hide_index=True)
                    else:
                        dff_lb = pd.DataFrame(rows_lb).rename(
                            columns={
                                "место": "Место",
                                "команда": "Команда",
                                "очки": "Очки",
                                "отставание": "Отставание от лидера",
                            }
                        )
                        st.dataframe(dff_lb, use_container_width=True, hide_index=True)

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
        "в блоке «Рекорды события» (группировка топов по канонической дистанции). "
        "Колонка **км** — числовая фактическая дистанция для дальнейшего расчёта суммарного километража участников."
    )
    src = mq.distance_aliases_file_path()
    st.code(str(src), language=None)

    current_rows = mq.load_distance_alias_rules()
    if not current_rows:
        current_rows = mq.default_distance_alias_rules()
    df = pd.DataFrame(current_rows)
    for col in ("alias", "canonical_key", "canonical_label", "km", "active"):
        if col not in df.columns:
            if col == "active":
                df[col] = True
            elif col == "km":
                df[col] = pd.NA
            else:
                df[col] = ""
    df = df[["alias", "canonical_key", "canonical_label", "km", "active"]]

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
            "km": st.column_config.NumberColumn(
                "км",
                help="Фактическая дистанция, км (можно оставить пустым).",
                min_value=0.0,
                format="%.6g",
                step=0.001,
            ),
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

    _section_anchor("admin-norm-distances")
    st.subheader("Нормализованный километраж (событие × дистанция)")
    st.caption(
        "Используется в KPI участника («Общий километраж»): для каждого финиша подбирается строка по "
        "**competition_id** и имени дистанции (**distances.name**). Если совпадения нет — учитывается "
        "**distances.distance_km** из базы."
    )
    nd_csv = mq.norm_distances_csv_path()
    st.markdown("Исходный CSV (начальное наполнение при пустой таблице):", unsafe_allow_html=True)
    st.code(str(nd_csv), language=None)
    if require_db(path):
        mq.ensure_norm_distances_schema(path)
        nd_rows = mq.query_norm_distances_all(path)
        df_nd = pd.DataFrame(nd_rows)
        if df_nd.empty:
            st.warning(
                "Таблица **norm_distances** пуста. Положите **бд/norm_distances.csv** рядом с приложением "
                "(или задайте **MARATHON_NORM_DISTANCES_CSV**) и откройте страницу снова для импорта."
            )
        else:
            for col in ("id", "competition_id"):
                if col in df_nd.columns:
                    df_nd[col] = pd.to_numeric(df_nd[col], errors="coerce").astype("Int64")
            if "distance_km" in df_nd.columns:
                df_nd["distance_km"] = pd.to_numeric(df_nd["distance_km"], errors="coerce")
            df_nd = df_nd[["id", "competition_id", "title_short", "title", "name", "distance_km"]]
            nd_cc: dict[str, Any] = {
                "id": st.column_config.NumberColumn(
                    "id",
                    help="Пусто — новая строка (авто id).",
                    min_value=1,
                    step=1,
                    format="%d",
                ),
                "competition_id": st.column_config.NumberColumn(
                    "competition_id",
                    min_value=1,
                    step=1,
                    format="%d",
                ),
                "title_short": st.column_config.TextColumn("Кратко"),
                "title": st.column_config.TextColumn("Событие"),
                "name": st.column_config.TextColumn("Дистанция (как в distances.name)"),
                "distance_km": st.column_config.NumberColumn(
                    "distance_km",
                    min_value=0.0,
                    format="%.6g",
                    step=0.001,
                ),
            }
            edited_nd = st.data_editor(
                df_nd,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="admin_norm_distances_editor",
                column_config=nd_cc,
            )
            c_nd1, c_nd2 = st.columns([1, 3])
            with c_nd1:
                save_nd = st.button("Сохранить norm_distances", key="admin_save_norm_distances", type="primary")
            with c_nd2:
                st.caption(
                    "Удалённые в редакторе строки будут удалены из таблицы после сохранения. "
                    "Пара (competition_id, name) должна быть уникальна."
                )
            if save_nd:
                rec_nd = edited_nd.to_dict(orient="records")
                errs_nd = mq.save_norm_distances_admin_rows(path, rec_nd)
                if errs_nd:
                    for e in errs_nd[:50]:
                        st.error(e)
                    if len(errs_nd) > 50:
                        st.error(f"… ещё {len(errs_nd) - 50} сообщений.")
                else:
                    st.success(f"Сохранено строк: **{len(rec_nd)}**.")
                    st.rerun()

    _section_anchor("admin-city-aliases")
    st.subheader("Справочник алиасов городов")
    st.caption(
        "Используется для нормализации написаний городов в географических блоках "
        "(Интересные факты, Команда, Общая статистика)."
    )
    norm_src = mq.norm_city_aliases_file_path()
    st.markdown("Базовый каталог (**norm_city.csv**):", unsafe_allow_html=True)
    st.code(str(norm_src), language=None)
    overlay_src = mq.city_aliases_file_path()
    st.markdown("JSON-оверлей правок (**city_aliases.json**):", unsafe_allow_html=True)
    st.code(str(overlay_src), language=None)
    st.caption(
        "Подгружает **norm_city.csv** как основную таблицу; поверх можно хранить отличия в JSON. "
        "При сохранении в JSON записываются только строки, отличные от **norm_city.csv**, и новые алиасы."
    )

    city_rows = mq.load_city_alias_rules()
    if not city_rows:
        city_rows = mq.default_city_alias_rules()
    df_city = pd.DataFrame(city_rows)
    for col in ("alias", "canonical_key", "canonical_label", "canonical_city_id", "active"):
        if col not in df_city.columns:
            df_city[col] = "" if col != "active" else True
    df_city = df_city[
        ["alias", "canonical_key", "canonical_label", "canonical_city_id", "active"]
    ]

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
            "canonical_city_id": st.column_config.TextColumn(
                "Канонический city id", help="Совпадение города для учёта (как в norm_city)."
            ),
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
            st.success("Справочник городов сохранён (JSON-оверлей при необходимости).")
            st.rerun()

    _section_anchor("admin-region-aliases")
    st.subheader("Справочник алиасов регионов")
    st.caption(
        "Склеивает разные строки профилей (profiles.region): один канонический key и название для UI "
        "(Интересные факты, география ВМ, команда)."
    )
    norm_reg = mq.norm_region_aliases_file_path()
    st.markdown("Базовый каталог (**norm_region.csv**):", unsafe_allow_html=True)
    st.code(str(norm_reg), language=None)
    overlay_reg = mq.city_aliases_file_path()
    st.markdown("Тот же JSON (**city_aliases.json**), ключ **region_rules** — оверлей к базе CSV.", unsafe_allow_html=True)
    st.code(str(overlay_reg), language=None)

    region_rows = mq.load_region_alias_rules()
    if not region_rows:
        region_rows = mq.default_region_alias_rules()
    df_region = pd.DataFrame(region_rows)
    for col in ("region_alias", "canonical_key", "canonical_label", "active"):
        if col not in df_region.columns:
            df_region[col] = "" if col != "active" else True
    df_region = df_region[["region_alias", "canonical_key", "canonical_label", "active"]]

    edited_region = st.data_editor(
        df_region,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="admin_region_aliases_editor",
        column_config={
            "region_alias": st.column_config.TextColumn("region_alias (как в profiles.region)"),
            "canonical_key": st.column_config.TextColumn("Канонический key"),
            "canonical_label": st.column_config.TextColumn("Регион в UI"),
            "active": st.column_config.CheckboxColumn("Активно"),
        },
    )
    c1r, c2r = st.columns([1, 3])
    with c1r:
        save_region_clicked = st.button("Сохранить справочник регионов", key="admin_save_region_aliases")
    with c2r:
        st.caption("Один region_alias не может указывать на разные canonical_key / Регион в UI.")
    if save_region_clicked:
        rows_r = edited_region.to_dict(orient="records")
        errs_r = mq.save_region_alias_rules(rows_r)
        if errs_r:
            for e in errs_r:
                st.error(e)
        else:
            st.success("Справочник регионов сохранён (оверлей в city_aliases.json при необходимости).")
            st.rerun()

    _section_anchor("admin-vo-district-aliases")
    st.subheader("Справочник алиасов: районы Вологодской области")
    st.caption(
        "Привязывает подпись **district** в `config/vologda_districts.geojson` к колонке **Район** в "
        "**norm_city.csv** и задаёт общий **canonical_key** и **Название для UI** для агрегатов и карты. "
        "При первой инициализации пустой таблицы данные можно подставить из CSV (см. путь ниже) или они "
        "заполняются автоматически из GeoJSON + эвристика по строкам VO в norm_city."
    )
    voda_csv = mq.vo_district_aliases_csv_path()
    st.markdown("Начальный CSV для импорта в пустую таблицу БД:", unsafe_allow_html=True)
    st.code(str(voda_csv), language=None)
    voda_geo_hint = mq.vologda_rayon_geojson_path()
    st.markdown("Файл полигонов (поле `district`):", unsafe_allow_html=True)
    st.code(str(voda_geo_hint), language=None)
    if require_db(path):
        mq.ensure_vo_district_aliases_schema(path)
        voda_rows = mq.query_vo_district_aliases_all(path)
        df_voda = pd.DataFrame(voda_rows)
        if df_voda.empty:
            st.warning(
                "Таблица **vo_district_aliases** пуста после инициализации — добавьте **бд/vo_district_aliases.csv** "
                "или убедитесь, что на диске есть **vologda_districts.geojson** для автозаполнения."
            )
        else:
            cols_voda = ["id", "geojson_district", "norm_city_rayon", "canonical_key", "ui_label", "active"]
            for c in cols_voda:
                if c not in df_voda.columns:
                    if c == "id":
                        df_voda[c] = pd.NA
                    elif c == "active":
                        df_voda[c] = True
                    else:
                        df_voda[c] = ""
            if "id" in df_voda.columns:
                df_voda["id"] = pd.to_numeric(df_voda["id"], errors="coerce").astype("Int64")
            if "active" in df_voda.columns:
                df_voda["active"] = df_voda["active"].astype(bool)
            df_voda = df_voda[cols_voda]
            voda_cc: dict[str, Any] = {
                "id": st.column_config.NumberColumn(
                    "id",
                    help="Пустая ячейка — новая строка.",
                    min_value=1,
                    step=1,
                    format="%d",
                ),
                "geojson_district": st.column_config.TextColumn(
                    "Район (district в GeoJSON)", help="Как в properties.district у полигона."
                ),
                "norm_city_rayon": st.column_config.TextColumn(
                    "Район (norm_city.csv)", help='Колонка «Район» для Вологодской области.'
                ),
                "canonical_key": st.column_config.TextColumn(
                    "Канонический key", help="Общий внутренний ключ агрегации (латиница/подчёркивания удобны)."
                ),
                "ui_label": st.column_config.TextColumn("Название для UI"),
                "active": st.column_config.CheckboxColumn("Активно"),
            }
            edited_voda = st.data_editor(
                df_voda,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="admin_vo_district_aliases_editor",
                column_config=voda_cc,
            )
            c_va, c_vb = st.columns([1, 3])
            with c_va:
                save_voda = st.button("Сохранить районы ВО", key="admin_save_vo_district_aliases", type="primary")
            with c_vb:
                st.caption(
                    "Активная строка должна иметь заполненный canonical_key и UI-название и хотя бы одно поле района. "
                    "Удалённые из таблицы строки будут удалены из БД при сохранении."
                )
            if save_voda:
                errs_voda = mq.save_vo_district_aliases_admin_rows(
                    path, edited_voda.to_dict(orient="records")
                )
                if errs_voda:
                    for e in errs_voda[:80]:
                        st.error(e)
                    if len(errs_voda) > 80:
                        st.error(f"… ещё {len(errs_voda) - 80}.")
                else:
                    st.success(f"Сохранено строк: **{len(edited_voda)}**.")
                    st.rerun()

    _section_anchor("admin-competitions")
    st.subheader("Таблица competitions")
    st.caption(
        "Редактирование метаданных событий (название, дата, год, вид спорта, признаки). "
        "Колонку **raw** можно менять только скриптами синхронизации."
    )
    if require_db(path):
        years_admin = mq.query_competition_years_admin(path)
        series_admin = mq.query_competition_series_admin(path)
        cnt_all = mq.query_competitions_admin_count(path, None, None)
        year_labels = ["Все годы"] + [str(y) for y in years_admin]
        series_labels = ["Все серии"] + series_admin

        fcol1, fcol2 = st.columns(2)
        with fcol1:
            picked = st.selectbox(
                "Фильтр по году соревнования",
                options=year_labels,
                key="admin_comp_year_select",
            )
        with fcol2:
            picked_series = st.selectbox(
                "Серия событий",
                options=series_labels,
                key="admin_comp_series_select",
            )
        year_val: int | None = None if picked == "Все годы" else int(picked)
        series_val: str | None = (
            None if picked_series == "Все серии" else str(picked_series).strip()
        )
        total = mq.query_competitions_admin_count(path, year_val, series_val)
        lim_cap = 800
        caption_tail = ""
        if total > lim_cap:
            caption_tail = (
                f"В базе **{total}** строк при выбранном фильтре — показываются первые **{lim_cap}**. "
                "Уточните год/серию или редактируйте порциями."
            )
        else:
            caption_tail = f"Строк к показу: **{total}**."
        if cnt_all != total:
            caption_tail += f" Всего событий в базе: **{cnt_all}**."
        st.caption(caption_tail)

        rows_admin = mq.query_competitions_admin_rows(
            path, year=year_val, series_title=series_val, limit=lim_cap
        )
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
    if _is_admin_user():
        pages.append("Админ панель")
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
    elif page == "Админ панель":
        page_admin()
    elif page == ADMIN_PANEL_PAGE:
        page_admin()
    else:
        page_cups()
    _scroll_to_section_once(page)


if __name__ == "__main__":
    main()
