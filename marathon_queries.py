"""
Слой SQL-запросов к marathon.db для analytics.py и Streamlit-дашборда.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import csv
from difflib import SequenceMatcher
from contextlib import contextmanager
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", "marathon.db"))
DEFAULT_DISTANCE_ALIASES_FILE = (
    Path(__file__).resolve().parent / "config" / "distance_aliases.json"
)
DEFAULT_CITY_ALIASES_FILE = (
    Path(__file__).resolve().parent / "config" / "city_aliases.json"
)
DEFAULT_CITY_REFERENCE_FILE = (
    Path(__file__).resolve().parent / "city-master" / "city-master" / "city.csv"
)
DEFAULT_NORM_CITY_ALIAS_FILE = (
    Path(__file__).resolve().parent / "city-master" / "city-master" / "norm_city.csv"
)
DEFAULT_NORM_REGION_ALIAS_FILE = (
    Path(__file__).resolve().parent / "city-master" / "city-master" / "norm_region.csv"
)
DEFAULT_CITY_NORMALIZATION_CHUNK = 1000
DEFAULT_STAGES_FILE = Path(__file__).resolve().parent / ".cursor" / "etapy.yaml"
LEGACY_STAGES_FILE = Path(__file__).resolve().parent / ".cursor" / "etapi.md"
# Fallback-карта этапов для Бегового кубка 2026 (cup_id=54), если файл карты недоступен.
DEFAULT_STAGE_INDEX_MAP: dict[int, int] = {
    298: 1,
    299: 2,
    300: 3,
    301: 4,
    302: 5,
    303: 6,
    304: 7,
    305: 8,
}


@contextmanager
def connect(db_path: Path | str | None = None) -> Iterator[sqlite3.Connection]:
    path = Path(db_path) if db_path is not None else DEFAULT_DB
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def q_all(db_path: Path | str, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connect(db_path) as c:
        return [dict(r) for r in c.execute(sql, params).fetchall()]


def q_one(db_path: Path | str, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connect(db_path) as c:
        row = c.execute(sql, params).fetchone()
        return dict(row) if row else None


def db_exists(db_path: Path | str | None = None) -> bool:
    return Path(db_path or DEFAULT_DB).is_file()


def _table_has_column(db_path: Path | str, table_name: str, column_name: str) -> bool:
    """Проверка наличия колонки в таблице SQLite (для совместимости старых дампов)."""
    with connect(db_path) as c:
        rows = c.execute(f"PRAGMA table_info({table_name})").fetchall()
    for r in rows:
        if str(r["name"]) == column_name:
            return True
    return False


def _norm_alias_token(value: str | None) -> str:
    """Мягкая нормализация текстового алиаса дистанции."""
    s = (value or "").strip().casefold().replace(",", ".")
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def distance_aliases_file_path() -> Path:
    raw = os.environ.get("DISTANCE_ALIASES_FILE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_DISTANCE_ALIASES_FILE


def default_distance_alias_rules() -> list[dict[str, Any]]:
    """Базовые правила справочника дистанций."""
    return [
        {
            "alias": "полмарафон",
            "canonical_key": "half_marathon",
            "canonical_label": "Полумарафон (21.1 км)",
            "active": True,
        },
        {
            "alias": "21 км",
            "canonical_key": "half_marathon",
            "canonical_label": "Полумарафон (21.1 км)",
            "active": True,
        },
        {
            "alias": "21 км 97,5м",
            "canonical_key": "half_marathon",
            "canonical_label": "Полумарафон (21.1 км)",
            "active": True,
        },
        {
            "alias": "21.0975 км",
            "canonical_key": "half_marathon",
            "canonical_label": "Полумарафон (21.1 км)",
            "active": True,
        },
        {
            "alias": "марафон",
            "canonical_key": "marathon",
            "canonical_label": "Марафон (42.195 км)",
            "active": True,
        },
        {
            "alias": "42 км",
            "canonical_key": "marathon",
            "canonical_label": "Марафон (42.195 км)",
            "active": True,
        },
        {
            "alias": "42,195 км",
            "canonical_key": "marathon",
            "canonical_label": "Марафон (42.195 км)",
            "active": True,
        },
    ]


def load_distance_alias_rules() -> list[dict[str, Any]]:
    """
    Читает справочник алиасов дистанций из JSON.
    Если файла нет или формат некорректен — отдаёт дефолтные правила.
    """
    p = distance_aliases_file_path()
    if not p.is_file():
        return default_distance_alias_rules()
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_distance_alias_rules()
    rows = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return default_distance_alias_rules()
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        alias = str(r.get("alias") or "").strip()
        canonical_key = str(r.get("canonical_key") or "").strip()
        canonical_label = str(r.get("canonical_label") or "").strip()
        active = bool(r.get("active", True))
        if not alias or not canonical_key or not canonical_label:
            continue
        out.append(
            {
                "alias": alias,
                "canonical_key": canonical_key,
                "canonical_label": canonical_label,
                "active": active,
            }
        )
    return out if out else default_distance_alias_rules()


def validate_distance_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    """Валидация правил перед сохранением."""
    errors: list[str] = []
    seen: dict[str, tuple[str, str]] = {}
    for i, r in enumerate(rows, start=1):
        alias = str(r.get("alias") or "").strip()
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        if not alias or not ckey or not clabel:
            errors.append(f"Строка {i}: заполните alias / canonical_key / canonical_label.")
            continue
        if not bool(r.get("active", True)):
            continue
        k = _norm_alias_token(alias)
        val = (ckey, clabel)
        if k in seen and seen[k] != val:
            errors.append(
                f"Конфликт alias '{alias}': уже связан с {seen[k][0]} / {seen[k][1]}."
            )
        else:
            seen[k] = val
    return errors


def save_distance_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    """Сохраняет справочник алиасов в JSON; возвращает список ошибок валидации."""
    errs = validate_distance_alias_rules(rows)
    if errs:
        return errs
    clean_rows: list[dict[str, Any]] = []
    for r in rows:
        alias = str(r.get("alias") or "").strip()
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        if not alias and not ckey and not clabel:
            continue
        clean_rows.append(
            {
                "alias": alias,
                "canonical_key": ckey,
                "canonical_label": clabel,
                "active": bool(r.get("active", True)),
            }
        )
    p = distance_aliases_file_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rules": clean_rows,
    }
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return []


def normalize_distance_by_alias_rules(
    raw_distance_name: str | None,
    rules: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Возвращает (ключ_группировки, label_для_UI) по справочнику алиасов."""
    src = str(raw_distance_name or "").strip()
    if not src:
        return ("—", "—")
    rule_rows = rules if rules is not None else load_distance_alias_rules()
    norm_src = _norm_alias_token(src)
    alias_map: dict[str, tuple[str, str]] = {}
    for r in rule_rows:
        if not bool(r.get("active", True)):
            continue
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        alias = str(r.get("alias") or "").strip()
        if not ckey or not clabel or not alias:
            continue
        alias_map[_norm_alias_token(alias)] = (ckey, clabel)
        alias_map[_norm_alias_token(clabel)] = (ckey, clabel)
        alias_map[_norm_alias_token(ckey)] = (ckey, clabel)
    if norm_src in alias_map:
        key, label = alias_map[norm_src]
        return (key, label)
    return (f"raw::{norm_src}", src)


def city_aliases_file_path() -> Path:
    raw = os.environ.get("CITY_ALIASES_FILE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_CITY_ALIASES_FILE


def city_reference_file_path() -> Path:
    raw = os.environ.get("CITY_REFERENCE_FILE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_CITY_REFERENCE_FILE


def norm_city_aliases_file_path() -> Path:
    """Нормализованный справочник городов (norm_city.csv в корне city-master)."""
    raw = os.environ.get("NORM_CITY_ALIAS_FILE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_NORM_CITY_ALIAS_FILE


def norm_region_aliases_file_path() -> Path:
    """Нормализованный справочник регионов (norm_region.csv рядом с norm_city.csv)."""
    raw = os.environ.get("NORM_REGION_ALIAS_FILE", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_NORM_REGION_ALIAS_FILE


def _norm_csv_header_cell(h: str | None) -> str:
    return (h or "").replace("\ufeff", "").strip().casefold()


def _norm_city_csv_row_to_rule(row: dict[str, Any]) -> dict[str, Any] | None:
    """Строка norm_city.csv -> rule c полями alias, canonical_key, canonical_label, canonical_city_id."""
    mm: dict[str, str] = {}
    for k, v in row.items():
        mm[_norm_csv_header_cell(str(k))] = str(v or "").strip()
    alias = (
        mm.get("alias", "")
        or mm.get("city_raw", "")
        or mm.get("город", "")
    )
    ck = mm.get("канонический key", "") or mm.get("canonical_key", "")
    clabel = (
        mm.get("название города в ui", "")
        or mm.get("canonical_label", "")
        or mm.get("название", "")
    )
    cid = mm.get("canonical_city_id", "") or mm.get("city_id", "")
    region = mm.get("region", "") or mm.get("регион", "")
    country = mm.get("country", "") or mm.get("страна", "")
    rayon = mm.get("район", "") or mm.get("district", "")
    alias = str(alias).strip()
    ck = str(ck).strip()
    clabel = str(clabel).strip()
    cid_s = str(cid).strip() if cid is not None and str(cid).strip() else ""
    if not alias or not ck or not clabel:
        return None
    return {
        "alias": alias,
        "canonical_key": ck,
        "canonical_label": clabel,
        "canonical_city_id": cid_s,
        "region": str(region).strip(),
        "country": str(country).strip(),
        "rayon": str(rayon).strip(),
        "active": True,
    }


def _load_norm_city_csv_rules() -> list[dict[str, Any]]:
    p = norm_city_aliases_file_path()
    if not p.is_file():
        return []
    try:
        txt = p.read_text(encoding="utf-8-sig", errors="ignore")
    except OSError:
        return []
    if not txt.strip():
        return []
    rdr = csv.DictReader(txt.splitlines(), delimiter=";")
    out: list[dict[str, Any]] = []
    for row in rdr:
        if not row:
            continue
        rec = _norm_city_csv_row_to_rule(dict(row))
        if rec:
            out.append(rec)
    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for rec in out:
        nk = _norm_city_token(str(rec.get("alias") or ""))
        if not nk:
            continue
        if nk not in seen:
            order.append(nk)
        seen[nk] = rec
    return [seen[k] for k in order]


def _read_city_aliases_json_payload() -> dict[str, Any]:
    """Сырой объект city_aliases.json (city rules + region_rules в одном файле)."""
    p = city_aliases_file_path()
    if not p.is_file():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_city_alias_rules_only() -> list[dict[str, Any]]:
    """Только JSON-оверлей (без объединения с norm_city.csv)."""
    payload = _read_city_aliases_json_payload()
    rows = payload.get("rules")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        alias = str(r.get("alias") or "").strip()
        canonical_key = str(r.get("canonical_key") or "").strip()
        canonical_label = str(r.get("canonical_label") or "").strip()
        active = bool(r.get("active", True))
        cid = str(r.get("canonical_city_id") or "").strip()
        region = str(r.get("region") or "").strip()
        country = str(r.get("country") or "").strip()
        rayon = str(r.get("rayon") or r.get("район") or "").strip()
        if not alias or not canonical_key or not canonical_label:
            continue
        row_d: dict[str, Any] = {
            "alias": alias,
            "canonical_key": canonical_key,
            "canonical_label": canonical_label,
            "active": active,
        }
        if cid:
            row_d["canonical_city_id"] = cid
        if region:
            row_d["region"] = region
        if country:
            row_d["country"] = country
        if rayon:
            row_d["rayon"] = rayon
        out.append(row_d)
    return out


def _merge_city_alias_rule_lists(base: list[dict[str, Any]], overlay: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Объединяет списки правил по нормализованному alias; overlay перекрывает base."""
    merged: dict[str, dict[str, Any]] = {}
    for r in base:
        nk = _norm_city_token(str(r.get("alias") or ""))
        if not nk:
            continue
        merged[nk] = dict(r)
    for r in overlay:
        nk = _norm_city_token(str(r.get("alias") or ""))
        if not nk:
            continue
        prev = merged.get(nk, {})
        merged[nk] = {**prev, **dict(r)}
    return sorted(
        merged.values(),
        key=lambda x: (_norm_city_token(str(x.get("alias") or "")), str(x.get("alias") or "").lower()),
    )


def _rule_signature_for_overlay_compare(r: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(r.get("canonical_key") or "").strip(),
        str(r.get("canonical_label") or "").strip(),
        "1" if bool(r.get("active", True)) else "0",
        str(r.get("canonical_city_id") or "").strip(),
        str(r.get("region") or "").strip(),
        str(r.get("country") or "").strip(),
        str(r.get("rayon") or "").strip(),
    )


def _str_from_editor_cell(v: Any) -> str:
    """Приводит ячейку data_editor/DataFrame к строке; убирает NaN из pandas/streamlit."""
    if v is None:
        return ""
    if isinstance(v, float):
        try:
            if v != v:  # NaN
                return ""
        except (TypeError, ValueError):
            pass
    s = str(v).strip()
    if not s:
        return ""
    if s.lower() == "nan":
        return ""
    return s


def _norm_city_token(value: str | None) -> str:
    s = _norm_alias_token(value)
    s = re.sub(r"^(г\.?|гор|город)\s+", "", s).strip()
    return s


def _is_missing_geo_label(value: str | None) -> bool:
    s = _norm_alias_token(value)
    return s in {"", "-", "—", "н/д", "#н/д", "na", "n/a", "none", "null", "unknown", "?"}


def _clean_geo_label(value: str | None, fallback: str = "—") -> str:
    s = str(value or "").strip()
    if _is_missing_geo_label(s):
        return fallback
    return s


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def _safe_coord(value: Any) -> float | None:
    try:
        x = float(str(value).strip().replace(",", "."))
        if x != x or abs(x) > 1e6:
            return None
        return x
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=4)
def _load_city_reference_index_cached(path_str: str, mtime_ns: int) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    p = Path(path_str)
    if not p.is_file():
        return out
    try:
        txt = p.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return out
    rdr = csv.DictReader(txt.splitlines())
    for row in rdr:
        city_label = str(row.get("city") or "").strip()
        if not city_label:
            continue
        city_norm = _norm_city_token(city_label)
        if not city_norm:
            continue
        rec = {
            "city_label": city_label,
            "city_norm": city_norm,
            "region_norm": _norm_alias_token(row.get("region")),
            "population": _safe_int(row.get("population"), 0),
            "capital_marker": _safe_int(row.get("capital_marker"), 0),
            "fias_id": str(row.get("fias_id") or "").strip(),
            "geo_lat": _safe_coord(row.get("geo_lat")),
            "geo_lon": _safe_coord(row.get("geo_lon")),
        }
        out.setdefault(city_norm, []).append(rec)
    for k, vals in out.items():
        vals.sort(
            key=lambda x: (
                -int(x.get("capital_marker") or 0),
                -int(x.get("population") or 0),
                str(x.get("city_label") or ""),
            )
        )
        out[k] = vals
    return out


def load_city_reference_index() -> dict[str, list[dict[str, Any]]]:
    p = city_reference_file_path()
    if not p.is_file():
        return {}
    try:
        mt = p.stat().st_mtime_ns
    except OSError:
        mt = 0
    return _load_city_reference_index_cached(str(p), mt)


def default_city_alias_rules() -> list[dict[str, Any]]:
    """Базовые правила справочника городов."""
    return [
        {
            "alias": "спб",
            "canonical_key": "sankt_peterburg",
            "canonical_label": "Санкт-Петербург",
            "active": True,
        },
        {
            "alias": "санкт петербург",
            "canonical_key": "sankt_peterburg",
            "canonical_label": "Санкт-Петербург",
            "active": True,
        },
        {
            "alias": "питер",
            "canonical_key": "sankt_peterburg",
            "canonical_label": "Санкт-Петербург",
            "active": True,
        },
        {
            "alias": "мск",
            "canonical_key": "moskva",
            "canonical_label": "Москва",
            "active": True,
        },
        {
            "alias": "г москва",
            "canonical_key": "moskva",
            "canonical_label": "Москва",
            "active": True,
        },
        {
            "alias": "нижний",
            "canonical_key": "nizhniy_novgorod",
            "canonical_label": "Нижний Новгород",
            "active": True,
        },
    ]


def load_city_alias_rules() -> list[dict[str, Any]]:
    """
    Справочник алиасов городов: norm_city.csv (база) + JSON-оверлей (поверх базы при совпадении alias).

    Если нет ни CSV, ни валидного JSON — возвращает встроенные дефолтные строки.
    """
    csv_rows = _load_norm_city_csv_rules()
    json_rows = _load_json_city_alias_rules_only()
    merged = _merge_city_alias_rule_lists(csv_rows, json_rows)
    if merged:
        return merged
    if csv_rows:
        return csv_rows
    if json_rows:
        return json_rows
    return default_city_alias_rules()


def validate_city_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    """Валидация правил городов перед сохранением."""
    errors: list[str] = []
    seen: dict[str, tuple[str, str]] = {}
    for i, r in enumerate(rows, start=1):
        alias = str(r.get("alias") or "").strip()
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        if not alias or not ckey or not clabel:
            errors.append(f"Строка {i}: заполните alias / canonical_key / canonical_label.")
            continue
        if not bool(r.get("active", True)):
            continue
        k = _norm_alias_token(alias)
        val = (ckey, clabel)
        if k in seen and seen[k] != val:
            errors.append(
                f"Конфликт alias '{alias}': уже связан с {seen[k][0]} / {seen[k][1]}."
            )
        else:
            seen[k] = val
    return errors


def save_city_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    """
    Сохраняет отличия от norm_city.csv в JSON-оверлей (горизонты правки без дубля всего каталога).
    Если norm_city.csv отсутствует — сохраняет полный набор строк в JSON.
    """
    # Дедупликация по нормализованному alias: сохраняем последнюю запись.
    dedup_map: dict[str, dict[str, Any]] = {}
    dedup_order: list[str] = []
    for r in rows:
        alias = _str_from_editor_cell(r.get("alias"))
        ckey = _str_from_editor_cell(r.get("canonical_key"))
        clabel = _str_from_editor_cell(r.get("canonical_label"))
        if not alias and not ckey and not clabel:
            continue
        norm_alias = _norm_city_token(alias)
        if not norm_alias:
            continue
        cid = _str_from_editor_cell(r.get("canonical_city_id"))
        row_out: dict[str, Any] = {
            "alias": alias,
            "canonical_key": ckey,
            "canonical_label": clabel,
            "active": bool(r.get("active", True)),
        }
        if cid:
            row_out["canonical_city_id"] = cid
        region = _str_from_editor_cell(r.get("region"))
        country = _str_from_editor_cell(r.get("country"))
        rayon = _str_from_editor_cell(r.get("rayon") if "rayon" in r else r.get("район"))
        if region:
            row_out["region"] = region
        if country:
            row_out["country"] = country
        if rayon:
            row_out["rayon"] = rayon
        if norm_alias not in dedup_map:
            dedup_order.append(norm_alias)
        dedup_map[norm_alias] = row_out
    clean_rows: list[dict[str, Any]] = [dedup_map[k] for k in dedup_order if k in dedup_map]

    errs = validate_city_alias_rules(clean_rows)
    if errs:
        return errs

    csv_baselines = { _norm_city_token(str(r.get("alias") or "")): r for r in _load_norm_city_csv_rules()}
    csv_path = norm_city_aliases_file_path()

    overlay_rows: list[dict[str, Any]] = []
    if csv_path.is_file() and csv_baselines:
        for r in clean_rows:
            nk = _norm_city_token(str(r.get("alias") or ""))
            base = csv_baselines.get(nk)
            if base is None:
                overlay_rows.append(dict(r))
            elif _rule_signature_for_overlay_compare(base) != _rule_signature_for_overlay_compare(r):
                overlay_rows.append(dict(r))
        to_write = overlay_rows
    else:
        to_write = clean_rows

    p = city_aliases_file_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overlay_source_norm_city_csv": str(csv_path) if csv_path.is_file() else "",
        "rules": to_write,
    }
    prev = _read_city_aliases_json_payload()
    if isinstance(prev.get("region_rules"), list):
        payload["region_rules"] = prev["region_rules"]
    o_r = prev.get("overlay_source_norm_region_csv")
    if isinstance(o_r, str) and o_r.strip():
        payload.setdefault("overlay_source_norm_region_csv", o_r.strip())

    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return []


def _norm_region_token(value: str | None) -> str:
    """Нормализация текста для сопоставления алиаса региона (как в profiles.region)."""
    return _norm_alias_token(value)


def _norm_region_csv_row_to_rule(row: dict[str, Any]) -> dict[str, Any] | None:
    mm: dict[str, str] = {}
    for k, v in row.items():
        mm[_norm_csv_header_cell(str(k))] = str(v or "").strip()
    alias = mm.get("region_alias", "") or mm.get("alias", "") or mm.get("region", "") or mm.get("регион", "")
    ck = mm.get("канонический key", "") or mm.get("canonical_key", "")
    clabel = mm.get("регион в ui", "") or mm.get("canonical_label", "") or mm.get("название", "")
    alias = str(alias).strip()
    ck = str(ck).strip()
    clabel = str(clabel).strip()
    if not alias or not ck or not clabel:
        return None
    return {
        "region_alias": alias,
        "canonical_key": ck,
        "canonical_label": clabel,
        "active": True,
    }


def _load_norm_region_csv_rules() -> list[dict[str, Any]]:
    p = norm_region_aliases_file_path()
    if not p.is_file():
        return []
    try:
        txt = p.read_text(encoding="utf-8-sig", errors="ignore")
    except OSError:
        return []
    if not txt.strip():
        return []
    rdr = csv.DictReader(txt.splitlines(), delimiter=";")
    out: list[dict[str, Any]] = []
    for row in rdr:
        if not row:
            continue
        rec = _norm_region_csv_row_to_rule(dict(row))
        if rec:
            out.append(rec)
    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for rec in out:
        nk = _norm_region_token(str(rec.get("region_alias") or ""))
        if not nk:
            continue
        if nk not in seen:
            order.append(nk)
        seen[nk] = rec
    return [seen[k] for k in order]


def _load_json_region_alias_rules_only() -> list[dict[str, Any]]:
    payload = _read_city_aliases_json_payload()
    rows = payload.get("region_rules")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        ra = str(r.get("region_alias") or r.get("alias") or "").strip()
        ck = str(r.get("canonical_key") or "").strip()
        cl = str(r.get("canonical_label") or "").strip()
        active = bool(r.get("active", True))
        if not ra or not ck or not cl:
            continue
        out.append(
            {"region_alias": ra, "canonical_key": ck, "canonical_label": cl, "active": active}
        )
    return out


def _merge_region_alias_rule_lists(base: list[dict[str, Any]], overlay: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for r in base:
        nk = _norm_region_token(str(r.get("region_alias") or r.get("alias") or ""))
        if not nk:
            continue
        merged[nk] = dict(r)
    for r in overlay:
        nk = _norm_region_token(str(r.get("region_alias") or r.get("alias") or ""))
        if not nk:
            continue
        prev = merged.get(nk, {})
        merged[nk] = {**prev, **dict(r)}
    return sorted(
        merged.values(),
        key=lambda x: (
            _norm_region_token(str(x.get("region_alias") or x.get("alias") or "")),
            str(x.get("region_alias") or x.get("alias") or "").lower(),
        ),
    )


def _rule_signature_for_region_overlay_compare(r: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(r.get("canonical_key") or "").strip(),
        str(r.get("canonical_label") or "").strip(),
        "1" if bool(r.get("active", True)) else "0",
    )


def default_region_alias_rules() -> list[dict[str, Any]]:
    return []


def load_region_alias_rules() -> list[dict[str, Any]]:
    csv_rows = _load_norm_region_csv_rules()
    json_rows = _load_json_region_alias_rules_only()
    merged = _merge_region_alias_rule_lists(csv_rows, json_rows)
    if merged:
        return merged
    if csv_rows:
        return csv_rows
    if json_rows:
        return json_rows
    return default_region_alias_rules()


def validate_region_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    seen: dict[str, tuple[str, str]] = {}
    for i, r in enumerate(rows, start=1):
        ra = str(r.get("region_alias") or r.get("alias") or "").strip()
        ck = str(r.get("canonical_key") or "").strip()
        cl = str(r.get("canonical_label") or "").strip()
        if not ra or not ck or not cl:
            errors.append(
                f"Строка {i}: заполните region_alias / canonical_key / canonical_label (Регион в UI)."
            )
            continue
        if not bool(r.get("active", True)):
            continue
        key = _norm_region_token(ra)
        val = (ck, cl)
        if key in seen and seen[key] != val:
            errors.append(
                f"Конфликт region_alias '{ra}': уже связан с {seen[key][0]} / {seen[key][1]}."
            )
        else:
            seen[key] = val
    return errors


def save_region_alias_rules(rows: list[dict[str, Any]]) -> list[str]:
    dedup_map: dict[str, dict[str, Any]] = {}
    dedup_order: list[str] = []
    for r in rows:
        ra = _str_from_editor_cell(r.get("region_alias") or r.get("alias"))
        ck = _str_from_editor_cell(r.get("canonical_key"))
        clabel = _str_from_editor_cell(r.get("canonical_label"))
        if not ra and not ck and not clabel:
            continue
        nk = _norm_region_token(ra)
        if not nk:
            continue
        row_out = {
            "region_alias": ra,
            "canonical_key": ck,
            "canonical_label": clabel,
            "active": bool(r.get("active", True)),
        }
        if nk not in dedup_map:
            dedup_order.append(nk)
        dedup_map[nk] = row_out

    clean_rows: list[dict[str, Any]] = [dedup_map[k] for k in dedup_order if k in dedup_map]
    errs = validate_region_alias_rules(clean_rows)
    if errs:
        return errs

    csv_baselines = {
        _norm_region_token(str(r.get("region_alias") or r.get("alias") or "")): r
        for r in _load_norm_region_csv_rules()
    }
    csv_path = norm_region_aliases_file_path()
    overlay_rows: list[dict[str, Any]] = []
    if csv_path.is_file() and csv_baselines:
        for r in clean_rows:
            nk = _norm_region_token(str(r.get("region_alias") or r.get("alias") or ""))
            base = csv_baselines.get(nk)
            if base is None:
                overlay_rows.append(dict(r))
            elif _rule_signature_for_region_overlay_compare(base) != _rule_signature_for_region_overlay_compare(r):
                overlay_rows.append(dict(r))
        to_write_regions = overlay_rows
    else:
        to_write_regions = clean_rows

    prev = _read_city_aliases_json_payload()
    payload = dict(prev)
    payload["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    payload["overlay_source_norm_region_csv"] = str(csv_path) if csv_path.is_file() else ""
    payload["region_rules"] = to_write_regions
    if "overlay_source_norm_city_csv" not in payload:
        csv_c = norm_city_aliases_file_path()
        payload["overlay_source_norm_city_csv"] = str(csv_c) if csv_c.is_file() else ""
    prev_rules = prev.get("rules")
    payload["rules"] = prev_rules if isinstance(prev_rules, list) else []

    p = city_aliases_file_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return []


def normalize_region_by_alias_rules(
    raw_region: str | None,
    rules: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Канонический key и название региона для UI по аналогии с городами."""
    src = str(raw_region or "").strip()
    if not src or src == "—":
        return ("—", "—")
    rr = rules if rules is not None else load_region_alias_rules()
    nn = _norm_region_token(src)
    alias_map: dict[str, tuple[str, str]] = {}
    for r in rr:
        if not bool(r.get("active", True)):
            continue
        ck = str(r.get("canonical_key") or "").strip()
        cl = str(r.get("canonical_label") or "").strip()
        ra = str(r.get("region_alias") or r.get("alias") or "").strip()
        if not ck or not cl or not ra:
            continue
        alias_map[_norm_region_token(ra)] = (ck, cl)
        alias_map[_norm_region_token(cl)] = (ck, cl)
        alias_map[_norm_region_token(ck)] = (ck, cl)
    if nn in alias_map:
        return alias_map[nn]
    return (f"raw::{nn}", src)


def rollup_region_aggregate_rows(
    rows: list[dict[str, Any]],
    rules: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Перегруппировка SQL GROUP BY по сырым строкам регионов после нормализации алиасов."""
    rr = rules if rules is not None else load_region_alias_rules()
    agg: dict[str, dict[str, Any]] = {}
    for r in rows:
        raw_reg = str(r.get("region") or "")
        rkey, rlabel = normalize_region_by_alias_rules(raw_reg, rr)
        cur = agg.get(rkey)
        p_add = int(r.get("participants") or 0)
        extras: dict[str, int] = {}
        if "starts" in r:
            extras["starts"] = int(r.get("starts") or 0)
        if cur is None:
            row_out: dict[str, Any] = {"region": rlabel, "participants": p_add, **extras}
            agg[rkey] = row_out
        else:
            cur["participants"] = int(cur.get("participants") or 0) + p_add
            if "starts" in cur and extras:
                cur["starts"] = int(cur.get("starts") or 0) + extras.get("starts", 0)

    out = list(agg.values())
    first_has_starts = bool(rows) and isinstance(rows[0], dict) and "starts" in rows[0]
    if first_has_starts:
        out.sort(
            key=lambda x: (
                -int(x.get("participants") or 0),
                -int(x.get("starts") or 0),
                str(x.get("region") or ""),
            )
        )
    else:
        out.sort(
            key=lambda x: (-int(x.get("participants") or 0), str(x.get("region") or ""))
        )
    return out


def normalize_city_by_alias_rules(
    raw_city_name: str | None,
    raw_region_name: str | None = None,
    rules: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Возвращает (ключ_группировки, label_для_UI) по справочнику алиасов городов."""
    src = str(raw_city_name or "").strip()
    if not src:
        return ("—", "—")
    rule_rows = rules if rules is not None else load_city_alias_rules()
    norm_src = _norm_city_token(src)
    alias_map: dict[str, tuple[str, str]] = {}
    for r in rule_rows:
        if not bool(r.get("active", True)):
            continue
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        alias = str(r.get("alias") or "").strip()
        if not ckey or not clabel or not alias:
            continue
        alias_map[_norm_city_token(alias)] = (ckey, clabel)
        alias_map[_norm_city_token(clabel)] = (ckey, clabel)
        alias_map[_norm_city_token(ckey)] = (ckey, clabel)
    if norm_src in alias_map:
        key, label = alias_map[norm_src]
        return (key, label)

    # 2-й слой нормализации: официальный справочник городов (city.csv).
    idx = load_city_reference_index()
    candidates = idx.get(norm_src, [])
    if candidates:
        if len(candidates) > 1:
            rr = _norm_alias_token(raw_region_name)
            if rr:
                matched = [x for x in candidates if str(x.get("region_norm") or "") == rr]
                if matched:
                    candidates = matched
        picked = candidates[0]
        city_label = str(picked.get("city_label") or src)
        city_key = str(picked.get("fias_id") or "") or str(picked.get("city_norm") or norm_src)
        return (f"cityref::{city_key}", city_label)

    return (f"raw::{norm_src}", src)


def resolve_city_geo_by_alias_rules(
    raw_city_name: str | None,
    raw_region_name: str | None = None,
    raw_country_name: str | None = None,
    rules: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Нормализует город и возвращает канонические geo-поля из справочника."""
    src_city = str(raw_city_name or "").strip() or "—"
    src_region = _clean_geo_label(raw_region_name)
    src_country = _clean_geo_label(raw_country_name)
    rr = rules if rules is not None else load_city_alias_rules()
    norm_src = _norm_city_token(src_city)

    alias_map: dict[str, dict[str, str]] = {}
    for r in rr:
        if not bool(r.get("active", True)):
            continue
        ckey = str(r.get("canonical_key") or "").strip()
        clabel = str(r.get("canonical_label") or "").strip()
        alias = str(r.get("alias") or "").strip()
        if not ckey or not clabel or not alias:
            continue
        rec = {
            "city_key": ckey,
            "city_label": clabel,
            "region_label": _clean_geo_label(r.get("region"), src_region),
            "country_label": _clean_geo_label(r.get("country"), src_country),
            "rayon_label": str(r.get("rayon") or "").strip(),
        }
        alias_map[_norm_city_token(alias)] = rec
        alias_map[_norm_city_token(clabel)] = rec
        alias_map[_norm_city_token(ckey)] = rec

    if norm_src in alias_map:
        return alias_map[norm_src]

    key, label = normalize_city_by_alias_rules(src_city, src_region, rr)
    return {
        "city_key": key,
        "city_label": label,
        "region_label": src_region,
        "country_label": src_country,
        "rayon_label": "",
    }


def ensure_city_normalization_schema(db_path: Path | str | None = None) -> None:
    """Создаёт служебные таблицы и колонки для нормализации городов."""
    path = Path(db_path) if db_path is not None else DEFAULT_DB
    if not path.is_file():
        return
    with connect(path) as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS city_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alias_raw TEXT NOT NULL,
                alias_norm TEXT NOT NULL,
                canonical_city_id TEXT NOT NULL,
                canonical_city_label TEXT NOT NULL,
                canonical_region_label TEXT,
                source TEXT NOT NULL DEFAULT 'manual',
                confidence REAL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(alias_norm)
            );
            CREATE TABLE IF NOT EXISTS city_normalization_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                city_raw TEXT,
                region_raw TEXT,
                country_raw TEXT,
                canonical_city_key TEXT,
                canonical_city_label TEXT,
                reason TEXT NOT NULL,
                normalize_score REAL,
                top_candidates_json TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(profile_id)
            );
            CREATE TABLE IF NOT EXISTS city_normalization_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dry_run INTEGER NOT NULL DEFAULT 1,
                processed INTEGER NOT NULL DEFAULT 0,
                updated_rows INTEGER NOT NULL DEFAULT 0,
                queued_rows INTEGER NOT NULL DEFAULT 0,
                skipped_non_ru INTEGER NOT NULL DEFAULT 0,
                details_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        for col, ctype in [
            ("canonical_city_id", "TEXT"),
            ("canonical_city_label", "TEXT"),
            ("canonical_region_label", "TEXT"),
            ("normalize_status", "TEXT"),
            ("normalize_score", "REAL"),
            ("normalized_at", "TEXT"),
        ]:
            if not _table_has_column(path, "profiles", col):
                c.execute(f"ALTER TABLE profiles ADD COLUMN {col} {ctype}")
        for col, ctype in [
            ("canonical_city_key", "TEXT"),
            ("canonical_city_label", "TEXT"),
        ]:
            if not _table_has_column(path, "city_normalization_queue", col):
                c.execute(f"ALTER TABLE city_normalization_queue ADD COLUMN {col} {ctype}")
        c.commit()


def _city_similarity_score(src: str, candidate: str) -> float:
    return float(SequenceMatcher(None, _norm_city_token(src), _norm_city_token(candidate)).ratio())


def _pick_best_city_candidate(
    city_raw: str,
    region_raw: str | None,
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, float, str]:
    if not candidates:
        return (None, 0.0, "not_found")
    rr = _norm_alias_token(region_raw)
    by_region = [x for x in candidates if rr and str(x.get("region_norm") or "") == rr]
    if len(by_region) == 1:
        return (by_region[0], 1.0, "auto_region")
    pool = by_region if by_region else candidates
    best = max(pool, key=lambda x: _city_similarity_score(city_raw, str(x.get("city_label") or "")))
    score = _city_similarity_score(city_raw, str(best.get("city_label") or ""))
    return (best, score, "auto_fuzzy")


def query_city_normalization_queue(
    db_path: Path | str,
    limit: int = 200,
    city_raw_search: str = "",
) -> list[dict[str, Any]]:
    ensure_city_normalization_schema(db_path)
    lim = max(1, int(limit))
    search = str(city_raw_search or "").strip()
    where_extra = ""
    params: tuple[Any, ...] = (lim,)
    if search:
        where_extra = " AND LOWER(COALESCE(q.city_raw, '')) LIKE ?"
        params = (f"%{search.casefold()}%", lim)
    return q_all(
        db_path,
        """
        SELECT
            q.id,
            q.profile_id,
            q.city_raw,
            q.region_raw,
            q.country_raw,
            q.canonical_city_key,
            q.canonical_city_label,
            q.reason,
            q.normalize_score,
            q.top_candidates_json,
            q.status,
            q.updated_at
        FROM city_normalization_queue q
        WHERE q.status = 'pending'
        """
        + where_extra
        + """
        ORDER BY COALESCE(q.normalize_score, 0.0) DESC, q.id ASC
        LIMIT ?
        """,
        params,
    )


def query_city_normalization_city_groups(
    db_path: Path | str,
    limit: int = 200,
    city_raw_search: str = "",
) -> list[dict[str, Any]]:
    """Группы хвоста по city_raw для пакетной ручной модерации."""
    ensure_city_normalization_schema(db_path)
    lim = max(1, int(limit))
    search = str(city_raw_search or "").strip()
    where_extra = ""
    params: tuple[Any, ...] = (lim,)
    if search:
        where_extra = " AND LOWER(COALESCE(city_raw, '')) LIKE ?"
        params = (f"%{search.casefold()}%", lim)
    return q_all(
        db_path,
        """
        SELECT
            city_raw,
            COUNT(*) AS items_count,
            MIN(COALESCE(normalize_score, 0.0)) AS min_score,
            MAX(COALESCE(normalize_score, 0.0)) AS max_score,
            COALESCE(MAX(canonical_city_key), '') AS canonical_city_key,
            COALESCE(MAX(canonical_city_label), '') AS canonical_city_label
        FROM city_normalization_queue
        WHERE status = 'pending'
        """
        + where_extra
        + """
        GROUP BY city_raw
        ORDER BY items_count DESC, city_raw ASC
        LIMIT ?
        """,
        params,
    )


def append_city_alias_rule(
    alias: str,
    canonical_key: str,
    canonical_label: str,
    *,
    canonical_city_id: str | None = None,
) -> list[str]:
    """Добавляет/обновляет один alias в JSON-оверлее городов."""
    alias_raw = str(alias or "").strip()
    ckey = str(canonical_key or "").strip()
    clabel = str(canonical_label or "").strip()
    cid_opt = str(canonical_city_id or "").strip()
    if not alias_raw or not ckey or not clabel:
        return ["Заполните alias / canonical_key / canonical_label."]
    rows = load_city_alias_rules()
    norm_alias = _norm_city_token(alias_raw)
    updated = False
    for r in rows:
        if _norm_city_token(str(r.get("alias") or "")) == norm_alias:
            r["canonical_key"] = ckey
            r["canonical_label"] = clabel
            r["active"] = True
            if cid_opt:
                r["canonical_city_id"] = cid_opt
            elif "canonical_city_id" in r:
                del r["canonical_city_id"]
            updated = True
            break
    if not updated:
        new_row: dict[str, Any] = {
            "alias": alias_raw,
            "canonical_key": ckey,
            "canonical_label": clabel,
            "active": True,
        }
        if cid_opt:
            new_row["canonical_city_id"] = cid_opt
        rows.append(new_row)
    return save_city_alias_rules(rows)


def apply_city_queue_decision(
    db_path: Path | str,
    queue_id: int,
    canonical_city_id: str,
    canonical_city_label: str,
    canonical_region_label: str,
    *,
    create_alias: bool = False,
) -> list[str]:
    """Применяет ручное решение по записи очереди и опционально пополняет alias-справочник."""
    ensure_city_normalization_schema(db_path)
    qid = int(queue_id)
    city_id = str(canonical_city_id or "").strip()
    city_label = str(canonical_city_label or "").strip()
    region_label = str(canonical_region_label or "").strip()
    if not city_id or not city_label:
        return ["Укажите canonical_city_id и canonical_city_label."]
    with connect(db_path) as c:
        row = c.execute(
            "SELECT profile_id, city_raw FROM city_normalization_queue WHERE id = ?",
            (qid,),
        ).fetchone()
        if not row:
            return [f"Запись очереди #{qid} не найдена."]
        pid = int(row["profile_id"])
        city_raw = str(row["city_raw"] or "").strip()
        now_utc = datetime.now(timezone.utc).isoformat()
        c.execute(
            """
            UPDATE profiles
            SET canonical_city_id = ?,
                canonical_city_label = ?,
                canonical_region_label = ?,
                normalize_status = 'manual',
                normalize_score = 1.0,
                normalized_at = ?
            WHERE id = ?
            """,
            (city_id, city_label, region_label, now_utc, pid),
        )
        c.execute(
            """
            UPDATE city_normalization_queue
            SET status = 'resolved', updated_at = ?
            WHERE id = ?
            """,
            (now_utc, qid),
        )
        c.commit()
    if create_alias and city_raw:
        errs = append_city_alias_rule(city_raw, city_id, city_label, canonical_city_id=city_id)
        if errs:
            return errs
    return []


def apply_city_queue_decision_for_city_raw(
    db_path: Path | str,
    city_raw: str,
    canonical_city_id: str,
    canonical_city_label: str,
    canonical_region_label: str,
    *,
    create_alias: bool = False,
) -> dict[str, Any]:
    """Пакетно применяет ручное решение ко всем pending-записям выбранного city_raw."""
    ensure_city_normalization_schema(db_path)
    src_city = str(city_raw or "").strip()
    city_id = str(canonical_city_id or "").strip()
    city_label = str(canonical_city_label or "").strip()
    region_label = str(canonical_region_label or "").strip()
    if not src_city:
        return {"errors": ["Укажите city_raw для пакетной правки."], "updated": 0}
    if not city_id or not city_label:
        return {"errors": ["Укажите canonical_city_id и canonical_city_label."], "updated": 0}
    with connect(db_path) as c:
        rows = c.execute(
            """
            SELECT id
            FROM city_normalization_queue
            WHERE status = 'pending' AND COALESCE(city_raw, '') = ?
            """,
            (src_city,),
        ).fetchall()
    updated = 0
    errors: list[str] = []
    for row in rows:
        errs = apply_city_queue_decision(
            db_path,
            int(row["id"]),
            city_id,
            city_label,
            region_label,
            create_alias=False,
        )
        if errs:
            errors.extend(errs)
        else:
            updated += 1
    if create_alias and src_city and not errors:
        alias_errs = append_city_alias_rule(src_city, city_id, city_label)
        if alias_errs:
            errors.extend(alias_errs)
    return {"errors": errors, "updated": updated}


def run_city_normalization_batch(
    db_path: Path | str | None = None,
    *,
    limit: int = DEFAULT_CITY_NORMALIZATION_CHUNK,
    offset: int = 0,
    dry_run: bool = True,
    fuzzy_auto_threshold: float = 0.95,
    fuzzy_review_threshold: float = 0.90,
) -> dict[str, Any]:
    """
    Батч-нормализация профилей по city/region/country.
    dry_run=True считает статистику без записи в БД.
    """
    path = Path(db_path) if db_path is not None else DEFAULT_DB
    ensure_city_normalization_schema(path)
    lim = max(1, int(limit))
    off = max(0, int(offset))
    status_counts: dict[str, int] = {}
    processed = 0
    updated_rows = 0
    queued_rows = 0
    skipped_non_ru = 0
    rules = load_city_alias_rules()
    ref_idx = load_city_reference_index()
    with connect(path) as c:
        rows = c.execute(
            """
            SELECT
                p.id,
                COALESCE(TRIM(p.city), '') AS city,
                COALESCE(TRIM(p.region), '') AS region,
                COALESCE(TRIM(p.country), '') AS country
            FROM profiles p
            ORDER BY p.id
            LIMIT ? OFFSET ?
            """,
            (lim, off),
        ).fetchall()
        now_utc = datetime.now(timezone.utc).isoformat()
        for row in rows:
            processed += 1
            pid = int(row["id"])
            city_raw = str(row["city"] or "").strip()
            region_raw = str(row["region"] or "").strip()
            country_raw = str(row["country"] or "").strip()
            best: dict[str, Any] | None = None
            if not city_raw:
                status = "need_review"
                score = 0.0
                candidate_payload = json.dumps([], ensure_ascii=False)
                reason = "empty_city"
                decided: dict[str, Any] | None = None
            elif country_raw and _norm_alias_token(country_raw) not in {"россия", "рф"}:
                skipped_non_ru += 1
                status = "skipped_non_ru"
                score = 0.0
                candidate_payload = json.dumps([], ensure_ascii=False)
                reason = "non_ru_country"
                decided = None
            else:
                key, label = normalize_city_by_alias_rules(city_raw, region_raw, rules=rules)
                if key.startswith("raw::"):
                    city_norm = _norm_city_token(city_raw)
                    candidates = ref_idx.get(city_norm, [])
                    best, fuzzy_score, detected_status = _pick_best_city_candidate(city_raw, region_raw, candidates)
                    score = float(fuzzy_score)
                    candidate_payload = json.dumps(
                        [
                            {
                                "city_label": x.get("city_label"),
                                "region_label": x.get("region_norm"),
                                "fias_id": x.get("fias_id"),
                            }
                            for x in candidates[:5]
                        ],
                        ensure_ascii=False,
                    )
                    if best and score >= fuzzy_auto_threshold:
                        status = detected_status
                        decided = {
                            "canonical_city_id": str(best.get("fias_id") or f"cityref::{city_norm}"),
                            "canonical_city_label": str(best.get("city_label") or city_raw),
                            "canonical_region_label": region_raw,
                        }
                        reason = "fuzzy_match"
                    elif best and score >= fuzzy_review_threshold:
                        status = "need_review"
                        reason = "fuzzy_uncertain"
                        decided = None
                    else:
                        status = "need_review"
                        reason = "not_found"
                        decided = None
                else:
                    score = 1.0
                    status = "auto_exact"
                    candidate_payload = json.dumps([], ensure_ascii=False)
                    reason = "alias_or_ref_exact"
                    canonical_city_id = key.replace("cityref::", "", 1)
                    decided = {
                        "canonical_city_id": canonical_city_id,
                        "canonical_city_label": label,
                        "canonical_region_label": region_raw,
                    }
            status_counts[status] = int(status_counts.get(status, 0)) + 1
            if dry_run:
                continue
            if decided:
                c.execute(
                    """
                    UPDATE profiles
                    SET canonical_city_id = ?,
                        canonical_city_label = ?,
                        canonical_region_label = ?,
                        normalize_status = ?,
                        normalize_score = ?,
                        normalized_at = ?
                    WHERE id = ?
                    """,
                    (
                        decided["canonical_city_id"],
                        decided["canonical_city_label"],
                        decided["canonical_region_label"],
                        status,
                        score,
                        now_utc,
                        pid,
                    ),
                )
                c.execute("DELETE FROM city_normalization_queue WHERE profile_id = ?", (pid,))
                updated_rows += 1
            elif status == "need_review":
                queue_key = ""
                queue_label = ""
                if best and score >= fuzzy_review_threshold:
                    queue_key = str(best.get("fias_id") or f"cityref::{_norm_city_token(city_raw)}")
                    queue_label = str(best.get("city_label") or city_raw)
                c.execute(
                    """
                    INSERT INTO city_normalization_queue
                    (
                        profile_id, city_raw, region_raw, country_raw, reason,
                        canonical_city_key, canonical_city_label,
                        normalize_score, top_candidates_json, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    ON CONFLICT(profile_id) DO UPDATE SET
                        city_raw = excluded.city_raw,
                        region_raw = excluded.region_raw,
                        country_raw = excluded.country_raw,
                        reason = excluded.reason,
                        canonical_city_key = excluded.canonical_city_key,
                        canonical_city_label = excluded.canonical_city_label,
                        normalize_score = excluded.normalize_score,
                        top_candidates_json = excluded.top_candidates_json,
                        status = 'pending',
                        updated_at = excluded.updated_at
                    """,
                    (
                        pid,
                        city_raw,
                        region_raw,
                        country_raw,
                        reason,
                        queue_key,
                        queue_label,
                        score,
                        candidate_payload,
                        now_utc,
                    ),
                )
                queued_rows += 1
        if not dry_run:
            c.execute(
                """
                INSERT INTO city_normalization_runs
                    (dry_run, processed, updated_rows, queued_rows, skipped_non_ru, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    0,
                    processed,
                    updated_rows,
                    queued_rows,
                    skipped_non_ru,
                    json.dumps({"status_counts": status_counts}, ensure_ascii=False),
                ),
            )
            c.commit()
    return {
        "dry_run": bool(dry_run),
        "processed": processed,
        "updated_rows": updated_rows,
        "queued_rows": queued_rows,
        "skipped_non_ru": skipped_non_ru,
        "status_counts": status_counts,
        "limit": lim,
        "offset": off,
    }


def ensure_cup_scoring_schema(db_path: Path | str | None = None) -> None:
    """Создаёт таблицы cup_scoring_computed_* в существующей БД (CREATE IF NOT EXISTS)."""
    import cup_scoring

    path = Path(db_path) if db_path is not None else DEFAULT_DB
    if not path.is_file():
        return
    with connect(path) as c:
        cup_scoring.ensure_cup_scoring_tables(c)


def ensure_team_scoring_schema(db_path: Path | str | None = None) -> None:
    """Создаёт таблицы расчёта командного зачёта (CREATE IF NOT EXISTS)."""
    path = Path(db_path) if db_path is not None else DEFAULT_DB
    if not path.is_file():
        return
    with connect(path) as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS team_scoring_stage_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_version TEXT NOT NULL,
                cup_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                stage_index INTEGER NOT NULL,
                competition_id INTEGER NOT NULL,
                profile_id INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                gender TEXT,
                age INTEGER,
                distance_id INTEGER,
                distance_km REAL,
                place_abs INTEGER,
                base_points INTEGER NOT NULL,
                bonus_points INTEGER NOT NULL,
                points_for_team INTEGER NOT NULL,
                first_place_cap INTEGER NOT NULL,
                source_result_id INTEGER,
                computed_at TEXT,
                UNIQUE(rule_version, cup_id, year, stage_index, profile_id)
            );
            CREATE TABLE IF NOT EXISTS team_scoring_member_totals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_version TEXT NOT NULL,
                cup_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                profile_id INTEGER NOT NULL,
                points_sum_all_stages INTEGER NOT NULL,
                points_best7_of8 INTEGER NOT NULL,
                stages_counted_json TEXT,
                computed_at TEXT,
                UNIQUE(rule_version, cup_id, year, team_name, profile_id)
            );
            CREATE TABLE IF NOT EXISTS team_scoring_team_totals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_version TEXT NOT NULL,
                cup_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                team_points_top5 INTEGER NOT NULL,
                members_counted_json TEXT,
                computed_at TEXT,
                UNIQUE(rule_version, cup_id, year, team_name)
            );
            """
        )
        c.commit()


def load_stage_index_map(path: Path | str | None = None) -> dict[int, int]:
    """Парсит карту этапов из .cursor/etapi.md (форматы `Этап 1 id 298` или `1:298`)."""
    p = Path(path) if path is not None else DEFAULT_STAGES_FILE
    if not p.is_file():
        p = LEGACY_STAGES_FILE
    if not p.is_file():
        return dict(DEFAULT_STAGE_INDEX_MAP)
    try:
        txt = p.read_text(encoding="utf-8")
    except OSError:
        return {}
    out: dict[int, int] = {}
    for line in txt.splitlines():
        s = line.strip()
        if not s:
            continue
        m1 = re.search(r"этап\s*(\d+)\s*id\s*(\d+)", s, flags=re.IGNORECASE)
        if m1:
            out[int(m1.group(2))] = int(m1.group(1))
            continue
        m2 = re.match(r"(\d+)\s*[:=-]\s*(\d+)$", s)
        if m2:
            out[int(m2.group(2))] = int(m2.group(1))
    return out if out else dict(DEFAULT_STAGE_INDEX_MAP)


def _score_linear(place_abs: int, first: int, second: int, linear_from_place: int) -> int:
    if place_abs <= 0:
        return 0
    if place_abs == 1:
        return first
    if place_abs == 2:
        return second
    start = second - 1
    delta = max(0, place_abs - linear_from_place)
    return max(0, start - delta)


def calc_team_stage_base_points(
    stage_index: int,
    distance_km: float | None,
    place_abs: int | None,
) -> tuple[int, int]:
    """
    Базовые очки этапа (без бонуса) и cap (= очки за 1 место для данного правила).
    Возвращает (base_points, first_place_cap).
    """
    if place_abs is None or place_abs <= 0:
        return (0, 0)
    km = float(distance_km or 0.0)
    p = int(place_abs)
    if 1 <= stage_index <= 6:
        if km >= 7.0:
            if p <= 6:
                return (600 - (p - 1) * 2, 600)
            return (max(0, 589 - (p - 7)), 600)
        if 5.0 <= km <= 6.0:
            if p <= 5:
                return (598 - (p - 1) * 2, 598)
            return (max(0, 589 - (p - 6)), 598)
        return (0, 0)
    if stage_index in (7, 8):
        if 10.0 <= km <= 21.0:
            if p == 1:
                return (602, 602)
            if p == 2:
                return (600, 602)
            return (max(0, 599 - (p - 3)), 602)
        if 4.7 <= km <= 5.3:
            if p <= 5:
                return (598 - (p - 1) * 2, 598)
            return (max(0, 589 - (p - 6)), 598)
        if 2.0 <= km <= 3.0:
            return (0, 0)
        return (0, 0)
    return (0, 0)


def _resolve_distance_km(distance_km: Any, distance_name: str | None) -> float:
    """Нормализация км дистанции: берём поле distance_km, иначе парсим из названия."""
    try:
        km = float(distance_km)
        if km > 0:
            return km
    except (TypeError, ValueError):
        pass
    name = (distance_name or "").strip().casefold().replace(",", ".")
    nums = re.findall(r"\d+(?:\.\d+)?", name)
    if not nums:
        return 0.0
    try:
        n0 = float(nums[0])
    except ValueError:
        return 0.0
    if "км" in name or "km" in name:
        return n0
    if "м" in name:
        return n0 / 1000.0
    return n0


def compute_team_scoring_for_cup_year(
    db_path: Path | str,
    cup_id: int,
    year: int,
    stage_map: dict[int, int] | None = None,
    rule_version: str = "team_v1",
) -> dict[str, int]:
    """
    Пересчитывает таблицы team_scoring_* для cup_id/year.
    Правила: лучшая строка участника в этапе, бонус 50+ = +15, cap очков не выше 1 места.
    """
    ensure_team_scoring_schema(db_path)
    stage_ix_map = stage_map if stage_map is not None else load_stage_index_map()
    if not stage_ix_map:
        return {"stage_rows": 0, "member_rows": 0, "team_rows": 0}
    comp_ids = sorted(stage_ix_map.keys())
    comp_placeholders = ",".join("?" * len(comp_ids))
    base_rows = q_all(
        db_path,
        f"""
        SELECT
            r.id AS result_id,
            r.competition_id,
            r.profile_id,
            TRIM(COALESCE(r.team, '')) AS team_name,
            COALESCE(NULLIF(TRIM(p.gender), ''), '') AS gender,
            p.age AS age,
            r.distance_id,
            d.distance_km,
            d.name AS distance_name,
            r.place_abs,
            r.place_gender
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
        LEFT JOIN profiles p ON p.id = r.profile_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE COALESCE(r.dnf, 0) = 0
          AND r.profile_id IS NOT NULL
          AND TRIM(COALESCE(r.team, '')) != ''
          AND (r.place_gender IS NOT NULL OR r.place_abs IS NOT NULL)
          AND r.competition_id IN ({comp_placeholders})
        """,
        tuple([year] + comp_ids),
    )
    by_stage_profile: dict[tuple[int, int], dict[str, Any]] = {}
    for r in base_rows:
        comp_id = int(r["competition_id"])
        if comp_id not in stage_ix_map:
            continue
        stage_idx = int(stage_ix_map[comp_id])
        km = _resolve_distance_km(r.get("distance_km"), r.get("distance_name"))
        place_for_score = r.get("place_gender")
        if place_for_score is None:
            place_for_score = r.get("place_abs")
        base_pts, cap = calc_team_stage_base_points(stage_idx, km, place_for_score)
        if base_pts <= 0:
            continue
        age = r.get("age")
        bonus = 15 if age is not None and int(age) >= 50 else 0
        pts_team = min(base_pts + bonus, cap)
        key = (stage_idx, int(r["profile_id"]))
        cur = by_stage_profile.get(key)
        cand = {
            "stage_index": stage_idx,
            "competition_id": comp_id,
            "profile_id": int(r["profile_id"]),
            "team_name": str(r.get("team_name") or "").strip(),
            "gender": str(r.get("gender") or "").strip(),
            "age": int(age) if age is not None else None,
            "distance_id": r.get("distance_id"),
            "distance_km": float(km),
            "place_abs": int(r.get("place_abs") or 0),
            "base_points": int(base_pts),
            "bonus_points": int(bonus),
            "points_for_team": int(pts_team),
            "first_place_cap": int(cap),
            "source_result_id": int(r["result_id"]),
        }
        if cur is None or int(cand["points_for_team"]) > int(cur["points_for_team"]):
            by_stage_profile[key] = cand
    chosen_rows = list(by_stage_profile.values())
    with connect(db_path) as c:
        now = datetime.now(timezone.utc).isoformat()
        c.execute(
            "DELETE FROM team_scoring_stage_points WHERE rule_version=? AND cup_id=? AND year=?",
            (rule_version, cup_id, year),
        )
        c.execute(
            "DELETE FROM team_scoring_member_totals WHERE rule_version=? AND cup_id=? AND year=?",
            (rule_version, cup_id, year),
        )
        c.execute(
            "DELETE FROM team_scoring_team_totals WHERE rule_version=? AND cup_id=? AND year=?",
            (rule_version, cup_id, year),
        )
        if chosen_rows:
            c.executemany(
                """
                INSERT INTO team_scoring_stage_points (
                    rule_version, cup_id, year, stage_index, competition_id, profile_id,
                    team_name, gender, age, distance_id, distance_km, place_abs,
                    base_points, bonus_points, points_for_team, first_place_cap,
                    source_result_id, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        rule_version,
                        cup_id,
                        year,
                        int(r["stage_index"]),
                        int(r["competition_id"]),
                        int(r["profile_id"]),
                        str(r["team_name"]),
                        str(r["gender"]),
                        r["age"],
                        r["distance_id"],
                        float(r["distance_km"]),
                        int(r["place_abs"]),
                        int(r["base_points"]),
                        int(r["bonus_points"]),
                        int(r["points_for_team"]),
                        int(r["first_place_cap"]),
                        int(r["source_result_id"]),
                        now,
                    )
                    for r in chosen_rows
                ],
            )
        # member totals: best 7 of 8
        stage_rows = [
            {
                "team_name": str(r["team_name"]),
                "profile_id": int(r["profile_id"]),
                "stage_index": int(r["stage_index"]),
                "points_for_team": int(r["points_for_team"]),
            }
            for r in chosen_rows
        ]
        by_member: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for r in stage_rows:
            k = (str(r["team_name"]), int(r["profile_id"]))
            by_member.setdefault(k, []).append(r)
        member_rows: list[tuple[Any, ...]] = []
        team_acc: dict[str, list[tuple[int, int]]] = {}
        for (team_name, profile_id), rr in by_member.items():
            rr_sorted = sorted(
                rr, key=lambda x: int(x.get("points_for_team") or 0), reverse=True
            )
            best7 = rr_sorted[:7]
            points_all = int(sum(int(x.get("points_for_team") or 0) for x in rr_sorted))
            points_best7 = int(sum(int(x.get("points_for_team") or 0) for x in best7))
            stages_json = json.dumps(
                [
                    {
                        "stage_index": int(x.get("stage_index") or 0),
                        "points": int(x.get("points_for_team") or 0),
                    }
                    for x in best7
                ],
                ensure_ascii=False,
            )
            member_rows.append(
                (
                    rule_version,
                    cup_id,
                    year,
                    team_name,
                    profile_id,
                    points_all,
                    points_best7,
                    stages_json,
                    now,
                )
            )
            team_acc.setdefault(team_name, []).append((profile_id, points_best7))
        if member_rows:
            c.executemany(
                """
                INSERT INTO team_scoring_member_totals (
                    rule_version, cup_id, year, team_name, profile_id,
                    points_sum_all_stages, points_best7_of8, stages_counted_json, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                member_rows,
            )
        team_rows: list[tuple[Any, ...]] = []
        for team_name, vals in team_acc.items():
            vals_sorted = sorted(vals, key=lambda x: x[1], reverse=True)
            top5 = vals_sorted[:5]
            team_points = int(sum(v for _, v in top5))
            members_json = json.dumps(
                [{"profile_id": int(pid), "points_best7_of8": int(pts)} for pid, pts in top5],
                ensure_ascii=False,
            )
            team_rows.append(
                (
                    rule_version,
                    cup_id,
                    year,
                    team_name,
                    team_points,
                    members_json,
                    now,
                )
            )
        if team_rows:
            c.executemany(
                """
                INSERT INTO team_scoring_team_totals (
                    rule_version, cup_id, year, team_name,
                    team_points_top5, members_counted_json, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                team_rows,
            )
        c.commit()
    return {
        "stage_rows": len(chosen_rows),
        "member_rows": len(member_rows),
        "team_rows": len(team_rows),
    }


def query_team_scoring_team_totals(
    db_path: Path | str, cup_id: int, year: int, rule_version: str = "team_v1"
) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT
            team_name AS команда,
            team_points_top5 AS очков,
            members_counted_json AS members_json
        FROM team_scoring_team_totals
        WHERE rule_version=? AND cup_id=? AND year=?
        ORDER BY очков DESC, команда
        """,
        (rule_version, cup_id, year),
    )


def query_team_scoring_member_totals(
    db_path: Path | str, cup_id: int, year: int, team_name: str | None = None, rule_version: str = "team_v1"
) -> list[dict[str, Any]]:
    if team_name is None:
        return q_all(
            db_path,
            """
            SELECT
                m.team_name AS команда,
                m.profile_id,
                TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
                m.points_best7_of8 AS очков_7из8,
                m.points_sum_all_stages AS очков_всего,
                m.stages_counted_json
            FROM team_scoring_member_totals m
            LEFT JOIN profiles p ON p.id = m.profile_id
            WHERE m.rule_version=? AND m.cup_id=? AND m.year=?
            ORDER BY m.team_name, m.points_best7_of8 DESC, участник
            """,
            (rule_version, cup_id, year),
        )
    return q_all(
        db_path,
        """
        SELECT
            m.team_name AS команда,
            m.profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            m.points_best7_of8 AS очков_7из8,
            m.points_sum_all_stages AS очков_всего,
            m.stages_counted_json
        FROM team_scoring_member_totals m
        LEFT JOIN profiles p ON p.id = m.profile_id
        WHERE m.rule_version=? AND m.cup_id=? AND m.year=? AND m.team_name=?
        ORDER BY m.points_best7_of8 DESC, участник
        """,
        (rule_version, cup_id, year, team_name),
    )


def query_team_scoring_stage_points(
    db_path: Path | str,
    cup_id: int,
    year: int,
    team_name: str | None = None,
    rule_version: str = "team_v1",
) -> list[dict[str, Any]]:
    """Детализация очков по этапам для участников командного зачёта."""
    if team_name is None:
        return q_all(
            db_path,
            """
            SELECT
                sp.team_name AS команда,
                sp.profile_id,
                sp.stage_index AS этап,
                sp.competition_id,
                COALESCE(c.title, '') AS событие,
                TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
                sp.place_abs AS место_абс,
                sp.base_points AS очков_база,
                sp.bonus_points AS очков_бонус,
                sp.points_for_team AS очков_в_командный,
                CASE WHEN sp.bonus_points > 0 THEN 'Группа 50+ бонус 15 очков' ELSE '' END AS комментарий
            FROM team_scoring_stage_points sp
            LEFT JOIN profiles p ON p.id = sp.profile_id
            LEFT JOIN competitions c ON c.id = sp.competition_id
            WHERE sp.rule_version=? AND sp.cup_id=? AND sp.year=?
            ORDER BY sp.team_name, sp.stage_index, sp.points_for_team DESC, участник
            """,
            (rule_version, cup_id, year),
        )
    return q_all(
        db_path,
        """
        SELECT
            sp.team_name AS команда,
            sp.profile_id,
            sp.stage_index AS этап,
            sp.competition_id,
            COALESCE(c.title, '') AS событие,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            sp.place_abs AS место_абс,
            sp.base_points AS очков_база,
            sp.bonus_points AS очков_бонус,
            sp.points_for_team AS очков_в_командный,
            CASE WHEN sp.bonus_points > 0 THEN 'Группа 50+ бонус 15 очков' ELSE '' END AS комментарий
        FROM team_scoring_stage_points sp
        LEFT JOIN profiles p ON p.id = sp.profile_id
        LEFT JOIN competitions c ON c.id = sp.competition_id
        WHERE sp.rule_version=? AND sp.cup_id=? AND sp.year=? AND sp.team_name=?
        ORDER BY sp.stage_index, sp.points_for_team DESC, участник
        """,
        (rule_version, cup_id, year, team_name),
    )


def query_team_championship_matrix(
    db_path: Path | str,
    cup_id: int,
    year: int,
    stage_map: dict[int, int] | None = None,
    rule_version: str = "team_v1",
) -> dict[str, Any]:
    """
    Матрица командного первенства:
    строки = команды, колонки = этапы (из stage_map), значение = 100-(место-1),
    где место считается по сумме очков top-5 участников команды на этапе.
    """
    smap = stage_map if stage_map is not None else load_stage_index_map()
    if not smap:
        return {"stage_columns": [], "rows": []}
    stage_to_comp: dict[int, int] = {}
    for comp_id, stage_idx in smap.items():
        if 1 <= int(stage_idx) <= 8:
            stage_to_comp[int(stage_idx)] = int(comp_id)
    if not stage_to_comp:
        return {"stage_columns": [], "rows": []}
    stage_indices = sorted(stage_to_comp.keys())
    comp_ids = [stage_to_comp[s] for s in stage_indices]
    ph = ",".join("?" * len(comp_ids))
    comp_rows = q_all(
        db_path,
        f"""
        SELECT id, COALESCE(NULLIF(TRIM(title), ''), 'Этап') AS title
        FROM competitions
        WHERE id IN ({ph})
        """,
        tuple(comp_ids),
    )
    comp_title: dict[int, str] = {int(r["id"]): str(r["title"]) for r in comp_rows}
    stage_columns: list[dict[str, Any]] = [
        {
            "stage_index": s,
            "competition_id": stage_to_comp[s],
            "label": comp_title.get(stage_to_comp[s], f"Этап {s}"),
        }
        for s in stage_indices
    ]

    stage_rows = q_all(
        db_path,
        """
        SELECT stage_index, team_name, profile_id, points_for_team
        FROM team_scoring_stage_points
        WHERE rule_version=? AND cup_id=? AND year=?
        """,
        (rule_version, cup_id, year),
    )
    from collections import defaultdict

    by_stage_team_profile: dict[tuple[int, str, int], int] = defaultdict(int)
    for r in stage_rows:
        st = int(r.get("stage_index") or 0)
        team = str(r.get("team_name") or "").strip()
        pid = int(r.get("profile_id") or 0)
        if st not in stage_to_comp or not team or pid <= 0:
            continue
        pts = int(r.get("points_for_team") or 0)
        key = (st, team, pid)
        if pts > by_stage_team_profile[key]:
            by_stage_team_profile[key] = pts

    by_stage_team_values: dict[int, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for (st, team, _pid), pts in by_stage_team_profile.items():
        by_stage_team_values[st][team].append(int(pts))

    stage_team_top5_sum: dict[int, dict[str, int]] = defaultdict(dict)
    all_teams: set[str] = set()
    for st, teams in by_stage_team_values.items():
        for team, arr in teams.items():
            all_teams.add(team)
            arr_sorted = sorted(arr, reverse=True)
            stage_team_top5_sum[st][team] = int(sum(arr_sorted[:5]))

    # ранжирование команд внутри каждого этапа и перевод в очки матрицы 100..1
    stage_rank_points: dict[int, dict[str, int]] = defaultdict(dict)
    for st, teams in stage_team_top5_sum.items():
        ordered = sorted(teams.items(), key=lambda x: x[1], reverse=True)
        for rank, (team, _v) in enumerate(ordered, start=1):
            stage_rank_points[st][team] = max(0, 100 - (rank - 1))

    row_list: list[dict[str, Any]] = []
    for team in sorted(all_teams):
        row: dict[str, Any] = {"Команда": team}
        total = 0
        for sc in stage_columns:
            st = int(sc["stage_index"])
            val = int(stage_rank_points.get(st, {}).get(team, 0))
            row[str(sc["label"])] = val
            total += val
        row["Итого"] = int(total)
        row_list.append(row)
    row_list.sort(key=lambda x: int(x.get("Итого") or 0), reverse=True)
    for i, r in enumerate(row_list, start=1):
        r["Место"] = i
    return {"stage_columns": stage_columns, "rows": row_list}


# ── Сводка и сезон ────────────────────────────────────────────────────────


def query_summary_row(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT
          (SELECT COUNT(*) FROM competitions)               AS соревнований,
          (SELECT COUNT(*) FROM distances)                  AS дистанций,
          (SELECT COUNT(*) FROM results)                    AS результатов,
          (SELECT COUNT(DISTINCT profile_id) FROM results WHERE profile_id IS NOT NULL)
                                                            AS уникальных_участников,
          (SELECT COUNT(*) FROM cups)                       AS кубков,
          (SELECT COUNT(*) FROM cup_results)               AS результатов_кубков,
          (SELECT COUNT(*) FROM profiles)                  AS профилей
        """,
    )


def query_events_by_year_sport(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT year AS год, sport AS вид,
               COUNT(*) AS событий,
               SUM(cs.total_members) AS стартов
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE year IS NOT NULL
        GROUP BY year, sport
        ORDER BY year DESC, событий DESC
        """,
    )


def query_distinct_years(db_path: Path | str) -> list[int]:
    rows = q_all(
        db_path,
        "SELECT DISTINCT year AS y FROM competitions WHERE year IS NOT NULL ORDER BY y DESC",
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_season_metrics(db_path: Path | str, year: int) -> dict[str, Any] | None:
    return q_one(
        db_path,
        """
        SELECT
          COUNT(DISTINCT c.id) AS events,
          COUNT(CASE WHEN r.dnf = 0 THEN 1 END) AS finishes,
          COUNT(DISTINCT CASE WHEN r.dnf = 0 AND r.profile_id IS NOT NULL
                              THEN r.profile_id END) AS unique_athletes,
          ROUND(COALESCE(SUM(CASE WHEN r.dnf = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS total_km
        FROM competitions c
        LEFT JOIN results r ON r.competition_id = c.id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE c.year = ?
        """,
        (year,),
    )


def query_season_sport_breakdown(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.sport AS вид,
               COUNT(DISTINCT c.id) AS событий,
               COUNT(CASE WHEN r.dnf = 0 THEN 1 END) AS финишей
        FROM competitions c
        LEFT JOIN results r ON r.competition_id = c.id
        WHERE c.year = ?
        GROUP BY c.sport
        ORDER BY финишей DESC
        """,
        (year,),
    )


def query_season_monthly_finishes(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT substr(c.date, 1, 7) AS month,
               COUNT(r.id) AS finishes
        FROM competitions c
        JOIN results r ON r.competition_id = c.id AND r.dnf = 0
        WHERE c.year = ? AND c.date IS NOT NULL AND length(c.date) >= 7
        GROUP BY month
        ORDER BY month
        """,
        (year,),
    )


def query_competitions_for_year(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.id, c.title AS событие, c.date AS дата, c.sport AS вид,
               cs.total_members AS участников
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE c.year = ?
        ORDER BY c.date DESC NULLS LAST, c.id
        """,
        (year,),
    )


# ── Участники (топы и распределения) ───────────────────────────────────────


def query_participants_top(db_path: Path | str, limit: int = 20) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT p.id,
               p.last_name || ' ' || p.first_name AS имя,
               p.gender AS пол, p.age AS возраст,
               p.city AS город,
               COUNT(r.id) AS стартов,
               ROUND(SUM(r.finish_time_sec)/3600.0, 1) AS часов_на_трассе,
               p.stat_first AS побед
        FROM profiles p
        JOIN results r ON r.profile_id = p.id AND r.dnf = 0
        GROUP BY p.id
        ORDER BY стартов DESC
        LIMIT {int(limit)}
        """,
    )


def query_gender_age_distribution(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT
            CASE p.gender WHEN 'm' THEN 'Мужчины' ELSE 'Женщины' END AS пол,
            CASE
                WHEN p.age < 20        THEN 'до 20'
                WHEN p.age BETWEEN 20 AND 34 THEN '20–34'
                WHEN p.age BETWEEN 35 AND 49 THEN '35–49'
                WHEN p.age BETWEEN 50 AND 64 THEN '50–64'
                ELSE '65+'
            END AS возраст,
            COUNT(DISTINCT p.id) AS участников,
            COUNT(r.id) AS стартов
        FROM profiles p
        JOIN results r ON r.profile_id = p.id
        WHERE p.gender != ''
        GROUP BY пол, возраст
        ORDER BY пол, возраст
        """,
    )


def query_cities_top(db_path: Path | str, limit: int = 15) -> list[dict[str, Any]]:
    rows = q_all(
        db_path,
        f"""
        SELECT p.city AS город, p.region AS регион,
               COUNT(DISTINCT p.id) AS участников,
               COUNT(r.id) AS стартов
        FROM profiles p
        JOIN results r ON r.profile_id = p.id
        WHERE p.city != ''
        GROUP BY p.city
        ORDER BY участников DESC
        LIMIT {int(limit)}
        """,
    )
    rules = load_city_alias_rules()
    agg: dict[str, dict[str, Any]] = {}
    for r in rows:
        _, label = normalize_city_by_alias_rules(r.get("город"), r.get("регион"), rules)
        key = _norm_alias_token(label)
        cur = agg.get(key)
        if cur is None:
            agg[key] = {
                "город": label,
                "регион": r.get("регион"),
                "участников": int(r.get("участников") or 0),
                "стартов": int(r.get("стартов") or 0),
            }
        else:
            cur["участников"] = int(cur.get("участников") or 0) + int(r.get("участников") or 0)
            cur["стартов"] = int(cur.get("стартов") or 0) + int(r.get("стартов") or 0)
    out = list(agg.values())
    out.sort(key=lambda x: (-int(x.get("участников") or 0), -int(x.get("стартов") or 0), str(x.get("город") or "")))
    return out[: int(limit)]


def query_profile_search(db_path: Path | str, needle: str, limit: int = 50) -> list[dict[str, Any]]:
    n = (needle or "").strip()
    if not n:
        return []
    lim = int(limit)
    if n.isdigit():
        return q_all(
            db_path,
            f"SELECT id, first_name, last_name, city, gender, age FROM profiles WHERE id = ? LIMIT {lim}",
            (int(n),),
        )
    pat = f"%{n}%"
    return q_all(
        db_path,
        f"""
        SELECT id, first_name, last_name, city, gender, age
        FROM profiles
        WHERE last_name LIKE ? OR first_name LIKE ?
        ORDER BY last_name, first_name
        LIMIT {lim}
        """,
        (pat, pat),
    )


def query_profile_search_enriched(
    db_path: Path | str, needle: str, limit: int = 50
) -> list[dict[str, Any]]:
    """Поиск профилей с доп. полями для удобного выбора (последний год, старты)."""
    n = (needle or "").strip()
    if not n:
        return []
    lim = int(limit)
    if n.isdigit():
        return q_all(
            db_path,
            f"""
            SELECT
                p.id,
                p.first_name,
                p.last_name,
                p.city,
                p.gender,
                p.age,
                MAX(c.year) AS last_year,
                COUNT(r.id) AS starts_total
            FROM profiles p
            LEFT JOIN results r ON r.profile_id = p.id
            LEFT JOIN competitions c ON c.id = r.competition_id
            WHERE p.id = ?
            GROUP BY p.id
            LIMIT {lim}
            """,
            (int(n),),
        )
    tokens = [t for t in re.split(r"\s+", n.strip()) if t]
    pat = f"%{n}%"
    where_parts: list[str] = [
        "(LOWER(COALESCE(p.last_name, '')) LIKE LOWER(?) OR LOWER(COALESCE(p.first_name, '')) LIKE LOWER(?))",
        "LOWER(TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, ''))) LIKE LOWER(?)",
        "LOWER(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, ''))) LIKE LOWER(?)",
    ]
    params: list[Any] = [pat, pat, pat, pat]
    for t in tokens:
        tpat = f"%{t}%"
        where_parts.append(
            "(LOWER(COALESCE(p.last_name, '')) LIKE LOWER(?) OR LOWER(COALESCE(p.first_name, '')) LIKE LOWER(?))"
        )
        params.extend([tpat, tpat])
    where_sql = " OR ".join(where_parts)
    return q_all(
        db_path,
        f"""
        SELECT
            p.id,
            p.first_name,
            p.last_name,
            p.city,
            p.gender,
            p.age,
            MAX(c.year) AS last_year,
            COUNT(r.id) AS starts_total,
            CASE
                WHEN LOWER(COALESCE(p.last_name, '')) = LOWER(?) THEN 0
                WHEN LOWER(COALESCE(p.last_name, '')) LIKE LOWER(?) THEN 1
                WHEN LOWER(COALESCE(p.first_name, '')) LIKE LOWER(?) THEN 2
                WHEN LOWER(TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, ''))) LIKE LOWER(?) THEN 3
                WHEN LOWER(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, ''))) LIKE LOWER(?) THEN 4
                ELSE 3
            END AS _rank
        FROM profiles p
        LEFT JOIN results r ON r.profile_id = p.id
        LEFT JOIN competitions c ON c.id = r.competition_id
        WHERE {where_sql}
        GROUP BY p.id
        ORDER BY _rank, p.last_name, p.first_name
        LIMIT {lim}
        """,
        tuple([n, f"{n}%", f"{n}%", pat, pat] + params),
    )


def query_profile_autocomplete_options(
    db_path: Path | str, limit: int = 20000
) -> list[dict[str, Any]]:
    """Список профилей для единого autocomplete-поля участника."""
    lim = int(limit)
    return q_all(
        db_path,
        f"""
        SELECT
            p.id,
            p.first_name,
            p.last_name,
            p.city,
            MAX(c.year) AS last_year,
            COUNT(r.id) AS starts_total
        FROM profiles p
        LEFT JOIN results r ON r.profile_id = p.id
        LEFT JOIN competitions c ON c.id = r.competition_id
        GROUP BY p.id
        ORDER BY p.last_name, p.first_name
        LIMIT {lim}
        """,
    )


# ── События (отчёты и карточка) ─────────────────────────────────────────────


def query_competitions_top(db_path: Path | str, limit: int = 20) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.id, c.title AS событие, c.year AS год,
               c.sport AS вид,
               cs.total_members AS участников,
               cs.male AS м, cs.female AS ж,
               cs.regions AS регионов
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        ORDER BY участников DESC NULLS LAST
        LIMIT {int(limit)}
        """,
    )


def query_avg_finish_by_sport_distance(db_path: Path | str, limit: int = 30) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.sport AS вид,
               d.name AS дистанция,
               ROUND(d.distance_km, 1) AS км,
               COUNT(r.id) AS финишей,
               MIN(r.finish_time) AS лучшее,
               TIME(ROUND(AVG(r.finish_time_sec)), 'unixepoch') AS среднее
        FROM results r
        JOIN distances d ON d.id = r.distance_id
        JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND r.finish_time_sec IS NOT NULL
          AND d.distance_km > 0
        GROUP BY c.sport, d.id
        ORDER BY c.sport, км DESC
        LIMIT {int(limit)}
        """,
    )


def query_dnf_events_top(db_path: Path | str, limit: int = 15) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT c.title AS событие, c.year AS год,
               cs.total_members AS стартов,
               cs.dnf AS сходов,
               ROUND(100.0 * cs.dnf / NULLIF(cs.total_members, 0), 1) AS dnf_pct
        FROM competition_stats cs
        JOIN competitions c ON c.id = cs.competition_id
        WHERE cs.dnf > 0
        ORDER BY dnf_pct DESC
        LIMIT {int(limit)}
        """,
    )


def query_competition_header(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.title, c.date, c.year, c.sport,
               cs.total_members AS участников,
               cs.male AS мужчин, cs.female AS женщин,
               cs.teams AS команд, cs.regions AS регионов,
               cst.status
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        LEFT JOIN competition_status cst ON cst.competition_id = c.id
        WHERE c.id = ?
        """,
        (comp_id,),
    )


def query_competition_distances(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT d.id, d.name, d.distance_km AS км, d.is_relay AS эстафета,
               COUNT(r.id) AS финишёров
        FROM distances d
        LEFT JOIN results r ON r.distance_id = d.id AND r.dnf = 0
        WHERE d.competition_id = ?
        GROUP BY d.id
        """,
        (comp_id,),
    )


def query_competition_top10(db_path: Path | str, comp_id: int, limit: int = 30) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT r.place_abs AS место,
               p.last_name || ' ' || p.first_name AS участник,
               p.city AS город, r.group_name AS группа,
               r.finish_time AS время, d.name AS дистанция
        FROM results r
        LEFT JOIN profiles p ON p.id = r.profile_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.competition_id = ? AND r.place_abs <= 10 AND r.dnf = 0
        ORDER BY d.id, r.place_abs
        LIMIT {int(limit)}
        """,
        (comp_id,),
    )


def query_competition_groups(db_path: Path | str, comp_id: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT r.group_name AS группа,
               COUNT(*) AS участников,
               MIN(r.finish_time) AS лучшее_время
        FROM results r
        WHERE r.competition_id = ? AND r.dnf = 0 AND r.group_name != ''
        GROUP BY r.group_name
        ORDER BY группа
        """,
        (comp_id,),
    )


def query_event_section_cards(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
) -> dict[str, Any]:
    """Карточки для раздела «Событие»: события, участники, команды, регионы, страны."""
    w, plist = build_competition_filter_sql(years, sports, None)
    params = tuple(plist)

    events = q_one(db_path, f"SELECT COUNT(*) AS n FROM competitions c WHERE {w}", params)
    total_events = int(events["n"]) if events and events.get("n") is not None else 0

    part = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT r.profile_id) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.profile_id IS NOT NULL AND COALESCE(r.dnf, 0) = 0 AND {w}
        """,
        params,
    )
    total_participants = int(part["n"]) if part and part.get("n") is not None else 0

    teams = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT TRIM(r.team)) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE COALESCE(r.dnf, 0) = 0 AND TRIM(COALESCE(r.team, '')) != '' AND {w}
        """,
        params,
    )
    total_teams = int(teams["n"]) if teams and teams.get("n") is not None else 0

    reg = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT reg_key) AS n
        FROM (
            SELECT CASE
                WHEN p.region_id IS NOT NULL THEN 'id:' || CAST(p.region_id AS TEXT)
                WHEN TRIM(COALESCE(p.region, '')) != '' THEN 'n:' || TRIM(p.region)
                ELSE NULL
            END AS reg_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE COALESCE(r.dnf, 0) = 0 AND {w}
        ) t
        WHERE reg_key IS NOT NULL
        """,
        params,
    )
    regions = int(reg["n"]) if reg and reg.get("n") is not None else 0

    cntr = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT c_key) AS n
        FROM (
            SELECT CASE
                WHEN TRIM(COALESCE(p.country, '')) != '' THEN TRIM(p.country)
                ELSE NULL
            END AS c_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE COALESCE(r.dnf, 0) = 0 AND {w}
        ) t
        WHERE c_key IS NOT NULL
        """,
        params,
    )
    countries = int(cntr["n"]) if cntr and cntr.get("n") is not None else 0

    return {
        "total_events": total_events,
        "total_participants": total_participants,
        "teams_distinct": total_teams,
        "regions_distinct": regions,
        "countries_distinct": countries,
    }


def query_event_series_title_short_ranking(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
) -> list[dict[str, Any]]:
    """
    Рейтинг серий по полю ``competitions.title_short``:
    только серии с **более чем одной** строкой в выборке; те же фильтры года/вид спорта,
    что раздел «События».

    По каждой серии возвращает:
      ``series_title``, ``editions`` (число событий),
      ``years_csv`` (уникальные годы через запятую по возрастанию),
      ``sports_csv`` (уникальные виды спорта через запятую),
      ``participants_sum``, ``teams_sum`` — суммы по полям из ``competition_stats``.

    Если колонки ``title_short`` в таблице нет — пустой список.
    """
    from collections import defaultdict

    if not _table_has_column(db_path, "competitions", "title_short"):
        return []
    w, plist = build_competition_filter_sql(years, sports, None)
    rows = q_all(
        db_path,
        f"""
        SELECT TRIM(c.title_short) AS series_title,
               c.year AS year,
               c.sport AS sport,
               COALESCE(cs.total_members, 0) AS total_members,
               COALESCE(cs.teams, 0) AS teams
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE {w}
          AND TRIM(COALESCE(c.title_short, '')) != ''
        """,
        tuple(plist),
    )
    editions: dict[str, int] = defaultdict(int)
    years_by: dict[str, set[int]] = defaultdict(set)
    sports_by: dict[str, set[str]] = defaultdict(set)
    mem_by: dict[str, int] = defaultdict(int)
    teams_by: dict[str, int] = defaultdict(int)

    for r in rows:
        key = str(r.get("series_title") or "").strip()
        if not key:
            continue
        editions[key] += 1
        y = r.get("year")
        if y is not None:
            try:
                years_by[key].add(int(y))
            except (TypeError, ValueError):
                pass
        sp = str(r.get("sport") or "").strip()
        if sp:
            sports_by[key].add(sp)
        try:
            mem_by[key] += int(r.get("total_members") or 0)
        except (TypeError, ValueError):
            pass
        try:
            teams_by[key] += int(r.get("teams") or 0)
        except (TypeError, ValueError):
            pass

    out: list[dict[str, Any]] = []
    for key, n in editions.items():
        if n <= 1:
            continue
        ys = sorted(years_by[key])
        years_csv = ", ".join(str(x) for x in ys)
        sps = sorted(sports_by[key])
        sports_csv = ", ".join(sps) if sps else "—"
        out.append(
            {
                "series_title": key,
                "editions": n,
                "years_csv": years_csv,
                "sports_csv": sports_csv,
                "participants_sum": mem_by[key],
                "teams_sum": teams_by[key],
            }
        )
    out.sort(key=lambda x: (-int(x.get("editions") or 0), str(x.get("series_title") or "").casefold()))
    return out


def query_event_section_events_table(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
) -> list[dict[str, Any]]:
    """Таблица «События» для раздела события с агрегатами по финишам."""
    w, plist = build_competition_filter_sql(years, sports, None)
    return q_all(
        db_path,
        f"""
        SELECT
            c.year AS "Год",
            c.title AS "Название события",
            c.sport AS "Вид спорта",
            COALESCE(cs.total_members, 0) AS "Количество участников",
            COALESCE(cs.teams, 0) AS "Количество команд",
            COALESCE(cs.regions, 0) AS "Регионов",
            COALESCE((
                SELECT COUNT(DISTINCT TRIM(COALESCE(p.country, '')))
                FROM results r
                INNER JOIN profiles p ON p.id = r.profile_id
                WHERE r.competition_id = c.id
                  AND COALESCE(r.dnf, 0) = 0
                  AND TRIM(COALESCE(p.country, '')) != ''
            ), 0) AS "Стран"
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE {w}
        ORDER BY c.year DESC, c.date DESC, c.title
        """,
        tuple(plist),
    )


def query_event_section_records_table(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
) -> list[dict[str, Any]]:
    """
    Таблица «Рекорды события»: топ-3 результата по каждой дистанции внутри серии
    (``competitions.title_short``), по финишному времени.
    """
    w, plist = build_competition_filter_sql(years, sports, None)
    series_expr = (
        "COALESCE(NULLIF(TRIM(c.title_short), ''), TRIM(c.title))"
        if _table_has_column(db_path, "competitions", "title_short")
        else "TRIM(c.title)"
    )
    return q_all(
        db_path,
        f"""
        WITH comp_pool AS (
            SELECT
                c.id,
                c.year,
                c.title,
                {series_expr} AS title_series
            FROM competitions c
            WHERE {w}
        ),
        ranked AS (
            SELECT
                cp.title_series AS event_series,
                cp.year AS event_year,
                d.id AS distance_id,
                COALESCE(NULLIF(TRIM(d.name), ''), '—') AS distance_name,
                TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS athlete_name,
                r.finish_time AS finish_time,
                r.finish_time_sec AS finish_time_sec,
                ROW_NUMBER() OVER (
                    PARTITION BY cp.title_series, d.id
                    ORDER BY r.finish_time_sec ASC, r.id ASC
                ) AS rn
            FROM results r
            INNER JOIN comp_pool cp ON cp.id = r.competition_id
            INNER JOIN distances d ON d.id = r.distance_id
            LEFT JOIN profiles p ON p.id = r.profile_id
            WHERE COALESCE(r.dnf, 0) = 0
              AND r.finish_time_sec IS NOT NULL
              AND TRIM(COALESCE(cp.title_series, '')) != ''
        )
        SELECT
            event_series AS "Событие",
            event_year AS "Год",
            distance_name AS "Дистанция",
            COALESCE(NULLIF(TRIM(athlete_name), ''), '—') AS "Участник",
            COALESCE(NULLIF(TRIM(finish_time), ''), '—') AS "Время"
        FROM ranked
        WHERE rn <= 3
        ORDER BY event_series, distance_id, finish_time_sec, event_year
        """,
        tuple(plist),
    )


def query_event_section_records_hierarchy(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """
    Иерархическая выборка рекордов для UI:
    серия события (title_short/title) -> дистанция -> топ-N результатов по времени
    отдельно для мужчин и женщин (по всем годам).
    """
    w, plist = build_competition_filter_sql(years, sports, None)
    n = max(1, int(top_n))
    series_expr = (
        "COALESCE(NULLIF(TRIM(c.title_short), ''), TRIM(c.title))"
        if _table_has_column(db_path, "competitions", "title_short")
        else "TRIM(c.title)"
    )
    base_rows = q_all(
        db_path,
        f"""
        WITH comp_pool AS (
            SELECT
                c.id,
                c.year,
                c.title,
                {series_expr} AS title_series
            FROM competitions c
            WHERE {w}
        )
        SELECT
            cp.title_series AS event_series,
            COALESCE(NULLIF(TRIM(d.name), ''), '—') AS distance_name,
            LOWER(TRIM(COALESCE(p.gender, ''))) AS gender_code,
            cp.year AS event_year,
            cp.title AS event_title,
            COALESCE(NULLIF(TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')), ''), '—') AS athlete_name,
            COALESCE(NULLIF(TRIM(r.finish_time), ''), '—') AS finish_time,
            r.finish_time_sec AS finish_time_sec
        FROM results r
        INNER JOIN comp_pool cp ON cp.id = r.competition_id
        INNER JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE COALESCE(r.dnf, 0) = 0
          AND r.finish_time_sec IS NOT NULL
          AND LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'f')
          AND TRIM(COALESCE(cp.title_series, '')) != ''
        """,
        tuple(plist),
    )
    rules = load_distance_alias_rules()
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in base_rows:
        event_series = str(row.get("event_series") or "").strip()
        if not event_series:
            continue
        gender_code = str(row.get("gender_code") or "").strip().lower()
        if gender_code not in {"m", "f"}:
            continue
        d_key, d_label = normalize_distance_by_alias_rules(row.get("distance_name"), rules)
        key = (event_series, d_key, gender_code)
        item = dict(row)
        item["distance_label"] = d_label
        grouped.setdefault(key, []).append(item)

    out: list[dict[str, Any]] = []
    for (event_series, _dkey, gender_code), items in grouped.items():
        items.sort(
            key=lambda r: (
                float(r.get("finish_time_sec") or 0),
                int(r.get("event_year") or 0),
                str(r.get("event_title") or ""),
            )
        )
        for rank, row in enumerate(items[:n], start=1):
            out.append(
                {
                    "Событие": event_series,
                    "Дистанция": row.get("distance_label") or "—",
                    "Пол": "Мужчины" if gender_code == "m" else "Женщины",
                    "Место": rank,
                    "Год": row.get("event_year"),
                    "Этап": row.get("event_title"),
                    "Участник": row.get("athlete_name"),
                    "Время": row.get("finish_time"),
                }
            )
    out.sort(
        key=lambda r: (
            str(r.get("Событие") or "").casefold(),
            str(r.get("Дистанция") or "").casefold(),
            0 if r.get("Пол") == "Мужчины" else 1,
            int(r.get("Место") or 0),
            int(r.get("Год") or 0),
            str(r.get("Этап") or "").casefold(),
        )
    )
    return out


def query_vm_records_champions_cards(
    db_path: Path | str,
    years: list[int] | None,
    sport: str | None,
) -> dict[str, dict[str, Any]]:
    """
    По той же логике, что «Рекорды ВМ»: серия событий + нормализованная дистанция + пол —
    считаем действующего рекордсмена (лучшее время в группе) и агрегируем:
    количество «слотов» рекорда на участника, отдельно для мужчин и женщин.

    Возвращает словарь:
      males: {'profile_id', 'participant', 'city', 'records'} | пустые значения если никого
      females: аналогично.
    Спорт задаётся отдельно (обычно run/ski/bike/trail_run), чтобы считать карточки по видам.
    """
    blank = {"profile_id": None, "participant": "", "city": "", "records": 0}
    sp = str(sport or "").strip().lower()
    if not sp:
        return {"males": dict(blank), "females": dict(blank)}
    sport_filter: list[str] | None = [sp]
    w, plist = build_competition_filter_sql(years, sport_filter, None)
    series_expr = (
        "COALESCE(NULLIF(TRIM(c.title_short), ''), TRIM(c.title))"
        if _table_has_column(db_path, "competitions", "title_short")
        else "TRIM(c.title)"
    )
    base_rows = q_all(
        db_path,
        f"""
        WITH comp_pool AS (
            SELECT
                c.id,
                c.year,
                c.title,
                c.sport,
                {series_expr} AS title_series
            FROM competitions c
            WHERE {w}
        )
        SELECT
            cp.title_series AS event_series,
            COALESCE(NULLIF(TRIM(d.name), ''), '—') AS distance_name,
            LOWER(TRIM(COALESCE(p.gender, ''))) AS gender_code,
            cp.year AS event_year,
            cp.title AS event_title,
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '')) AS last_name,
            TRIM(COALESCE(p.first_name, '')) AS first_name,
            COALESCE(NULLIF(TRIM(p.city), ''), '—') AS city_raw,
            COALESCE(NULLIF(TRIM(r.finish_time), ''), '—') AS finish_time,
            r.finish_time_sec AS finish_time_sec
        FROM results r
        INNER JOIN comp_pool cp ON cp.id = r.competition_id
        INNER JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE COALESCE(r.dnf, 0) = 0
          AND r.finish_time_sec IS NOT NULL
          AND r.profile_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'f')
          AND TRIM(COALESCE(cp.title_series, '')) != ''
        """,
        tuple(plist),
    )
    rules = load_distance_alias_rules()
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in base_rows:
        event_series = str(row.get("event_series") or "").strip()
        if not event_series:
            continue
        gender_code = str(row.get("gender_code") or "").strip().lower()
        if gender_code not in {"m", "f"}:
            continue
        d_key, d_label = normalize_distance_by_alias_rules(row.get("distance_name"), rules)
        item = dict(row)
        item["distance_label"] = d_label
        key = (event_series, d_key, gender_code)
        grouped.setdefault(key, []).append(item)

    def _participant_label(row: dict[str, Any]) -> str:
        ln = str(row.get("last_name") or "").strip()
        fn = str(row.get("first_name") or "").strip()
        lab = (ln + " " + fn).strip()
        return lab if lab else "—"

    def _winner_for_bucket(items: list[dict[str, Any]]) -> tuple[int | None, str]:
        items_sorted = sorted(
            items,
            key=lambda r: (
                float(r.get("finish_time_sec") or 0),
                int(r.get("event_year") or 0),
                str(r.get("event_title") or ""),
                int(r.get("profile_id") or 0),
            ),
        )
        wrow = items_sorted[0]
        pid_raw = wrow.get("profile_id")
        try:
            pid = int(pid_raw) if pid_raw is not None else None
        except (TypeError, ValueError):
            pid = None
        raw_city = str(wrow.get("city_raw") or "").strip()
        city_out = raw_city if raw_city else "—"
        return pid, city_out

    male_counts: dict[int, tuple[int, str]] = {}
    female_counts: dict[int, tuple[int, str]] = {}
    pid_to_label: dict[int, str] = {}

    for (_es, _dk, gc), items in grouped.items():
        pid, city = _winner_for_bucket(items)
        if pid is None or pid <= 0:
            continue
        wrow_best = sorted(
            items,
            key=lambda r: (
                float(r.get("finish_time_sec") or 0),
                int(r.get("event_year") or 0),
                str(r.get("event_title") or ""),
            ),
        )[0]
        pid_to_label[pid] = _participant_label(wrow_best)
        tgt = male_counts if gc == "m" else female_counts
        cur_cnt, cur_city = tgt.get(pid, (0, city))
        tgt[pid] = (cur_cnt + 1, city)

    def _best_from(counts: dict[int, tuple[int, str]]) -> dict[str, Any]:
        if not counts:
            return dict(blank)
        best_pid: int | None = None
        best_n = -1
        for pid_val, (n_rec, _c) in counts.items():
            if n_rec > best_n or (
                n_rec == best_n
                and (
                    best_pid is None or pid_val < best_pid
                )
            ):
                best_n = n_rec
                best_pid = pid_val
        if best_pid is None or best_n <= 0:
            return dict(blank)
        _city = counts.get(best_pid, (best_n, "—"))[1]
        return {
            "profile_id": best_pid,
            "participant": pid_to_label.get(best_pid, ""),
            "city": _city,
            "records": best_n,
        }

    return {"males": _best_from(male_counts), "females": _best_from(female_counts)}


# ── Кубки ─────────────────────────────────────────────────────────────────


def query_cups_summary(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.id, c.title AS кубок, c.year AS год, c.sport AS вид,
               COUNT(DISTINCT cc.competition_id) AS событий,
               COUNT(DISTINCT cr.profile_id) AS участников
        FROM cups c
        LEFT JOIN cup_competitions cc ON cc.cup_id = c.id
        LEFT JOIN cup_results cr ON cr.cup_id = c.id
        GROUP BY c.id
        ORDER BY год DESC
        """,
    )


def query_cup_leaders(db_path: Path | str, limit: int = 40) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        f"""
        SELECT cu.title AS кубок,
               p.last_name || ' ' || p.first_name AS участник,
               cr.group_name AS группа,
               cr.place_abs AS место,
               ROUND(cr.total_points, 1) AS очков,
               cr.competitions_count AS стартов_в_кубке
        FROM cup_results cr
        JOIN cups cu ON cu.id = cr.cup_id
        LEFT JOIN profiles p ON p.id = cr.profile_id
        WHERE cr.place_abs <= 10
        ORDER BY cu.id, cr.distance_id, cr.place_abs
        LIMIT {int(limit)}
        """,
    )


def query_profile_cup_result_years(db_path: Path | str) -> list[int]:
    """Годы, для которых в базе есть строки profile_cup_results."""
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT year AS y
        FROM profile_cup_results
        WHERE year IS NOT NULL
        ORDER BY y DESC
        """,
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_profile_cup_summaries_for_year(db_path: Path | str, year: int) -> list[dict[str, Any]]:
    """
    Сводка кубков за год по profile_cup_results: один ряд на cup_id.
    """
    return q_all(
        db_path,
        """
        SELECT
            pcr.cup_id AS id,
            MAX(COALESCE(cu.title, pcr.cup_title)) AS кубок,
            COUNT(DISTINCT pcr.profile_id) AS участников
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        WHERE pcr.year = ? AND pcr.cup_id IS NOT NULL
        GROUP BY pcr.cup_id
        ORDER BY MAX(COALESCE(cu.title, pcr.cup_title))
        """,
        (year,),
    )


def query_profile_cup_detail_for_year_cup(
    db_path: Path | str, year: int, cup_id: int
) -> list[dict[str, Any]]:
    """
    Строки profile_cup_results за год и кубок + ФИО из profiles.
    Места из официальной таблицы cup_results (JOIN по cup_id, profile_id, distance_id):
    cup_place_abs / cup_place_gender / cup_place_group; pcr_place_abs — fallback из profile_cup_results.

    Колонка ``очков``: из ``cup_results.raw`` — массив ``competition_points[]`` (поля
    ``competition_id`` + ``points``), элемент с ``competition_id`` как у отображаемого этапа
    (тот же подзапрос, что для «Событие»); иначе скаляр в ``$.competition_points`` для старых
    дампов; иначе ``cup_scoring_computed_finishes`` / ``profile_cup_results.total_points``.
    """
    ensure_cup_scoring_schema(db_path)
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    comp_one = _sql_profile_cup_stage_competition_id_one("pcr")
    # Массив объектов {competition_id, points, ...} — см. API кубка в cup_results.raw
    pts_from_cr_array = f"""COALESCE(
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.competition_points') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      ),
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.competitionPoints') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      ),
      (
        SELECT CAST(json_extract(j.value, '$.points') AS REAL)
        FROM json_each(cr.raw, '$.result.competition_points') AS j
        WHERE CAST(json_extract(j.value, '$.competition_id') AS INTEGER) = CAST(({comp_one}) AS INTEGER)
        LIMIT 1
      )
    )"""
    pts_from_cr_scalar = """(
      CASE
        WHEN cr.raw IS NULL OR TRIM(COALESCE(cr.raw, '')) = '' THEN NULL
        WHEN json_type(cr.raw, '$.competition_points') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.competition_points') AS REAL)
        WHEN json_type(cr.raw, '$.competitionPoints') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.competitionPoints') AS REAL)
        WHEN json_type(cr.raw, '$.result.competition_points') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.result.competition_points') AS REAL)
        WHEN json_type(cr.raw, '$.result.competitionPoints') IN ('integer', 'real', 'text')
          THEN CAST(json_extract(cr.raw, '$.result.competitionPoints') AS REAL)
        ELSE NULL
      END
    )"""
    pts_from_cr_raw = f"""COALESCE({pts_from_cr_array}, {pts_from_cr_scalar})"""
    return q_all(
        db_path,
        f"""
        SELECT
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            COALESCE(p.last_name, '') AS last_name,
            COALESCE(p.gender, '') AS gender,
            {ev} AS событие,
            {dist} AS дистанция,
            pcr.group_name AS группа,
            COALESCE(
              {pts_from_cr_raw},
              (
                SELECT CAST(f.points_awarded AS REAL)
                FROM cup_scoring_computed_finishes f
                WHERE f.rule_version = '2026_run_v1'
                  AND f.cup_id = pcr.cup_id
                  AND f.year = pcr.year
                  AND f.profile_id = pcr.profile_id
                  AND f.competition_id = ({comp_one})
                LIMIT 1
              ),
              ROUND(pcr.total_points, 1)
            ) AS очков,
            pcr.raw AS raw,
            pcr.place_abs AS pcr_place_abs,
            cr.place_abs AS cup_place_abs,
            cr.place_gender AS cup_place_gender,
            cr.place_group AS cup_place_group
        FROM profile_cup_results pcr
        LEFT JOIN profiles p ON p.id = pcr.profile_id
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.year = ? AND pcr.cup_id = ?
        ORDER BY {dist},
                 (COALESCE(cr.place_abs, pcr.place_abs) IS NULL),
                 COALESCE(cr.place_abs, pcr.place_abs),
                 участник
        """,
        (year, cup_id),
    )


def cup_detail_resolve_display_place(
    row: dict[str, Any],
    gender_mode: str,
    age_group: str,
) -> Any:
    """
    Значение колонки «место» для таблицы результатов кубка:
    при выбранной возрастной группе — cup_results.place_group;
    при фильтре «Мужчины»/«Женщины» — cup_results.place_gender;
    в абсолютном зачёте — cup_results.place_abs.
    Если в cup_results нет строки — fallback на place_abs из profile_cup_results (pcr_place_abs).
    """
    pcr_abs = row.get("pcr_place_abs")
    ca = row.get("cup_place_abs")
    cg = row.get("cup_place_gender")
    cgr = row.get("cup_place_group")
    if age_group and str(age_group).strip() and age_group != "Все":
        return cgr if cgr is not None else pcr_abs
    if gender_mode in ("Мужчины", "Женщины"):
        return cg if cg is not None else pcr_abs
    return ca if ca is not None else pcr_abs


def query_cup_team_score_rows(db_path: Path | str, cup_id: int, year: int) -> list[dict[str, Any]]:
    """
    Строки очков кубка (как на вкладке «Личное первенство»): profile_cup_results,
    только участники, у которых в финишах соревнований этого кубка за год
    указана команда (results.team через cup_competitions).
    Очки — total_points из profile_cup_results; команда — MIN(TRIM(team)) по финишам в зачёте кубка.
    """
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT
            tm.team AS команда,
            pcr.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS участник,
            {cupn} AS кубок,
            {ev} AS событие,
            {dist} AS дистанция,
            COALESCE(pcr.group_name, '') AS группа,
            pcr.total_points AS очков,
            pcr.raw AS raw,
            COALESCE(cr.place_abs, pcr.place_abs) AS место_абс
        FROM profile_cup_results pcr
        INNER JOIN (
            SELECT r.profile_id, MIN(TRIM(r.team)) AS team
            FROM results r
            INNER JOIN cup_competitions cc
              ON cc.competition_id = r.competition_id AND cc.cup_id = ?
            INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
            WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
            GROUP BY r.profile_id
        ) tm ON tm.profile_id = pcr.profile_id
        LEFT JOIN profiles p ON p.id = pcr.profile_id
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.year = ? AND pcr.cup_id = ?
        ORDER BY tm.team, {dist}, участник
        """,
        (cup_id, year, year, cup_id),
    )


def query_profile_cup_results_lines_for_member(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> list[dict[str, Any]]:
    """
    Все строки **profile_cup_results** участника по кубку и году (одна строка ответа = одна строка БД):
    событие, дистанция, место абсолют (cup_results / fallback pcr), очки, raw для времени.
    """
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    return q_all(
        db_path,
        f"""
        SELECT
            {ev} AS событие,
            {dist} AS дистанция,
            COALESCE(cr.place_abs, pcr.place_abs) AS место_абс,
            pcr.total_points AS очков,
            pcr.raw AS raw
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        LEFT JOIN cup_results cr
          ON cr.cup_id = pcr.cup_id
         AND cr.profile_id = pcr.profile_id
         AND cr.distance_id = pcr.distance_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )


def query_profile_cup_team_member_competition_rows(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> list[dict[str, Any]]:
    """
    Все финиши участника в соревнованиях кубка за год: по строке на **results**
    (cup_competitions связывает competition_id с cup_id).
    Колонка ``очков`` — ``profile_cup_results.total_points`` только при явном совпадении
    ``competition.id`` / ``competition_id`` / ``result.competition.id`` в **raw** с
    ``results.competition_id`` (в т.ч. через CAST). Сопоставление по названию — в UI
    через ``map_profile_cup_points_by_title_distance`` (только текст из **raw**), иначе
    снова схлопывается в один этап.
    """
    return q_all(
        db_path,
        """
        SELECT
            c.title AS событие,
            COALESCE(NULLIF(TRIM(d.name), ''), '') AS дистанция,
            r.place_abs AS место_абс,
            r.competition_id AS competition_id,
            (
              SELECT pcr.total_points
              FROM profile_cup_results pcr
              WHERE pcr.profile_id = r.profile_id
                AND pcr.cup_id = ?
                AND pcr.year = ?
                AND (
                  json_extract(pcr.raw, '$.competition.id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.competition.id') AS INTEGER) = r.competition_id
                  OR json_extract(pcr.raw, '$.competition_id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.competition_id') AS INTEGER) = r.competition_id
                  OR json_extract(pcr.raw, '$.result.competition.id') = r.competition_id
                  OR CAST(json_extract(pcr.raw, '$.result.competition.id') AS INTEGER) = r.competition_id
                )
              ORDER BY pcr.id
              LIMIT 1
            ) AS очков,
          COALESCE(NULLIF(TRIM(r.finish_time), ''), '') AS время
        FROM results r
        INNER JOIN cup_competitions cc
          ON cc.competition_id = r.competition_id AND cc.cup_id = ?
        INNER JOIN competitions c ON c.id = r.competition_id AND c.year = ?
        INNER JOIN distances d ON d.id = r.distance_id AND d.competition_id = c.id
        WHERE r.profile_id = ?
          AND COALESCE(r.dnf, 0) = 0
        ORDER BY c.date ASC, c.id ASC, d.id ASC
        """,
        (cup_id, year, cup_id, year, profile_id),
    )


def aggregate_team_cup_points_top_five(members: list[dict[str, Any]]) -> int:
    """
    Командный зачёт: сумма индивидуальных очков пяти лучших участников.
    По каждому участнику — сумма int(round(total_points)) по его строкам profile_cup_results в этой выборке.
    """
    from collections import defaultdict

    by_pid: dict[int, int] = defaultdict(int)
    for r in members:
        pid = int(r["profile_id"])
        v = r.get("очков")
        try:
            row_pts = int(round(float(v))) if v is not None else 0
        except (TypeError, ValueError):
            row_pts = 0
        by_pid[pid] += row_pts
    scores = sorted(by_pid.values(), reverse=True)
    return int(sum(scores[:5]))


def parse_profile_cup_raw_age_group_label(raw: str | None) -> str:
    """
    Метка возрастной/зачётной группы из profile_cup_results.raw
    (JSON элемента списка API …/profile/{id}/cup-results/?year=).
    """
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    group = item.get("group")
    if isinstance(group, dict):
        name = group.get("name") or group.get("title")
        if name is not None and str(name).strip():
            return str(name).strip()
        try:
            af = group.get("age_from")
            at = group.get("age_to")
            if af is not None and at is not None:
                return f"{int(af)}-{int(at)}"
        except (TypeError, ValueError):
            pass
    return ""


def parse_profile_cup_raw_finish_time(raw: str | None) -> str:
    """Время финиша из JSON profile_cup_results.raw (если API отдал)."""
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    for key in ("total_time", "finish_time", "time", "totalTime", "result_time"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    res = item.get("result")
    if isinstance(res, dict):
        for key in ("total_time", "finish_time", "time", "total_time"):
            v = res.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def parse_profile_cup_raw_event_title(raw: str | None) -> str:
    """Название события из JSON profile_cup_results.raw (если API отдал вложенный объект)."""
    if not raw or not str(raw).strip():
        return ""
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if not isinstance(item, dict):
        return ""
    comp = item.get("competition")
    if isinstance(comp, dict):
        t = comp.get("title") or comp.get("title_short") or comp.get("name")
        if t is not None and str(t).strip():
            return str(t).strip()
    for key in ("competition_title", "event_title", "marathon_title"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    res = item.get("result")
    if isinstance(res, dict):
        c2 = res.get("competition")
        if isinstance(c2, dict):
            t2 = c2.get("title") or c2.get("title_short") or c2.get("name")
            if t2 is not None and str(t2).strip():
                return str(t2).strip()
    return ""


def parse_profile_cup_raw_competition_id(raw: str | None) -> int | None:
    """Идентификатор соревнования из JSON profile_cup_results.raw (связь со строкой results.competition_id)."""
    if not raw or not str(raw).strip():
        return None
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(item, dict):
        return None
    def _to_int_id(v: Any) -> int | None:
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        try:
            return int(float(str(v).strip()))
        except (TypeError, ValueError):
            return None

    for key in ("competition_id", "competitionId", "event_id", "marathon_id", "eventId"):
        got = _to_int_id(item.get(key))
        if got is not None:
            return got
    mar = item.get("marathon")
    if isinstance(mar, dict):
        got = _to_int_id(mar.get("id"))
        if got is not None:
            return got
    comp = item.get("competition")
    if isinstance(comp, dict):
        for k in ("id", "pk", "competition_id"):
            got = _to_int_id(comp.get(k))
            if got is not None:
                return got
    res = item.get("result")
    if isinstance(res, dict):
        c2 = res.get("competition")
        if isinstance(c2, dict):
            got = _to_int_id(c2.get("id"))
            if got is not None:
                return got
    return None


def map_profile_cup_points_by_competition_id(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> dict[int, float]:
    """
    Словарь competition_id → total_points из **profile_cup_results** (по ``raw``).
    При нескольких строках с одним id берётся последняя по ``pcr.id``.
    """
    rows = q_all(
        db_path,
        """
        SELECT id, raw, total_points
        FROM profile_cup_results
        WHERE profile_id = ? AND cup_id = ? AND year = ?
        ORDER BY id
        """,
        (profile_id, cup_id, year),
    )
    out: dict[int, float] = {}
    for r in rows:
        cid = parse_profile_cup_raw_competition_id(r.get("raw"))
        if cid is None:
            continue
        v = r.get("total_points")
        try:
            out[cid] = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            out[cid] = 0.0
    return out


def norm_cup_match_title(title: str | None) -> str:
    """Нормализация названия для сопоставления строк кубка (без года в скобках)."""
    if not title or not str(title).strip():
        return ""
    s = str(title).strip().casefold()
    if " (" in s:
        s = s.split(" (", 1)[0].strip()
    return " ".join(s.split())


def norm_cup_match_distance(dist: str | None) -> str:
    """Нормализация подписи дистанции."""
    if not dist or not str(dist).strip():
        return ""
    s = str(dist).strip().casefold().replace(",", ".")
    s = s.replace("км", "km")
    return " ".join(s.split())


def profile_cup_raw_event_title_strings(raw: str | None) -> list[str]:
    """Все варианты названия этапа из ``profile_cup_results.raw`` (как для карты очков)."""
    raw_titles: list[str] = []

    def _add_title(s: str | None) -> None:
        if s is None or not str(s).strip():
            return
        t0 = str(s).strip()
        if t0 not in raw_titles:
            raw_titles.append(t0)

    _add_title(parse_profile_cup_raw_event_title(raw))
    try:
        item = json.loads(raw or "")
    except (json.JSONDecodeError, TypeError):
        item = None
    if isinstance(item, dict):
        comp = item.get("competition")
        if isinstance(comp, dict):
            for k in ("title_short", "title", "name"):
                v2 = comp.get(k)
                if v2 is not None and str(v2).strip():
                    _add_title(str(v2))
        rc = item.get("result")
        if isinstance(rc, dict):
            c3 = rc.get("competition")
            if isinstance(c3, dict):
                for k in ("title_short", "title", "name"):
                    v3 = c3.get(k)
                    if v3 is not None and str(v3).strip():
                        _add_title(str(v3))
    return raw_titles


def profile_cup_title_norms_from_raw(raw: str | None) -> set[str]:
    """Нормализованные названия этапа из ``raw``."""
    out: set[str] = set()
    for t in profile_cup_raw_event_title_strings(raw):
        n = norm_cup_match_title(t)
        if n:
            out.add(n)
    return out


def profile_cup_distance_norms_from_pcr_row(row: dict[str, Any]) -> set[str]:
    """Нормализованные подписи дистанции из строки ``profile_cup_results`` (+ ``cd``)."""
    out: set[str] = set()
    for k in ("distance_name", "cd_name"):
        v = row.get(k)
        if v is not None and str(v).strip():
            n = norm_cup_match_distance(str(v).strip())
            if n:
                out.add(n)
    return out


def assign_profile_cup_points_to_result_lines(
    db_path: Path | str,
    profile_id: int,
    cup_id: int,
    year: int,
    lines: list[dict[str, Any]],
) -> list[int | None]:
    """
    Для строк из ``query_profile_cup_team_member_competition_rows`` (есть ``competition_id``)
    сопоставляет каждую строку финиша с **одной** записью ``profile_cup_results`` без повторов:
    сначала по ``competition.id`` в ``raw``, иначе по паре (нормализованное название, дистанция).

    Для таблицы только из ``profile_cup_results`` (без ключа ``competition_id``) возвращает
    список из ``None`` — там очки уже в колонке ``очков``.
    """
    if not lines:
        return []
    if not all("competition_id" in ln for ln in lines):
        return [None] * len(lines)

    pcr_rows = q_all(
        db_path,
        """
        SELECT pcr.id, pcr.raw, pcr.total_points, pcr.distance_name, cd.name AS cd_name
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )
    if not pcr_rows:
        return [None] * len(lines)

    used: set[int] = set()
    out: list[int | None] = []

    def _pts_int(row: dict[str, Any]) -> int | None:
        v = row.get("total_points")
        try:
            return int(round(float(v))) if v is not None else None
        except (TypeError, ValueError):
            return None

    for line in lines:
        best: dict[str, Any] | None = None
        lcid = line.get("competition_id")
        lcid_i: int | None = None
        if lcid is not None:
            try:
                lcid_i = int(lcid)
            except (TypeError, ValueError):
                lcid_i = None

        if lcid_i is not None:
            for r in pcr_rows:
                rid = int(r["id"])
                if rid in used:
                    continue
                if parse_profile_cup_raw_competition_id(r.get("raw")) == lcid_i:
                    best = r
                    break

        if best is None:
            lt = norm_cup_match_title((line.get("событие") or "").strip())
            ld = norm_cup_match_distance((line.get("дистанция") or "").strip())
            if lt and ld:
                cands: list[dict[str, Any]] = []
                for r in pcr_rows:
                    rid = int(r["id"])
                    if rid in used:
                        continue
                    tns = profile_cup_title_norms_from_raw(r.get("raw"))
                    dns = profile_cup_distance_norms_from_pcr_row(r)
                    if lt in tns and ld in dns:
                        cands.append(r)
                if len(cands) == 1:
                    best = cands[0]
                elif len(cands) > 1:
                    best = min(cands, key=lambda x: int(x["id"]))

        if best is not None:
            used.add(int(best["id"]))
            out.append(_pts_int(best))
        else:
            out.append(None)

    return out


def _map_cup_register_title_distance_keys(
    out: dict[tuple[str, str], float], titles: list[str], d: str, pts: float
) -> None:
    """Пишет ключи (title_norm, d) либо (title_norm, '') если дистанции в строке pcr нет."""
    seen_t: set[str] = set()
    for tit in titles:
        t = norm_cup_match_title(tit)
        if not t or t in seen_t:
            continue
        seen_t.add(t)
        if d:
            out[(t, d)] = pts
        else:
            out[(t, "")] = pts


def map_profile_cup_points_by_title_distance(
    db_path: Path | str, profile_id: int, cup_id: int, year: int
) -> dict[tuple[str, str], float]:
    """
    Ключ (нормализованное название, нормализованная дистанция) → ``total_points``
    по строкам **profile_cup_results**.

    Используются только названия из **raw** (и поля ``competition.*`` внутри JSON), без
    «разрешённого» заголовка этапа из SQL: иначе для нескольких строк кубка получается
    один и тот же этап (последний по дате), все ключи схлопываются и в UI остаётся одна
    сумма очков.
    """
    rows = q_all(
        db_path,
        """
        SELECT pcr.raw, pcr.total_points, pcr.distance_name, cd.name AS cd_name
        FROM profile_cup_results pcr
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.cup_id = ? AND pcr.year = ?
        ORDER BY pcr.id
        """,
        (profile_id, cup_id, year),
    )
    out: dict[tuple[str, str], float] = {}
    for r in rows:
        d_raw = (r.get("distance_name") or "").strip()
        if not d_raw:
            d_raw = (r.get("cd_name") or "").strip()
        d = norm_cup_match_distance(d_raw)
        v = r.get("total_points")
        try:
            pts = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            pts = 0.0
        raw_titles = profile_cup_raw_event_title_strings(r.get("raw"))
        if not raw_titles and not d:
            continue
        if raw_titles:
            _map_cup_register_title_distance_keys(out, raw_titles, d, pts)
        elif d:
            # только дистанция без названия — не создаём ключи, чтобы не размазать очки
            pass
    return out


def _sql_profile_cup_stage_event_title_one(pcr: str = "pcr") -> str:
    """Одно соревнование (последнее по дате среди подходящих под строку profile_cup_results)."""
    return f"""(
      SELECT c2.title
      FROM cup_competitions cc2
      INNER JOIN competitions c2 ON c2.id = cc2.competition_id AND c2.year = {pcr}.year
      INNER JOIN results r2 ON r2.competition_id = c2.id
        AND r2.profile_id = {pcr}.profile_id AND COALESCE(r2.dnf, 0) = 0
      LEFT JOIN distances d2 ON d2.id = r2.distance_id AND d2.competition_id = c2.id
      LEFT JOIN cup_distances cd2 ON cd2.id = {pcr}.distance_id AND cd2.cup_id = {pcr}.cup_id
      WHERE cc2.cup_id = {pcr}.cup_id
        AND (
          cd2.id IS NULL
          OR (
            (d2.distance_km IS NOT NULL AND cd2.distance_km IS NOT NULL
              AND ABS(d2.distance_km - cd2.distance_km) < 0.06)
            OR (TRIM(COALESCE(d2.name, '')) != ''
              AND TRIM(d2.name) = TRIM(COALESCE(cd2.name, '')))
            OR (TRIM(COALESCE({pcr}.distance_name, '')) != ''
              AND TRIM(d2.name) = TRIM({pcr}.distance_name))
          )
        )
      ORDER BY c2.date DESC, c2.id DESC
      LIMIT 1
    )"""


def _sql_profile_cup_stage_competition_id_one(pcr: str = "pcr") -> str:
    """Тот же этап, что ``_sql_profile_cup_stage_event_title_one``, но ``competitions.id``."""
    return f"""(
      SELECT c2.id
      FROM cup_competitions cc2
      INNER JOIN competitions c2 ON c2.id = cc2.competition_id AND c2.year = {pcr}.year
      INNER JOIN results r2 ON r2.competition_id = c2.id
        AND r2.profile_id = {pcr}.profile_id AND COALESCE(r2.dnf, 0) = 0
      LEFT JOIN distances d2 ON d2.id = r2.distance_id AND d2.competition_id = c2.id
      LEFT JOIN cup_distances cd2 ON cd2.id = {pcr}.distance_id AND cd2.cup_id = {pcr}.cup_id
      WHERE cc2.cup_id = {pcr}.cup_id
        AND (
          cd2.id IS NULL
          OR (
            (d2.distance_km IS NOT NULL AND cd2.distance_km IS NOT NULL
              AND ABS(d2.distance_km - cd2.distance_km) < 0.06)
            OR (TRIM(COALESCE(d2.name, '')) != ''
              AND TRIM(d2.name) = TRIM(COALESCE(cd2.name, '')))
            OR (TRIM(COALESCE({pcr}.distance_name, '')) != ''
              AND TRIM(d2.name) = TRIM({pcr}.distance_name))
          )
        )
      ORDER BY c2.date DESC, c2.id DESC
      LIMIT 1
    )"""


def _sql_profile_cup_stage_event_display(pcr: str = "pcr") -> str:
    """
    Одна строка на запись profile_cup_results: одно название события
    (по связке cup_competitions + results + дистанция этапа), иначе из raw JSON.
    """
    one = _sql_profile_cup_stage_event_title_one(pcr)
    return f"""COALESCE(
      NULLIF(TRIM(CAST({one} AS TEXT)), ''),
      NULLIF(TRIM(json_extract({pcr}.raw, '$.competition.title')), ''),
      NULLIF(TRIM(json_extract({pcr}.raw, '$.competition.title_short')), '')
    )"""


def _sql_profile_cup_distance_display(pcr: str = "pcr", cd: str = "cd") -> str:
    """Дистанция: из API-строки или из cup_distances по id."""
    return f"""COALESCE(
      NULLIF(TRIM({pcr}.distance_name), ''),
      {cd}.name,
      ''
    )"""


def _sql_profile_cup_name_from_cups(pcr: str = "pcr", cu: str = "cu") -> str:
    """Название кубка из таблицы cups, иначе из строки profile_cup_results."""
    return f"COALESCE({cu}.title, {pcr}.cup_title)"


# ── Профиль ───────────────────────────────────────────────────────────────


def query_profile_row(db_path: Path | str, pid: int) -> dict[str, Any] | None:
    return q_one(db_path, "SELECT * FROM profiles WHERE id=?", (pid,))


def query_profile_results_history(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.year AS год, c.title AS событие, c.sport AS вид,
               d.name AS дистанция, ROUND(d.distance_km,1) AS км,
               r.finish_time AS время,
               r.place_abs AS место_абс,
               r.place_gender AS место_пол,
               r.group_name AS группа
        FROM results r
        JOIN competitions c ON c.id = r.competition_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND r.dnf = 0
        ORDER BY c.date DESC
        """,
        (pid,),
    )


def query_profile_cup_rows(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT pcr.year AS год,
               {cupn} AS кубок,
               {ev} AS событие,
               {dist} AS дистанция,
               pcr.place_abs AS место, pcr.group_name AS группа,
               ROUND(pcr.total_points, 1) AS очков
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ?
        ORDER BY год DESC, кубок, событие
        """,
        (pid,),
    )


def parse_profile_active_years(raw: str | None) -> list[int]:
    """Годы активности из JSON в profiles.raw (ключ active_years, см. import_profiles_csv)."""
    if not raw or not str(raw).strip():
        return []
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(obj, dict):
        return []
    ay = obj.get("active_years")
    if not isinstance(ay, list):
        return []
    out: list[int] = []
    for x in ay:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return sorted(set(out), reverse=True)


def query_profile_participation_years(db_path: Path | str, pid: int) -> list[int]:
    """Годы, в которых у участника есть финиш в событиях или строка в profile_cup_results."""
    rows = q_all(
        db_path,
        """
        SELECT y FROM (
            SELECT DISTINCT c.year AS y
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE r.profile_id = ? AND r.dnf = 0 AND c.year IS NOT NULL
            UNION
            SELECT DISTINCT year AS y
            FROM profile_cup_results
            WHERE profile_id = ? AND year IS NOT NULL
        )
        ORDER BY y DESC
        """,
        (pid, pid),
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_profile_results_history_for_year(
    db_path: Path | str, pid: int, year: int
) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        """
        SELECT c.year AS год, c.title AS событие, c.sport AS вид,
               d.name AS дистанция, ROUND(d.distance_km,1) AS км,
               r.finish_time AS время,
               r.place_abs AS место_абс,
               r.place_gender AS место_пол,
               r.group_name AS группа
        FROM results r
        JOIN competitions c ON c.id = r.competition_id
        JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND r.dnf = 0 AND c.year = ?
        ORDER BY c.date DESC
        """,
        (pid, year),
    )


def query_profile_cup_rows_for_year(
    db_path: Path | str, pid: int, year: int
) -> list[dict[str, Any]]:
    ev = _sql_profile_cup_stage_event_display("pcr")
    dist = _sql_profile_cup_distance_display("pcr", "cd")
    cupn = _sql_profile_cup_name_from_cups("pcr", "cu")
    return q_all(
        db_path,
        f"""
        SELECT pcr.year AS год,
               {cupn} AS кубок,
               {ev} AS событие,
               {dist} AS дистанция,
               pcr.place_abs AS место, pcr.group_name AS группа,
               ROUND(pcr.total_points, 1) AS очков
        FROM profile_cup_results pcr
        LEFT JOIN cups cu ON cu.id = pcr.cup_id
        LEFT JOIN cup_distances cd ON cd.id = pcr.distance_id AND cd.cup_id = pcr.cup_id
        WHERE pcr.profile_id = ? AND pcr.year = ?
        ORDER BY кубок, событие
        """,
        (pid, year),
    )


def query_profile_kpi_all_time(db_path: Path | str, pid: int) -> dict[str, Any]:
    """KPI участника за все годы по таблице results."""
    row = q_one(
        db_path,
        """
        SELECT
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            COUNT(DISTINCT c.id) AS events_distinct,
            COUNT(DISTINCT c.year) AS active_years,
            COUNT(DISTINCT CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.distance_id END) AS distances_distinct,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            MIN(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.finish_time_sec END) AS best_finish_time_sec,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 1 THEN 1 ELSE 0 END) AS first_places,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 2 THEN 1 ELSE 0 END) AS second_places,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 3 THEN 1 ELSE 0 END) AS third_places
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ?
        """,
        (pid,),
    )
    return row or {}


def query_profile_kpi_year(db_path: Path | str, pid: int, year: int) -> dict[str, Any]:
    """KPI участника за конкретный год по таблице results."""
    row = q_one(
        db_path,
        """
        SELECT
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            COUNT(DISTINCT c.id) AS events_distinct,
            COUNT(DISTINCT CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.distance_id END) AS distances_distinct,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            MIN(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.finish_time_sec END) AS best_finish_time_sec,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 1 THEN 1 ELSE 0 END) AS first_places,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 2 THEN 1 ELSE 0 END) AS second_places,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND COALESCE(r.place_abs, 999999) = 3 THEN 1 ELSE 0 END) AS third_places
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND c.year = ?
        """,
        (pid, year),
    )
    return row or {}


def query_profile_yearly_trends(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    """Динамика участника по годам: старты, финиши, dnf, км, среднее время."""
    return q_all(
        db_path,
        """
        SELECT
            c.year AS year,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            ROUND(AVG(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.finish_time_sec END), 1) AS avg_finish_time_sec,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS dnf_pct
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE r.profile_id = ? AND c.year IS NOT NULL
        GROUP BY c.year
        ORDER BY c.year
        """,
        (pid,),
    )


def query_profile_events_table(
    db_path: Path | str,
    pid: int,
    years: list[int] | None = None,
    sports: list[str] | None = None,
    distance_labels: list[str] | None = None,
    include_dnf: bool = True,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Универсальная таблица стартов участника с фильтрами."""
    where: list[str] = ["r.profile_id = ?"]
    params: list[Any] = [pid]
    if years:
        where.append("c.year IN (" + ",".join("?" * len(years)) + ")")
        params.extend(years)
    if sports:
        where.append("c.sport IN (" + ",".join("?" * len(sports)) + ")")
        params.extend(sports)
    if distance_labels:
        where.append("d.name IN (" + ",".join("?" * len(distance_labels)) + ")")
        params.extend(distance_labels)
    if not include_dnf:
        where.append("COALESCE(r.dnf, 0) = 0")
    wsql = " AND ".join(where)
    return q_all(
        db_path,
        f"""
        SELECT
            c.year AS год,
            c.date AS дата,
            c.title AS событие,
            c.sport AS вид,
            COALESCE(d.name, '') AS дистанция,
            ROUND(COALESCE(d.distance_km, 0), 2) AS км,
            COALESCE(r.finish_time, '') AS время,
            r.finish_time_sec AS время_сек,
            r.place_abs AS место_абс,
            r.place_gender AS место_пол,
            COALESCE(r.group_name, '') AS группа,
            COALESCE(r.team, '') AS команда,
            CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 'finish' ELSE 'dnf' END AS статус
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE {wsql}
        ORDER BY c.date DESC, c.id DESC, r.id DESC
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_profile_event_series_rows(
    db_path: Path | str,
    pid: int,
    years: list[int] | None = None,
    sports: list[str] | None = None,
    series_shorts: list[str] | None = None,
    include_dnf: bool = True,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    """Строки стартов участника для вкладки «Серия событий»."""
    series_expr = (
        "COALESCE(NULLIF(TRIM(c.title_short), ''), TRIM(c.title))"
        if _table_has_column(db_path, "competitions", "title_short")
        else "TRIM(c.title)"
    )
    where: list[str] = ["r.profile_id = ?"]
    params: list[Any] = [pid]
    if years:
        where.append("c.year IN (" + ",".join("?" * len(years)) + ")")
        params.extend(years)
    if sports:
        where.append("c.sport IN (" + ",".join("?" * len(sports)) + ")")
        params.extend(sports)
    if series_shorts:
        where.append(series_expr + " IN (" + ",".join("?" * len(series_shorts)) + ")")
        params.extend(series_shorts)
    if not include_dnf:
        where.append("COALESCE(r.dnf, 0) = 0")
    wsql = " AND ".join(where)
    rows = q_all(
        db_path,
        f"""
        SELECT
            c.year AS Год,
            c.title AS Событие,
            COALESCE(c.sport, '') AS вид,
            COALESCE(d.name, '') AS Дистанция,
            COALESCE(r.finish_time, '') AS время,
            r.place_abs AS место_абс,
            r.place_gender AS место_пол,
            COALESCE(r.team, '') AS команда,
            CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 'finish' ELSE 'dnf' END AS статус,
            {series_expr} AS _series_short
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE {wsql}
        ORDER BY c.date DESC, c.id DESC, r.id DESC
        LIMIT {int(limit)}
        """,
        tuple(params),
    )
    rules = load_distance_alias_rules()
    for r in rows:
        _, d_label = normalize_distance_by_alias_rules(r.get("Дистанция"), rules)
        r["Дистанция"] = d_label
    return rows


def query_profile_personal_bests(
    db_path: Path | str,
    pid: int,
    year: int | None = None,
    top_n: int = 1000,
) -> list[dict[str, Any]]:
    """PB по нормализованным дистанциям (через справочник алиасов)."""
    where = "r.profile_id = ? AND COALESCE(r.dnf, 0) = 0 AND r.finish_time_sec IS NOT NULL"
    params: list[Any] = [pid]
    if year is not None:
        where += " AND c.year = ?"
        params.append(year)
    rows = q_all(
        db_path,
        f"""
        SELECT
            c.year AS year,
            c.date AS event_date,
            c.title AS event_title,
            c.sport AS sport,
            COALESCE(d.name, '') AS distance_name,
            r.finish_time AS finish_time,
            r.finish_time_sec AS finish_time_sec,
            r.place_abs AS place_abs
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE {where}
        ORDER BY r.finish_time_sec ASC, c.date ASC, c.id ASC
        LIMIT {int(top_n)}
        """,
        tuple(params),
    )
    rules = load_distance_alias_rules()
    best_by_distance: dict[str, dict[str, Any]] = {}
    for r in rows:
        d_key, d_label = normalize_distance_by_alias_rules(r.get("distance_name"), rules)
        sec = r.get("finish_time_sec")
        try:
            sec_f = float(sec)
        except (TypeError, ValueError):
            continue
        cur = best_by_distance.get(d_key)
        if cur is None or sec_f < float(cur.get("время_сек") or 1e18):
            best_by_distance[d_key] = {
                "дистанция": d_label,
                "вид": str(r.get("sport") or "").strip(),
                "время": r.get("finish_time"),
                "время_сек": sec_f,
                "год": r.get("year"),
                "дата": r.get("event_date"),
                "событие": r.get("event_title"),
                "место_абс": r.get("place_abs"),
            }
    out = list(best_by_distance.values())
    out.sort(key=lambda x: (str(x.get("дистанция") or "").casefold(), float(x.get("время_сек") or 0)))
    return out


def query_profile_team_summary(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    """Команды участника: число финишей, событий и годы активности."""
    rows = q_all(
        db_path,
        """
        SELECT
            TRIM(r.team) AS team_name,
            COUNT(*) AS finishes,
            COUNT(DISTINCT c.id) AS events,
            GROUP_CONCAT(DISTINCT c.year) AS years_csv
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.profile_id = ?
          AND COALESCE(r.dnf, 0) = 0
          AND TRIM(COALESCE(r.team, '')) != ''
        GROUP BY TRIM(r.team)
        ORDER BY finishes DESC, team_name
        """,
        (pid,),
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        years_raw = str(r.get("years_csv") or "").split(",")
        years_vals: list[int] = []
        for y in years_raw:
            ys = y.strip()
            if ys.isdigit():
                years_vals.append(int(ys))
        years_vals = sorted(set(years_vals))
        out.append(
            {
                "команда": r.get("team_name"),
                "финишей": r.get("finishes"),
                "событий": r.get("events"),
                "годы": ", ".join(str(y) for y in years_vals) if years_vals else "—",
            }
        )
    return out


def query_profile_data_quality(db_path: Path | str, pid: int) -> list[dict[str, Any]]:
    """Диагностика качества данных участника (для админской вкладки)."""
    p = query_profile_row(db_path, pid) or {}
    stats = q_one(
        db_path,
        """
        SELECT
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            SUM(CASE WHEN COALESCE(dnf, 0) = 0 AND (finish_time_sec IS NULL OR finish_time_sec <= 0) THEN 1 ELSE 0 END) AS bad_finish_time,
            SUM(CASE WHEN COALESCE(dnf, 0) = 0 AND TRIM(COALESCE(finish_time, '')) = '' THEN 1 ELSE 0 END) AS missing_finish_time_text,
            SUM(CASE WHEN distance_id IS NULL THEN 1 ELSE 0 END) AS missing_distance_id,
            SUM(CASE WHEN competition_id IS NULL THEN 1 ELSE 0 END) AS missing_competition_id
        FROM results
        WHERE profile_id = ?
        """,
        (pid,),
    ) or {}
    checks: list[dict[str, Any]] = []
    checks.append({"метрика": "Пустой пол в profiles", "значение": 1 if not str(p.get("gender") or "").strip() else 0})
    checks.append({"метрика": "Пустой город в profiles", "значение": 1 if not str(p.get("city") or "").strip() else 0})
    checks.append({"метрика": "Стартов всего", "значение": int(stats.get("starts_total") or 0)})
    checks.append({"метрика": "Финишей", "значение": int(stats.get("finishes_total") or 0)})
    checks.append({"метрика": "DNF", "значение": int(stats.get("dnf_total") or 0)})
    checks.append({"метрика": "Финиши без finish_time_sec", "значение": int(stats.get("bad_finish_time") or 0)})
    checks.append({"метрика": "Финиши без finish_time (текст)", "значение": int(stats.get("missing_finish_time_text") or 0)})
    checks.append({"метрика": "Строки без distance_id", "значение": int(stats.get("missing_distance_id") or 0)})
    checks.append({"метрика": "Строки без competition_id", "значение": int(stats.get("missing_competition_id") or 0)})
    return checks


# ── Общая статистика (фильтры: год, вид спорта, кубок) ───────────────────────


def build_competition_filter_sql(
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> tuple[str, list[Any]]:
    """Фильтр по алиасу `c` (competitions). Пустой список / None = без ограничения по этому полю."""
    parts: list[str] = []
    params: list[Any] = []
    if years:
        parts.append("c.year IN (" + ",".join("?" * len(years)) + ")")
        params.extend(years)
    if sports:
        parts.append("c.sport IN (" + ",".join("?" * len(sports)) + ")")
        params.extend(sports)
    if cup_ids:
        parts.append(
            "c.id IN (SELECT competition_id FROM cup_competitions WHERE cup_id IN ("
            + ",".join("?" * len(cup_ids))
            + "))"
        )
        params.extend(cup_ids)
    return (" AND ".join(parts) if parts else "1=1"), params


def query_distinct_sports(db_path: Path | str) -> list[str]:
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT sport AS s FROM competitions
        WHERE sport IS NOT NULL AND TRIM(sport) != ''
        ORDER BY sport
        """,
    )
    return [str(r["s"]) for r in rows if r.get("s") is not None]


def parse_competition_date_value(value: Any) -> date | None:
    """Парсинг даты из поля ``competitions.date`` (ISO, дд.мм.гггг и частично ISO-дата)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    head10 = raw.replace("T", " ")[:10]
    formats = ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d")
    for fmt in formats:
        try:
            return datetime.strptime(head10, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(head10.replace(" ", "T")).date()
    except ValueError:
        return None


def query_competition_calendar_month_counts_year(
    db_path: Path | str,
    year: int,
) -> list[int]:
    """Число событий с валидной датой в каждом месяце ``year`` (индекс 0 = январь)."""
    y = int(year)
    rows = q_all(
        db_path,
        """
        SELECT COALESCE(date, '') AS date
        FROM competitions
        WHERE date IS NOT NULL AND TRIM(CAST(date AS TEXT)) != ''
        """,
        (),
    )
    cnt = [0] * 12
    for r in rows:
        dtp = parse_competition_date_value(r.get("date"))
        if dtp is None or dtp.year != y:
            continue
        m = max(1, min(12, int(dtp.month)))
        cnt[m - 1] += 1
    return cnt


def query_competitions_calendar_month_events(
    db_path: Path | str,
    year: int,
    month: int,
    *,
    today: date | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    События месяца в ``competitions`` с успешным разбором ``date``: кортеж
    (**прошедшие**, **предстоящие/сегодня**).

    Прошедшие: ``date < cut`` (календарный день), иначе — в предстоящих.

    Если колонки ``page_url`` нет — возвращает пустую строку.
    """
    cut = today if today is not None else date.today()
    y = int(year)
    m = max(1, min(12, int(month)))
    page_sel = (
        "COALESCE(TRIM(c.page_url), '') AS page_url"
        if _table_has_column(db_path, "competitions", "page_url")
        else "'' AS page_url"
    )
    rows = q_all(
        db_path,
        f"""
        SELECT
            c.id,
            COALESCE(TRIM(c.title), '') AS title,
            COALESCE(c.date, '') AS date,
            COALESCE(TRIM(c.sport), '') AS sport,
            {page_sel}
        FROM competitions c
        WHERE c.date IS NOT NULL AND TRIM(CAST(c.date AS TEXT)) != ''
        """,
        (),
    )
    past: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    for r in rows:
        dtp = parse_competition_date_value(r.get("date"))
        if dtp is None or dtp.year != y or dtp.month != m:
            continue
        item = {
            "id": int(r.get("id") or 0),
            "title": str(r.get("title") or "").strip() or "—",
            "sport": str(r.get("sport") or "").strip(),
            "page_url": str(r.get("page_url") or "").strip(),
            "day_of_month": dtp.day,
            "event_date_iso": dtp.isoformat(),
        }
        if dtp < cut:
            past.append(item)
        else:
            upcoming.append(item)
    sort_key = lambda x: (
        int(x.get("day_of_month") or 0),
        str(x.get("title") or "").casefold(),
        int(x.get("id") or 0),
    )
    past.sort(key=sort_key)
    upcoming.sort(key=sort_key)
    return past, upcoming


def query_upcoming_competitions_calendar_month(
    db_path: Path | str,
    year: int,
    month: int,
    *,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """
    Совместимость: только **предстоящие/сегодня** как раньше (``date >= today`` для календарного дня).
    """
    _past, fut = query_competitions_calendar_month_events(
        db_path, year, month, today=today
    )
    return fut


def query_cups_for_filter(db_path: Path | str) -> list[dict[str, Any]]:
    return q_all(
        db_path,
        "SELECT id, title, year FROM cups ORDER BY year DESC, title",
    )


def query_cups_for_obsh_header_filter(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
) -> list[dict[str, Any]]:
    """
    Кубки, у которых есть соревнование, удовлетворяющее отбору по году и виду спорта
    (пусто / None = без ограничения по полю; подмножество для мультиселекта «Кубок»).
    """
    w_parts: list[str] = []
    params: list[Any] = []
    if years:
        w_parts.append("c.year IN (" + ",".join("?" * len(years)) + ")")
        params.extend(years)
    if sports:
        w_parts.append("c.sport IN (" + ",".join("?" * len(sports)) + ")")
        params.extend(sports)
    inner = " AND ".join(w_parts) if w_parts else "1=1"
    return q_all(
        db_path,
        f"""
        SELECT
            cu.id,
            cu.title,
            COALESCE(cu.year, MAX(c.year)) AS year
        FROM cups cu
        JOIN cup_competitions cc ON cc.cup_id = cu.id
        JOIN competitions c ON c.id = cc.competition_id
        WHERE {inner}
        GROUP BY cu.id, cu.title, cu.year
        ORDER BY cu.year DESC, cu.title
        """,
        tuple(params),
    )


def query_general_stats_events_table(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
    bar_year: int | None = None,
) -> list[dict[str, Any]]:
    """
    События (competitions) с числом участников из competition_stats;
    тот же фильтр, что и у «Общей статистики»; bar_year — уточнение по клику на гистограмму по годам.
    """
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params: list[Any] = list(plist)
    if bar_year is not None:
        w = f"({w}) AND c.year = ?"
        params.append(bar_year)
    return q_all(
        db_path,
        f"""
        SELECT
            c.year AS год,
            c.title AS "Событие",
            COALESCE(cs.total_members, 0) AS "Количество участников"
        FROM competitions c
        LEFT JOIN competition_stats cs ON cs.competition_id = c.id
        WHERE {w}
        ORDER BY c.year DESC, c.title
        """,
        tuple(params),
    )


def query_general_stats_cards(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> dict[str, Any]:
    """Карточки: события, уникальные участники (финиши), регионы, команды, страны."""
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)

    events = q_one(db_path, f"SELECT COUNT(*) AS n FROM competitions c WHERE {w}", params)
    total_events = int(events["n"]) if events else 0

    part = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT r.profile_id) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.profile_id IS NOT NULL AND r.dnf = 0 AND {w}
        """,
        params,
    )
    total_participants = int(part["n"]) if part and part.get("n") is not None else 0

    reg = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT reg_key) AS n FROM (
            SELECT CASE
                WHEN p.region_id IS NOT NULL THEN 'id:' || CAST(p.region_id AS TEXT)
                WHEN TRIM(COALESCE(p.region, '')) != '' THEN 'n:' || TRIM(p.region)
                ELSE NULL
            END AS reg_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE {w}
        ) t
        WHERE reg_key IS NOT NULL
        """,
        params,
    )
    regions = int(reg["n"]) if reg and reg.get("n") is not None else 0

    teams = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT TRIM(r.team)) AS n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != '' AND {w}
        """,
        params,
    )
    total_teams = int(teams["n"]) if teams and teams.get("n") is not None else 0

    cntr = q_one(
        db_path,
        f"""
        SELECT COUNT(DISTINCT c_key) AS n FROM (
            SELECT CASE
                WHEN TRIM(COALESCE(p.country, '')) != '' THEN TRIM(p.country)
                ELSE NULL
            END AS c_key
            FROM profiles p
            INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE {w}
        ) t
        WHERE c_key IS NOT NULL
        """,
        params,
    )
    countries = int(cntr["n"]) if cntr and cntr.get("n") is not None else 0

    return {
        "total_events": total_events,
        "total_participants": total_participants,
        "regions_distinct": regions,
        "teams_distinct": total_teams,
        "countries_distinct": countries,
    }


def query_chart_events_by_year(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT c.year AS year, COUNT(*) AS events
        FROM competitions c
        WHERE c.year IS NOT NULL AND {w}
        GROUP BY c.year
        ORDER BY c.year
        """,
        params,
    )


def query_chart_unique_participants_by_year(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT c.year AS year, COUNT(DISTINCT r.profile_id) AS participants
        FROM competitions c
        INNER JOIN results r ON r.competition_id = c.id
        WHERE c.year IS NOT NULL AND r.profile_id IS NOT NULL AND r.dnf = 0 AND {w}
        GROUP BY c.year
        ORDER BY c.year
        """,
        params,
    )


def query_chart_events_by_sport(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT COALESCE(NULLIF(TRIM(c.sport), ''), 'other') AS sport, COUNT(*) AS n
        FROM competitions c
        WHERE {w}
        GROUP BY sport
        ORDER BY n DESC
        """,
        params,
    )


def query_chart_participants_by_gender(
    db_path: Path | str,
    years: list[int] | None,
    sports: list[str] | None,
    cup_ids: list[int] | None,
) -> list[dict[str, Any]]:
    w, plist = build_competition_filter_sql(years, sports, cup_ids)
    params = tuple(plist)
    return q_all(
        db_path,
        f"""
        SELECT COALESCE(NULLIF(TRIM(p.gender), ''), 'не указан') AS gender,
               COUNT(DISTINCT p.id) AS n
        FROM profiles p
        INNER JOIN results r ON r.profile_id = p.id AND r.dnf = 0
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE {w}
        GROUP BY gender
        ORDER BY n DESC
        """,
        params,
    )


# ── Команды (MVP: поле team в results) ─────────────────────────────────────


def query_teams_top(
    db_path: Path | str,
    year: int | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    if year is not None:
        return q_all(
            db_path,
            f"""
            SELECT TRIM(r.team) AS команда,
                   COUNT(*) AS финишей,
                   COUNT(DISTINCT r.profile_id) AS участников
            FROM results r
            JOIN competitions c ON c.id = r.competition_id
            WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
              AND c.year = ?
            GROUP BY команда
            ORDER BY финишей DESC
            LIMIT {int(limit)}
            """,
            (year,),
        )
    return q_all(
        db_path,
        f"""
        SELECT TRIM(r.team) AS команда,
               COUNT(*) AS финишей,
               COUNT(DISTINCT r.profile_id) AS участников
        FROM results r
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
        GROUP BY команда
        ORDER BY финишей DESC
        LIMIT {int(limit)}
        """,
    )


def query_team_names_for_select(
    db_path: Path | str,
    search: str | None,
    limit: int = 500,
) -> list[str]:
    """Уникальные названия команд из финишей (dnf=0), опционально поиск подстроки (без учёта регистра)."""
    lim = int(limit)
    base = """
        SELECT DISTINCT TRIM(r.team) AS name
        FROM results r
        WHERE r.dnf = 0 AND TRIM(COALESCE(r.team, '')) != ''
    """
    if search and search.strip():
        pat = f"%{search.strip()}%"
        rows = q_all(
            db_path,
            base + " AND LOWER(TRIM(r.team)) LIKE LOWER(?) ORDER BY name LIMIT ?",
            (pat, lim),
        )
    else:
        rows = q_all(db_path, base + " ORDER BY name LIMIT ?", (lim,))
    return [str(r["name"]) for r in rows if r.get("name")]


def query_team_stats(db_path: Path | str, team_name: str) -> dict[str, Any] | None:
    """
    Статистика по точному названию команды (TRIM(results.team)).
    Участники — уникальные profile_id с финишами; финиши — все строки results с dnf=0.
    """
    t = team_name.strip()
    if not t:
        return None
    agg = q_one(
        db_path,
        """
        SELECT
          COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants,
          COUNT(*) AS finishes,
          COUNT(DISTINCT c.year) AS active_years_n
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        """,
        (t,),
    )
    if not agg or int(agg.get("finishes") or 0) == 0:
        return None
    yrows = q_all(
        db_path,
        """
        SELECT DISTINCT c.year AS y
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        ORDER BY c.year
        """,
        (t,),
    )
    years_sorted = [int(x["y"]) for x in yrows if x.get("y") is not None]
    return {
        "participants": int(agg.get("participants") or 0),
        "finishes": int(agg.get("finishes") or 0),
        "active_years_count": int(agg.get("active_years_n") or 0),
        "active_years_list": years_sorted,
    }


def query_team_year_options_for_cups(db_path: Path | str, team_name: str) -> list[int]:
    """Годы для фильтра кубков: годы активности команды в событиях ∪ годы строк profile_cup_results её участников."""
    t = team_name.strip()
    if not t:
        return []
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT y FROM (
            SELECT pcr.year AS y
            FROM profile_cup_results pcr
            WHERE pcr.profile_id IN (
                SELECT DISTINCT r.profile_id
                FROM results r
                WHERE r.dnf = 0 AND TRIM(r.team) = ? AND r.profile_id IS NOT NULL
            )
            UNION
            SELECT c.year AS y
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            WHERE r.dnf = 0 AND TRIM(r.team) = ? AND c.year IS NOT NULL
        )
        ORDER BY y DESC
        """,
        (t, t),
    )
    return [int(r["y"]) for r in rows if r.get("y") is not None]


def query_team_cup_points_for_year(
    db_path: Path | str,
    team_name: str,
    year: int,
) -> list[dict[str, Any]]:
    """Очки в кубках (profile_cup_results) для участников команды за выбранный год."""
    t = team_name.strip()
    return q_all(
        db_path,
        """
        SELECT
            p.id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS athlete,
            pcr.cup_title AS cup,
            pcr.distance_name AS distance,
            pcr.place_abs AS place,
            pcr.group_name AS cup_group,
            ROUND(COALESCE(pcr.total_points, 0), 2) AS points
        FROM profile_cup_results pcr
        INNER JOIN profiles p ON p.id = pcr.profile_id
        WHERE pcr.year = ?
          AND pcr.profile_id IN (
              SELECT DISTINCT r.profile_id
              FROM results r
              WHERE r.dnf = 0 AND TRIM(r.team) = ? AND r.profile_id IS NOT NULL
          )
        ORDER BY pcr.cup_title, pcr.distance_name, athlete
        """,
        (year, t),
    )


def query_team_kpi_extended(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
) -> dict[str, Any] | None:
    """Расширенные KPI команды: участники, старты, финиши, DNF, километраж, годы."""
    t = team_name.strip()
    if not t:
        return None
    params: list[Any] = [t]
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    row = q_one(
        db_path,
        f"""
        SELECT
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants_distinct,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            COUNT(DISTINCT c.year) AS active_years_count
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        """,
        tuple(params),
    )
    if not row:
        return None
    years_rows = q_all(
        db_path,
        f"""
        SELECT DISTINCT c.year AS y
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE TRIM(COALESCE(r.team, '')) = ?
          AND c.year IS NOT NULL
          {year_sql}
        ORDER BY c.year
        """,
        tuple(params),
    )
    starts_total = int(row.get("starts_total") or 0)
    if starts_total <= 0:
        return None
    years = [int(x["y"]) for x in years_rows if x.get("y") is not None]
    return {
        "participants_distinct": int(row.get("participants_distinct") or 0),
        "starts_total": starts_total,
        "finishes_total": int(row.get("finishes_total") or 0),
        "dnf_total": int(row.get("dnf_total") or 0),
        "km_total": float(row.get("km_total") or 0.0),
        "active_years_count": int(row.get("active_years_count") or 0),
        "active_years_list": years,
    }


def query_team_roster_stats(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
) -> list[dict[str, Any]]:
    """Состав команды с базовой статистикой по участникам."""
    t = team_name.strip()
    params: list[Any] = [t]
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    return q_all(
        db_path,
        f"""
        SELECT
            p.id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS athlete,
            COALESCE(p.gender, '') AS gender,
            p.age AS age,
            COALESCE(p.city, '') AS city,
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            MIN(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.place_abs END) AS best_place_abs
        FROM results r
        LEFT JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        GROUP BY p.id, athlete, gender, age, city
        ORDER BY finishes_total DESC, starts_total DESC, athlete
        """,
        tuple(params),
    )


def query_team_events_table(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
    event_search: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Таблица событий команды: участие, финиши и лучший участник."""
    t = team_name.strip()
    where: list[str] = ["TRIM(COALESCE(r.team, '')) = ?"]
    params: list[Any] = [t]
    if year is not None:
        where.append("c.year = ?")
        params.append(int(year))
    if event_search and event_search.strip():
        where.append("LOWER(COALESCE(c.title, '')) LIKE LOWER(?)")
        params.append(f"%{event_search.strip()}%")
    wsql = " AND ".join(where)
    return q_all(
        db_path,
        f"""
        SELECT
            c.id AS competition_id,
            COALESCE(c.title, '') AS event_title,
            COALESCE(c.date, '') AS event_date,
            c.year AS year,
            COALESCE(c.sport, '') AS sport,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS team_participants,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS team_finishes,
            MIN(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN r.place_abs END) AS best_place_abs,
            MIN(
                CASE
                    WHEN COALESCE(r.dnf, 0) = 0 AND r.place_abs = (
                        SELECT MIN(r2.place_abs)
                        FROM results r2
                        WHERE r2.competition_id = r.competition_id
                          AND TRIM(COALESCE(r2.team, '')) = TRIM(COALESCE(r.team, ''))
                          AND COALESCE(r2.dnf, 0) = 0
                    )
                    THEN TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, ''))
                END
            ) AS best_athlete
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE {wsql}
        GROUP BY c.id, event_title, event_date, year, sport
        ORDER BY event_date DESC, c.id DESC
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_team_sport_distance_slices(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Срезы команды по виду спорта и дистанциям."""
    t = team_name.strip()
    params: list[Any] = [t]
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    by_sport = q_all(
        db_path,
        f"""
        SELECT
            COALESCE(c.sport, '') AS sport,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        GROUP BY COALESCE(c.sport, '')
        ORDER BY starts DESC, sport
        """,
        tuple(params),
    )
    by_distance = q_all(
        db_path,
        f"""
        SELECT
            COALESCE(d.name, '—') AS distance,
            ROUND(COALESCE(d.distance_km, 0), 2) AS distance_km,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        GROUP BY distance, distance_km
        ORDER BY starts DESC, distance_km DESC, distance
        """,
        tuple(params),
    )
    return {"by_sport": by_sport, "by_distance": by_distance}


def query_team_geography(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """География участников команды: города/регионы/страны."""
    t = team_name.strip()
    params: list[Any] = [t]
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    city_rows = q_all(
        db_path,
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(p.city), ''), '—') AS city,
            COALESCE(NULLIF(TRIM(p.region), ''), '—') AS region,
            COALESCE(NULLIF(TRIM(p.country), ''), '—') AS country,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        GROUP BY city, region, country
        ORDER BY participants DESC, city, region, country
        LIMIT 30
        """,
        tuple(params),
    )
    city_rules = load_city_alias_rules()
    city_agg: dict[str, dict[str, Any]] = {}
    region_agg: dict[str, dict[str, Any]] = {}
    country_agg: dict[str, dict[str, Any]] = {}
    for r in city_rows:
        geo = resolve_city_geo_by_alias_rules(r.get("city"), r.get("region"), r.get("country"), city_rules)
        label = str(geo.get("city_label") or "—")
        key = _norm_alias_token(label) or _norm_alias_token(str(r.get("city") or ""))
        participants = int(r.get("participants") or 0)
        if key not in city_agg:
            city_agg[key] = {"city": label, "participants": participants}
        else:
            city_agg[key]["participants"] = int(city_agg[key].get("participants") or 0) + participants

        region_lbl = _clean_geo_label(geo.get("region_label"))
        rk = _norm_alias_token(region_lbl) or region_lbl.casefold()
        if rk not in region_agg:
            region_agg[rk] = {"region": region_lbl, "participants": participants}
        else:
            region_agg[rk]["participants"] = int(region_agg[rk].get("participants") or 0) + participants

        country_lbl = _clean_geo_label(geo.get("country_label"))
        ck = _norm_alias_token(country_lbl) or country_lbl.casefold()
        if ck not in country_agg:
            country_agg[ck] = {"country": country_lbl, "participants": participants}
        else:
            country_agg[ck]["participants"] = int(country_agg[ck].get("participants") or 0) + participants

    cities = list(city_agg.values())
    cities.sort(
        key=lambda x: (-int(x.get("participants") or 0), str(x.get("city") or ""))
    )
    cities = cities[:30]
    regions = sorted(
        region_agg.values(),
        key=lambda x: (-int(x.get("participants") or 0), str(x.get("region") or "")),
    )
    regions = [r for r in regions if not _is_missing_geo_label(str(r.get("region") or ""))][:30]
    countries = sorted(
        country_agg.values(),
        key=lambda x: (-int(x.get("participants") or 0), str(x.get("country") or "")),
    )
    countries = [r for r in countries if not _is_missing_geo_label(str(r.get("country") or ""))][:30]
    return {"cities": cities, "regions": regions, "countries": countries}


def query_team_yearly_trends(db_path: Path | str, team_name: str) -> list[dict[str, Any]]:
    """Годовой тренд команды: старты, финиши, DNF, километраж, DNF%."""
    t = team_name.strip()
    if not t:
        return []
    return q_all(
        db_path,
        """
        SELECT
            c.year AS year,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS dnf_pct
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE TRIM(COALESCE(r.team, '')) = ? AND c.year IS NOT NULL
        GROUP BY c.year
        ORDER BY c.year
        """,
        (t,),
    )


def query_team_data_quality(
    db_path: Path | str,
    team_name: str,
    year: int | None = None,
) -> list[dict[str, Any]]:
    """Диагностика качества данных команды (admin-only в UI)."""
    t = team_name.strip()
    if not t:
        return []
    params: list[Any] = [t]
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    row = q_one(
        db_path,
        f"""
        SELECT
            COUNT(*) AS starts_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes_total,
            SUM(CASE WHEN COALESCE(r.dnf, 0) != 0 THEN 1 ELSE 0 END) AS dnf_total,
            SUM(CASE WHEN TRIM(COALESCE(r.team, '')) = '' THEN 1 ELSE 0 END) AS missing_team,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND r.place_abs IS NULL THEN 1 ELSE 0 END) AS missing_place_abs,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND r.place_gender IS NULL THEN 1 ELSE 0 END) AS missing_place_gender,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND r.place_group IS NULL THEN 1 ELSE 0 END) AS missing_place_group,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND (d.distance_km IS NULL OR d.distance_km <= 0) THEN 1 ELSE 0 END) AS bad_distance_km,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 AND (r.finish_time_sec IS NULL OR r.finish_time_sec <= 0) THEN 1 ELSE 0 END) AS bad_finish_time_sec
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE TRIM(COALESCE(r.team, '')) = ? {year_sql}
        """,
        tuple(params),
    ) or {}
    checks = [
        {"метрика": "Стартов всего", "значение": int(row.get("starts_total") or 0)},
        {"метрика": "Финишей", "значение": int(row.get("finishes_total") or 0)},
        {"метрика": "DNF", "значение": int(row.get("dnf_total") or 0)},
        {"метрика": "Пустая команда", "значение": int(row.get("missing_team") or 0)},
        {"метрика": "Финиши без place_abs", "значение": int(row.get("missing_place_abs") or 0)},
        {"метрика": "Финиши без place_gender", "значение": int(row.get("missing_place_gender") or 0)},
        {"метрика": "Финиши без place_group", "значение": int(row.get("missing_place_group") or 0)},
        {"метрика": "Финиши с некорректной distance_km", "значение": int(row.get("bad_distance_km") or 0)},
        {"метрика": "Финиши без finish_time_sec", "значение": int(row.get("bad_finish_time_sec") or 0)},
    ]
    return checks


def _build_interesting_facts_where(year: int | None, sport: str | None) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    if year is not None:
        parts.append("c.year = ?")
        params.append(int(year))
    if sport and str(sport).strip():
        parts.append("c.sport = ?")
        params.append(str(sport).strip())
    return (" AND ".join(parts) if parts else "1=1"), params


def _canonical_sport_code(value: str | None) -> str:
    """Нормализация sport с учетом алиасов."""
    s = _norm_alias_token(value)
    alias_map = {
        "run": "run",
        "running": "run",
        "road_run": "run",
        "roadrun": "run",
        "trail_run": "trail_run",
        "trail run": "trail_run",
        "trailrun": "trail_run",
        "trail": "trail_run",
        "bike": "bike",
        "cycling": "bike",
        "velo": "bike",
        "ski": "ski",
        "xc_ski": "ski",
        "cross_country_ski": "ski",
        "service": "service",
        "other": "other",
    }
    if not s:
        return "other"
    return alias_map.get(s, s)


def query_interesting_facts_longest_series_by_sport(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> list[dict[str, Any]]:
    """Для каждого вида спорта возвращает самую длинную серию по title_short."""
    if not _table_has_column(db_path, "competitions", "title_short"):
        return []
    w, params = _build_interesting_facts_where(year, None)
    rows = q_all(
        db_path,
        f"""
        SELECT DISTINCT
            c.id AS competition_id,
            TRIM(COALESCE(c.title_short, '')) AS title_short,
            TRIM(COALESCE(c.sport, '')) AS sport
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE r.profile_id IS NOT NULL
          AND {w}
          AND TRIM(COALESCE(c.title_short, '')) != ''
        """,
        tuple(params),
    )
    requested = _canonical_sport_code(sport) if sport and str(sport).strip() else ""
    by_sport_series: dict[tuple[str, str], set[int]] = {}
    for r in rows:
        sport_can = _canonical_sport_code(r.get("sport"))
        if requested and sport_can != requested:
            continue
        title = str(r.get("title_short") or "").strip()
        if not title:
            continue
        cid = int(r.get("competition_id") or 0)
        by_sport_series.setdefault((sport_can, title), set()).add(cid)

    best_by_sport: dict[str, dict[str, Any]] = {}
    for (sport_can, title), ids in by_sport_series.items():
        editions = len(ids)
        prev = best_by_sport.get(sport_can)
        if prev is None or editions > int(prev.get("editions") or 0) or (
            editions == int(prev.get("editions") or 0)
            and str(title).casefold() < str(prev.get("series_title") or "").casefold()
        ):
            best_by_sport[sport_can] = {
                "sport": sport_can,
                "series_title": title,
                "editions": editions,
            }
    return sorted(best_by_sport.values(), key=lambda x: str(x.get("sport") or ""))


def query_interesting_facts_record_leaders_by_sport(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> list[dict[str, Any]]:
    """Лидеры по числу действующих рекордов (по видам спорта, мужчины/женщины)."""
    series_expr = (
        "COALESCE(NULLIF(TRIM(c.title_short), ''), TRIM(c.title))"
        if _table_has_column(db_path, "competitions", "title_short")
        else "TRIM(c.title)"
    )
    w, params = _build_interesting_facts_where(year, None)
    requested = _canonical_sport_code(sport) if sport and str(sport).strip() else ""
    base_rows = q_all(
        db_path,
        f"""
        SELECT
            {series_expr} AS event_series,
            TRIM(COALESCE(c.sport, '')) AS sport,
            COALESCE(NULLIF(TRIM(d.name), ''), '—') AS distance_name,
            LOWER(TRIM(COALESCE(p.gender, ''))) AS gender_code,
            c.year AS event_year,
            c.title AS event_title,
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
            r.finish_time_sec AS finish_time_sec
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        INNER JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE COALESCE(r.dnf, 0) = 0
          AND r.finish_time_sec IS NOT NULL
          AND r.profile_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'f')
          AND TRIM(COALESCE({series_expr}, '')) != ''
          AND {w}
        """,
        tuple(params),
    )
    dist_rules = load_distance_alias_rules()
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in base_rows:
        sport_can = _canonical_sport_code(row.get("sport"))
        if requested and sport_can != requested:
            continue
        event_series = str(row.get("event_series") or "").strip()
        if not event_series:
            continue
        gender_code = str(row.get("gender_code") or "").strip().lower()
        if gender_code not in {"m", "f"}:
            continue
        d_key, _ = normalize_distance_by_alias_rules(row.get("distance_name"), dist_rules)
        grouped.setdefault((sport_can, event_series, d_key, gender_code), []).append(dict(row))

    counts: dict[tuple[str, str, int], int] = {}
    labels: dict[int, str] = {}
    for (sport_can, _series, _dkey, gc), items in grouped.items():
        winner = sorted(
            items,
            key=lambda r: (
                float(r.get("finish_time_sec") or 0.0),
                int(r.get("event_year") or 0),
                str(r.get("event_title") or ""),
                int(r.get("profile_id") or 0),
            ),
        )[0]
        pid = int(winner.get("profile_id") or 0)
        if pid <= 0:
            continue
        labels[pid] = str(winner.get("participant") or "").strip() or f"id {pid}"
        key = (sport_can, gc, pid)
        counts[key] = int(counts.get(key, 0) or 0) + 1

    by_sport: dict[str, dict[str, Any]] = {}
    for (sport_can, gc, pid), n in counts.items():
        rec = by_sport.setdefault(
            sport_can,
            {
                "sport": sport_can,
                "male_participant": "—",
                "male_records": 0,
                "female_participant": "—",
                "female_records": 0,
            },
        )
        if gc == "m":
            if n > int(rec.get("male_records") or 0):
                rec["male_records"] = n
                rec["male_participant"] = labels.get(pid, f"id {pid}")
        else:
            if n > int(rec.get("female_records") or 0):
                rec["female_records"] = n
                rec["female_participant"] = labels.get(pid, f"id {pid}")
    return sorted(by_sport.values(), key=lambda x: str(x.get("sport") or ""))


def query_interesting_facts_wins_leaders_by_sport(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> list[dict[str, Any]]:
    """Лидеры по количеству побед в событиях (profiles.stat_first), по видам спорта и полу."""
    w, params = _build_interesting_facts_where(year, None)
    requested = _canonical_sport_code(sport) if sport and str(sport).strip() else ""
    rows = q_all(
        db_path,
        f"""
        WITH base AS (
            SELECT DISTINCT
                TRIM(COALESCE(c.sport, '')) AS sport,
                p.id AS profile_id,
                LOWER(TRIM(COALESCE(p.gender, ''))) AS gender_code,
                TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
                COALESCE(p.stat_first, 0) AS wins
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            INNER JOIN profiles p ON p.id = r.profile_id
            WHERE r.profile_id IS NOT NULL
              AND LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'f')
              AND {w}
        )
        SELECT sport, gender_code, profile_id, participant, wins
        FROM base
        """,
        tuple(params),
    )
    by_sport: dict[str, dict[str, Any]] = {}
    for r in rows:
        sport_can = _canonical_sport_code(r.get("sport"))
        if requested and sport_can != requested:
            continue
        gc = str(r.get("gender_code") or "").strip().lower()
        if gc not in {"m", "f"}:
            continue
        rec = by_sport.setdefault(
            sport_can,
            {
                "sport": sport_can,
                "male_participant": "—",
                "male_wins": 0,
                "female_participant": "—",
                "female_wins": 0,
            },
        )
        wins = int(r.get("wins") or 0)
        participant = str(r.get("participant") or "").strip() or f"id {int(r.get('profile_id') or 0)}"
        if gc == "m":
            if wins > int(rec.get("male_wins") or 0):
                rec["male_wins"] = wins
                rec["male_participant"] = participant
        else:
            if wins > int(rec.get("female_wins") or 0):
                rec["female_wins"] = wins
                rec["female_participant"] = participant
    return sorted(by_sport.values(), key=lambda x: str(x.get("sport") or ""))


def query_interesting_facts_loyal_participants(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    min_starts: int = 1,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Самые преданные участники: максимум активных лет и стартов."""
    w, params = _build_interesting_facts_where(year, sport)
    params = list(params) + [max(1, int(min_starts))]
    return q_all(
        db_path,
        f"""
        SELECT
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
            COUNT(DISTINCT c.year) AS active_years,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE r.profile_id IS NOT NULL AND {w}
        GROUP BY r.profile_id, participant
        HAVING COUNT(*) >= ?
        ORDER BY active_years DESC, starts DESC, km_total DESC, participant
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_finish_rate(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    min_starts: int = 5,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Железные финишеры: максимум finish-rate при достаточном числе стартов."""
    w, params = _build_interesting_facts_where(year, sport)
    params = list(params) + [max(1, int(min_starts))]
    return q_all(
        db_path,
        f"""
        SELECT
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1
            ) AS finish_rate_pct
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE r.profile_id IS NOT NULL AND {w}
        GROUP BY r.profile_id, participant
        HAVING COUNT(*) >= ?
        ORDER BY finish_rate_pct DESC, finishes DESC, starts DESC, participant
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_universal_participants(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    min_starts: int = 1,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Универсалы по спорту: участники с максимальным покрытием видов спорта."""
    params: list[Any] = []
    year_sql = ""
    if year is not None:
        year_sql = " AND c.year = ?"
        params.append(int(year))
    sport_sql = ""
    if sport and str(sport).strip():
        sport_sql = " AND c.sport = ?"
        params.append(str(sport).strip())
    params.append(max(1, int(min_starts)))
    return q_all(
        db_path,
        f"""
        SELECT
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
            COUNT(*) AS starts,
            COUNT(DISTINCT COALESCE(NULLIF(TRIM(c.sport), ''), 'unknown')) AS sports_count,
            GROUP_CONCAT(DISTINCT COALESCE(NULLIF(TRIM(c.sport), ''), 'unknown')) AS sports_list
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE r.profile_id IS NOT NULL {year_sql}{sport_sql}
        GROUP BY r.profile_id, participant
        HAVING COUNT(*) >= ?
        ORDER BY sports_count DESC, starts DESC, participant
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_km_leaders(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    min_starts: int = 1,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Лидеры по суммарному километражу (по финишам)."""
    w, params = _build_interesting_facts_where(year, sport)
    params = list(params) + [max(1, int(min_starts))]
    return q_all(
        db_path,
        f"""
        SELECT
            r.profile_id AS profile_id,
            TRIM(COALESCE(p.last_name, '') || ' ' || COALESCE(p.first_name, '')) AS participant,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE r.profile_id IS NOT NULL AND {w}
        GROUP BY r.profile_id, participant
        HAVING COUNT(*) >= ?
        ORDER BY km_total DESC, finishes DESC, participant
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_distance_frequency(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Топ дистанций по числу стартов и участников."""
    w, params = _build_interesting_facts_where(year, sport)
    return q_all(
        db_path,
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(d.name), ''), '—') AS distance,
            ROUND(COALESCE(d.distance_km, 0), 2) AS distance_km,
            COUNT(*) AS starts,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN distances d ON d.id = r.distance_id
        WHERE {w}
        GROUP BY distance, distance_km
        ORDER BY starts DESC, participants DESC, distance_km DESC, distance
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_km_by_sport(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> list[dict[str, Any]]:
    """Суммарный километраж по видам спорта."""
    params: list[Any] = []
    where_parts: list[str] = []
    if year is not None:
        where_parts.append("c.year = ?")
        params.append(int(year))
    if sport and str(sport).strip():
        where_parts.append("c.sport = ?")
        params.append(str(sport).strip())
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return q_all(
        db_path,
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(c.sport), ''), 'unknown') AS sport,
            ROUND(COALESCE(SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN d.distance_km ELSE 0 END), 0), 1) AS km_total,
            COUNT(*) AS starts,
            SUM(CASE WHEN COALESCE(r.dnf, 0) = 0 THEN 1 ELSE 0 END) AS finishes
        FROM competitions c
        LEFT JOIN results r ON r.competition_id = c.id
        LEFT JOIN distances d ON d.id = r.distance_id
        {where_sql}
        GROUP BY COALESCE(NULLIF(TRIM(c.sport), ''), 'unknown')
        ORDER BY km_total DESC, starts DESC, COALESCE(NULLIF(TRIM(c.sport), ''), 'unknown')
        """,
        tuple(params),
    )


def query_interesting_facts_team_longevity(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    min_starts: int = 5,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Команды-долгожители: длительность периода активности и объем стартов."""
    w, params = _build_interesting_facts_where(year, sport)
    params = list(params) + [max(1, int(min_starts))]
    return q_all(
        db_path,
        f"""
        SELECT
            TRIM(r.team) AS team,
            MIN(c.year) AS first_year,
            MAX(c.year) AS last_year,
            (MAX(c.year) - MIN(c.year) + 1) AS active_span_years,
            COUNT(*) AS starts,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        WHERE TRIM(COALESCE(r.team, '')) != '' AND c.year IS NOT NULL AND {w}
        GROUP BY team
        HAVING COUNT(*) >= ?
        ORDER BY active_span_years DESC, starts DESC, participants DESC, team
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def query_interesting_facts_starts_per_participant(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> dict[str, int]:
    """Счётчики для карточек среднего стартов на участника (общий/м/ж)."""
    w, params = _build_interesting_facts_where(year, sport)
    row = q_one(
        db_path,
        f"""
        SELECT
            COUNT(*) AS starts_total,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants_total,
            SUM(CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'м') THEN 1 ELSE 0 END) AS starts_male,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'м') THEN r.profile_id END) AS participants_male,
            SUM(CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('f', 'ж') THEN 1 ELSE 0 END) AS starts_female,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('f', 'ж') THEN r.profile_id END) AS participants_female
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE {w}
        """,
        tuple(params),
    ) or {}
    return {
        "starts_total": int(row.get("starts_total") or 0),
        "participants_total": int(row.get("participants_total") or 0),
        "starts_male": int(row.get("starts_male") or 0),
        "participants_male": int(row.get("participants_male") or 0),
        "starts_female": int(row.get("starts_female") or 0),
        "participants_female": int(row.get("participants_female") or 0),
    }


def query_interesting_facts_starts_per_participant_per_year(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> dict[str, float]:
    """Средние за год: старты/участники (общий, мужчины, женщины)."""
    w, params = _build_interesting_facts_where(year, sport)
    row = q_one(
        db_path,
        f"""
        WITH yearly AS (
            SELECT
                c.year AS y,
                COUNT(*) AS starts_total,
                COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants_total,
                SUM(CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'м') THEN 1 ELSE 0 END) AS starts_male,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('m', 'м') THEN r.profile_id END) AS participants_male,
                SUM(CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('f', 'ж') THEN 1 ELSE 0 END) AS starts_female,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(COALESCE(p.gender, ''))) IN ('f', 'ж') THEN r.profile_id END) AS participants_female
            FROM results r
            INNER JOIN competitions c ON c.id = r.competition_id
            LEFT JOIN profiles p ON p.id = r.profile_id
            WHERE {w}
              AND c.year IS NOT NULL
            GROUP BY c.year
        )
        SELECT
            AVG(starts_total) AS avg_starts_total,
            AVG(participants_total) AS avg_participants_total,
            AVG(starts_male) AS avg_starts_male,
            AVG(participants_male) AS avg_participants_male,
            AVG(starts_female) AS avg_starts_female,
            AVG(participants_female) AS avg_participants_female
        FROM yearly
        """,
        tuple(params),
    ) or {}
    return {
        "avg_starts_total": float(row.get("avg_starts_total") or 0.0),
        "avg_participants_total": float(row.get("avg_participants_total") or 0.0),
        "avg_starts_male": float(row.get("avg_starts_male") or 0.0),
        "avg_participants_male": float(row.get("avg_participants_male") or 0.0),
        "avg_starts_female": float(row.get("avg_starts_female") or 0.0),
        "avg_participants_female": float(row.get("avg_participants_female") or 0.0),
    }


def query_interesting_facts_geography(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
    limit: int = 10,
) -> dict[str, list[dict[str, Any]]]:
    """География участников: города, регионы и страны."""
    w, params = _build_interesting_facts_where(year, sport)
    city_rows = q_all(
        db_path,
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(p.city), ''), '—') AS city,
            COALESCE(NULLIF(TRIM(p.region), ''), '—') AS region,
            COALESCE(NULLIF(TRIM(p.country), ''), '—') AS country,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants,
            COUNT(*) AS starts
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE {w}
        GROUP BY city, region, country
        ORDER BY participants DESC, starts DESC, city, region, country
        LIMIT {int(limit)}
        """,
        tuple(params),
    )
    city_rules = load_city_alias_rules()
    city_agg: dict[str, dict[str, Any]] = {}
    region_agg: dict[str, dict[str, Any]] = {}
    country_agg: dict[str, dict[str, Any]] = {}
    for r in city_rows:
        geo = resolve_city_geo_by_alias_rules(r.get("city"), r.get("region"), r.get("country"), city_rules)
        label = str(geo.get("city_label") or "—")
        key = _norm_alias_token(label) or _norm_alias_token(str(r.get("city") or ""))
        cur = city_agg.get(key)
        participants = int(r.get("participants") or 0)
        starts = int(r.get("starts") or 0)
        if cur is None:
            city_agg[key] = {
                "city": label,
                "participants": participants,
                "starts": starts,
            }
        else:
            cur["participants"] = int(cur.get("participants") or 0) + participants
            cur["starts"] = int(cur.get("starts") or 0) + starts

        region_lbl = _clean_geo_label(geo.get("region_label"))
        rk = _norm_alias_token(region_lbl) or region_lbl.casefold()
        rcur = region_agg.get(rk)
        if rcur is None:
            region_agg[rk] = {"region": region_lbl, "participants": participants, "starts": starts}
        else:
            rcur["participants"] = int(rcur.get("participants") or 0) + participants
            rcur["starts"] = int(rcur.get("starts") or 0) + starts

        country_lbl = _clean_geo_label(geo.get("country_label"))
        ck = _norm_alias_token(country_lbl) or country_lbl.casefold()
        ccur = country_agg.get(ck)
        if ccur is None:
            country_agg[ck] = {"country": country_lbl, "participants": participants, "starts": starts}
        else:
            ccur["participants"] = int(ccur.get("participants") or 0) + participants
            ccur["starts"] = int(ccur.get("starts") or 0) + starts
    cities = list(city_agg.values())
    cities.sort(
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("city") or ""),
        )
    )
    cities = cities[: int(limit)]
    regions = sorted(
        region_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("region") or ""),
        ),
    )
    regions = [r for r in regions if not _is_missing_geo_label(str(r.get("region") or ""))][: int(limit)]
    countries = sorted(
        country_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("country") or ""),
        ),
    )
    countries = [r for r in countries if not _is_missing_geo_label(str(r.get("country") or ""))][: int(limit)]
    return {"cities": cities, "regions": regions, "countries": countries}


def lookup_city_coordinates(
    city_display_label: str | None,
    region_display: str | None = None,
) -> tuple[float | None, float | None]:
    """
    Координаты центра города по эталону city.csv (ключ — нормализованное название).
    При нескольких вариантах в одном регионе — уточняет по региону.
    """
    src = str(city_display_label or "").strip()
    if not src or src == "—":
        return (None, None)
    nk = _norm_city_token(src)
    if not nk:
        return (None, None)
    idx = load_city_reference_index()
    candidates = idx.get(nk) or []
    if not candidates:
        return (None, None)
    pool = list(candidates)
    rr = _norm_alias_token(region_display) if region_display else ""
    if len(pool) > 1 and rr:
        matched = [x for x in pool if str(x.get("region_norm") or "") == rr]
        if matched:
            pool = matched
    top = pool[0]
    lat = top.get("geo_lat")
    lon = top.get("geo_lon")
    if lat is None or lon is None:
        return (None, None)
    try:
        la = float(lat)
        lo = float(lon)
    except (TypeError, ValueError):
        return (None, None)
    return (la, lo)


def query_vm_geography_page(
    db_path: Path | str,
    year: int | None = None,
    sport: str | None = None,
) -> dict[str, Any]:
    """
    Данные для страницы «География ВМ»: регионы, страны, города (с координатами для карты).
    Счётчик — уникальные profile_id по стартам под выбранными фильтрами.
    """
    w, params = _build_interesting_facts_where(year, sport)

    raw_city_rows = q_all(
        db_path,
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(p.city), ''), '—') AS city,
            COALESCE(NULLIF(TRIM(p.region), ''), '—') AS region,
            COALESCE(NULLIF(TRIM(p.country), ''), '—') AS country,
            COUNT(DISTINCT CASE WHEN r.profile_id IS NOT NULL THEN r.profile_id END) AS participants,
            COUNT(*) AS starts
        FROM results r
        INNER JOIN competitions c ON c.id = r.competition_id
        LEFT JOIN profiles p ON p.id = r.profile_id
        WHERE r.profile_id IS NOT NULL AND {w}
        GROUP BY city, region, country
        """,
        tuple(params),
    )

    city_rules = load_city_alias_rules()
    vm_region_rules = load_region_alias_rules()
    vo_region_tokens = {
        _norm_alias_token("Вологодская область"),
        _norm_alias_token("Вологодская Область"),
    }
    city_agg: dict[str, dict[str, Any]] = {}
    region_by_key: dict[str, str] = {}
    region_agg: dict[str, dict[str, Any]] = {}
    country_agg: dict[str, dict[str, Any]] = {}
    vo_district_agg: dict[str, dict[str, Any]] = {}
    for r in raw_city_rows:
        raw_region = str(r.get("region") or "")
        geo = resolve_city_geo_by_alias_rules(r.get("city"), raw_region or None, r.get("country"), city_rules)
        label = str(geo.get("city_label") or "—")
        region_from_city = _clean_geo_label(geo.get("region_label"), _clean_geo_label(raw_region))
        _, reg_ui = normalize_region_by_alias_rules(region_from_city, vm_region_rules)
        country_ui = _clean_geo_label(geo.get("country_label"), _clean_geo_label(r.get("country")))
        lbl = str(label or "").strip() or "—"
        key = _norm_alias_token(lbl)
        if not key:
            key = _norm_alias_token(str(r.get("city") or "")) or lbl.casefold()
        cur = city_agg.get(key)
        participants = int(r.get("participants") or 0)
        starts = int(r.get("starts") or 0)
        if cur is None:
            city_agg[key] = {
                "city": lbl,
                "participants": participants,
                "starts": starts,
            }
            region_by_key[key] = reg_ui if reg_ui != "—" else region_from_city
        else:
            cur["participants"] = int(cur.get("participants") or 0) + participants
            cur["starts"] = int(cur.get("starts") or 0) + starts

        rk = _norm_alias_token(region_by_key[key]) or region_by_key[key].casefold()
        rcur = region_agg.get(rk)
        if rcur is None:
            region_agg[rk] = {"region": region_by_key[key], "participants": participants, "starts": starts}
        else:
            rcur["participants"] = int(rcur.get("participants") or 0) + participants
            rcur["starts"] = int(rcur.get("starts") or 0) + starts

        ck = _norm_alias_token(country_ui) or country_ui.casefold()
        ccur = country_agg.get(ck)
        if ccur is None:
            country_agg[ck] = {"country": country_ui, "participants": participants, "starts": starts}
        else:
            ccur["participants"] = int(ccur.get("participants") or 0) + participants
            ccur["starts"] = int(ccur.get("starts") or 0) + starts

        eff_region = region_by_key.get(key, region_from_city)
        if _norm_alias_token(eff_region) in vo_region_tokens:
            district = _clean_geo_label(geo.get("rayon_label"), "Не указан")
            dk = _norm_alias_token(district) or district.casefold()
            dcur = vo_district_agg.get(dk)
            if dcur is None:
                vo_district_agg[dk] = {"district": district, "participants": participants, "starts": starts}
            else:
                dcur["participants"] = int(dcur.get("participants") or 0) + participants
                dcur["starts"] = int(dcur.get("starts") or 0) + starts

    cities_sorted = sorted(
        city_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("city") or ""),
        ),
    )

    map_points: list[dict[str, Any]] = []
    for k, row in city_agg.items():
        lbl = str(row.get("city") or "")
        rg = region_by_key.get(k, "")
        lat, lon = lookup_city_coordinates(lbl, rg)
        if lat is not None and lon is not None:
            map_points.append(
                {
                    "city": lbl,
                    "participants": int(row.get("participants") or 0),
                    "lat": lat,
                    "lon": lon,
                }
            )

    regions = sorted(
        region_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("region") or ""),
        ),
    )
    regions = [r for r in regions if not _is_missing_geo_label(str(r.get("region") or ""))]
    countries = sorted(
        country_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("country") or ""),
        ),
    )
    countries = [r for r in countries if not _is_missing_geo_label(str(r.get("country") or ""))]
    vo_districts = sorted(
        vo_district_agg.values(),
        key=lambda x: (
            -int(x.get("participants") or 0),
            -int(x.get("starts") or 0),
            str(x.get("district") or ""),
        ),
    )
    return {
        "regions": regions,
        "countries": countries,
        "cities": cities_sorted,
        "map_points": map_points,
        "vologda_districts": vo_districts,
    }


def is_team_scoring_enabled(cup_id: int, year: int) -> bool:
    """Новый контур расчёта team_scoring включён только для cup_id=54 и year=2026."""
    return int(cup_id) == 54 and int(year) == 2026


def query_data_health(db_path: Path | str) -> dict[str, Any]:
    """Мини-чеклист для дашборда / документации."""
    row = q_one(
        db_path,
        """
        SELECT
          (SELECT COUNT(*) FROM results WHERE profile_id IS NOT NULL) AS results_with_profile,
          (SELECT COUNT(*) FROM results) AS results_total,
          (SELECT COUNT(*) FROM profiles) AS profiles_count,
          (SELECT COUNT(*) FROM profile_cup_results) AS profile_cup_rows
        """,
    )
    return row or {}


# ── Админка: таблица competitions ─────────────────────────────────────────────

_ADMIN_COMP_EDIT_COLS = (
    "title",
    "title_short",
    "date",
    "year",
    "sport",
    "is_relay",
    "is_published",
    "page_url",
)


def query_competition_years_admin(db_path: Path | str) -> list[int]:
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT year AS y
        FROM competitions
        WHERE year IS NOT NULL
        ORDER BY year DESC
        """,
        (),
    )
    out: list[int] = []
    for r in rows:
        try:
            out.append(int(r["y"]))
        except (TypeError, ValueError):
            continue
    return out


def query_competition_series_admin(db_path: Path | str) -> list[str]:
    if not _table_has_column(db_path, "competitions", "title_short"):
        return []
    rows = q_all(
        db_path,
        """
        SELECT DISTINCT TRIM(title_short) AS s
        FROM competitions
        WHERE TRIM(COALESCE(title_short, '')) != ''
        ORDER BY s COLLATE NOCASE
        """,
        (),
    )
    return [str(r.get("s") or "").strip() for r in rows if str(r.get("s") or "").strip()]


def query_competitions_admin_rows(
    db_path: Path | str,
    *,
    year: int | None,
    series_title: str | None = None,
    limit: int = 800,
) -> list[dict[str, Any]]:
    cols = ["id"]
    cols.extend(
        c for c in _ADMIN_COMP_EDIT_COLS if _table_has_column(db_path, "competitions", c)
    )
    col_sql = ", ".join(cols)
    lim = max(10, min(int(limit), 5000))

    where: list[str] = []
    params_l: list[Any] = []
    if year is not None:
        where.append("year = ?")
        params_l.append(int(year))
    if series_title is not None and str(series_title).strip():
        where.append("TRIM(COALESCE(title_short, '')) = ?")
        params_l.append(str(series_title).strip())

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = (
        f"SELECT {col_sql} FROM competitions{where_sql} "
        "ORDER BY date DESC NULLS LAST, id DESC LIMIT ?"
    )
    params_l.append(lim)
    params: tuple[Any, ...] = tuple(params_l)

    return q_all(db_path, sql, params)


def query_competitions_admin_count(
    db_path: Path | str,
    year: int | None,
    series_title: str | None = None,
) -> int:
    where: list[str] = []
    params_l: list[Any] = []
    if year is not None:
        where.append("year = ?")
        params_l.append(int(year))
    if series_title is not None and str(series_title).strip():
        where.append("TRIM(COALESCE(title_short, '')) = ?")
        params_l.append(str(series_title).strip())
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    row = q_one(db_path, f"SELECT COUNT(*) AS n FROM competitions{where_sql}", tuple(params_l))
    if not row:
        return 0
    try:
        return int(row.get("n") or 0)
    except (TypeError, ValueError):
        return 0


def _norm_sql_int_optional(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and (value != value):  # NaN
        return None
    if isinstance(value, str) and not str(value).strip():
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _norm_flag01(value: Any) -> int:
    if value in (True, 1):
        return 1
    if value in (False, 0, None, ""):
        return 0
    s = str(value).strip().casefold()
    if s in ("1", "true", "yes", "on", "да"):
        return 1
    try:
        return 1 if int(float(s)) else 0
    except (TypeError, ValueError):
        return 0


def _norm_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and (value != value):
        return ""
    return str(value)


def save_competitions_admin_rows(
    db_path: Path | str,
    rows: list[dict[str, Any]],
) -> list[str]:
    """
    Обновляет существующие строки по id. Поле id и сырой raw не меняются здесь.
    Возвращает список сообщений об ошибках (пустой — всё ок).
    """
    dbp = Path(db_path)
    # Какие колонки реально есть
    editable: list[str] = [
        c for c in _ADMIN_COMP_EDIT_COLS if _table_has_column(dbp, "competitions", c)
    ]
    if not editable:
        return ["В таблице competitions нет ожидаемых колонок для редактирования."]

    set_sql = ", ".join(f"{c} = ?" for c in editable)
    errs: list[str] = []

    with connect(dbp) as conn:
        for raw in rows:
            try:
                rid = raw.get("id")
                if rid is None or (isinstance(rid, float) and rid != rid):
                    errs.append("Пропуск строки: нет id.")
                    continue
                if isinstance(rid, str) and not str(rid).strip().isdigit():
                    errs.append("Пропуск строки: неверный id.")
                    continue
                cid = int(rid)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                errs.append("Пропуск строки: неверный id.")
                continue

            row = conn.execute(
                "SELECT 1 FROM competitions WHERE id = ? LIMIT 1", (cid,)
            ).fetchone()
            if not row:
                errs.append(f"id={cid}: запись не найдена.")
                continue

            values: list[Any] = []
            for c in editable:
                if c == "year":
                    yv = _norm_sql_int_optional(raw.get("year"))
                    values.append(yv)
                elif c in ("is_relay", "is_published"):
                    values.append(_norm_flag01(raw.get(c)))
                else:
                    values.append(_norm_str(raw.get(c)))

            conn.execute(
                f"UPDATE competitions SET {set_sql} WHERE id = ?",
                (*values, cid),
            )

        conn.commit()

    return errs
