"""
MVP-дашборд marathon.db (Streamlit).

Палитра: фон #FFFFFF, основной текст светло-чёрный #1a1a1a, приглушённый #767676, ссылки #93BDDD.

Запуск из корня проекта:
  pip install -r requirements.txt
  streamlit run app.py

Путь к БД: переменная MARATHON_DB или поле [marathon] path в .streamlit/secrets.toml
"""

from __future__ import annotations

import datetime
from typing import Any
import html
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import marathon_queries as mq

DEFAULT_DB = Path(os.environ.get("MARATHON_DB", str(mq.DEFAULT_DB)))

# Основные цвета сервиса
VM_LINK = "#93BDDD"
VM_TEXT = "#1a1a1a"
VM_PAGE_BG = "#ffffff"
VM_CARD_BORDER = "#dddddd"
VM_MUTED = "#767676"
# Акценты UI (границы, графики, плашки)
VM_ACCENT = "#93BDDD"
VM_BLUE = "#93BDDD"


def db_path() -> Path:
    try:
        if hasattr(st, "secrets") and st.secrets and "marathon" in st.secrets:
            p = st.secrets["marathon"].get("path")
            if p:
                return Path(p)
    except Exception:
        pass
    return DEFAULT_DB


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
            background: {VM_PAGE_BG};
            color: {VM_TEXT};
        }}
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] p {{
            color: {VM_TEXT} !important;
        }}
        section[data-testid="stSidebar"] a, section[data-testid="stSidebar"] a:visited {{
            color: {VM_LINK} !important;
        }}
        section[data-testid="stSidebar"] .stMarkdown {{
            color: {VM_MUTED} !important;
        }}
        section[data-testid="stSidebar"] hr {{
            border-color: {VM_CARD_BORDER};
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
            color: {VM_TEXT};
        }}
        .vm-sidebar-text-nav a {{
            color: {VM_TEXT};
            text-decoration: none;
            font-weight: 400;
        }}
        .vm-sidebar-text-nav a:hover {{
            text-decoration: underline;
            color: {VM_LINK};
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
        /* Участник: выбранный год в st.pills — синяя плашка, остальные годы — синий текст */
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
    st.markdown(
        f'<div class="vm-brand-bar">ВОЛОГДА МАРАФОН<span class="sub">локальная аналитика · MVP</span></div>',
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


def render_sidebar_text_nav(pages: tuple[str, ...], current: str) -> None:
    """Пункты меню: обычный текст, текущий раздел — полужирно; остальные — ссылка ?i= (без иконок)."""
    parts: list[str] = ['<div class="vm-sidebar-text-nav">']
    for j, title in enumerate(pages):
        esc = html.escape(title)
        if title == current:
            parts.append(f'<p class="vm-sidebar-here">{esc}</p>')
        else:
            parts.append(f'<p><a href="?i={j}">{esc}</a></p>')
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
    st.header("Общая статистика")
    path = db_path()
    if not require_db(path):
        return

    years_all = mq.query_distinct_years(path)
    sports_all = mq.query_distinct_sports(path)
    cups_rows = mq.query_cups_for_filter(path)
    cup_labels = {int(r["id"]): f"{r.get('title', '')} ({r.get('year', '—')})" for r in cups_rows}

    st.caption("Пустой фильтр по году / виду / кубку означает «все значения».")
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        sel_years = st.multiselect("Год", options=years_all, default=years_all, key="gs_years")
    with fc2:
        sel_sports = st.multiselect("Вид спорта", options=sports_all, default=sports_all, key="gs_sports")
    with fc3:
        cup_ids_opts = list(cup_labels.keys())
        sel_cup_ids = st.multiselect(
            "Кубок",
            options=cup_ids_opts,
            format_func=lambda i: cup_labels.get(int(i), str(i)),
            default=cup_ids_opts,
            key="gs_cups",
        )

    yf = sel_years if sel_years else None
    sf = sel_sports if sel_sports else None
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

    st.subheader("Графики")
    g1, g2 = st.columns(2)
    with g1:
        dfe = pd.DataFrame(mq.query_chart_events_by_year(path, yf, sf, cf))
        if dfe.empty:
            st.caption("Нет данных для гистограммы событий по годам.")
        else:
            fig_e = px.bar(
                dfe,
                x="year",
                y="events",
                labels={"year": "Год", "events": "Событий"},
                title="Количество событий по годам",
            )
            fig_e.update_traces(marker_color=VM_ACCENT)
            fig_e.update_layout(
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color=VM_TEXT),
                xaxis_type="category",
            )
            st.plotly_chart(fig_e, use_container_width=True)

    with g2:
        dfp = pd.DataFrame(mq.query_chart_unique_participants_by_year(path, yf, sf, cf))
        if dfp.empty:
            st.caption("Нет данных для гистограммы участников по годам.")
        else:
            fig_p = px.bar(
                dfp,
                x="year",
                y="participants",
                labels={"year": "Год", "participants": "Уникальных участников"},
                title="Уникальных участников по годам",
            )
            fig_p.update_traces(marker_color=VM_ACCENT)
            fig_p.update_layout(
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color=VM_TEXT),
                xaxis_type="category",
            )
            st.plotly_chart(fig_p, use_container_width=True)

    p1, p2 = st.columns(2)
    with p1:
        dfs = pd.DataFrame(mq.query_chart_events_by_sport(path, yf, sf, cf))
        if dfs.empty:
            st.caption("Нет данных для диаграммы по видам спорта.")
        else:
            fig_s = px.pie(
                dfs,
                names="sport",
                values="n",
                title="События по видам спорта",
                hole=0.35,
                color_discrete_sequence=px.colors.sequential.RdPu,
            )
            fig_s.update_layout(font=dict(color=VM_TEXT), paper_bgcolor="white")
            st.plotly_chart(fig_s, use_container_width=True)

    with p2:
        dfg = pd.DataFrame(mq.query_chart_participants_by_gender(path, yf, sf, cf))
        if dfg.empty:
            st.caption("Нет данных для диаграммы по полу.")
        else:
            map_g = {"m": "муж.", "f": "жен.", "не указан": "не указан"}
            dfg["gender_label"] = dfg["gender"].map(lambda g: map_g.get(str(g).lower(), str(g)))
            fig_g = px.pie(
                dfg,
                names="gender_label",
                values="n",
                title="Участники по полу (уникальные профили)",
                hole=0.35,
                color_discrete_sequence=px.colors.sequential.Blues_r,
            )
            fig_g.update_layout(font=dict(color=VM_TEXT), paper_bgcolor="white")
            st.plotly_chart(fig_g, use_container_width=True)


def page_event() -> None:
    st.header("Событие")
    path = db_path()
    if not require_db(path):
        return
    years = mq.query_distinct_years(path)
    if not years:
        st.warning("Нет годов в базе.")
        return
    year = st.selectbox("Год", years, index=0, key="ev_year")
    comps = mq.query_competitions_for_year(path, year)
    if not comps:
        st.info("Нет событий за выбранный год.")
        return
    labels = [f"{c.get('id')} — {c.get('событие', '')[:60]}" for c in comps]
    idx = st.selectbox("Соревнование", range(len(labels)), format_func=lambda i: labels[i])
    comp_id = int(comps[idx]["id"])

    st.subheader("Сводка")
    st.dataframe(pd.DataFrame(mq.query_competition_header(path, comp_id)), use_container_width=True)
    st.subheader("Дистанции")
    st.dataframe(pd.DataFrame(mq.query_competition_distances(path, comp_id)), use_container_width=True)
    st.subheader("Топ-10 зачёта (фрагмент)")
    st.dataframe(pd.DataFrame(mq.query_competition_top10(path, comp_id)), use_container_width=True)
    st.subheader("Группы")
    st.dataframe(pd.DataFrame(mq.query_competition_groups(path, comp_id)), use_container_width=True)


def _stat_int(val: object) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


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


def page_participant() -> None:
    st.header("Участник")
    path = db_path()
    if not require_db(path):
        return

    needle = st.text_input(
        "Поиск по имени, фамилии или id",
        placeholder="Например: Иванов или 2139",
        key="part_needle",
    )
    pid_direct = st.number_input(
        "Или введите id участника напрямую",
        min_value=0,
        value=0,
        step=1,
        key="part_pid_direct",
    )

    rows: list[dict] = []
    if needle.strip():
        rows = mq.query_profile_search(path, needle.strip(), 50)

    table_pid: int | None = None
    if rows:
        st.caption("Компактный выбор: **таблица** — выделите одну строку (участника).")
        df_pick = pd.DataFrame(
            [
                {
                    "id": int(r["id"]),
                    "Фамилия": (r.get("last_name") or "").strip(),
                    "Имя": (r.get("first_name") or "").strip(),
                    "Город": (r.get("city") or "").strip(),
                }
                for r in rows
            ]
        )
        h = min(320, 72 + len(df_pick) * 36)
        ev = st.dataframe(
            df_pick,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="part_search_pick_df",
            height=h,
        )
        if ev.selection.rows:
            table_pid = int(df_pick.iloc[ev.selection.rows[0]]["id"])

    active_pid: int | None = None
    if pid_direct and pid_direct > 0:
        active_pid = int(pid_direct)
    elif table_pid is not None:
        active_pid = table_pid

    if active_pid is None:
        if needle.strip() and not rows:
            st.warning("Ничего не найдено.")
        elif needle.strip() and rows:
            st.caption("Выберите строку в таблице или введите **id** участника выше.")
        elif not needle.strip() and (not pid_direct or pid_direct <= 0):
            st.caption("Введите запрос в поиск или id участника.")
        return

    show_participant_dashboard(path, active_pid)


def show_participant_dashboard(path: Path, pid: int) -> None:
    p = mq.query_profile_row(path, pid)
    if not p:
        st.warning(f"Профиль #{pid} не найден.")
        return

    title = f"{(p.get('last_name') or '').strip()} {(p.get('first_name') or '').strip()}".strip() or "Участник"
    st.subheader(f"{title} · id {pid}")
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.95rem;margin:0 0 12px 0;">'
        f"Пол: <b>{html.escape(str(p.get('gender') or '—'))}</b> · "
        f"Возраст: <b>{html.escape(str(p.get('age') if p.get('age') is not None else '—'))}</b> · "
        f"Город: <b>{html.escape(str(p.get('city') or '—'))}</b> · "
        f"Клуб: <b>{html.escape(str(p.get('club') or '—'))}</b></p>",
        unsafe_allow_html=True,
    )

    st.markdown("##### Сводная статистика (профиль)")
    s1, s2, s3, s4, s5, s6 = st.columns(6, gap="small")
    with s1:
        metric_plaque("Событий (stat_competitions)", _stat_int(p.get("stat_competitions")))
    with s2:
        metric_plaque("Км всего (stat_km)", _stat_int(p.get("stat_km")))
    with s3:
        metric_plaque("Марафонов (stat_marathons)", _stat_int(p.get("stat_marathons")))
    with s4:
        metric_plaque("Первых мест", _stat_int(p.get("stat_first")))
    with s5:
        metric_plaque("Вторых мест", _stat_int(p.get("stat_second")))
    with s6:
        metric_plaque("Третьих мест", _stat_int(p.get("stat_third")))

    raw_years = mq.parse_profile_active_years(p.get("raw"))
    ay_text = ", ".join(str(y) for y in raw_years) if raw_years else "—"
    metric_plaque("Годы активности (raw → active_years)", ay_text)

    db_years = mq.query_profile_participation_years(path, pid)
    year_opts = sorted(set(db_years) | set(raw_years), reverse=True)
    cy = datetime.date.today().year
    if not year_opts:
        year_opts = [cy]
    default_y = cy if cy in year_opts else year_opts[0]

    sk = f"participant_year_filter_{pid}"
    if sk not in st.session_state or st.session_state[sk] not in year_opts:
        st.session_state[sk] = default_y

    st.markdown(
        f'<p style="color:{VM_TEXT};font-weight:600;margin:16px 0 6px 0;">Год</p>'
        f'<p style="color:{VM_MUTED};font-size:0.85rem;margin:0 0 8px 0;">'
        f"Фильтрует таблицы вкладок «События» и «Кубки».</p>",
        unsafe_allow_html=True,
    )
    st.pills(
        "Год",
        options=year_opts,
        selection_mode="single",
        key=sk,
        label_visibility="collapsed",
    )
    y_filter = int(st.session_state[sk])

    tab_ev, tab_cup = st.tabs(["События", "Кубки"])
    with tab_ev:
        ev_df = pd.DataFrame(mq.query_profile_results_history_for_year(path, pid, y_filter))
        if ev_df.empty:
            st.caption(f"Нет финишей в соревнованиях за **{y_filter}**.")
        else:
            st.dataframe(ev_df, use_container_width=True, hide_index=True)
    with tab_cup:
        cup_df = pd.DataFrame(mq.query_profile_cup_rows_for_year(path, pid, y_filter))
        if cup_df.empty:
            st.caption(
                f"Нет строк в **profile_cup_results** за **{y_filter}**. "
                "При необходимости: `python fill_profile_cup_results.py`."
            )
        else:
            st.dataframe(cup_df, use_container_width=True, hide_index=True)


def page_team() -> None:
    st.header("Команда")
    st.caption(
        "Команда задаётся полем **team** в результатах соревнований. "
        "Очки в кубках берутся из **profile_cup_results** для участников команды."
    )
    path = db_path()
    if not require_db(path):
        return

    search = st.text_input(
        "Поиск по названию",
        placeholder="Начните вводить название…",
        key="team_search",
    )
    names = mq.query_team_names_for_select(path, search or None, limit=500)
    if not names:
        st.warning("В базе нет команд с непустым полем team в финишах (dnf=0).")
        return

    team = st.selectbox("Выберите команду", options=names, index=0, key="team_pick")
    if not team:
        return

    stats = mq.query_team_stats(path, team)
    if not stats:
        st.error("Нет финишей для этой команды в выбранных данных.")
        return

    st.subheader("Сводка по команде")
    t1, t2, t3 = st.columns(3, gap="small")
    with t1:
        metric_plaque("Участников (уник.)", stats["participants"])
    with t2:
        metric_plaque("Всего финишей", stats["finishes"])
    with t3:
        metric_plaque("Активных лет участия", stats["active_years_count"])

    years_str = ", ".join(str(y) for y in stats.get("active_years_list") or [])
    st.markdown(
        f'<p style="color:{VM_MUTED};font-size:0.9rem;margin:4px 0 16px 0;">'
        f"<b>Годы участия в событиях:</b> {years_str or '—'}</p>",
        unsafe_allow_html=True,
    )

    st.subheader("Очки в кубках")
    year_opts = mq.query_team_year_options_for_cups(path, team)
    cy = datetime.date.today().year
    if not year_opts:
        year_opts = [cy]
    default_year = cy if cy in year_opts else year_opts[0]
    cup_year = st.selectbox(
        "Год учёта кубков",
        options=year_opts,
        index=year_opts.index(default_year) if default_year in year_opts else 0,
        key="team_cup_year",
        help="По умолчанию — текущий календарный год, если есть данные; иначе последний год из базы.",
    )

    cup_rows = mq.query_team_cup_points_for_year(path, team, int(cup_year))
    if not cup_rows:
        st.info(f"За **{cup_year}** нет строк в profile_cup_results для участников этой команды.")
    else:
        df = pd.DataFrame(cup_rows)
        df = df.rename(
            columns={
                "athlete": "Участник",
                "cup": "Кубок",
                "distance": "Дистанция",
                "place": "Место",
                "cup_group": "Группа",
                "points": "Очки",
            }
        )
        drop_cols = [c for c in ("profile_id",) if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")
        st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("Топ команд по числу финишей (справочно)"):
        st.dataframe(
            pd.DataFrame(mq.query_teams_top(path, year=None, limit=40)),
            use_container_width=True,
        )


def page_cups() -> None:
    st.header("Кубки")
    path = db_path()
    if not require_db(path):
        return

    st.caption(
        "Сводка кубков за год — **profile_cup_results**. Личное первенство — те же строки + **cup_results**. "
        "Командное первенство — сумма очков **пяти лучших участников** (из **profile_cup_results.total_points**, "
        "команда из **results** по **cup_competitions**). "
        "При пустой базе: `python crawler_full.py` / `python sync.py` или `python fill_profile_cup_results.py`."
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

    st.caption("Выберите кубок в списке — таблица результатов внизу обновится (без отдельного списка).")
    nopts = list(range(len(summaries)))
    idx = st.radio(
        "Кубок",
        options=nopts,
        format_func=lambda i: (
            f"{str(summaries[i].get('кубок') or '—')}"
            f"  ·  {int(summaries[i].get('участников') or 0)} участн."
        ),
        key=f"cup_filter_{year}",
        label_visibility="collapsed",
        horizontal=False,
    )
    if idx is None:
        idx = 0
    cup_id = int(summaries[int(idx)]["id"])
    cup_title = str(summaries[int(idx)].get("кубок") or f"#{cup_id}")

    st.subheader(f"Результаты: {cup_title} · {year}")
    tab_ind, tab_team = st.tabs(["Личное первенство", "Командный зачёт"])

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
                f"Показано строк: **{len(filtered)}** из {len(detail)}. "
                "**Место** из `cup_results` (абсолют / по полу / в группе); "
                "если строки в `cup_results` нет — `place_abs` из `profile_cup_results`. "
                "**Очки** — из `cup_results.raw` → массив **competition_points** (элемент с тем же "
                "**competition_id**, что и этап строки, поле **points**); иначе расчёт "
                "`cup_scoring_computed_finishes` (**2026_run_v1**, `python compute_cup_scoring.py`); "
                "иначе `profile_cup_results.total_points`."
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
                key=f"cups_team_filter_{year}_{cup_id}",
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
                    "(**results** + **cup_competitions**). Очки — только из **profile_cup_results.total_points**: "
                    "по **`raw.competition.id`** (как у **results.competition_id**) или по паре **название этапа из raw + дистанция** "
                    "(без одной общей суммы на все строки)."
                )
                with st.spinner("Загрузка…"):
                    frag = _cup_team_hierarchy_html(
                        _cup_team_aggregate_points(score_rows), path, cup_id, year
                    )
                if hasattr(st, "html"):
                    st.html(frag)
                else:
                    st.markdown(frag, unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(
        page_title="ВологдаМарафон — аналитика",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_vm_styles()

    PAGES: tuple[str, ...] = (
        "Общая статистика",
        "Событие",
        "Участник",
        "Команда",
        "Кубки",
    )
    i_from_url = _sidebar_read_nav_i_from_url()
    if i_from_url is not None and 0 <= i_from_url < len(PAGES):
        st.session_state["nav_page"] = PAGES[i_from_url]
    if st.session_state.get("nav_page") not in PAGES:
        st.session_state["nav_page"] = PAGES[0]
    page: str = st.session_state["nav_page"]
    render_sidebar_text_nav(PAGES, page)

    path = db_path()
    st.sidebar.divider()
    st.sidebar.caption("База данных")
    st.sidebar.code(str(path), language=None)
    if path.is_file():
        tot = mq.query_general_stats_cards(path, None, None, None)
        st.sidebar.caption("Сводно по всей базе")
        sidebar_stat_card("Общее количество событий", tot.get("total_events", 0))
        sidebar_stat_card(
            "Общее количество участников (уникальных по profile_id)",
            tot.get("total_participants", 0),
        )
        sidebar_stat_card("Общее количество команд", tot.get("teams_distinct", 0))
    st.sidebar.divider()
    st.sidebar.markdown(
        f'<p style="font-size:11px;color:{VM_MUTED};">Официальный сайт: '
        f'<a href="https://vologdamarafon.ru" target="_blank" rel="noopener" '
        f'style="color:{VM_LINK};">vologdamarafon.ru</a></p>',
        unsafe_allow_html=True,
    )

    if page == "Общая статистика":
        page_general_statistics()
    elif page == "Событие":
        page_event()
    elif page == "Участник":
        page_participant()
    elif page == "Команда":
        page_team()
    else:
        page_cups()


if __name__ == "__main__":
    main()
