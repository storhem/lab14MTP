"""Задание 6 (повышенное): Streamlit-дашборд с анализом вакансий hh.ru в реальном времени."""

import json
import time
from pathlib import Path
from datetime import datetime

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

# Импортируем модуль анализа
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "analyzer"))
from analysis import load_jsonl_files, clean_data, aggregate_by_area, aggregate_by_employer

st.set_page_config(
    page_title="hh.ru Vacancy Analyzer",
    page_icon="💼",
    layout="wide",
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
REFRESH_INTERVAL = 30  # секунд


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_data(data_dir: str) -> pl.DataFrame:
    df = load_jsonl_files(data_dir)
    if df.is_empty():
        return df
    return clean_data(df)


def render_header(df: pl.DataFrame):
    st.title("💼 Анализ вакансий hh.ru — Вариант 7")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего вакансий", len(df))
    if "area_name" in df.columns:
        col2.metric("Регионов", df["area_name"].n_unique())
    if "employer_name" in df.columns:
        col3.metric("Работодателей", df["employer_name"].n_unique())
    if "salary_from" in df.columns:
        with_salary = df.filter(pl.col("salary_from") > 0)
        if not with_salary.is_empty():
            avg = with_salary["salary_from"].mean()
            col4.metric("Средняя з/п от", f"{avg:,.0f} ₽")

    st.caption(f"Данные обновлены: {datetime.now().strftime('%H:%M:%S')} | Авто-обновление: {REFRESH_INTERVAL}с")


def render_area_chart(df: pl.DataFrame):
    st.subheader("Вакансии по регионам")
    if "area_name" not in df.columns:
        st.warning("Нет данных о регионах")
        return

    area_data = (
        df.group_by("area_name")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(15)
    )

    fig = px.bar(
        area_data.to_pandas(),
        x="count",
        y="area_name",
        orientation="h",
        color="count",
        color_continuous_scale="Blues",
        labels={"count": "Вакансий", "area_name": "Регион"},
    )
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        yaxis={"categoryorder": "total ascending"},
        margin=dict(l=0, r=0, t=20, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_salary_charts(df: pl.DataFrame):
    st.subheader("Анализ зарплат")
    if "salary_from" not in df.columns:
        st.warning("Нет данных о зарплатах")
        return

    salary_df = df.filter(pl.col("salary_from") > 0)
    if salary_df.is_empty():
        st.info("Нет вакансий с указанной зарплатой")
        return

    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(
            salary_df.to_pandas(),
            x="salary_from",
            nbins=25,
            title="Распределение зарплат (от)",
            labels={"salary_from": "Зарплата от (руб.)", "count": "Вакансий"},
            color_discrete_sequence=["#636EFA"],
        )
        fig.update_layout(margin=dict(t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "area_name" in salary_df.columns:
            top_areas = (
                salary_df.group_by("area_name")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .head(8)["area_name"]
                .to_list()
            )
            box_df = salary_df.filter(pl.col("area_name").is_in(top_areas))
            fig2 = px.box(
                box_df.to_pandas(),
                x="area_name",
                y="salary_from",
                title="Box-plot зарплат по регионам",
                labels={"salary_from": "Зарплата (руб.)", "area_name": "Регион"},
            )
            fig2.update_layout(showlegend=False, margin=dict(t=40, b=0))
            st.plotly_chart(fig2, use_container_width=True)


def render_skills_chart(df: pl.DataFrame):
    st.subheader("Топ навыков в вакансиях")
    tech_keywords = [
        "Python", "Go", "Java", "JavaScript", "TypeScript", "React", "Vue",
        "SQL", "PostgreSQL", "MySQL", "MongoDB", "Redis", "Kafka", "Docker",
        "Kubernetes", "Git", "Linux", "AWS", "REST", "gRPC", "Rust", "C++",
    ]

    if "snippet_requirement" not in df.columns and "snippet" not in df.columns:
        st.info("Нет данных о требованиях")
        return

    req_col = "snippet_requirement" if "snippet_requirement" in df.columns else None
    if req_col is None:
        return

    skill_counts = {}
    for kw in tech_keywords:
        count = df.filter(
            pl.col(req_col).str.contains(f"(?i){kw}", literal=False)
        ).height
        if count > 0:
            skill_counts[kw] = count

    if not skill_counts:
        st.info("Навыки не найдены в требованиях")
        return

    sorted_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)
    fig = go.Figure(go.Bar(
        x=[s[1] for s in sorted_skills],
        y=[s[0] for s in sorted_skills],
        orientation="h",
        marker_color="rgb(26, 118, 255)",
    ))
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Упоминаний",
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_employers_table(df: pl.DataFrame):
    st.subheader("Топ работодателей")
    if "employer_name" not in df.columns:
        return

    employers = (
        df.group_by("employer_name")
        .agg([
            pl.len().alias("Вакансий"),
            pl.col("salary_from").filter(pl.col("salary_from") > 0).mean().alias("Средняя з/п от"),
        ])
        .sort("Вакансий", descending=True)
        .head(10)
        .rename({"employer_name": "Работодатель"})
    )
    st.dataframe(employers.to_pandas(), use_container_width=True)


def render_raw_data(df: pl.DataFrame):
    with st.expander("Просмотр сырых данных"):
        cols = ["id", "name", "area_name", "employer_name", "salary_from", "salary_to", "published_at"]
        available_cols = [c for c in cols if c in df.columns]
        st.dataframe(df.select(available_cols).to_pandas(), use_container_width=True)


def main():
    # Sidebar с настройками
    with st.sidebar:
        st.header("Настройки")
        data_dir = st.text_input("Директория данных", str(DATA_DIR))
        auto_refresh = st.checkbox("Авто-обновление", value=True)
        refresh_sec = st.slider("Интервал (сек)", 10, 300, REFRESH_INTERVAL)
        if st.button("Обновить сейчас"):
            st.cache_data.clear()
            st.rerun()

    # Загрузка данных
    df = load_data(data_dir)

    if df.is_empty():
        st.warning("Данные не найдены. Запустите Go-сборщик для сбора вакансий.")
        st.code("cd src/collector && go run . --output ../../data", language="bash")
        st.stop()

    # Нормализация колонок
    if "area" in df.columns and df["area"].dtype == pl.Struct:
        df = df.with_columns(pl.col("area").struct.field("name").alias("area_name"))
    if "employer" in df.columns and df["employer"].dtype == pl.Struct:
        df = df.with_columns(pl.col("employer").struct.field("name").alias("employer_name"))
    if "snippet" in df.columns and df["snippet"].dtype == pl.Struct:
        df = df.with_columns(pl.col("snippet").struct.field("requirement").alias("snippet_requirement"))

    render_header(df)
    st.divider()

    col1, col2 = st.columns([1, 1])
    with col1:
        render_area_chart(df)
    with col2:
        render_employers_table(df)

    render_salary_charts(df)
    render_skills_chart(df)
    render_raw_data(df)

    # Авто-обновление
    if auto_refresh:
        time.sleep(refresh_sec)
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
