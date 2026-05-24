"""Задание 9: визуализация данных вакансий (Plotly + Matplotlib)."""

import json
from pathlib import Path
from typing import Optional

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def plot_salary_distribution(df: pl.DataFrame, output_dir: str = "plots") -> str:
    """Гистограмма распределения зарплат по регионам."""
    Path(output_dir).mkdir(exist_ok=True)

    salary_df = df.filter(pl.col("salary_from") > 0)
    if salary_df.is_empty():
        return ""

    fig = px.histogram(
        salary_df.to_pandas(),
        x="salary_from",
        nbins=30,
        title="Распределение нижней границы зарплат",
        labels={"salary_from": "Зарплата от (руб.)", "count": "Количество вакансий"},
        color_discrete_sequence=["#636EFA"],
    )
    fig.update_layout(
        xaxis_tickformat=",",
        bargap=0.1,
        template="plotly_white",
    )
    path = f"{output_dir}/salary_distribution.html"
    fig.write_html(path)

    # PNG версия
    try:
        png_path = f"{output_dir}/salary_distribution.png"
        fig.write_image(png_path)
    except Exception:
        pass

    return path


def plot_vacancies_by_area(df: pl.DataFrame, output_dir: str = "plots") -> str:
    """Горизонтальная гистограмма вакансий по регионам."""
    Path(output_dir).mkdir(exist_ok=True)

    if "area_name" not in df.columns:
        return ""

    area_counts = (
        df.group_by("area_name")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(15)
    )

    fig = px.bar(
        area_counts.to_pandas(),
        x="count",
        y="area_name",
        orientation="h",
        title="Топ-15 регионов по количеству вакансий",
        labels={"count": "Количество вакансий", "area_name": "Регион"},
        color="count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(template="plotly_white", yaxis={"categoryorder": "total ascending"})

    path = f"{output_dir}/vacancies_by_area.html"
    fig.write_html(path)
    try:
        fig.write_image(f"{output_dir}/vacancies_by_area.png")
    except Exception:
        pass

    return path


def plot_salary_box_by_area(df: pl.DataFrame, output_dir: str = "plots") -> str:
    """Box plot зарплат по топ-10 регионам."""
    Path(output_dir).mkdir(exist_ok=True)

    if "area_name" not in df.columns or "salary_from" not in df.columns:
        return ""

    # Топ-10 регионов по количеству вакансий с зарплатой
    top_areas = (
        df.filter(pl.col("salary_from") > 0)
        .group_by("area_name")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10)["area_name"]
        .to_list()
    )

    filtered = df.filter(
        pl.col("salary_from") > 0,
        pl.col("area_name").is_in(top_areas),
    )

    if filtered.is_empty():
        return ""

    fig = px.box(
        filtered.to_pandas(),
        x="area_name",
        y="salary_from",
        title="Распределение зарплат по топ-10 регионам",
        labels={"salary_from": "Зарплата от (руб.)", "area_name": "Регион"},
        color="area_name",
    )
    fig.update_layout(template="plotly_white", showlegend=False)

    path = f"{output_dir}/salary_box.html"
    fig.write_html(path)
    try:
        fig.write_image(f"{output_dir}/salary_box.png")
    except Exception:
        pass

    return path


def plot_skills_heatmap(skills: dict[str, int], output_dir: str = "plots") -> str:
    """Тепловая карта топ-навыков из вакансий."""
    Path(output_dir).mkdir(exist_ok=True)

    if not skills:
        return ""

    sorted_skills = sorted(skills.items(), key=lambda x: x[1], reverse=True)[:20]
    labels = [s[0] for s in sorted_skills]
    counts = [s[1] for s in sorted_skills]

    fig = go.Figure(go.Bar(
        x=counts,
        y=labels,
        orientation="h",
        marker_color="rgb(26, 118, 255)",
    ))
    fig.update_layout(
        title="Топ-20 технических навыков в вакансиях",
        xaxis_title="Количество упоминаний",
        yaxis={"categoryorder": "total ascending"},
        template="plotly_white",
    )

    path = f"{output_dir}/skills_heatmap.html"
    fig.write_html(path)
    try:
        fig.write_image(f"{output_dir}/skills_heatmap.png")
    except Exception:
        pass

    return path


def plot_timeline(df: pl.DataFrame, output_dir: str = "plots") -> str:
    """Временной ряд публикации вакансий."""
    Path(output_dir).mkdir(exist_ok=True)

    if "published_at" not in df.columns:
        return ""

    timeline = (
        df.with_columns(
            pl.col("published_at").str.slice(0, 10).alias("date")
        )
        .group_by("date")
        .agg(pl.len().alias("count"))
        .sort("date")
    )

    fig = px.line(
        timeline.to_pandas(),
        x="date",
        y="count",
        title="Динамика публикации вакансий по дням",
        labels={"date": "Дата", "count": "Количество вакансий"},
        markers=True,
    )
    fig.update_layout(template="plotly_white")

    path = f"{output_dir}/timeline.html"
    fig.write_html(path)
    try:
        fig.write_image(f"{output_dir}/timeline.png")
    except Exception:
        pass

    return path


def generate_all_plots(df: pl.DataFrame, output_dir: str = "plots") -> list[str]:
    """Генерирует все графики и возвращает пути к файлам."""
    paths = []
    for fn in [
        plot_salary_distribution,
        plot_vacancies_by_area,
        plot_salary_box_by_area,
        plot_timeline,
    ]:
        try:
            path = fn(df, output_dir)
            if path:
                paths.append(path)
                print(f"Saved: {path}")
        except Exception as e:
            print(f"[warn] {fn.__name__}: {e}")
    return paths
