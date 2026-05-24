"""Модуль анализа данных вакансий с использованием Polars и DuckDB."""

import json
import time
import logging
from pathlib import Path
from typing import Optional

import polars as pl
import duckdb

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Задание 4 (средн.): Импорт данных и базовый анализ                          #
# --------------------------------------------------------------------------- #

def load_jsonl_files(data_dir: str) -> pl.DataFrame:
    """Загружает JSONL-файлы из директории в Polars DataFrame."""
    paths = list(Path(data_dir).glob("*.jsonl"))
    if not paths:
        logger.warning(f"No .jsonl files found in {data_dir}")
        return pl.DataFrame()

    frames = []
    for path in paths:
        try:
            df = pl.read_ndjson(str(path))
            frames.append(df)
        except Exception as e:
            logger.error(f"Failed to read {path}: {e}")

    if not frames:
        return pl.DataFrame()

    df = pl.concat(frames, how="diagonal_relaxed")
    logger.info(f"Loaded {len(df)} rows from {len(paths)} files")
    return df


def show_basic_info(df: pl.DataFrame) -> None:
    """Задание 4: выводит первые 5 строк и базовую информацию."""
    print("\n=== Первые 5 вакансий ===")
    print(df.head(5))
    print(f"\n=== Информация о данных ===")
    print(f"Строк: {len(df)}, Колонок: {len(df.columns)}")
    print(f"Типы: {df.dtypes}")
    print(f"Пропуски: {df.null_count()}")


# --------------------------------------------------------------------------- #
# Задание 5 (средн.): Очистка и валидация                                     #
# --------------------------------------------------------------------------- #

def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """Очищает DataFrame: дубликаты, пропуски, типы данных."""
    initial = len(df)

    # Удалить дубликаты по ID
    df = df.unique(subset=["id"], keep="first")
    after_dedup = len(df)
    logger.info(f"Removed {initial - after_dedup} duplicates")

    # Нормализовать поля из вложенных структур
    if "salary" in df.columns and df["salary"].dtype == pl.Struct:
        df = df.with_columns([
            pl.col("salary").struct.field("from").alias("salary_from"),
            pl.col("salary").struct.field("to").alias("salary_to"),
        ]).drop("salary")

    if "area" in df.columns and df["area"].dtype == pl.Struct:
        df = df.with_columns(
            pl.col("area").struct.field("name").alias("area_name")
        ).drop("area")

    if "employer" in df.columns and df["employer"].dtype == pl.Struct:
        df = df.with_columns(
            pl.col("employer").struct.field("name").alias("employer_name")
        ).drop("employer")

    if "snippet" in df.columns and df["snippet"].dtype == pl.Struct:
        df = df.with_columns(
            pl.col("snippet").struct.field("requirement").alias("snippet_requirement")
        ).drop("snippet")

    # Нормализовать типы зарплат
    for col in ["salary_from", "salary_to"]:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Int64, strict=False).fill_null(0)
            )

    # Убрать вакансии с пустым названием
    if "name" in df.columns:
        df = df.filter(pl.col("name").is_not_null() & (pl.col("name").str.len_chars() > 2))

    logger.info(f"After cleaning: {len(df)} rows (removed {after_dedup - len(df)} invalid)")
    return df


# --------------------------------------------------------------------------- #
# Задание 6 (средн.): Агрегационный анализ                                   #
# --------------------------------------------------------------------------- #

def aggregate_by_area(df: pl.DataFrame) -> pl.DataFrame:
    """Агрегирует вакансии по регионам."""
    if "area_name" not in df.columns:
        return pl.DataFrame()

    result = df.group_by("area_name").agg([
        pl.len().alias("count"),
        pl.col("salary_from").filter(pl.col("salary_from") > 0).mean().alias("avg_salary_from"),
        pl.col("salary_to").filter(pl.col("salary_to") > 0).mean().alias("avg_salary_to"),
        pl.col("salary_from").filter(pl.col("salary_from") > 0).min().alias("min_salary"),
        pl.col("salary_to").filter(pl.col("salary_to") > 0).max().alias("max_salary"),
    ]).sort("count", descending=True)

    return result


def aggregate_by_employer(df: pl.DataFrame) -> pl.DataFrame:
    """Агрегирует вакансии по работодателям."""
    if "employer_name" not in df.columns:
        return pl.DataFrame()

    return df.group_by("employer_name").agg([
        pl.len().alias("vacancy_count"),
        pl.col("salary_from").filter(pl.col("salary_from") > 0).mean().alias("avg_salary"),
    ]).sort("vacancy_count", descending=True).head(20)


# --------------------------------------------------------------------------- #
# Задание 7 (средн.): Сохранение в Parquet                                    #
# --------------------------------------------------------------------------- #

def save_to_parquet(df: pl.DataFrame, path: str) -> None:
    """Сохраняет DataFrame в Parquet-формат."""
    df.write_parquet(path)
    size_mb = Path(path).stat().st_size / 1_048_576
    logger.info(f"Saved {len(df)} rows to {path} ({size_mb:.2f} MB)")


# --------------------------------------------------------------------------- #
# Задание 8 (средн.): Анализ через DuckDB и сравнение производительности      #
# --------------------------------------------------------------------------- #

def analyze_with_duckdb(parquet_path: str) -> dict:
    """Выполняет SQL-анализ через DuckDB и замеряет производительность."""
    conn = duckdb.connect()

    queries = {
        "top_areas": f"""
            SELECT area_name, COUNT(*) as count,
                   AVG(salary_from) as avg_from, AVG(salary_to) as avg_to
            FROM read_parquet('{parquet_path}')
            WHERE area_name IS NOT NULL
            GROUP BY area_name
            ORDER BY count DESC
            LIMIT 10
        """,
        "salary_distribution": f"""
            SELECT
                CASE
                    WHEN salary_from < 50000 THEN '< 50K'
                    WHEN salary_from < 100000 THEN '50K-100K'
                    WHEN salary_from < 200000 THEN '100K-200K'
                    WHEN salary_from < 300000 THEN '200K-300K'
                    ELSE '> 300K'
                END as salary_range,
                COUNT(*) as count
            FROM read_parquet('{parquet_path}')
            WHERE salary_from > 0
            GROUP BY salary_range
            ORDER BY MIN(salary_from)
        """,
        "percentile_salary": f"""
            SELECT
                area_name,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_from) as median_salary,
                COUNT(*) as count
            FROM read_parquet('{parquet_path}')
            WHERE salary_from > 0 AND area_name IS NOT NULL
            GROUP BY area_name
            HAVING COUNT(*) >= 5
            ORDER BY median_salary DESC
        """,
    }

    results = {}
    for name, sql in queries.items():
        t0 = time.perf_counter()
        df = conn.execute(sql).pl()
        elapsed = time.perf_counter() - t0
        results[name] = {"df": df, "time_ms": elapsed * 1000}
        logger.info(f"DuckDB [{name}]: {elapsed * 1000:.1f}ms, {len(df)} rows")
        print(f"\n=== DuckDB: {name} ({elapsed*1000:.1f}ms) ===")
        print(df)

    return results


def compare_polars_vs_duckdb(df: pl.DataFrame, parquet_path: str) -> dict:
    """Сравнение производительности Polars vs DuckDB для аналитики."""
    # Polars
    t0 = time.perf_counter()
    polars_result = aggregate_by_area(df)
    polars_time = (time.perf_counter() - t0) * 1000

    # DuckDB
    conn = duckdb.connect()
    sql = f"""
        SELECT area_name, COUNT(*) as count, AVG(salary_from) as avg_salary
        FROM read_parquet('{parquet_path}')
        WHERE area_name IS NOT NULL
        GROUP BY area_name ORDER BY count DESC
    """
    t0 = time.perf_counter()
    conn.execute(sql).fetchdf()
    duckdb_time = (time.perf_counter() - t0) * 1000

    comparison = {
        "polars_ms": polars_time,
        "duckdb_ms": duckdb_time,
        "winner": "polars" if polars_time < duckdb_time else "duckdb",
    }
    print(f"\n=== Производительность ===")
    print(f"Polars: {polars_time:.1f}ms")
    print(f"DuckDB: {duckdb_time:.1f}ms")
    print(f"Победитель: {comparison['winner']}")
    return comparison
