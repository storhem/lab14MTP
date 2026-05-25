"""
Оценка производительности ETL-конвейера (Задание 4, повышенный уровень).

Измеряет по каждому этапу:
  - время выполнения (мс)
  - потребление памяти RSS процесса (МБ) до и после
  - количество строк на входе и выходе

Дополнительно:
  - объём данных: JSONL → Parquet (размер файлов, коэффициент сжатия)
  - сравнение Polars vs DuckDB на идентичных запросах (3 сценария)
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import duckdb
import polars as pl

try:
    import psutil
    _PROC = psutil.Process(os.getpid())
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    _PROC = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rss_mb() -> float:
    if not HAS_PSUTIL:
        return 0.0
    return _PROC.memory_info().rss / 1_048_576


def _dir_size_mb(path: str) -> float:
    total = sum(f.stat().st_size for f in Path(path).glob("*.jsonl") if f.is_file())
    return total / 1_048_576


def _file_size_mb(path: str) -> float:
    p = Path(path)
    return p.stat().st_size / 1_048_576 if p.exists() else 0.0


@dataclass
class StageResult:
    name: str
    time_ms: float
    mem_before_mb: float
    mem_after_mb: float
    rows_in: int = 0
    rows_out: int = 0
    note: str = ""

    @property
    def mem_delta_mb(self) -> float:
        return self.mem_after_mb - self.mem_before_mb


def _run(name: str, fn: Callable[[], Any], rows_in: int = 0, note: str = "") -> tuple[Any, StageResult]:
    mem_before = _rss_mb()
    t0 = time.perf_counter()
    result = fn()
    elapsed_ms = (time.perf_counter() - t0) * 1_000
    mem_after = _rss_mb()
    rows_out = len(result) if hasattr(result, "__len__") else 0
    return result, StageResult(
        name=name,
        time_ms=elapsed_ms,
        mem_before_mb=mem_before,
        mem_after_mb=mem_after,
        rows_in=rows_in,
        rows_out=rows_out,
        note=note,
    )


# ---------------------------------------------------------------------------
# Benchmark сценарии Polars vs DuckDB
# ---------------------------------------------------------------------------

@dataclass
class EngineComparison:
    scenario: str
    polars_ms: float
    duckdb_ms: float

    @property
    def winner(self) -> str:
        return "Polars" if self.polars_ms < self.duckdb_ms else "DuckDB"

    @property
    def speedup(self) -> float:
        slower = max(self.polars_ms, self.duckdb_ms)
        faster = min(self.polars_ms, self.duckdb_ms)
        return slower / faster if faster > 0 else 1.0


def _bench_engines(df: pl.DataFrame, parquet_path: str) -> list[EngineComparison]:
    conn = duckdb.connect()
    results = []

    # Сценарий 1: GROUP BY COUNT — простая агрегация
    t0 = time.perf_counter()
    df.group_by("area_name").agg(pl.len().alias("count")).sort("count", descending=True)
    polars_ms = (time.perf_counter() - t0) * 1_000

    t0 = time.perf_counter()
    conn.execute(f"""
        SELECT area_name, COUNT(*) AS count
        FROM read_parquet('{parquet_path}')
        WHERE area_name IS NOT NULL
        GROUP BY area_name ORDER BY count DESC
    """).fetchall()
    duckdb_ms = (time.perf_counter() - t0) * 1_000
    results.append(EngineComparison("GROUP BY COUNT", polars_ms, duckdb_ms))

    # Сценарий 2: фильтрация + агрегация по зарплате
    t0 = time.perf_counter()
    (df.filter(pl.col("salary_from") > 0)
       .group_by("area_name")
       .agg([
           pl.col("salary_from").mean().alias("avg"),
           pl.col("salary_from").min().alias("min"),
           pl.col("salary_from").max().alias("max"),
       ])
       .sort("avg", descending=True))
    polars_ms = (time.perf_counter() - t0) * 1_000

    t0 = time.perf_counter()
    conn.execute(f"""
        SELECT area_name,
               AVG(salary_from) AS avg,
               MIN(salary_from) AS min,
               MAX(salary_from) AS max
        FROM read_parquet('{parquet_path}')
        WHERE salary_from > 0 AND area_name IS NOT NULL
        GROUP BY area_name ORDER BY avg DESC
    """).fetchall()
    duckdb_ms = (time.perf_counter() - t0) * 1_000
    results.append(EngineComparison("AVG/MIN/MAX salary", polars_ms, duckdb_ms))

    # Сценарий 3: медиана через PERCENTILE_CONT (DuckDB нативно, Polars — quantile)
    t0 = time.perf_counter()
    (df.filter(pl.col("salary_from") > 0)
       .group_by("area_name")
       .agg(pl.col("salary_from").quantile(0.5, interpolation="nearest").alias("median"))
       .sort("median", descending=True))
    polars_ms = (time.perf_counter() - t0) * 1_000

    t0 = time.perf_counter()
    conn.execute(f"""
        SELECT area_name,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_from) AS median
        FROM read_parquet('{parquet_path}')
        WHERE salary_from > 0 AND area_name IS NOT NULL
        GROUP BY area_name
        HAVING COUNT(*) >= 3
        ORDER BY median DESC
    """).fetchall()
    duckdb_ms = (time.perf_counter() - t0) * 1_000
    results.append(EngineComparison("PERCENTILE_CONT (median)", polars_ms, duckdb_ms))

    conn.close()
    return results


# ---------------------------------------------------------------------------
# Отчёт
# ---------------------------------------------------------------------------

def _print_report(
    stages: list[StageResult],
    comparisons: list[EngineComparison],
    jsonl_dir: str,
    parquet_path: str,
) -> None:
    W = 72
    SEP = "─" * W

    def row(label: str, *cols: str, widths=(34, 11, 12, 9)) -> str:
        parts = [label.ljust(widths[0])]
        for col, w in zip(cols, widths[1:]):
            parts.append(col.rjust(w))
        return "  ".join(parts)

    print(f"\n{'═' * W}")
    print(f"  Оценка производительности ETL-конвейера".center(W))
    print(f"{'═' * W}")

    if not HAS_PSUTIL:
        print("  [!] psutil не установлен — замеры памяти недоступны (pip install psutil)")

    # --- Этапы конвейера ---
    print(f"\n  {'ЭТАПЫ КОНВЕЙЕРА':}")
    print(f"  {SEP}")
    print("  " + row("Этап", "Время(мс)", "ΔRSS(МБ)", "Строк"))
    print(f"  {SEP}")
    for s in stages:
        delta = f"{s.mem_delta_mb:+.1f}" if HAS_PSUTIL else "  n/a"
        rows = str(s.rows_out) if s.rows_out else "—"
        note = f"  ← {s.note}" if s.note else ""
        print("  " + row(s.name, f"{s.time_ms:.1f}", delta, rows) + note)
    print(f"  {SEP}")
    total_ms = sum(s.time_ms for s in stages)
    print("  " + row("Итого", f"{total_ms:.1f}", "", ""))

    # --- Объём данных ---
    jsonl_mb = _dir_size_mb(jsonl_dir)
    parquet_mb = _file_size_mb(parquet_path)
    ratio = jsonl_mb / parquet_mb if parquet_mb > 0 else 0

    print(f"\n  {'ОБЪЁМ ДАННЫХ':}")
    print(f"  {SEP}")
    print(f"  {'JSONL-файлы':<34}  {jsonl_mb:>10.2f} МБ")
    print(f"  {'Parquet-файл':<34}  {parquet_mb:>10.2f} МБ")
    if ratio > 0:
        print(f"  {'Коэффициент сжатия (JSONL/Parquet)':<34}  {ratio:>10.1f}×")

    # Arrow Flight оценка (schema: 9 столбцов, ~100 байт на строку)
    if stages:
        raw_rows = next((s.rows_out for s in stages if "Загрузка" in s.name), 0)
        if raw_rows:
            arrow_est_mb = raw_rows * 100 / 1_048_576
            print(f"  {'Arrow Flight (оценка, ~100 б/строку)':<34}  {arrow_est_mb:>10.3f} МБ")

    # --- Polars vs DuckDB ---
    print(f"\n  {'POLARS vs DUCKDB (одинаковые запросы)':}")
    print(f"  {SEP}")
    header = row("Сценарий", "Polars(мс)", "DuckDB(мс)", "Победитель")
    print("  " + header)
    print(f"  {SEP}")
    for c in comparisons:
        winner_str = f"{c.winner}  ×{c.speedup:.1f}"
        print("  " + row(c.scenario, f"{c.polars_ms:.1f}", f"{c.duckdb_ms:.1f}", winner_str))

    polars_wins = sum(1 for c in comparisons if c.winner == "Polars")
    duckdb_wins = len(comparisons) - polars_wins
    print(f"  {SEP}")
    print(f"  Polars побеждает: {polars_wins}/{len(comparisons)}   "
          f"DuckDB побеждает: {duckdb_wins}/{len(comparisons)}")
    print(f"  Примечание: DuckDB медленнее на первом запросе из-за инициализации движка.")
    print(f"{'═' * W}\n")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def run_benchmark(data_dir: str, parquet_path: str) -> None:
    """
    Запускает полный бенчмарк конвейера и выводит отчёт.

    Args:
        data_dir:     путь к директории с JSONL-файлами (выход Go-сборщика)
        parquet_path: путь к Parquet-файлу (должен существовать к моменту вызова)
    """
    from analysis import load_jsonl_files, clean_data, aggregate_by_area, save_to_parquet

    stages: list[StageResult] = []

    # Этап 1: загрузка JSONL
    df, s = _run("Загрузка JSONL", lambda: load_jsonl_files(data_dir))
    stages.append(s)
    if df.is_empty():
        print("[benchmark] Нет данных для бенчмарка — запустите Go-сборщик.")
        return

    # Этап 2: очистка данных
    df_clean, s = _run("Очистка данных (clean_data)", lambda: clean_data(df), rows_in=len(df))
    stages.append(s)

    # Этап 3: агрегация Polars
    _, s = _run("Агрегация по регионам (Polars)", lambda: aggregate_by_area(df_clean), rows_in=len(df_clean))
    stages.append(s)

    # Этап 4: запись Parquet
    bench_parquet = parquet_path.replace(".parquet", "_bench.parquet")
    _, s = _run(
        "Запись Parquet",
        lambda: save_to_parquet(df_clean, bench_parquet),
        rows_in=len(df_clean),
    )
    s.rows_out = len(df_clean)
    s.note = f"{_file_size_mb(bench_parquet):.2f} МБ на диске"
    stages.append(s)

    # Этап 5: DuckDB — три запроса
    conn = duckdb.connect()

    def _duckdb_top_areas():
        return conn.execute(f"""
            SELECT area_name, COUNT(*) AS cnt, AVG(salary_from) AS avg_sal
            FROM read_parquet('{bench_parquet}')
            WHERE area_name IS NOT NULL GROUP BY area_name ORDER BY cnt DESC LIMIT 10
        """).fetchall()

    def _duckdb_salary_dist():
        return conn.execute(f"""
            SELECT CASE
                WHEN salary_from < 50000  THEN '< 50K'
                WHEN salary_from < 100000 THEN '50K-100K'
                WHEN salary_from < 200000 THEN '100K-200K'
                ELSE '> 200K' END AS range, COUNT(*) AS cnt
            FROM read_parquet('{bench_parquet}')
            WHERE salary_from > 0 GROUP BY range ORDER BY MIN(salary_from)
        """).fetchall()

    def _duckdb_percentile():
        return conn.execute(f"""
            SELECT area_name,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_from) AS median
            FROM read_parquet('{bench_parquet}')
            WHERE salary_from > 0 AND area_name IS NOT NULL
            GROUP BY area_name HAVING COUNT(*) >= 3
            ORDER BY median DESC
        """).fetchall()

    _, s = _run("DuckDB: топ регионов (GROUP BY)", _duckdb_top_areas, rows_in=len(df_clean))
    stages.append(s)
    _, s = _run("DuckDB: распределение зарплат", _duckdb_salary_dist, rows_in=len(df_clean))
    stages.append(s)
    _, s = _run("DuckDB: медиана (PERCENTILE_CONT)", _duckdb_percentile, rows_in=len(df_clean))
    stages.append(s)
    conn.close()

    # Сравнение движков
    comparisons = _bench_engines(df_clean, bench_parquet)

    # Отчёт
    _print_report(stages, comparisons, data_dir, bench_parquet)

    # Убираем временный файл
    Path(bench_parquet).unlink(missing_ok=True)
