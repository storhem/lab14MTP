"""Точка входа Python-анализатора: загружает данные, анализирует, сохраняет."""

import argparse
import logging
import sys
from pathlib import Path

from analysis import (
    load_jsonl_files,
    show_basic_info,
    clean_data,
    aggregate_by_area,
    aggregate_by_employer,
    save_to_parquet,
    analyze_with_duckdb,
    compare_polars_vs_duckdb,
)
from visualize import generate_all_plots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="hh.ru vacancy analyzer")
    parser.add_argument("--data-dir", default="./data", help="Directory with JSONL files")
    parser.add_argument("--output-dir", default="./output", help="Output directory")
    parser.add_argument("--plots-dir", default="./plots", help="Plots output directory")
    parser.add_argument("--skip-plots", action="store_true")
    parser.add_argument(
        "--arrow-host", default="",
        help="Arrow Flight сервер (host). Если задан — получить агрегации через Arrow Flight RPC",
    )
    parser.add_argument("--arrow-port", type=int, default=50051, help="Arrow Flight порт")
    args = parser.parse_args()

    Path(args.output_dir).mkdir(exist_ok=True)
    Path(args.plots_dir).mkdir(exist_ok=True)

    # Задание 3 (повышенное): получение агрегаций через Arrow Flight RPC
    if args.arrow_host:
        logger.info(f"Connecting to Arrow Flight at {args.arrow_host}:{args.arrow_port}...")
        try:
            from arrow_client import VacancyFlightClient
            with VacancyFlightClient(args.arrow_host, args.arrow_port) as client:
                arrow_df = client.fetch_all()
            if not arrow_df.is_empty():
                print(f"\n=== Arrow Flight: получено {len(arrow_df)} строк агрегаций ===")
                print(arrow_df)
            else:
                logger.warning("Arrow Flight: нет данных (сборщик ещё не накопил окно?)")
        except Exception as e:
            logger.warning(f"Arrow Flight недоступен: {e}")

    # Задание 4: загрузка данных
    logger.info("Loading data...")
    df = load_jsonl_files(args.data_dir)
    if df.is_empty():
        logger.error("No data found. Run the Go collector first.")
        sys.exit(1)
    show_basic_info(df)

    # Задание 5: очистка
    logger.info("Cleaning data...")
    df = clean_data(df)

    # Задание 6: агрегация
    logger.info("Aggregating...")
    by_area = aggregate_by_area(df)
    by_employer = aggregate_by_employer(df)
    print("\n=== Вакансии по регионам ===")
    print(by_area)
    print("\n=== Топ работодателей ===")
    print(by_employer)

    # Задание 7: сохранение в Parquet
    parquet_path = f"{args.output_dir}/vacancies.parquet"
    logger.info(f"Saving to Parquet: {parquet_path}")
    save_to_parquet(df, parquet_path)

    # Задание 8: DuckDB + сравнение производительности
    logger.info("Analyzing with DuckDB...")
    analyze_with_duckdb(parquet_path)
    compare_polars_vs_duckdb(df, parquet_path)

    # Задание 9: визуализация
    if not args.skip_plots:
        logger.info("Generating visualizations...")
        generate_all_plots(df, args.plots_dir)

    logger.info("Analysis complete!")


if __name__ == "__main__":
    main()
