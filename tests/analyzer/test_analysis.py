"""Тесты Python-анализатора вакансий."""

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src" / "analyzer"))

from analysis import (
    load_jsonl_files,
    clean_data,
    aggregate_by_area,
    aggregate_by_employer,
    save_to_parquet,
    analyze_with_duckdb,
)


def make_vacancy(id: str, name: str, area_name: str, area_id: str, salary_from: int | None, salary_to: int | None, employer: str) -> dict:
    return {
        "id": id,
        "name": name,
        "area": {"id": area_id, "name": area_name},
        "employer": {"id": "e1", "name": employer},
        "salary": {"from": salary_from, "to": salary_to, "currency": "RUR", "gross": False} if salary_from or salary_to else None,
        "snippet": {"requirement": "Python Go Docker", "responsibility": "разработка"},
        "published_at": "2025-05-01T10:00:00Z",
        "collected_at": "2025-05-01T10:01:00Z",
    }


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> str:
    vacancies = [
        make_vacancy("1", "Go Developer", "Москва", "1", 150000, 250000, "ООО Ромашка"),
        make_vacancy("2", "Python Engineer", "Москва", "1", 120000, 200000, "ООО Ромашка"),
        make_vacancy("3", "Backend Dev", "Санкт-Петербург", "2", 100000, 180000, "ООО Василёк"),
        make_vacancy("4", "Data Engineer", "Екатеринбург", "3", None, None, "ПАО Трубы"),
        make_vacancy("1", "Go Developer", "Москва", "1", 150000, 250000, "ООО Ромашка"),  # дубликат
    ]
    jsonl_file = tmp_path / "test.jsonl"
    with open(jsonl_file, "w", encoding="utf-8") as f:
        for v in vacancies:
            f.write(json.dumps(v, ensure_ascii=False) + "\n")
    return str(tmp_path)


def test_load_jsonl(sample_jsonl):
    df = load_jsonl_files(sample_jsonl)
    assert len(df) == 5
    assert "id" in df.columns
    assert "name" in df.columns


def test_load_empty_dir(tmp_path):
    df = load_jsonl_files(str(tmp_path))
    assert df.is_empty()


def test_clean_removes_duplicates(sample_jsonl):
    df = load_jsonl_files(sample_jsonl)
    df_clean = clean_data(df)
    # Дубликат по ID должен быть удалён
    assert df_clean["id"].n_unique() == len(df_clean)
    assert len(df_clean) == 4


def test_aggregate_by_area(sample_jsonl):
    df = load_jsonl_files(sample_jsonl)
    df = clean_data(df)
    result = aggregate_by_area(df)
    assert not result.is_empty()
    assert "area_name" in result.columns
    assert "count" in result.columns
    # Москва должна быть первой (2 вакансии после удаления дубликата)
    assert result["area_name"][0] == "Москва"
    assert result["count"][0] == 2


def test_aggregate_by_employer(sample_jsonl):
    df = load_jsonl_files(sample_jsonl)
    df = clean_data(df)
    result = aggregate_by_employer(df)
    assert not result.is_empty()
    assert "employer_name" in result.columns


def test_save_and_load_parquet(sample_jsonl, tmp_path):
    df = load_jsonl_files(sample_jsonl)
    df = clean_data(df)
    parquet_path = str(tmp_path / "test.parquet")
    save_to_parquet(df, parquet_path)
    assert Path(parquet_path).exists()
    df2 = pl.read_parquet(parquet_path)
    assert len(df2) == len(df)


def test_duckdb_analysis(sample_jsonl, tmp_path):
    df = load_jsonl_files(sample_jsonl)
    df = clean_data(df)
    parquet_path = str(tmp_path / "test.parquet")
    save_to_parquet(df, parquet_path)
    results = analyze_with_duckdb(parquet_path)
    assert "top_areas" in results
    assert not results["top_areas"]["df"].is_empty()
    assert results["top_areas"]["time_ms"] > 0


def test_salary_aggregation(sample_jsonl):
    df = load_jsonl_files(sample_jsonl)
    df = clean_data(df)
    result = aggregate_by_area(df)
    moscow = result.filter(pl.col("area_name") == "Москва")
    assert not moscow.is_empty()
    # avg_salary_from для Москвы: (150000 + 120000) / 2 = 135000
    assert moscow["avg_salary_from"][0] == pytest.approx(135000, rel=0.01)
