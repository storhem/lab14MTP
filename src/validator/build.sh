#!/bin/bash
# Скрипт сборки Rust-библиотеки для валидации вакансий.
# Требует: rustup, cargo

set -e

echo "=== Building Rust vacancy validator ==="

# Проверяем наличие rustup
if ! command -v cargo &>/dev/null; then
    echo "ERROR: Rust not installed. Install from https://rustup.rs"
    exit 1
fi

# Сборка как статическая библиотека (для cgo)
echo "Building static library..."
cargo build --release --no-default-features

echo "Built: target/release/libvacancy_validator.a"
echo "Header: vacancy_validator.h"

# Сборка Python-расширения через maturin (для PyO3)
echo ""
echo "=== Building Python extension (PyO3) ==="
if command -v maturin &>/dev/null; then
    maturin develop --features python
    echo "Python module installed: vacancy_validator"
else
    echo "maturin not found. Install: pip install maturin"
    echo "Then run: maturin develop --features python"
fi

echo "Done."
