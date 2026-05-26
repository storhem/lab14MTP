# Лабораторная работа №14 — Конвейеры обработки данных на Python и Go

**Студент:** Сторожетдинов Максим  
**Вариант:** 7 — Анализ вакансий (hh.ru API, публичный)  
**Уровень:** Повышенная сложность  

## Описание

ETL-конвейер для сбора, агрегации и анализа вакансий с hh.ru. Реализованы все задания повышенной сложности:

1. **Задание 1** — Распределённый сборщик на Go с координацией через etcd. Несколько экземпляров сборщика разделяют шарды (регион × ключевое слово) через distributed lock.
2. **Задание 2** — Оконная агрегация (tumbling window) в Go: каждые N секунд агрегируются собранные вакансии — средняя/мин/макс зарплата, топ навыков, количество по регионам.
3. **Задание 3** — Передача данных через Apache Arrow Flight RPC: Go-сервер → Python-клиент.
4. **Задание 4** — Rust-библиотека для валидации данных о вакансиях, интеграция через cgo (Go) и PyO3 (Python).
5. **Задание 5** — Docker Compose + Kubernetes (minikube) с HPA-автоскалированием сборщика.
6. **Задание 6** — Веб-дашборд на Streamlit: топ навыки, зарплаты, регионы, работодатели, авто-обновление.

## Архитектура

```
hh.ru API
    │
    ▼
┌─────────────────────────────────────────────────┐
│  Go Collectors (2+ instances)                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │worker-1  │  │worker-2  │  │worker-N  │      │
│  │shards1-3 │  │shards4-6 │  │...       │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       │etcd coord   │             │             │
│       └─────────────┼─────────────┘             │
│                     ▼                           │
│       ┌─────────────────────────┐               │
│       │   TumblingWindow (60s)  │               │
│       │   Aggregation           │               │
│       └──────────┬──────────────┘               │
│                  │                              │
│       ┌──────────▼──────────────┐               │
│       │  Arrow Flight Server    │               │
│       │  :50051                 │               │
│       └──────────┬──────────────┘               │
│                  │                              │
│       ┌──────────▼──────────────┐               │
│       │  JSONL files /data/     │               │
│       └─────────────────────────┘               │
└─────────────────────────────────────────────────┘
          │Arrow Flight           │JSONL
          ▼                       ▼
┌──────────────────┐  ┌────────────────────────────┐
│  Python Analyzer │  │  Streamlit Dashboard       │
│  ─────────────── │  │  ──────────────────────    │
│  Polars + DuckDB │  │  Real-time charts:         │
│  Parquet output  │  │  - vacancies by area       │
│  Visualizations  │  │  - salary distribution     │
│                  │  │  - top skills              │
└──────────────────┘  └────────────────────────────┘
          │
          ▼
    Rust Validator (cgo / PyO3)
    ─────────────────────────
    Validates: name length, salary range,
    area_id presence, realistic values
```

## Технологический стек

| Компонент | Технология |
|-----------|-----------|
| Сборщик | Go 1.21+, goroutines, channels |
| Координация | etcd 3.5 |
| Передача данных | Apache Arrow Flight RPC |
| Валидация | Rust (cdylib + PyO3) |
| Анализ | Python, Polars, DuckDB |
| Визуализация | Plotly, Streamlit |
| Инфраструктура | Docker Compose, Kubernetes (minikube) |

## Требования

- Go 1.21+
- Python 3.10+
- Docker & Docker Compose
- Rust + Cargo (для задания 4)
- minikube (для задания 5, опционально)

## Быстрый старт

### 1. Сборка Rust-валидатора

```bash
# Установить Rust: winget install Rustlang.Rustup
cd src/validator
bash build.sh
```

### 2. Запуск через Docker Compose

```bash
# Запуск etcd + двух сборщиков + дашборда
docker-compose up -d

# Запуск анализатора (разово, после сбора данных)
docker-compose --profile analyze up analyzer

# Остановка
docker-compose down
```

### 3. Запуск локально (без Docker)

```bash
# Запустить etcd (например через Docker)
docker run -d -p 2379:2379 bitnami/etcd:3.5 \
  -e ALLOW_NONE_AUTHENTICATION=yes

# Запустить Go-сборщик
cd src/collector
go run . --etcd localhost:2379 --output ../../data --window 60

# В другом терминале — анализатор
cd src/analyzer
pip install -r requirements.txt
python main.py --data-dir ../../data --output-dir ../../output

# Дашборд
cd src/dashboard
streamlit run app.py
```

### 4. Kubernetes (minikube)

```bash
# Подготовка
minikube start
eval $(minikube docker-env)

# Сборка образов
docker build -t lab14/collector:latest src/collector/
docker build -t lab14/dashboard:latest src/dashboard/

# Деплой
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/etcd.yaml
kubectl apply -f k8s/collector.yaml
kubectl apply -f k8s/dashboard.yaml

# Доступ к дашборду
minikube service dashboard -n lab14

# Автоскалирование (нагрузочный тест)
kubectl get hpa -n lab14 --watch
```

## Структура проекта

```
lab14MTP/
├── src/
│   ├── collector/          # Go-сборщик
│   │   ├── main.go         # точка входа
│   │   ├── hh/             # hh.ru API клиент
│   │   ├── window/         # tumbling window агрегация
│   │   ├── etcd/           # etcd координация
│   │   ├── arrow/          # Arrow Flight сервер
│   │   ├── validator/      # интерфейс к Rust (cgo)
│   │   └── Dockerfile
│   ├── validator/          # Rust-библиотека
│   │   ├── src/lib.rs      # логика валидации
│   │   ├── Cargo.toml
│   │   ├── vacancy_validator.h  # C-заголовок для cgo
│   │   └── build.sh
│   ├── analyzer/           # Python-анализатор
│   │   ├── main.py
│   │   ├── analysis.py     # Polars + DuckDB анализ
│   │   ├── visualize.py    # Plotly визуализации
│   │   ├── arrow_client.py # Arrow Flight клиент
│   │   └── Dockerfile
│   └── dashboard/          # Streamlit-дашборд
│       ├── app.py
│       └── Dockerfile
├── tests/
│   ├── collector/          # Go-тесты (window, validator)
│   └── analyzer/           # Python-тесты (pytest)
├── k8s/                    # Kubernetes манифесты
│   ├── namespace.yaml
│   ├── etcd.yaml
│   ├── collector.yaml      # Deployment + HPA
│   └── dashboard.yaml
├── docker-compose.yml
├── PROMPT_LOG.md
└── README.md
```

## Запуск тестов

```bash
# Go-тесты
cd tests/collector
go test ./... -v

# Python-тесты
cd tests/analyzer  # или из корня:
python -m pytest tests/analyzer/ -v

# Rust-тесты (требует cargo)
cd src/validator
cargo test
```

## Примеры данных

Вакансия после обработки:
```json
{
  "id": "91234567",
  "name": "Go Developer",
  "area_name": "Москва",
  "employer_name": "ООО Технологии",
  "salary_from": 150000,
  "salary_to": 250000,
  "snippet_requirement": "Опыт Go, Docker, Kubernetes",
  "published_at": "2025-05-01T10:00:00Z"
}
```

## Авторы и контакты

Лабораторная работа выполнена с использованием Claude Code (Anthropic).
