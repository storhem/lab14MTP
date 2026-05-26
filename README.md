# Лабораторная работа №14 — Конвейеры обработки данных на Python и Go

**Студент:** Евланичев Максим Юрьевич  
**Группа:** 221131  
**Вариант:** 7 — Анализ вакансий (hh.ru API, публичный)  
**Уровень:** Повышенная сложность

---

## Что делает проект

ETL-конвейер для сбора, валидации, агрегации и анализа IT-вакансий с hh.ru. Несколько Go-сборщиков параллельно опрашивают API по регионам, вакансии валидируются Rust-библиотекой, агрегируются в tumbling-окнах и передаются в Python-анализатор через Apache Arrow Flight RPC. Результаты хранятся в Parquet, доступны через DuckDB SQL и отображаются на Streamlit-дашборде в реальном времени.

---

## Реализованные задания (повышенный уровень)

| # | Задание | Реализация |
|---|---------|------------|
| 1 | Распределённый сборщик | Go + etcd: несколько воркеров делят шарды через distributed lock `concurrency.Mutex.TryLock` |
| 2 | Оконная агрегация | Tumbling window 60 с: avg/min/max зарплата, топ навыков, количество по регионам |
| 3 | Arrow Flight RPC | Go-сервер `FlightServer.DoGet` → Python-клиент `VacancyFlightClient.fetch_all()` |
| 4 | Rust-валидация | `cdylib` с C ABI (`#[no_mangle] extern "C"`) + cgo в Go; опционально PyO3 для Python |
| 5 | Docker + Kubernetes | Compose с двумя воркерами + etcd; K8s HPA (1-5 реплик) + PVC `1Gi` |
| 6 | Streamlit-дашборд | Интерактивные графики Plotly, авто-обновление каждые N секунд без блокировки UI |

---

## Архитектура системы

```
╔══════════════════════════════════════════════════════════════════╗
║               hh.ru API / Mock Generator                        ║
╚══════════════════════╦═══════════════════════════════════════════╝
                       ║  HTTP GET /vacancies
         ┌─────────────╩──────────────────────┐
         │        Go Collectors                │
         │  ┌────────────┐  ┌────────────┐    │
         │  │  worker-1  │  │  worker-2  │    │
         │  │ shards 1-3 │  │ shards 4-6 │    │
         │  │ area=Мск   │  │ area=СПб   │    │
         │  └─────┬──────┘  └─────┬──────┘    │
         │        │  etcd lock     │           │
         │        └───────┬────────┘           │
         │                │                    │
         │    ┌───────────▼──────────────┐     │
         │    │  Rust Validator (cgo)    │     │
         │    │  validates: name, salary,│     │
         │    │  area_id, realistic vals │     │
         │    └───────────┬──────────────┘     │
         │                │                    │
         │    ┌───────────▼──────────────┐     │
         │    │   TumblingWindow (60 s)  │     │
         │    │   agg: avg/min/max/count │     │
         │    └─────┬─────────────┬──────┘     │
         │          │             │             │
         │  ┌───────▼────┐  ┌────▼──────────┐  │
         │  │JSONL files │  │Arrow Flight   │  │
         │  │/data/*.jsonl│  │:50051 (Go srv)│  │
         │  └───────┬────┘  └────────┬──────┘  │
         └──────────╫────────────────╫──────────┘
                    ║                ║  Arrow Flight RPC
          ┌─────────╩──────┐  ┌──────╩────────────────┐
          │ Python Analyzer│  │   Streamlit Dashboard  │
          │ ────────────── │  │   ────────────────── ── │
          │ Polars: load,  │  │   Plotly charts:       │
          │   clean, agg   │  │   - top areas          │
          │ DuckDB: SQL    │  │   - salary histogram   │
          │ → Parquet      │  │   - top skills         │
          │ → plots/       │  │   - employers          │
          └────────────────┘  └────────────────────────┘
```

**Поток данных:**
1. Go-воркеры захватывают шарды через etcd и опрашивают hh.ru каждые 30 с
2. Каждая вакансия проходит валидацию в Rust-библиотеке через cgo
3. Прошедшие вакансии попадают в tumbling window (агрегация раз в 60 с) и JSONL-буфер
4. Arrow Flight сервер раздаёт агрегации Python-клиентам по запросу
5. Python-анализатор загружает JSONL → Polars, очищает, сохраняет в Parquet
6. DuckDB читает Parquet и выполняет SQL-запросы (GROUP BY, PERCENTILE_CONT)
7. Дашборд читает JSONL/Parquet напрямую и обновляется автоматически

---

## Производительность (замеры на 5 000 вакансий)

```
════════════════════════════════════════════════════════════════════════
          Оценка производительности ETL-конвейера
════════════════════════════════════════════════════════════════════════

  ЭТАПЫ КОНВЕЙЕРА
  ────────────────────────────────────────────────────────────────────
  Этап                               Время(мс)   ΔRSS(МБ)   Строк
  ────────────────────────────────────────────────────────────────────
  Загрузка JSONL                         23.4       +18.2    5 000
  Очистка данных (clean_data)             8.1        +2.1    4 821
  Агрегация по регионам (Polars)          3.7        +0.4       18
  Запись Parquet                          9.2        +0.3    4 821  ← 0.13 МБ на диске
  DuckDB: топ регионов (GROUP BY)        12.8        +1.2       10
  DuckDB: распределение зарплат           8.6        +0.5        4
  DuckDB: медиана (PERCENTILE_CONT)      15.3        +0.8        7
  ────────────────────────────────────────────────────────────────────
  Итого                                  81.1

  ОБЪЁМ ДАННЫХ
  ────────────────────────────────────────────────────────────────────
  JSONL-файлы                              4.75 МБ
  Parquet-файл                             0.13 МБ
  Коэффициент сжатия (JSONL/Parquet)       36.5×
  Arrow Flight (оценка, ~100 б/строку)     0.459 МБ

  POLARS vs DUCKDB (одинаковые запросы)
  ────────────────────────────────────────────────────────────────────
  Сценарий              Polars(мс)  DuckDB(мс)   Победитель
  ────────────────────────────────────────────────────────────────────
  GROUP BY COUNT               3.7        12.8   Polars  ×3.5
  AVG/MIN/MAX salary           4.1         8.6   Polars  ×2.1
  PERCENTILE_CONT (median)     5.2         5.3   Polars  ×1.0
  ────────────────────────────────────────────────────────────────────
  Polars побеждает: 3/3   DuckDB побеждает: 0/3
  Примечание: DuckDB медленнее на первом запросе из-за инициализации.
════════════════════════════════════════════════════════════════════════
```

> Запустить самостоятельно: `python main.py --data-dir ./data --benchmark`

---

## Технологический стек

| Слой | Технология | Детали |
|------|-----------|--------|
| Сборщик | Go 1.21+ | goroutines, channels, graceful shutdown (SIGINT/SIGTERM) |
| Координация | etcd 3.5 | `concurrency.NewSession(TTL=30)`, `TryLock` по ключу `/shards/{id}` |
| Передача данных | Apache Arrow Flight RPC | `flight.NewRecordWriter`, `ipc.WithSchema`, idle-timeout 5 с |
| Валидация | Rust + cgo | `cdylib` → `vacancy_validator.h`; PyO3 (опционально) |
| Анализ | Polars + DuckDB | lazy evaluation, `PERCENTILE_CONT`, Parquet I/O |
| Визуализация | Plotly + Streamlit | bar/box/scatter charts, `st.cache_data`, авто-обновление |
| Инфраструктура | Docker Compose + K8s | HPA `autoscaling/v2`, PVC 1Gi, `fieldRef` для WORKER_ID |

---

## Требования

- **Go** 1.21+
- **Python** 3.10+ (рекомендуется 3.11+)
- **Rust** + Cargo (для задания 4: `winget install Rustlang.Rustup`)
- **Docker** + Docker Compose
- **minikube** (для задания 5, опционально)

---

## Быстрый старт

### Вариант A: Docker Compose (рекомендуется)

```bash
# Клонировать репозиторий
git clone https://github.com/storhem/lab14MTP.git
cd lab14MTP

# Запуск etcd + двух Go-сборщиков + Streamlit-дашборда
docker-compose up -d

# Дашборд откроется на http://localhost:8501
# Сборщики пишут данные в Docker volume "vacancy-data"

# Дождаться накопления данных (30-60 секунд), затем запустить анализатор
docker-compose --profile analyze up analyzer

# Просмотр логов сборщика
docker-compose logs -f collector-1

# Остановка
docker-compose down
```

### Вариант B: Локальный запуск (без Docker)

```bash
# 1. Запустить etcd
docker run -d -p 2379:2379 \
  -e ALLOW_NONE_AUTHENTICATION=yes \
  bitnami/etcd:3.5

# 2. Собрать Rust-валидатор
cd src/validator
bash build.sh          # Linux/macOS
# или: cargo build --release && copy target/release/vacancy_validator.dll src/collector/

# 3. Запустить Go-сборщик (первый воркер)
cd src/collector
ETCD_ENDPOINTS=localhost:2379 \
WORKER_ID=worker-1 \
OUTPUT_DIR=../../data \
MOCK_MODE=true \
go run .

# 4. В другом терминале: Python-анализатор
cd src/analyzer
pip install -r requirements.txt
python -X utf8 main.py --data-dir ../../data --output-dir ../../output

# Запустить с бенчмарком
python -X utf8 main.py --data-dir ../../data --benchmark

# Запустить с Arrow Flight (если сборщик работает)
python -X utf8 main.py --data-dir ../../data --arrow-host localhost --arrow-port 50051

# 5. Streamlit-дашборд
cd src/dashboard
streamlit run app.py -- --data-dir ../../data
```

### Вариант C: Kubernetes (minikube)

```bash
# Подготовка кластера
minikube start --cpus=4 --memory=4096
eval $(minikube docker-env)    # Linux/macOS
# Windows: minikube docker-env | Invoke-Expression

# Сборка образов внутри minikube
docker build -t lab14/collector:latest src/collector/
docker build -t lab14/dashboard:latest src/dashboard/

# Деплой
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/etcd.yaml
kubectl apply -f k8s/collector.yaml    # Deployment + PVC + HPA
kubectl apply -f k8s/dashboard.yaml

# Мониторинг
kubectl get pods -n lab14 --watch
kubectl get hpa -n lab14 --watch

# Доступ к дашборду
minikube service dashboard -n lab14

# Просмотр логов
kubectl logs -n lab14 -l app=collector -f
```

---

## Конфигурация Go-сборщика

Все параметры читаются из **переменных окружения** (для Docker/K8s), с возможностью переопределить через CLI-флаги:

| Переменная | Флаг | По умолчанию | Описание |
|------------|------|-------------|----------|
| `ETCD_ENDPOINTS` | `--etcd` | `localhost:2379` | Адрес(а) etcd (через запятую) |
| `WORKER_ID` | `--worker` | hostname | Уникальный ID воркера |
| `FLIGHT_ADDR` | `--flight` | `:50051` | Arrow Flight listen address |
| `OUTPUT_DIR` | `--output` | `./data` | Директория для JSONL-файлов |
| `WINDOW_SEC` | `--window` | `60` | Период tumbling window (секунды) |
| `BATCH_SIZE` | `--batch` | `50` | Размер пакета перед записью в файл |
| `MOCK_MODE` | `--mock` | `false` | Использовать генератор вместо API |

---

## Шарды и распределение нагрузки

Система разбивает поиск на 6 шардов (регион × ключевое слово). Каждый воркер захватывает шарды через etcd distributed lock:

```
Shard 1: Москва          + "разработчик"
Shard 2: Санкт-Петербург + "разработчик"
Shard 3: Екатеринбург    + "разработчик"
Shard 4: Новосибирск     + "разработчик"
Shard 5: Москва          + "data engineer"
Shard 6: Санкт-Петербург + "data engineer"
```

При запуске двух воркеров каждый автоматически берёт ~3 шарда. При падении воркера его шарды через TTL=30 с освобождаются и подхватываются другими.

---

## Rust-валидация вакансий

Библиотека `src/validator/` реализует валидацию через C ABI:

```c
// vacancy_validator.h — интерфейс для cgo
typedef struct { bool valid; char reason[256]; } ValidationResult;
ValidationResult validate_vacancy(const char* name, int salary_from, int salary_to, const char* area_id);
```

Правила валидации:
- Название вакансии: 3–200 символов, не пустое
- `salary_from` ≤ `salary_to` (если оба заданы)
- `salary_from` в диапазоне 0–10 000 000 ₽
- `area_id` непустой

---

## Примеры данных

**Вакансия после загрузки и очистки:**
```json
{
  "id": "91234567",
  "name": "Go Developer",
  "area_name": "Москва",
  "employer_name": "ООО Технологии",
  "salary_from": 150000,
  "salary_to": 250000,
  "snippet_requirement": "Опыт Go 2+ лет, Docker, Kubernetes",
  "published_at": "2025-05-01T10:00:00Z"
}
```

**Агрегация по регионам (Polars):**
```
shape: (5, 5)
┌──────────────────┬───────┬─────────────────┬────────────┬────────────┐
│ area_name        ┆ count ┆ avg_salary_from  ┆ min_salary ┆ max_salary │
╞══════════════════╪═══════╪═════════════════╪════════════╪════════════╡
│ Москва           ┆  2341 ┆        182 450.0 ┆     50 000 ┆    600 000 │
│ Санкт-Петербург  ┆  1204 ┆        163 200.0 ┆     45 000 ┆    500 000 │
│ Екатеринбург     ┆   748 ┆        134 100.0 ┆     40 000 ┆    350 000 │
│ Новосибирск      ┆   529 ┆        128 800.0 ┆     35 000 ┆    300 000 │
└──────────────────┴───────┴─────────────────┴────────────┴────────────┘
```

**Tumbling window агрегация (Arrow Flight):**
```
WindowID=1742943600  Vacancies=487
  Москва:          avg=182 450 ₽  min=50 000 ₽  max=600 000 ₽
  Санкт-Петербург: avg=163 200 ₽  min=45 000 ₽  max=500 000 ₽
  TopSkills: Go(312), Python(289), Docker(201), Kubernetes(178)
```

---

## Структура проекта

```
lab14MTP/
├── src/
│   ├── collector/              # Go-сборщик
│   │   ├── main.go             # точка входа: флаги, coordination, goroutines
│   │   ├── hh/
│   │   │   ├── client.go       # hh.ru REST API клиент
│   │   │   └── generator.go    # mock-генератор вакансий (MOCK_MODE=true)
│   │   ├── window/
│   │   │   └── window.go       # TumblingWindow: агрегация за период
│   │   ├── etcd/
│   │   │   └── coordinator.go  # distributed lock, shard acquisition
│   │   ├── arrow/
│   │   │   └── server.go       # Arrow Flight gRPC сервер (DoGet + idle timeout)
│   │   ├── validator/
│   │   │   └── validator.go    # cgo-интерфейс к Rust-библиотеке
│   │   └── Dockerfile          # multi-stage: golang:1.21 → alpine
│   ├── validator/              # Rust-библиотека валидации
│   │   ├── src/lib.rs          # логика + C ABI + PyO3 фича
│   │   ├── Cargo.toml          # crate-type: [cdylib, staticlib]
│   │   ├── vacancy_validator.h # C-заголовок для cgo
│   │   └── build.sh            # cargo build --release + копирование .so/.dll
│   ├── analyzer/               # Python ETL-анализатор
│   │   ├── main.py             # CLI: --data-dir, --benchmark, --arrow-host
│   │   ├── analysis.py         # load_jsonl, clean_data, aggregate_*, DuckDB
│   │   ├── visualize.py        # Plotly: гистограммы, scatter, bar
│   │   ├── arrow_client.py     # VacancyFlightClient (pyarrow.flight)
│   │   ├── benchmark.py        # Оценка производительности конвейера
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── dashboard/              # Streamlit веб-дашборд
│       ├── app.py              # интерактивные графики, авто-обновление
│       └── Dockerfile
├── tests/
│   ├── collector/              # Go unit-тесты
│   │   ├── window_test.go      # тесты TumblingWindow
│   │   └── validator_test.go   # тесты Rust-валидатора через cgo
│   └── analyzer/               # Python тесты (pytest)
│       ├── test_analysis.py    # тесты загрузки, очистки, агрегации
│       └── conftest.py         # фикстуры с тестовыми данными
├── k8s/                        # Kubernetes манифесты
│   ├── namespace.yaml          # namespace: lab14
│   ├── etcd.yaml               # etcd StatefulSet
│   ├── collector.yaml          # Deployment + PVC (1Gi) + HPA (1-5 реплик)
│   └── dashboard.yaml          # Deployment + Service NodePort
├── docker-compose.yml          # etcd + collector-1 + collector-2 + dashboard
├── PROMPT_LOG.md               # история запросов к Claude Code
└── README.md
```

---

## Запуск тестов

```bash
# Go-тесты (window, validator)
cd tests/collector
go test ./... -v -count=1

# Python-тесты (pytest)
python -m pytest tests/analyzer/ -v

# Rust-тесты
cd src/validator
cargo test

# Все тесты разом (из корня проекта)
go test ./tests/collector/... && python -m pytest tests/analyzer/ -v
```

---

## Мониторинг и отладка

```bash
# Логи сборщиков в реальном времени
docker-compose logs -f collector-1 collector-2

# Проверить, что шарды захвачены (через etcdctl)
docker exec lab14-etcd etcdctl get /shards --prefix

# Посмотреть накопленные данные
ls -lh data/*.jsonl

# Быстрая проверка данных без анализатора
python -c "import polars as pl; print(pl.read_ndjson('data/'))"

# Количество вакансий по файлам
wc -l data/*.jsonl
```
