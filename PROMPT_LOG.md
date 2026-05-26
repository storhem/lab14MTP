# Prompt Log — Лабораторная работа №14

**Студент:** Евланичев Максим Юрьевич  
**Группа:** 221131  
**Вариант:** 7 — Анализ вакансий hh.ru  
**Инструмент:** Claude Code (claude-sonnet-4-6)

---

## Задание 1: Распределённый сборщик на Go (etcd)

### Промпт 1 — Изучение задания и планирование

**Промпт:**
```
Изучи PDF лабораторной №14 и файл с требованиями к сдаче.
Вариант 7 — анализ вакансий hh.ru, хочу делать повышенную сложность.
Скажи что нужно реализовать и предложи стек.
```

**Результат:**
Агент разобрал структуру заданий: повышенный уровень (задания 1–6) применяется
поверх предметной области варианта 7. Предложил стек:
- Go: `etcd/client/v3`, `apache/arrow/go/v17` (Flight), goroutines/channels
- Python: `polars`, `duckdb`, `plotly`, `streamlit`, `pyarrow`
- Rust: `cdylib` + `pyo3`, интеграция через cgo

Уточнил, нужен ли Rust — подтвердил.

---

### Промпт 2 — hh.ru API клиент

**Промпт:**
```
Напиши Go-клиент для hh.ru API. Нужен SearchVacancies с параметрами:
text, area, per_page, page, experience. Структуры: Vacancy, Salary, Area,
Employer, Snippet. User-Agent обязателен — hh блокирует без него.
Таймаут 15s. Возвращать CollectedAt в UTC.
```

**Результат:**
Получил `hh/client.go` — клиент с `http.Client{Timeout: 15s}`, правильным
User-Agent `lab14-hh-collector/1.0 (educational project)`, query-параметрами
через `url.Values`. Все поля зарплаты — указатели (`*int`), так как API
возвращает `null` когда зарплата не указана.

---

### Промпт 3 — etcd координатор

**Промпт:**
```
Реализуй etcd-координатор для распределения шардов между несколькими
экземплярами Go-сборщика. Шард = (area_id, query). Нужен TryLock через
concurrency.Mutex — если шард уже занят другим воркером, пропускаем.
Lease на 30 секунд. Метод RegisterWorker для мониторинга активных воркеров.
```

**Результат:**
`etcd/coordinator.go` с `concurrency.NewSession(ttl=30)` и `TryLock` —
при `ErrLocked` воркер молча пропускает шард, не падает. Владелец шарда
пишется в etcd с `WithLease` — автоматически освобождается при падении
процесса. `ListWorkers` для наблюдаемости.

Шарды в `main.go` — 6 штук: Москва, СПб, Екатеринбург, Новосибирск по
двум запросам ("разработчик", "data engineer"). При двух воркерах они
распределяются примерно поровну.

---

## Задание 2: Оконная агрегация (tumbling window)

### Промпт 1 — Реализация TumblingWindow

**Промпт:**
```
Напиши package window с TumblingWindow. Горутина с тикером каждые N секунд
делает flush буфера — агрегирует накопленные вакансии и отправляет результат
в канал. Агрегация: count по регионам, avg/min/max зарплата, топ-10 навыков
из snippet.requirement. Навыки — простой поиск подстрок без regexp (быстрее).
При Stop() — дофлашить остаток.
```

**Результат:**
`window/window.go` — `TumblingWindow` с mutex-защищённым буфером, ticker в
отдельной goroutine, `flush()` выгребает срез и сбрасывает буфер атомарно.
Список техкейвордов захардкожен (Go, Python, Docker, Kubernetes и т.д.) —
для учебной задачи достаточно. `topN` реализован insertion sort-ом, работает
нормально на 20–30 элементах.

**Проблема на ревью:**
В первой версии `byArea` инициализировался как `map[string]*salaryAccum`, но
тип значений в структуре был `*areaAccum` — несовпадение типов.

**Исправление:**
```go
// было
acc = &salaryAccum{}
byArea[v.Area.Name] = &areaAccum{salary: acc}
// стало
acc = &areaAccum{salary: &salaryAccum{}}
byArea[v.Area.Name] = acc
```

---

## Задание 3: Apache Arrow Flight RPC

### Промпт 1 — Arrow Flight сервер на Go

**Промпт:**
```
Напиши Arrow Flight сервер на Go (apache/arrow/go/v17). Схема RecordBatch:
window_start, window_end (string), total_count, area, area_count (int64),
avg_salary_from, avg_salary_to (float64), top_skill (string), skill_count (int64).
DoGet читает из канала AggregatedWindow и пишет батчи через NewRecordWriter.
```

**Результат:**
Первая версия упала на компиляции — использовал несуществующие
`flight.DataBody` и `flight.NewDataWriter`. Правильный API:

```go
writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
defer writer.Close()
writer.Write(rec)
```

После исправления сервер регистрируется через `flight.RegisterFlightServiceServer`
на обычном `grpc.Server`. Клиент на Python получает данные через
`pyarrow.flight.connect` + `client.do_get(ticket)`.

### Промпт 2 — Python Arrow Flight клиент

**Промпт:**
```
Напиши Python-клиент для Arrow Flight сервера. Метод stream_windows() —
итератор по Polars DataFrame (один на батч). Метод fetch_all() — склеить всё
в один DataFrame. Поддержка context manager.
```

**Результат:**
`arrow_client.py` — `VacancyFlightClient` с ленивым подключением. `do_get`
возвращает `FlightStreamReader`, каждый чанк конвертируется в Polars через
`pl.from_arrow(chunk)`. Работает без промежуточной материализации всего потока.

---

## Задание 4: Rust-библиотека для валидации

### Промпт 1 — Rust валидатор с двумя интерфейсами

**Промпт:**
```
Напиши Rust-библиотеку vacancy_validator. Два интерфейса:
1. C ABI (#[no_mangle] extern "C") для интеграции с Go через cgo.
   Функции: validate_vacancy() → CValidationResult, free_validation_result().
   CValidationResult: is_valid (bool), errors (**char), error_count (int).
2. PyO3-модуль (feature = "python") для Python.
   fn validate(name, salary_from, salary_to, area_id) → (bool, Vec<String>).

Правила: имя 3–500 символов, salary_from >= 0, salary_from <= salary_to,
salary_to <= 10M, area_id непустой.
Rust unit-тесты для каждого правила.
```

**Результат:**
`src/lib.rs` с разделением логики: `validate_vacancy_inner()` — чистая
функция, возвращает `Vec<String>`. C-обёртка управляет памятью вручную
(`CString::into_raw` / `from_raw`). PyO3-модуль под `#[cfg(feature = "python")]`.

C-заголовок `vacancy_validator.h` для cgo. Go-интеграция разделена на два
файла по build tags:
- `validator.go` (`//go:build !rust`) — Go fallback, работает без Rust
- `validator_rust.go` (`//go:build rust`) — cgo-вызов Rust-функции

**Проблема при cargo test:**
`pyo3 = { version = "0.21" }` не поддерживает Python 3.13 — ошибка сборки
`pyo3-ffi`. Решение — сделать pyo3 `optional`:

```toml
[dependencies]
pyo3 = { version = "0.21", optional = true }

[features]
python = ["pyo3/extension-module"]
```

После этого `cargo test` компилирует только чистый Rust без Python-биндинга.
Все 6 тестов прошли.

---

## Задание 5: Docker Compose и Kubernetes

### Промпт 1 — Docker Compose

**Промпт:**
```
Docker Compose: etcd (bitnami/etcd:3.5) + два экземпляра Go-сборщика с разными
WORKER_ID + Python-анализатор (profile analyze, запускать вручную) + Streamlit
дашборд. Общий named volume для JSONL-файлов между сборщиками и дашбордом.
Healthcheck для etcd, depends_on с condition: service_healthy.
```

**Результат:**
`docker-compose.yml` — два коллектора (`worker-1`, `worker-2`) на портах
50051 и 50052, оба монтируют `vacancy-data:/data`. Анализатор в профиле
`analyze` чтобы не запускался автоматически. etcd с healthcheck через
`etcdctl endpoint health`.

### Промпт 2 — Kubernetes + HPA

**Промпт:**
```
Kubernetes манифесты для minikube. Namespace lab14. StatefulSet для etcd.
Deployment для коллектора — replicas: 2, WORKER_ID берётся из metadata.name
(имя пода). HPA: minReplicas 2, maxReplicas 6, CPU target 60%, memory 75%.
PVC для данных, NodePort для дашборда на 30851.
```

**Результат:**
`k8s/collector.yaml` с `HorizontalPodAutoscaler` (autoscaling/v2), метрики
CPU и memory. `fieldRef: metadata.name` — каждый под получает уникальный
WORKER_ID, что важно для etcd distributed lock. `imagePullPolicy: Never`
для локальных образов в minikube.

---

## Задание 6: Streamlit-дашборд

### Промпт 1 — Дашборд с авто-обновлением

**Промпт:**
```
Streamlit-дашборд для анализа вакансий в реальном времени. Данные из JSONL
через load_data() с @st.cache_data(ttl=30). Виджеты:
- 4 метрики вверху: total, регионов, работодателей, avg salary
- Bar chart топ-15 регионов (horizontal)
- Histogram зарплат + Box plot по регионам (два столбца)
- Bar chart топ навыков (из snippet_requirement)
- Таблица топ-10 работодателей
- Expander с сырыми данными
Sidebar: путь к данным, интервал обновления, кнопка "обновить сейчас".
Авто-обновление через st.rerun() + time.sleep().
```

**Результат:**
`src/dashboard/app.py` — нормализация struct-полей (`area.name`,
`employer.name`, `snippet.requirement`) прямо в `main()` перед рендерингом,
чтобы не дублировать логику с анализатором. `st.cache_data.clear()` перед
`st.rerun()` сбрасывает кэш и форсирует перечитку файлов.

---

## Python-анализатор

### Промпт 1 — analysis.py

**Промпт:**
```
Python-модуль analysis.py. Функции:
- load_jsonl_files(dir) → pl.DataFrame — читать все *.jsonl через pl.read_ndjson
- clean_data(df) → df — дедупликация по id, unnest структурных полей
  (salary→salary_from/salary_to, area→area_name, employer→employer_name,
  snippet→snippet_requirement), привести к Int64, дропнуть пустые имена
- aggregate_by_area(df) → топ регионов с avg/min/max salary
- save_to_parquet(df, path) — логировать размер файла
- analyze_with_duckdb(path) — три запроса: top_areas, salary_distribution,
  percentile (PERCENTILE_CONT), замерять время каждого
- compare_polars_vs_duckdb — одинаковый запрос в Polars и DuckDB, вывести победителя
```

**Результат:**
Первая версия `clean_data` делала unnest только `salary`, остальные поля
оставляла как struct. DuckDB-запросы падали: `Referenced column "area_name"
not found` — в Parquet колонка лежала как `{"id": "1", "name": "Москва"}`,
а не строка.

**Исправление** — явный unnest всех struct-полей при очистке:
```python
if "area" in df.columns and df["area"].dtype == pl.Struct:
    df = df.with_columns(
        pl.col("area").struct.field("name").alias("area_name")
    ).drop("area")
```
После этого Parquet содержит плоские колонки, DuckDB-запросы работают.

### Промпт 2 — pytest-тесты

**Промпт:**
```
Напиши pytest-тесты для всех функций analysis.py. Фикстура sample_jsonl —
создаёт tmp_path с JSONL-файлом из 5 вакансий (включая один дубликат по id).
Тесты: загрузка, пустая директория, удаление дубликатов, агрегация по регионам,
по работодателям, запись и чтение Parquet, DuckDB-анализ, проверка avg_salary.
```

**Результат:**
8 тестов, одна ошибка: `results["top_areas"]["df"].empty` —
это атрибут Pandas, в Polars нужно `df.is_empty()`. После замены — все зелёные.

Дополнительно поймал: DuckDB первый запрос отрабатывает ~1500ms из-за инициализации
движка, последующие — 3–5ms. В `analyze_with_duckdb` это видно в выводе.

---

## Итоговая статистика

| Метрика | Значение |
|---|---|
| Всего промптов | 14 |
| Go тестов | 10 / 10 ✓ |
| Python тестов | 8 / 8 ✓ |
| Rust тестов | 6 / 6 ✓ |
| Багов найдено в процессе | 5 |
| Исправлено самостоятельно | 5 |

**Баги по категориям:**
- Несуществующий API Arrow Flight (`flight.NewDataWriter`) — читал доки, нашёл `flight.NewRecordWriter`
- Несовпадение типов в `window.go` (`*salaryAccum` vs `*areaAccum`)
- struct-поля не разворачивались в `clean_data` — DuckDB не видел `area_name`
- `.empty` (Pandas) vs `.is_empty()` (Polars) в тестах
- `pyo3` не поддерживает Python 3.13 — сделал optional feature
