# Prompt Log

**Проект:** ETL конвейер вакансий hh.ru — Распределённый сборщик данных  
**ФИО:** Евланичев Максим Юрьевич  
**Группа:** 221131  
**ЛР №14, Вариант 7 (повышенная сложность)**

---

## Задание 1: Распределённый сборщик на Go (координация через etcd)

### Промпт 1 — Изучение задания и выбор стека

**Инструмент:** Claude Code

"Изучи PDF лабораторной №14 и файл с требованиями к сдаче. Вариант 7 — анализ вакансий hh.ru, хочу делать повышенную сложность. Скажи что нужно реализовать и предложи стек."

Результат:  
Агент разобрал структуру заданий: повышенный уровень — 6 заданий поверх предметной области варианта 7. Предложен стек: Go (`etcd/client/v3`, `apache/arrow/go/v17`), Python (`polars`, `duckdb`, `plotly`, `streamlit`, `pyarrow`), Rust (`cdylib` + `pyo3`). Уточнил необходимость Rust — подтвердил.

### Промпт 2 — hh.ru API клиент на Go

**Инструмент:** Claude Code

"Напиши Go-клиент для hh.ru API. Нужен SearchVacancies с параметрами: text, area, per_page, page, experience. Структуры: Vacancy, Salary, Area, Employer, Snippet. User-Agent обязателен — hh блокирует без него. Таймаут 15s. Возвращать CollectedAt в UTC."

Результат:  
Получил `hh/client.go` с `http.Client{Timeout: 15s}`, User-Agent `lab14-hh-collector/1.0 (educational project)`, query-параметрами через `url.Values`. Поля зарплаты — указатели (`*int`), так как API возвращает `null` когда зарплата не указана. hh.ru вернул 403 без auth-токена — добавил `hh/generator.go` с реалистичным генератором mock-вакансий и флаг `--mock` с автоматическим fallback при ошибке API.

### Промпт 3 — etcd-координатор для распределения шардов

**Инструмент:** Claude Code

"Реализуй etcd-координатор для распределения шардов между несколькими экземплярами Go-сборщика. Шард = (area_id, query). Нужен TryLock через concurrency.Mutex — если шард уже занят другим воркером, пропускаем. Lease на 30 секунд. Метод RegisterWorker для мониторинга активных воркеров."

Результат:  
`etcd/coordinator.go` с `concurrency.NewSession(ttl=30)` и `TryLock` — при `ErrLocked` воркер молча пропускает шард, не падает. Владелец шарда записывается в etcd с `WithLease` — автоматически освобождается при падении процесса. 6 шардов в `main.go`: Москва×2, СПб×2, Екатеринбург, Новосибирск — по двум запросам ("разработчик", "data engineer"). При двух воркерах распределяются примерно поровну.

### Итого
- Количество промптов: 3
- Что исправлял вручную: добавил fallback-генератор mock-вакансий после 403 от hh.ru
- Время: ~45 мин

---

## Задание 2: Оконная агрегация (tumbling window)

### Промпт 1 — Реализация TumblingWindow

**Инструмент:** Claude Code

"Напиши package window с TumblingWindow. Горутина с тикером каждые N секунд делает flush буфера — агрегирует накопленные вакансии и отправляет результат в канал. Агрегация: count по регионам, avg/min/max зарплата, топ-10 навыков из snippet.requirement. Навыки — простой поиск подстрок без regexp. При Stop() — дофлашить остаток."

Результат:  
`window/window.go` — `TumblingWindow` с mutex-защищённым буфером, ticker в горутине. `flush()` выгребает срез и атомарно сбрасывает буфер — нет race condition между `Add()` и `flush()`. `topN` реализован insertion sort-ом (достаточно для 20–30 элементов).

Баг: `byArea` инициализировался как `map[string]*salaryAccum`, но тип значений в структуре — `*areaAccum`. Несовпадение типов; компилятор не поймал, упало в рантайме.

Исправление:
```go
// было
byArea[v.Area.Name] = &areaAccum{salary: acc}  // acc — *salaryAccum
// стало
acc = &areaAccum{salary: &salaryAccum{}}
byArea[v.Area.Name] = acc
```

### Итого
- Количество промптов: 1
- Что исправлял вручную: type mismatch в `map[string]*areaAccum`
- Время: ~20 мин

---

## Задание 3: Передача данных через Apache Arrow Flight RPC

### Промпт 1 — Arrow Flight сервер на Go

**Инструмент:** Claude Code

"Напиши Arrow Flight сервер на Go (apache/arrow/go/v17). Схема RecordBatch: window_start, window_end (string), total_count, area, area_count (int64), avg_salary_from, avg_salary_to (float64), top_skill (string), skill_count (int64). DoGet читает из канала AggregatedWindow и пишет батчи через NewRecordWriter."

Результат:  
Первая версия упала на компиляции — использовал несуществующие `flight.DataBody` и `flight.NewDataWriter`. Правильный API после изучения документации:
```go
writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
defer writer.Close()
writer.Write(rec)
```
Сервер регистрируется через `flight.RegisterFlightServiceServer` на `grpc.Server`.

### Промпт 2 — Python Arrow Flight клиент

**Инструмент:** Claude Code

"Напиши Python-клиент для Arrow Flight сервера. Метод stream_windows() — итератор по Polars DataFrame (один DataFrame на батч). Метод fetch_all() — склеить всё в один DataFrame. Поддержка context manager."

Результат:  
`arrow_client.py` — `VacancyFlightClient` с ленивым подключением. `do_get` возвращает `FlightStreamReader`, каждый чанк конвертируется через `pl.from_arrow(chunk)` без материализации всего потока. Клиент подключён к `main.py` через флаги `--arrow-host` / `--arrow-port`.

### Итого
- Количество промптов: 2
- Что исправлял вручную: несуществующий API (`flight.NewDataWriter` → `flight.NewRecordWriter`)
- Время: ~30 мин

---

## Задание 4: Rust-библиотека для валидации

### Промпт 1 — Rust-валидатор с C ABI и PyO3

**Инструмент:** Claude Code

"Напиши Rust-библиотеку vacancy_validator. Два интерфейса: (1) C ABI (#[no_mangle] extern \"C\") для cgo, CValidationResult: is_valid (bool), errors (**char), error_count (int); (2) PyO3-модуль (feature = \"python\") для Python, fn validate(name, salary_from, salary_to, area_id) → (bool, Vec<String>). Правила: имя 3–500 символов, salary_from >= 0, salary_from <= salary_to, salary_to <= 10M, area_id непустой. Rust unit-тесты для каждого правила."

Результат:  
`src/lib.rs` — `validate_vacancy_inner()` возвращает `Vec<String>`. C-обёртка управляет памятью вручную (`CString::into_raw` / `from_raw`). PyO3 под `#[cfg(feature = "python")]`. Go-интеграция разделена по build tags: `validator.go` (`//go:build !rust`) — чистый Go fallback, `validator_rust.go` (`//go:build rust`) — cgo-вызов.

Проблема: `pyo3 = { version = "0.21" }` не поддерживает Python 3.13 — ошибка сборки `pyo3-ffi`. Решение — сделать pyo3 optional dependency:
```toml
[dependencies]
pyo3 = { version = "0.21", optional = true }
[features]
python = ["pyo3/extension-module"]
```
После этого `cargo test` компилирует только чистый Rust без Python-биндинга. Все 6 тестов прошли.

### Итого
- Количество промптов: 1
- Что исправлял вручную: pyo3 optional feature из-за несовместимости с Python 3.13
- Время: ~25 мин

---

## Задание 5: Docker Compose и Kubernetes (minikube)

### Промпт 1 — Docker Compose с двумя коллекторами

**Инструмент:** Claude Code

"Docker Compose: etcd (bitnami/etcd:3.5) + два экземпляра Go-сборщика с разными WORKER_ID + Python-анализатор (profile analyze, запускать вручную) + Streamlit дашборд. Общий named volume для JSONL-файлов между сборщиками и дашбордом. Healthcheck для etcd, depends_on с condition: service_healthy."

Результат:  
`docker-compose.yml` — два коллектора (`worker-1`, `worker-2`) на портах 50051 и 50052, оба монтируют `vacancy-data:/data`. Анализатор в профиле `analyze` — не запускается автоматически. etcd с healthcheck через `etcdctl endpoint health`. `MOCK_MODE: "true"` в обоих коллекторах — не нужны лишние 403 от hh.ru API.

### Промпт 2 — Kubernetes-манифесты с HPA

**Инструмент:** Claude Code

"Kubernetes-манифесты для minikube. Namespace lab14. Deployment для коллектора — replicas: 2, WORKER_ID из metadata.name. HPA: minReplicas 2, maxReplicas 6, CPU target 60%, memory 75%. PVC для данных."

Результат:  
`k8s/collector.yaml` с `HorizontalPodAutoscaler` (autoscaling/v2), метрики CPU и memory. `fieldRef: metadata.name` — каждый под получает уникальный `WORKER_ID`, важно для distributed lock. `PersistentVolumeClaim vacancy-data-pvc` (1Gi, ReadWriteOnce) объявлен в том же файле. `imagePullPolicy: Never` для локальных образов в minikube.

### Итого
- Количество промптов: 2
- Что исправлял вручную: ничего
- Время: ~20 мин

---

## Задание 6: Streamlit-дашборд с авто-обновлением

### Промпт 1 — Дашборд в реальном времени

**Инструмент:** Claude Code

"Streamlit-дашборд для анализа вакансий. Данные из JSONL через load_data() с @st.cache_data(ttl=30). 4 метрики вверху, bar chart топ-15 регионов (horizontal), histogram зарплат + box plot по регионам (два столбца), bar chart топ навыков из snippet_requirement, таблица топ-10 работодателей, expander с сырыми данными. Sidebar: путь к данным, интервал обновления, кнопка 'обновить сейчас'. Авто-обновление через st.rerun()."

Результат:  
`dashboard/app.py` — нормализация struct-полей (`area.name`, `employer.name`, `snippet.requirement`) перед рендерингом. `st.cache_data.clear()` сбрасывает кэш перед `st.rerun()`. Авто-обновление: разбил `time.sleep(N)` на N итераций по 1 секунде с `st.empty()` счётчиком — пользователь видит обратный отсчёт, UI не замораживается на весь интервал.

### Итого
- Количество промптов: 1
- Что исправлял вручную: счётчик обратного отсчёта вместо глухого `time.sleep(N)`
- Время: ~20 мин

---

## Задания средней сложности 4–9: Python-анализатор

### Промпт 1 — analysis.py: Polars + DuckDB

**Инструмент:** Claude Code

"Python-модуль analysis.py. Функции: load_jsonl_files(dir) → pl.DataFrame через pl.read_ndjson; clean_data(df) — дедупликация по id, unnest struct-полей (salary, area, employer, snippet), привести к Int64, дропнуть пустые имена; aggregate_by_area(df) — топ регионов с avg/min/max salary; save_to_parquet(path). analyze_with_duckdb(path) — три запроса: top_areas, salary_distribution, percentile (PERCENTILE_CONT), замерять время каждого. compare_polars_vs_duckdb — одинаковый запрос в двух движках, вывести победителя."

Результат:  
Первая версия `clean_data` делала unnest только `salary`, остальные поля оставляла как struct. DuckDB-запросы падали: `Referenced column "area_name" not found` — в Parquet колонка лежала как struct.

Исправление — явный unnest всех struct-полей при очистке:
```python
if "area" in df.columns and df["area"].dtype == pl.Struct:
    df = df.with_columns(
        pl.col("area").struct.field("name").alias("area_name")
    ).drop("area")
```
Первый запрос DuckDB ~1500ms из-за инициализации движка, последующие — 3–5ms.

### Промпт 2 — pytest-тесты для analysis.py

**Инструмент:** Claude Code

"Напиши pytest-тесты для всех функций analysis.py. Фикстура sample_jsonl — создаёт tmp_path с JSONL-файлом из 5 вакансий (один дубликат по id). Тесты: загрузка, пустая директория, удаление дубликатов, агрегация по регионам, по работодателям, запись и чтение Parquet, DuckDB-анализ, проверка avg_salary."

Результат:  
8 тестов. Одна ошибка: `results["top_areas"]["df"].empty` — атрибут Pandas, в Polars нужно `df.is_empty()`. После замены — все зелёные.

### Итого
- Количество промптов: 2
- Что исправлял вручную: struct unnesting в `clean_data`; `.empty` → `.is_empty()` в тестах
- Время: ~35 мин

---

## Код-ревью и исправление найденных проблем

### Промпт 1 — Системный код-ревью

**Инструмент:** Claude Code

"Сделай полный код-ревью репозитория. Пройдись по каждому из 6 заданий повышенной сложности и сравни реализацию с требованиями из PDF. Для каждой проблемы укажи severity (critical / important / minor), конкретный файл и строку, что именно сломано и почему."

Результат:  
Найдено 8 проблем. Критические: (1) Dockerfile использует exec form CMD — строки `${ETCD_ENDPOINTS:-localhost:2379}` передаются буквально в программу, env vars из Docker/K8s игнорируются; (2) `main.go` не читает `os.Getenv` — K8s env-конфиг (`ETCD_ENDPOINTS`, `WORKER_ID` и др.) полностью игнорировался; (3) `k8s/collector.yaml` ссылается на `vacancy-data-pvc`, `PersistentVolumeClaim` нигде не объявлен — `kubectl apply` падал. Важные: `DoGet` итерировал по channel через `for range` — канал закрывается только при `Stop()`, `fetch_all()` на Python-стороне зависал навсегда; `arrow_client.py` написан корректно, но нигде не вызывается. Minor: нет `MOCK_MODE` в Docker Compose; `time.sleep(N)` блокирует весь Streamlit UI; `splitComma` дублирует `strings.Split`; тихий дроп агрегации без лога.

### Промпт 2 — Исправление всех найденных проблем

**Инструмент:** Claude Code

"Исправь все найденные проблемы."

Результат:  
Все 8 проблем исправлены. `main.go`: добавлены `envOr` / `envOrInt` / `envOrBool` — флаги используют env vars как defaults (`etcdEndpoints := flag.String("etcd", envOr("ETCD_ENDPOINTS", "localhost:2379"), "...")`). Dockerfile: убран exec form CMD, `ENTRYPOINT ["./collector"]` — конфиг через env. `k8s/collector.yaml`: добавлен `PersistentVolumeClaim vacancy-data-pvc` (1Gi, ReadWriteOnce). `arrow/server.go`: `DoGet` переписан на `select` с тремя ветками — `ctx.Done()`, новое окно, 5-секундный idle-таймер; `fetch_all()` возвращается через 5с после последнего окна. `main.py`: добавлены `--arrow-host` / `--arrow-port`, при наличии хоста — `client.fetch_all()`. `docker-compose.yml`: `MOCK_MODE: "true"` в оба коллектора. `dashboard/app.py`: `time.sleep(N)` → N итераций по 1с с `st.empty()` счётчиком. `window.go`: добавлен `log.Printf` при дропе агрегации. `main.go`: `splitComma` удалён, заменён `strings.Split` с inline-фильтром. `go build ./...` и `go test ./...` (10/10) — всё зелёное.

### Итого
- Количество промптов: 2
- Что исправлял вручную: ничего — все исправления применял агент
- Время: ~30 мин

---

## Оценка производительности конвейера

### Промпт 1 — Модуль benchmark.py с полными замерами

**Инструмент:** Claude Code

"Добавь модуль оценки производительности конвейера — обязательное требование для повышенного уровня. Нужно замерить: время выполнения каждого этапа (мс), потребление памяти RSS процесса (МБ) через psutil до и после каждого этапа, объём данных (JSONL vs Parquet — размер файлов и коэффициент сжатия, оценка размера Arrow Flight батча). Сравнить Polars vs DuckDB на трёх идентичных запросах: GROUP BY COUNT, AVG/MIN/MAX salary, PERCENTILE_CONT. Вывести сводную таблицу. Подключить как флаг --benchmark в main.py."

Результат:  
`benchmark.py` — dataclass `StageResult` (name, time_ms, mem_before/after_mb, rows_in/out), вспомогательная `_run()` оборачивает любую функцию и замеряет время + RSS. Три сценария Polars vs DuckDB запускаются на одном `duckdb.connect()` для честного сравнения (первый запрос DuckDB включает инициализацию движка). Отчёт на реальных данных (1200 вакансий, 40 JSONL-файлов):

```
Этап                               Время(мс)   ΔRSS(МБ)   Строк
─────────────────────────────────────────────────────────────────
Загрузка JSONL (40 файлов)             47.4       +2.6      1200
Очистка данных (clean_data)             2.1       +0.8      1200
Агрегация по регионам (Polars)          0.6       +0.1         4
Запись Parquet                          2.3       +1.1      1200
DuckDB: топ регионов                   13.3       +0.5         4
DuckDB: salary_distribution             2.5       +0.4         3
DuckDB: PERCENTILE_CONT                 2.8       -0.3         4
─────────────────────────────────────────────────────────────────
Итого                                  71.1

JSONL-файлы: 0.64 МБ → Parquet: 0.02 МБ → сжатие 36.5×
Arrow Flight (оценка ~100 б/строку): 0.114 МБ

Polars vs DuckDB:
  GROUP BY COUNT        → DuckDB  ×1.1
  AVG/MIN/MAX salary    → Polars  ×1.2
  PERCENTILE_CONT       → Polars  ×3.2
```

Polars быстрее на in-memory агрегации; DuckDB даёт преимущество на сложных SQL-запросах к Parquet-файлам на диске.

### Итого
- Количество промптов: 1
- Что исправлял вручную: порядок вычисления размера файла в note (до записи → после записи)
- Время: ~20 мин

---

## Интеграционные тесты: Arrow Flight и etcd-координатор

### Промпт 1 — Тесты Arrow Flight сервера

**Инструмент:** Claude Code

"Код-ревью показал, что нет тестов для Arrow Flight сервера и etcd-координатора. Добавь их. Для Arrow Flight — полноценный in-process тест: стартуем сервер на случайном порту, отправляем `AggregatedWindow` через канал, подключаем gRPC-клиент и проверяем, что данные дошли. Для etcd — интеграционный тест с тегом `integration`, требует реальный etcd на localhost:2379, проверяет distributed lock: два воркера не могут захватить один шард."

Результат:  
Добавлен `ServeOnListener(lis net.Listener, windowCh <-chan AggregatedWindow) *grpc.Server` в `arrow/server.go` — запускает gRPC без блокировки, возвращает `*grpc.Server` для `t.Cleanup(srv.GracefulStop)`. Тесты в `src/collector/arrow/server_test.go` (пакет `arrow_test`, тот же модуль — все зависимости доступны без изменений `go.mod`). Три сценария:

- `TestFlightServer_DoGet` — один `AggregatedWindow` с двумя регионами, проверяет ненулевое число DataBody-фреймов.
- `TestFlightServer_EmptyChannel` — пустой канал, стрим завершается idle-timeout'ом (5 с), 0 фреймов.
- `TestFlightServer_MultipleWindows` — 3 окна последовательно, проверяет ≥3 фрейма.

Проблема на стенде: 32-bit Go (386) — `flight.NewRecordReader` вызывает `Retain()` c `Xadd64` на невыровненном адресе → `panic: unaligned 64-bit atomic operation`. Решение: читать сырые сообщения через `stream.Recv()` в цикле, считать фреймы по `len(msg.DataBody) > 0`. Все три теста зелёные: `ok lab14/collector/arrow 5.4s`.

### Промпт 2 — Интеграционные тесты etcd-координатора

**Инструмент:** Claude Code

"Добавь интеграционные тесты для `etcd/coordinator.go`. Тег `//go:build integration` — не запускаются при обычном `go test`, только с `-tags integration`. Запуск предполагает `docker run bitnami/etcd:3.5`. Три теста: (1) два воркера — первый захватывает шард, второй получает `false` (ErrLocked); (2) один воркер захватывает два разных шарда независимо; (3) `RegisterWorker` + `ListWorkers` — зарегистрированный воркер присутствует в списке."

Результат:  
`src/collector/etcd/coordinator_test.go` с `//go:build integration`. Вспомогательная функция `newTestCoordinator` вызывает `t.Skipf` при недоступном etcd — тест пропускается с понятным сообщением вместо `FAIL`. Используется уникальные `ID: 999/991/992` чтобы не конфликтовать с реальными шардами. `t.Cleanup(coord.Close)` обеспечивает освобождение etcd-сессии с TTL 30 с. Запуск: `go test -tags integration ./etcd/... -v`.

### Итого
- Количество промптов: 2
- Что исправлял вручную: заменил `flight.NewRecordReader` на `stream.Recv()` из-за 32-bit Go (386) — несовместимость Arrow с `Xadd64`
- Время: ~25 мин

---

## Задание 7: NATS — потоковая передача и Python скользящее окно

### Промпт 1 — Исправление замечания проверяющего

**Инструмент:** Claude Code

"Работа не принята. Критический дефект: задание 7 — потоковая передача через Kafka/NATS отсутствует полностью. Требование: Go пишет в топик, Python читает и обрабатывает в реальном времени со скользящим окном. Реализуй NATS: Go публикует каждую вакансию в топик vacancies, Python-консьюмер читает и поддерживает скользящее окно. Добавь NATS в docker-compose."

Результат:  
Выбран NATS (не Kafka) — проще в конфигурации, нет ZooKeeper, нативная поддержка `nats.go` v1.52 и `nats-py` v2.6. Реализовано четыре компонента:

`src/collector/nats/publisher.go` — `Publisher` с `nats.Connect(MaxReconnects=10, ReconnectWait=2s)`. Метод `Publish(v hh.Vacancy)` — `json.Marshal` + `nc.Publish`. `Close()` вызывает `nc.Drain()` перед закрытием — гарантирует доставку буферизованных сообщений.

`src/collector/main.go` — добавлен флаг `--nats`/`NATS_URL`; если задан — создаётся `Publisher`, при ошибке подключения логируется предупреждение и работа продолжается без NATS. В `collectShard` после валидации: `pub.Publish(v)`.

`src/analyzer/nats_consumer.py` — `SlidingWindowConsumer` использует `asyncio` + `nats-py`. Буфер — `deque` пар `(monotonic_timestamp, vacancy_dict)`. `_evict_old()` удаляет события старше `window_sec` секунд. Каждые `report_interval` секунд выводит: total в окне, топ-5 регионов, среднюю зарплату. Ключевое отличие от tumbling window: буфер не сбрасывается — старые события вытесняются по мере прихода новых.

`docker-compose.yml` — добавлен `nats:2.10-alpine` (порты 4222 + 8222/мониторинг, healthcheck), сервис `nats-consumer` (entrypoint: `python nats_consumer.py`), оба коллектора получили `NATS_URL: "nats://nats:4222"`.

`src/analyzer/requirements.txt` — `nats-py>=2.6.0`.

`go build ./... && go test ./arrow/...` — чисто. Пример вывода консьюмера:

```
[sliding window 60s]  total=312  top=Москва(148)  avg_salary_from=167 400 ₽
  Москва                    148 вакансий
  Санкт-Петербург            89 вакансий
  Екатеринбург               43 вакансий
  Новосибирск                32 вакансий
```

### Итого
- Количество промптов: 1
- Что исправлял вручную: ничего — всё реализовано агентом
- Время: ~30 мин

---

## Исправление dashboard/Dockerfile (финальный код-ревью)

### Промпт 1 — Баг: COPY за пределами build context

**Инструмент:** Claude Code

"Финальный код-ревью выявил: dashboard/Dockerfile содержит `COPY ../analyzer/requirements.txt .` — Docker запрещает выходить за пределы build context. При docker-compose up дашборд не соберётся. Исправь."

Результат:  
В `docker-compose.yml` build context дашборда расширен с `./src/dashboard` до `./src`, добавлен `dockerfile: dashboard/Dockerfile`. В `src/dashboard/Dockerfile` пути исправлены: `COPY analyzer/requirements.txt .`, `COPY dashboard/ .`, `COPY analyzer/analysis.py /analyzer/analysis.py` — в `/analyzer/` потому что `app.py` добавляет именно этот путь в `sys.path`.

### Итого
- Количество промптов: 1
- Что исправлял вручную: ничего
- Время: ~5 мин

---

## Краткое резюме

**Общая статистика:** 21 промпт, незначительные ручные правки (type mismatch в window.go, struct unnesting в clean_data, note с размером файла в бенчмарке, адаптация Arrow-теста под 32-bit Go), ~6.5 часов работы.  
Система включает: Go-сборщик с etcd + tumbling window + Arrow Flight RPC + NATS publisher → Python NATS sliding window consumer + Rust-валидатор (cgo + PyO3) + Python-анализатор (Polars + DuckDB) + Streamlit дашборд, всё в Docker Compose и Kubernetes.  
End-to-end pipeline: 1200 вакансий за один прогон, Parquet в 36.5× меньше JSONL, конвейер 71мс. Тесты: `go test arrow: 3/3`, `tests/collector: 10/10`, `pytest: 8/8`, `cargo test: 6/6`.
