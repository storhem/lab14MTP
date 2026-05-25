FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0: Rust-валидатор не встраивается в Docker-образ (нет Rust toolchain).
# В контейнере используется чистая Go-реализация валидатора (validator/validator.go).
# Для локальной сборки с Rust: go build -tags rust
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o collector .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/collector .
RUN mkdir -p /data

EXPOSE 50051

# Конфигурация через переменные окружения (читаются в main.go):
#   ETCD_ENDPOINTS  — адрес etcd (default: localhost:2379)
#   WORKER_ID       — уникальный ID воркера (default: hostname)
#   FLIGHT_ADDR     — адрес Arrow Flight сервера (default: :50051)
#   OUTPUT_DIR      — директория для JSONL файлов (default: /data)
#   WINDOW_SEC      — длина tumbling window в секундах (default: 60)
#   BATCH_SIZE      — размер батча перед записью (default: 50)
#   MOCK_MODE       — true/1 для генерации mock данных (default: false)
ENTRYPOINT ["./collector"]
