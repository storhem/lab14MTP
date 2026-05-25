FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o collector .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/collector .
RUN mkdir -p /data

EXPOSE 50051
ENTRYPOINT ["./collector"]
CMD ["--etcd", "${ETCD_ENDPOINTS:-localhost:2379}", \
     "--worker", "${WORKER_ID:-worker-1}", \
     "--flight", "${FLIGHT_ADDR:-:50051}", \
     "--output", "${OUTPUT_DIR:-/data}", \
     "--window", "${WINDOW_SEC:-60}", \
     "--batch", "${BATCH_SIZE:-50}"]
