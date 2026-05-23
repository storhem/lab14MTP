package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	arrowsrv "lab14/collector/arrow"
	etcdcoord "lab14/collector/etcd"
	"lab14/collector/hh"
	"lab14/collector/validator"
	"lab14/collector/window"
)

// Шарды (регион + запрос) — распределяются между экземплярами сборщика.
var shards = []etcdcoord.Shard{
	{ID: 1, AreaID: "1", AreaName: "Москва", Query: "разработчик"},
	{ID: 2, AreaID: "2", AreaName: "Санкт-Петербург", Query: "разработчик"},
	{ID: 3, AreaID: "3", AreaName: "Екатеринбург", Query: "разработчик"},
	{ID: 4, AreaID: "66", AreaName: "Новосибирск", Query: "разработчик"},
	{ID: 5, AreaID: "1", AreaName: "Москва", Query: "data engineer"},
	{ID: 6, AreaID: "2", AreaName: "Санкт-Петербург", Query: "data engineer"},
}

func main() {
	etcdEndpoints := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	workerID := flag.String("worker", "", "worker ID (default: hostname)")
	flightAddr := flag.String("flight", ":50051", "Arrow Flight listen address")
	outputDir := flag.String("output", "./data", "output directory for JSON files")
	windowSec := flag.Int("window", 60, "tumbling window duration in seconds")
	batchSize := flag.Int("batch", 50, "batch size before writing to file")
	flag.Parse()

	if *workerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = fmt.Sprintf("worker-%d", os.Getpid())
		}
		*workerID = hostname
	}

	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown по сигналу
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Подключаемся к etcd
	coord, err := etcdcoord.NewCoordinator(
		splitComma(*etcdEndpoints),
		*workerID,
	)
	if err != nil {
		log.Fatalf("etcd coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.RegisterWorker(ctx); err != nil {
		log.Printf("[warn] register worker: %v", err)
	}

	// Tumbling window для агрегации
	win := window.NewTumblingWindow(time.Duration(*windowSec) * time.Second)
	win.Start()

	// Arrow Flight сервер для Python-клиента
	windowCh := make(chan window.AggregatedWindow, 32)
	go func() {
		for agg := range win.Out() {
			select {
			case windowCh <- agg:
			default:
				log.Printf("[warn] window channel full, dropping aggregation")
			}
		}
	}()
	go func() {
		if err := arrowsrv.StartFlightServer(*flightAddr, windowCh); err != nil {
			log.Printf("[arrow] server error: %v", err)
		}
	}()

	// Канал вакансий с буфером
	vacancyCh := make(chan hh.Vacancy, 500)

	// Горутина пакетной записи в JSON
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		writeBatches(ctx, vacancyCh, *outputDir, *batchSize)
	}()

	// Сборщик данных — пробуем получить шарды и собирать данные
	client := hh.NewClient()
	var collectWg sync.WaitGroup

	for _, shard := range shards {
		acquired, err := coord.AcquireShard(ctx, shard)
		if err != nil {
			log.Printf("[warn] acquire shard %d: %v", shard.ID, err)
			continue
		}
		if !acquired {
			log.Printf("[coordinator] shard %d already owned by another worker, skipping", shard.ID)
			continue
		}

		collectWg.Add(1)
		go func(s etcdcoord.Shard) {
			defer collectWg.Done()
			collectShard(ctx, client, s, vacancyCh, win)
		}(shard)
	}

	// Ждём сигнала завершения
	select {
	case sig := <-sigCh:
		log.Printf("[main] received signal %v, shutting down...", sig)
	case <-ctx.Done():
	}

	cancel()
	collectWg.Wait()
	close(vacancyCh)
	writerWg.Wait()
	win.Stop()

	log.Println("[main] shutdown complete")
}

// collectShard непрерывно собирает вакансии по заданному шарду.
func collectShard(ctx context.Context, client *hh.Client, shard etcdcoord.Shard, out chan<- hh.Vacancy, win *window.TumblingWindow) {
	log.Printf("[collector] shard=%d area=%s query=%q — started", shard.ID, shard.AreaName, shard.Query)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	collect := func() {
		resp, err := client.SearchVacancies(hh.SearchParams{
			Text:    shard.Query,
			AreaID:  shard.AreaID,
			PerPage: 100,
		})
		if err != nil {
			log.Printf("[collector] shard=%d error: %v", shard.ID, err)
			return
		}

		validated := 0
		for _, v := range resp.Items {
			salaryFrom, salaryTo := 0, 0
			if v.Salary != nil {
				if v.Salary.From != nil {
					salaryFrom = *v.Salary.From
				}
				if v.Salary.To != nil {
					salaryTo = *v.Salary.To
				}
			}
			res := validator.Validate(v.Name, salaryFrom, salaryTo, v.Area.ID)
			if !res.Valid {
				continue
			}
			validated++
			win.Add(v)
			select {
			case out <- v:
			case <-ctx.Done():
				return
			}
		}
		log.Printf("[collector] shard=%d found=%d validated=%d", shard.ID, resp.Found, validated)
	}

	collect()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collect()
		}
	}
}

// writeBatches получает вакансии из канала и пакетно записывает в JSON-файлы.
func writeBatches(ctx context.Context, ch <-chan hh.Vacancy, dir string, batchSize int) {
	batch := make([]hh.Vacancy, 0, batchSize)
	batchNum := 0

	flush := func() {
		if len(batch) == 0 {
			return
		}
		filename := fmt.Sprintf("%s/vacancies_%d_%d.jsonl",
			dir, time.Now().Unix(), batchNum)
		f, err := os.Create(filename)
		if err != nil {
			log.Printf("[writer] create file: %v", err)
			return
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, v := range batch {
			if err := enc.Encode(v); err != nil {
				log.Printf("[writer] encode: %v", err)
			}
		}
		log.Printf("[writer] wrote %d vacancies to %s", len(batch), filename)
		batchNum++
		batch = batch[:0]
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case v, ok := <-ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, v)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

func splitComma(s string) []string {
	result := []string{}
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if part := s[start:i]; part != "" {
				result = append(result, part)
			}
			start = i + 1
		}
	}
	return result
}
