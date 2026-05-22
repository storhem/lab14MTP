package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Shard описывает часть работы: регион + ключевое слово для поиска вакансий.
type Shard struct {
	ID      int    `json:"id"`
	AreaID  string `json:"area_id"`
	AreaName string `json:"area_name"`
	Query   string `json:"query"`
}

// Coordinator управляет распределением шардов между экземплярами сборщика.
type Coordinator struct {
	client   *clientv3.Client
	session  *concurrency.Session
	workerID string
	prefix   string
}

func NewCoordinator(endpoints []string, workerID string) (*Coordinator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd connect: %w", err)
	}

	session, err := concurrency.NewSession(client, concurrency.WithTTL(30))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("etcd session: %w", err)
	}

	return &Coordinator{
		client:   client,
		session:  session,
		workerID: workerID,
		prefix:   "/lab14/shards/",
	}, nil
}

// AcquireShard пытается получить эксклюзивный шард для этого воркера.
// Использует distributed lock — только один воркер держит шард одновременно.
func (c *Coordinator) AcquireShard(ctx context.Context, shard Shard) (bool, error) {
	lockKey := fmt.Sprintf("%s%d/lock", c.prefix, shard.ID)
	mu := concurrency.NewMutex(c.session, lockKey)

	if err := mu.TryLock(ctx); err != nil {
		if err == concurrency.ErrLocked {
			return false, nil
		}
		return false, fmt.Errorf("try lock shard %d: %w", shard.ID, err)
	}

	data, _ := json.Marshal(map[string]string{
		"worker": c.workerID,
		"shard":  fmt.Sprintf("%d", shard.ID),
	})
	statusKey := fmt.Sprintf("%s%d/owner", c.prefix, shard.ID)
	_, err := c.client.Put(ctx, statusKey, string(data), clientv3.WithLease(c.session.Lease()))
	if err != nil {
		_ = mu.Unlock(ctx)
		return false, err
	}

	log.Printf("[coordinator] worker=%s acquired shard=%d (%s)", c.workerID, shard.ID, shard.AreaName)
	return true, nil
}

// RegisterWorker регистрирует воркера в etcd (для мониторинга).
func (c *Coordinator) RegisterWorker(ctx context.Context) error {
	key := fmt.Sprintf("/lab14/workers/%s", c.workerID)
	data, _ := json.Marshal(map[string]string{
		"worker_id":  c.workerID,
		"started_at": time.Now().UTC().Format(time.RFC3339),
	})
	_, err := c.client.Put(ctx, key, string(data), clientv3.WithLease(c.session.Lease()))
	return err
}

// ListWorkers возвращает список активных воркеров.
func (c *Coordinator) ListWorkers(ctx context.Context) ([]string, error) {
	resp, err := c.client.Get(ctx, "/lab14/workers/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	workers := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		workers = append(workers, string(kv.Value))
	}
	return workers, nil
}

func (c *Coordinator) Close() {
	c.session.Close()
	c.client.Close()
}
