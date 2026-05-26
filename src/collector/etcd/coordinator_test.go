//go:build integration

// Интеграционные тесты etcd-координатора.
// Требуют работающий etcd на localhost:2379.
// Запуск:
//
//	docker run -d -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:3.5
//	go test -tags integration ./etcd/...
package etcd_test

import (
	"context"
	"strings"
	"testing"
	"time"

	etcdcoord "lab14/collector/etcd"
)

const testEtcdAddr = "localhost:2379"

func newTestCoordinator(t *testing.T, workerID string) *etcdcoord.Coordinator {
	t.Helper()
	coord, err := etcdcoord.NewCoordinator([]string{testEtcdAddr}, workerID)
	if err != nil {
		t.Skipf("etcd недоступен (%s): %v — запустите etcd или пропустите -tags integration", testEtcdAddr, err)
	}
	t.Cleanup(coord.Close)
	return coord
}

// TestCoordinator_AcquireShard проверяет, что два воркера не могут захватить один шард.
func TestCoordinator_AcquireShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shard := etcdcoord.Shard{ID: 999, AreaID: "99", AreaName: "TestArea", Query: "test"}

	coord1 := newTestCoordinator(t, "test-worker-a")
	coord2 := newTestCoordinator(t, "test-worker-b")

	ok1, err := coord1.AcquireShard(ctx, shard)
	if err != nil {
		t.Fatalf("coord1.AcquireShard: %v", err)
	}
	if !ok1 {
		t.Fatal("первый воркер должен захватить шард")
	}

	ok2, err := coord2.AcquireShard(ctx, shard)
	if err != nil {
		t.Fatalf("coord2.AcquireShard: %v", err)
	}
	if ok2 {
		t.Fatal("второй воркер не должен захватить уже занятый шард")
	}
}

// TestCoordinator_ShardsAreIndependent проверяет, что разные шарды захватываются независимо.
func TestCoordinator_ShardsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shard1 := etcdcoord.Shard{ID: 991, AreaID: "1", AreaName: "A", Query: "go"}
	shard2 := etcdcoord.Shard{ID: 992, AreaID: "2", AreaName: "B", Query: "go"}

	coord := newTestCoordinator(t, "test-worker-indep")

	ok1, err := coord.AcquireShard(ctx, shard1)
	if err != nil || !ok1 {
		t.Fatalf("должен захватить shard1: ok=%v err=%v", ok1, err)
	}

	ok2, err := coord.AcquireShard(ctx, shard2)
	if err != nil || !ok2 {
		t.Fatalf("должен захватить shard2: ok=%v err=%v", ok2, err)
	}
}

// TestCoordinator_RegisterAndListWorkers проверяет регистрацию воркера и чтение списка.
func TestCoordinator_RegisterAndListWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coord := newTestCoordinator(t, "test-worker-reg-list")

	if err := coord.RegisterWorker(ctx); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	workers, err := coord.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) == 0 {
		t.Fatal("ожидался хотя бы один воркер в списке")
	}

	found := false
	for _, w := range workers {
		if strings.Contains(w, "test-worker-reg-list") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("зарегистрированный воркер не найден в списке: %v", workers)
	}
}
