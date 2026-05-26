package arrow_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	arrowsrv "lab14/collector/arrow"
	"lab14/collector/window"
)

// startTestServer запускает Flight сервер на случайном TCP-порту и возвращает адрес.
func startTestServer(t *testing.T, windowCh chan window.AggregatedWindow) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := arrowsrv.ServeOnListener(lis, windowCh)
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// dialFlightClient подключает gRPC Flight клиент к тестовому серверу.
func dialFlightClient(t *testing.T, addr string) flight.FlightServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return flight.NewFlightServiceClient(conn)
}

// recvMessages читает все FlightData-сообщения из стрима DoGet и возвращает количество
// сообщений с данными (пропуская schema-сообщение без DataBody).
// Использует прямой Recv() вместо flight.NewRecordReader — совместимо с 32-bit платформами.
func recvMessages(t *testing.T, stream flight.FlightService_DoGetClient) int {
	t.Helper()
	var count int
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logf("stream.Recv завершён: %v", err)
			break
		}
		if len(msg.DataBody) > 0 {
			count++
		}
	}
	return count
}

// TestFlightServer_DoGet проверяет, что сервер отдаёт записи и завершает стрим по idle timeout.
func TestFlightServer_DoGet(t *testing.T) {
	windowCh := make(chan window.AggregatedWindow, 4)
	addr := startTestServer(t, windowCh)
	time.Sleep(30 * time.Millisecond)

	windowCh <- window.AggregatedWindow{
		WindowStart: time.Now().Add(-time.Minute),
		WindowEnd:   time.Now(),
		TotalCount:  42,
		ByArea: map[string]window.AreaStats{
			"Москва":          {Count: 30},
			"Санкт-Петербург": {Count: 12},
		},
	}
	close(windowCh)

	client := dialFlightClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte("vacancies")})
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}

	// Ожидаем: schema-сообщение + минимум 1 сообщение с данными
	msgCount := recvMessages(t, stream)
	if msgCount == 0 {
		t.Errorf("ожидался хотя бы 1 DataBody-фрейм, получено 0")
	}
}

// TestFlightServer_EmptyChannel проверяет, что при пустом канале сервер завершает стрим
// по idle timeout (5 с) без паники и без передачи данных.
func TestFlightServer_EmptyChannel(t *testing.T) {
	windowCh := make(chan window.AggregatedWindow, 4)
	addr := startTestServer(t, windowCh)
	time.Sleep(30 * time.Millisecond)
	// Канал не закрываем — сервер должен завершить стрим по idle timeout

	client := dialFlightClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte("vacancies")})
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}

	msgCount := recvMessages(t, stream)
	if msgCount != 0 {
		t.Errorf("ожидалось 0 DataBody-фреймов для пустого канала, получено %d", msgCount)
	}
}

// TestFlightServer_MultipleWindows проверяет передачу нескольких окон подряд.
func TestFlightServer_MultipleWindows(t *testing.T) {
	const numWindows = 3

	windowCh := make(chan window.AggregatedWindow, 8)
	addr := startTestServer(t, windowCh)
	time.Sleep(30 * time.Millisecond)

	for i := range numWindows {
		windowCh <- window.AggregatedWindow{
			WindowStart: time.Now().Add(-time.Duration(i+1) * time.Minute),
			WindowEnd:   time.Now().Add(-time.Duration(i) * time.Minute),
			TotalCount:  10 * (i + 1),
			ByArea:      map[string]window.AreaStats{"Москва": {Count: 10 * (i + 1)}},
		}
	}
	close(windowCh)

	client := dialFlightClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte("vacancies")})
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}

	msgCount := recvMessages(t, stream)
	if msgCount < numWindows {
		t.Errorf("ожидалось минимум %d фреймов для %d окон, получено %d", numWindows, numWindows, msgCount)
	}
}
