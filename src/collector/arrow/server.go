package arrow

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/grpc"

	"lab14/collector/window"
)

// FlightServer — Apache Arrow Flight RPC сервер для передачи агрегированных данных.
type FlightServer struct {
	flight.BaseFlightServer
	windows <-chan window.AggregatedWindow
}

func NewFlightServer(windows <-chan window.AggregatedWindow) *FlightServer {
	return &FlightServer{windows: windows}
}

var windowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "window_start", Type: arrow.BinaryTypes.String},
	{Name: "window_end", Type: arrow.BinaryTypes.String},
	{Name: "total_count", Type: arrow.PrimitiveTypes.Int64},
	{Name: "area", Type: arrow.BinaryTypes.String},
	{Name: "area_count", Type: arrow.PrimitiveTypes.Int64},
	{Name: "avg_salary_from", Type: arrow.PrimitiveTypes.Float64},
	{Name: "avg_salary_to", Type: arrow.PrimitiveTypes.Float64},
	{Name: "top_skill", Type: arrow.BinaryTypes.String},
	{Name: "skill_count", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// idleTimeout — сколько ждать следующего окна прежде чем завершить стрим.
// Позволяет Python-клиенту получить накопленные окна и завершить fetch_all().
const idleTimeout = 5 * time.Second

func (s *FlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	alloc := memory.NewGoAllocator()
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(windowSchema), ipc.WithAllocator(alloc))
	defer writer.Close()

	ctx := stream.Context()
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case w, ok := <-s.windows:
			if !ok {
				return nil
			}
			idle.Reset(idleTimeout)
			rec := buildRecord(alloc, w)
			if err := writer.Write(rec); err != nil {
				rec.Release()
				return fmt.Errorf("write record: %w", err)
			}
			rec.Release()
		case <-idle.C:
			// Нет новых окон — завершаем стрим, клиент получит все данные.
			return nil
		}
	}
}

// buildRecord строит Arrow Record из агрегированного окна.
func buildRecord(alloc memory.Allocator, w window.AggregatedWindow) arrow.Record {
	b := array.NewRecordBuilder(alloc, windowSchema)
	defer b.Release()

	windowStart := w.WindowStart.Format("2006-01-02T15:04:05Z")
	windowEnd := w.WindowEnd.Format("2006-01-02T15:04:05Z")

	topSkill := ""
	topSkillCount := int64(0)
	if len(w.TopSkills) > 0 {
		topSkill = w.TopSkills[0].Skill
		topSkillCount = int64(w.TopSkills[0].Count)
	}

	for areaName, stats := range w.ByArea {
		b.Field(0).(*array.StringBuilder).Append(windowStart)
		b.Field(1).(*array.StringBuilder).Append(windowEnd)
		b.Field(2).(*array.Int64Builder).Append(int64(w.TotalCount))
		b.Field(3).(*array.StringBuilder).Append(areaName)
		b.Field(4).(*array.Int64Builder).Append(int64(stats.Count))
		b.Field(5).(*array.Float64Builder).Append(stats.SalaryStats.AvgFrom)
		b.Field(6).(*array.Float64Builder).Append(stats.SalaryStats.AvgTo)
		b.Field(7).(*array.StringBuilder).Append(topSkill)
		b.Field(8).(*array.Int64Builder).Append(topSkillCount)
	}

	return b.NewRecord()
}

// StartFlightServer запускает gRPC Arrow Flight сервер.
func StartFlightServer(addr string, windows <-chan window.AggregatedWindow) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv := NewFlightServer(windows)
	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, srv)

	log.Printf("[arrow] Flight server listening on %s", addr)
	return grpcSrv.Serve(lis)
}
