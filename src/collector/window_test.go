package collector_test

import (
	"testing"
	"time"

	"lab14/collector/hh"
	"lab14/collector/window"
)

func makeVacancy(name, area, areaID string, salaryFrom, salaryTo int) hh.Vacancy {
	v := hh.Vacancy{
		ID:          "test-" + name,
		Name:        name,
		Area:        hh.Area{ID: areaID, Name: area},
		CollectedAt: time.Now(),
	}
	if salaryFrom > 0 || salaryTo > 0 {
		s := &hh.Salary{Currency: "RUR"}
		if salaryFrom > 0 {
			s.From = &salaryFrom
		}
		if salaryTo > 0 {
			s.To = &salaryTo
		}
		v.Salary = s
	}
	return v
}

func TestTumblingWindow_Aggregate(t *testing.T) {
	win := window.NewTumblingWindow(200 * time.Millisecond)
	win.Start()
	defer win.Stop()

	win.Add(makeVacancy("Go Developer", "Москва", "1", 150000, 250000))
	win.Add(makeVacancy("Python Engineer", "Москва", "1", 120000, 200000))
	win.Add(makeVacancy("Backend Dev", "СПб", "2", 100000, 180000))

	var agg window.AggregatedWindow
	select {
	case agg = <-win.Out():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("window did not emit within timeout")
	}

	if agg.TotalCount != 3 {
		t.Errorf("expected TotalCount=3, got %d", agg.TotalCount)
	}
	if _, ok := agg.ByArea["Москва"]; !ok {
		t.Error("expected Москва in ByArea")
	}
	if agg.ByArea["Москва"].Count != 2 {
		t.Errorf("expected 2 vacancies in Москва, got %d", agg.ByArea["Москва"].Count)
	}
}

func TestTumblingWindow_EmptyFlush(t *testing.T) {
	win := window.NewTumblingWindow(150 * time.Millisecond)
	win.Start()
	defer win.Stop()

	// Не добавляем вакансий — окно не должно выдавать ничего
	select {
	case <-win.Out():
		t.Error("expected no output for empty window")
	case <-time.After(300 * time.Millisecond):
		// OK
	}
}

func TestTumblingWindow_SalaryAggregation(t *testing.T) {
	win := window.NewTumblingWindow(150 * time.Millisecond)
	win.Start()
	defer win.Stop()

	win.Add(makeVacancy("Dev 1", "Москва", "1", 100000, 200000))
	win.Add(makeVacancy("Dev 2", "Москва", "1", 200000, 300000))

	var agg window.AggregatedWindow
	select {
	case agg = <-win.Out():
	case <-time.After(400 * time.Millisecond):
		t.Fatal("no window output")
	}

	moscow := agg.ByArea["Москва"]
	if moscow.SalaryStats.WithSalary != 2 {
		t.Errorf("expected 2 with salary, got %d", moscow.SalaryStats.WithSalary)
	}
	// avg from = (100000+200000)/2 = 150000
	if moscow.SalaryStats.AvgFrom != 150000 {
		t.Errorf("expected avg_from=150000, got %.0f", moscow.SalaryStats.AvgFrom)
	}
}
