package window

import (
	"sync"
	"time"

	"lab14/collector/hh"
)

// AggregatedWindow — результат оконной агрегации за период.
type AggregatedWindow struct {
	WindowStart  time.Time         `json:"window_start"`
	WindowEnd    time.Time         `json:"window_end"`
	TotalCount   int               `json:"total_count"`
	ByArea       map[string]AreaStats `json:"by_area"`
	TopSkills    []SkillCount      `json:"top_skills"`
	SalaryStats  SalaryStats       `json:"salary_stats"`
}

type AreaStats struct {
	Count      int         `json:"count"`
	SalaryStats SalaryStats `json:"salary_stats"`
}

type SalaryStats struct {
	AvgFrom  float64 `json:"avg_from"`
	AvgTo    float64 `json:"avg_to"`
	MinFrom  int     `json:"min_from"`
	MaxTo    int     `json:"max_to"`
	WithSalary int   `json:"with_salary"`
}

type SkillCount struct {
	Skill string `json:"skill"`
	Count int    `json:"count"`
}

// TumblingWindow реализует tumbling window агрегацию.
// Каждые duration секунд накопленные вакансии агрегируются и отправляются в out.
type TumblingWindow struct {
	duration time.Duration
	mu       sync.Mutex
	buffer   []hh.Vacancy
	out      chan AggregatedWindow
	stop     chan struct{}
}

func NewTumblingWindow(duration time.Duration) *TumblingWindow {
	return &TumblingWindow{
		duration: duration,
		buffer:   make([]hh.Vacancy, 0, 256),
		out:      make(chan AggregatedWindow, 8),
		stop:     make(chan struct{}),
	}
}

func (w *TumblingWindow) Add(v hh.Vacancy) {
	w.mu.Lock()
	w.buffer = append(w.buffer, v)
	w.mu.Unlock()
}

func (w *TumblingWindow) Out() <-chan AggregatedWindow {
	return w.out
}

func (w *TumblingWindow) Start() {
	go func() {
		ticker := time.NewTicker(w.duration)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				w.flush(time.Now())
				close(w.out)
				return
			case t := <-ticker.C:
				w.flush(t)
			}
		}
	}()
}

func (w *TumblingWindow) Stop() {
	close(w.stop)
}

func (w *TumblingWindow) flush(t time.Time) {
	w.mu.Lock()
	vacancies := w.buffer
	w.buffer = make([]hh.Vacancy, 0, 256)
	w.mu.Unlock()

	if len(vacancies) == 0 {
		return
	}

	agg := aggregate(vacancies, t.Add(-w.duration), t)
	select {
	case w.out <- agg:
	default:
	}
}

func aggregate(vacancies []hh.Vacancy, start, end time.Time) AggregatedWindow {
	byArea := make(map[string]*areaAccum)
	skillCounter := make(map[string]int)

	globalSalary := &salaryAccum{}

	for _, v := range vacancies {
		acc, ok := byArea[v.Area.Name]
		if !ok {
			acc = &areaAccum{salary: &salaryAccum{}}
			byArea[v.Area.Name] = acc
		}
		acc.count++

		if v.Salary != nil {
			acc.salary.add(v.Salary)
			globalSalary.add(v.Salary)
		}

		// Извлекаем ключевые слова из требований (простая эвристика)
		extractKeywords(v.Snippet.Requirement, skillCounter)
	}

	areaStats := make(map[string]AreaStats, len(byArea))
	for name, acc := range byArea {
		areaStats[name] = AreaStats{
			Count:       acc.count,
			SalaryStats: acc.salary.stats(),
		}
	}

	return AggregatedWindow{
		WindowStart: start,
		WindowEnd:   end,
		TotalCount:  len(vacancies),
		ByArea:      areaStats,
		TopSkills:   topN(skillCounter, 10),
		SalaryStats: globalSalary.stats(),
	}
}

type areaAccum struct {
	count  int
	salary *salaryAccum
}

type salaryAccum struct {
	sumFrom    int
	sumTo      int
	minFrom    int
	maxTo      int
	countFrom  int
	countTo    int
	withSalary int
}

func (a *salaryAccum) add(s *hh.Salary) {
	a.withSalary++
	if s.From != nil {
		a.sumFrom += *s.From
		a.countFrom++
		if a.minFrom == 0 || *s.From < a.minFrom {
			a.minFrom = *s.From
		}
	}
	if s.To != nil {
		a.sumTo += *s.To
		a.countTo++
		if *s.To > a.maxTo {
			a.maxTo = *s.To
		}
	}
}

func (a *salaryAccum) stats() SalaryStats {
	s := SalaryStats{MinFrom: a.minFrom, MaxTo: a.maxTo, WithSalary: a.withSalary}
	if a.countFrom > 0 {
		s.AvgFrom = float64(a.sumFrom) / float64(a.countFrom)
	}
	if a.countTo > 0 {
		s.AvgTo = float64(a.sumTo) / float64(a.countTo)
	}
	return s
}

// Простое выделение технических навыков из текста требований.
var techKeywords = []string{
	"Python", "Go", "Golang", "Java", "JavaScript", "TypeScript", "React", "Vue",
	"SQL", "PostgreSQL", "MySQL", "MongoDB", "Redis", "Kafka", "Docker", "Kubernetes",
	"CI/CD", "Git", "Linux", "AWS", "GCP", "Azure", "REST", "gRPC", "Rust",
	"C++", "C#", ".NET", "PHP", "Ruby", "Swift", "Kotlin", "Scala",
}

func extractKeywords(text string, counter map[string]int) {
	for _, kw := range techKeywords {
		if containsIgnoreCase(text, kw) {
			counter[kw]++
		}
	}
}

func containsIgnoreCase(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	sLower := toLower(s)
	subLower := toLower(substr)
	for i := 0; i <= len(sLower)-len(subLower); i++ {
		if sLower[i:i+len(subLower)] == subLower {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		b[i] = c
	}
	return string(b)
}

func topN(counter map[string]int, n int) []SkillCount {
	skills := make([]SkillCount, 0, len(counter))
	for k, v := range counter {
		skills = append(skills, SkillCount{Skill: k, Count: v})
	}
	// Простая сортировка (insertion sort, достаточно для небольших списков)
	for i := 1; i < len(skills); i++ {
		for j := i; j > 0 && skills[j].Count > skills[j-1].Count; j-- {
			skills[j], skills[j-1] = skills[j-1], skills[j]
		}
	}
	if n > len(skills) {
		n = len(skills)
	}
	return skills[:n]
}
