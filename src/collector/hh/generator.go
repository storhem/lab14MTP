package hh

import (
	"fmt"
	"math/rand"
	"time"
)

var jobTitles = []string{
	"Go Developer", "Python Developer", "Backend Engineer", "Senior Backend Developer",
	"Data Engineer", "ML Engineer", "DevOps Engineer", "Platform Engineer",
	"Software Engineer", "Golang Developer", "Python Backend Developer",
	"Site Reliability Engineer", "Data Scientist", "Analytics Engineer",
}

var companies = []string{
	"Яндекс", "VK", "Тинькофф", "Сбер", "Авито", "OZON", "Wildberries",
	"HeadHunter", "Kaspersky Lab", "JetBrains", "Skyeng", "Lamoda",
	"Delivery Club", "Самокат", "Leroy Merlin", "Мегафон", "Билайн",
}

var areas = []struct {
	ID   string
	Name string
}{
	{"1", "Москва"},
	{"2", "Санкт-Петербург"},
	{"3", "Екатеринбург"},
	{"66", "Новосибирск"},
	{"4", "Нижний Новгород"},
	{"88", "Краснодар"},
}

var requirements = []string{
	"Опыт работы с Go от 2 лет. Знание Docker, Kubernetes, PostgreSQL.",
	"Python 3.8+, FastAPI или Django. Опыт с Kafka, Redis. Понимание микросервисной архитектуры.",
	"Go или Python. Опыт работы с gRPC, REST API. CI/CD, Git.",
	"Знание SQL, PostgreSQL. Polars или Pandas. Опыт построения ETL-пайплайнов.",
	"Go concurrency: goroutines, channels. etcd или Consul для service discovery.",
	"Python, Airflow, Spark или Flink. Работа с большими данными, Parquet, ClickHouse.",
	"Kubernetes, Helm, Terraform. Опыт с мониторингом: Prometheus, Grafana.",
	"Rust или C++, опыт системного программирования. Linux internals.",
}

// GenerateVacancies создаёт N реалистичных вакансий для заданного региона и запроса.
func GenerateVacancies(areaID, areaName, query string, n int) []Vacancy {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vacancies := make([]Vacancy, 0, n)

	for i := 0; i < n; i++ {
		title := jobTitles[r.Intn(len(jobTitles))]
		company := companies[r.Intn(len(companies))]
		req := requirements[r.Intn(len(requirements))]

		salaryFrom := (r.Intn(20)+8)*10000 // 80k–280k
		salaryTo := salaryFrom + r.Intn(10)*10000

		from := salaryFrom
		to := salaryTo
		v := Vacancy{
			ID:   fmt.Sprintf("gen-%s-%d-%d", areaID, i, time.Now().UnixNano()),
			Name: title,
			Salary: &Salary{
				From:     &from,
				To:       &to,
				Currency: "RUR",
				Gross:    false,
			},
			Area:     Area{ID: areaID, Name: areaName},
			Employer: Employer{ID: fmt.Sprintf("e%d", r.Intn(1000)), Name: company},
			Snippet: Snippet{
				Requirement:    req,
				Responsibility: "Разработка и поддержка высоконагруженных сервисов.",
			},
			PublishedAt: time.Now().Add(-time.Duration(r.Intn(168)) * time.Hour).Format(time.RFC3339),
			CollectedAt: time.Now().UTC(),
		}
		vacancies = append(vacancies, v)
	}
	return vacancies
}
