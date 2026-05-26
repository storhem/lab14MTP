// Package nats реализует публикацию вакансий в NATS-топик.
// Go-сборщик пишет каждую валидную вакансию в реальном времени;
// Python-консьюмер читает и обрабатывает данные со скользящим окном.
package nats

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"lab14/collector/hh"
)

const DefaultSubject = "vacancies"

// Publisher публикует вакансии в NATS-топик.
type Publisher struct {
	nc      *natsclient.Conn
	subject string
}

// NewPublisher подключается к NATS и возвращает готовый Publisher.
// При разрыве соединения выполняет до 10 попыток переподключения.
func NewPublisher(url, subject string) (*Publisher, error) {
	nc, err := natsclient.Connect(url,
		natsclient.MaxReconnects(10),
		natsclient.ReconnectWait(2*time.Second),
		natsclient.DisconnectErrHandler(func(_ *natsclient.Conn, err error) {
			log.Printf("[nats] disconnected: %v", err)
		}),
		natsclient.ReconnectHandler(func(_ *natsclient.Conn) {
			log.Printf("[nats] reconnected to %s", url)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect %s: %w", url, err)
	}
	if subject == "" {
		subject = DefaultSubject
	}
	log.Printf("[nats] publisher ready: url=%s subject=%s", url, subject)
	return &Publisher{nc: nc, subject: subject}, nil
}

// Publish сериализует вакансию в JSON и публикует в NATS (fire-and-forget).
func (p *Publisher) Publish(v hh.Vacancy) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal vacancy %s: %w", v.ID, err)
	}
	return p.nc.Publish(p.subject, data)
}

// Close дожидается доставки всех буферизованных сообщений и закрывает соединение.
func (p *Publisher) Close() {
	if err := p.nc.Drain(); err != nil {
		log.Printf("[nats] drain: %v", err)
	}
	p.nc.Close()
}
