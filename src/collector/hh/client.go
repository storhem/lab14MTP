package hh

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const baseURL = "https://api.hh.ru"

type Salary struct {
	From     *int   `json:"from"`
	To       *int   `json:"to"`
	Currency string `json:"currency"`
	Gross    bool   `json:"gross"`
}

type Area struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Employer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Snippet struct {
	Requirement  string `json:"requirement"`
	Responsibility string `json:"responsibility"`
}

type Vacancy struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Salary      *Salary   `json:"salary"`
	Area        Area      `json:"area"`
	Employer    Employer  `json:"employer"`
	Snippet     Snippet   `json:"snippet"`
	PublishedAt string    `json:"published_at"`
	CollectedAt time.Time `json:"collected_at"`
}

type SearchResponse struct {
	Items []Vacancy `json:"items"`
	Found int       `json:"found"`
	Pages int       `json:"pages"`
	Page  int       `json:"page"`
}

type SearchParams struct {
	Text       string
	AreaID     string
	PerPage    int
	Page       int
	Experience string
}

type Client struct {
	http    *http.Client
	baseURL string
}

func NewClient() *Client {
	return &Client{
		http:    &http.Client{Timeout: 15 * time.Second},
		baseURL: baseURL,
	}
}

func (c *Client) SearchVacancies(params SearchParams) (*SearchResponse, error) {
	u, err := url.Parse(c.baseURL + "/vacancies")
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("text", params.Text)
	if params.AreaID != "" {
		q.Set("area", params.AreaID)
	}
	if params.PerPage > 0 {
		q.Set("per_page", fmt.Sprintf("%d", params.PerPage))
	}
	if params.Page > 0 {
		q.Set("page", fmt.Sprintf("%d", params.Page))
	}
	if params.Experience != "" {
		q.Set("experience", params.Experience)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "lab14-hh-collector/1.0 (educational project)")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var result SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	now := time.Now().UTC()
	for i := range result.Items {
		result.Items[i].CollectedAt = now
	}

	return &result, nil
}
