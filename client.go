package pgbhub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Config struct {
	Endpoint string
	APIKey   string
	Database string
	HTTPClient *http.Client
}

type Client struct {
	endpoint   string
	apiKey     string
	database   string
	httpClient *http.Client
}

type ActionResponse struct {
	Action     string          `json:"action"`
	Status     string          `json:"status"`
	DryRun     bool            `json:"dry_run"`
	DurationMS float64         `json:"duration_ms"`
	Results    json.RawMessage `json:"results"`
	OutboxID   *int64          `json:"outbox_id,omitempty"`
	Message    string          `json:"message,omitempty"`
}

type SchemaColumn struct {
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	TypeTS       string `json:"type_ts"`
	TypePy       string `json:"type_py"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}

type SchemaTable struct {
	Name        string         `json:"name"`
	Schema      string         `json:"schema"`
	Type        string         `json:"type"`
	Columns     []SchemaColumn `json:"columns"`
	PrimaryKeys []string       `json:"primary_keys"`
}

type Schema struct {
	Version  string                 `json:"version"`
	Database string                 `json:"database"`
	Tables   map[string]SchemaTable `json:"tables"`
	Actions  map[string]any         `json:"actions"`
}

func NewClient(cfg Config) *Client {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("PGBHUB_ENDPOINT")
		if endpoint == "" {
			endpoint = "https://cloud.pgbhub.com"
		}
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("PGBHUB_API_KEY")
	}
	db := cfg.Database
	if db == "" {
		db = "postgres"
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}

	return &Client{
		endpoint:   strings.TrimRight(endpoint, "/"),
		apiKey:     apiKey,
		database:   db,
		httpClient: hc,
	}
}

func (c *Client) GetSchema(ctx context.Context) (*Schema, error) {
	url := fmt.Sprintf("%s/api/v1/databases/%s/schema.json", c.endpoint, c.database)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch schema [%d]: %s", resp.StatusCode, string(b))
	}

	var schema Schema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (c *Client) InvokeAction(ctx context.Context, action string, params map[string]any, dryRun bool) (*ActionResponse, error) {
	url := fmt.Sprintf("%s/v1/actions/invoke", c.endpoint)
	bodyMap := map[string]any{
		"action":  action,
		"params":  params,
		"dry_run": dryRun,
		"dbname":  c.database,
	}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("action '%s' failed [%d]: %s", action, resp.StatusCode, string(b))
	}

	var result ActionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
