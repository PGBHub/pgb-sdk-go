package pgbhub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Config struct {
	Endpoint string
	APIKey   string
	Database string
	// ProjectID is required by QueuesClient, whose routes are addressed
	// by project rather than database.
	ProjectID  string
	HTTPClient *http.Client
}

type Client struct {
	endpoint   string
	apiKey     string
	database   string
	projectID  string
	httpClient *http.Client

	// Auth is the Built-In PGB Auth-as-a-Service client.
	Auth *AuthClient
	// Storage covers file/media download (upload/list/delete are not yet
	// exposed in the Go SDK -- tracked as follow-up breadth work).
	Storage *StorageClient
	// Functions covers deploy/delete/invoke/list of serverless cloud functions.
	Functions *FunctionsClient
	// Cron covers scheduled job management, addressed by database name.
	Cron *CronClient
	// Jobs covers the on-demand asynchronous background job queue.
	Jobs *JobsClient
	// Queues covers durable message queues (send/receive/ack/nack).
	Queues *QueuesClient
	// Realtime covers broadcast/subscribe channels.
	Realtime *RealtimeClient
}

// APIError is returned by any request that reaches the server and gets
// back a non-2xx response, carrying the server's own parsed error
// message instead of a bare status code.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("PGBHub API error [%d]: %s", e.StatusCode, e.Message)
}

// doRequest sends an authenticated JSON request to the PGBHub control
// plane and decodes the response into out (if non-nil). extraHeaders
// lets callers add e.g. X-PGB-User-Token without a bespoke request
// builder at every call site -- every method added after GetSchema/
// InvokeAction below is built on this helper.
func (c *Client) doRequest(ctx context.Context, method, path string, body any, extraHeaders map[string]string, out any) error {
	reqURL := c.endpoint + path
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(respBody, &errBody)
		msg := errBody.Error
		if msg == "" {
			msg = string(respBody)
		}
		return &APIError{StatusCode: resp.StatusCode, Message: msg}
	}

	if out != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, out)
	}
	return nil
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
	projectID := cfg.ProjectID
	if projectID == "" {
		projectID = os.Getenv("PGBHUB_PROJECT_ID")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}

	c := &Client{
		endpoint:   strings.TrimRight(endpoint, "/"),
		apiKey:     apiKey,
		database:   db,
		projectID:  projectID,
		httpClient: hc,
	}
	c.Auth = &AuthClient{client: c, Admin: &AuthAdminClient{client: c}}
	c.Storage = &StorageClient{client: c}
	c.Functions = &FunctionsClient{client: c}
	c.Cron = &CronClient{client: c}
	c.Jobs = &JobsClient{client: c}
	c.Queues = &QueuesClient{client: c}
	c.Realtime = &RealtimeClient{client: c}
	return c
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

// ─── Auth-as-a-Service ──────────────────────────────────────────────────

// AuthUser is a user registered in a project's Built-In PGB Auth.
type AuthUser struct {
	ID          string         `json:"id"`
	Email       string         `json:"email"`
	IsVerified  bool           `json:"is_verified"`
	RawUserMeta map[string]any `json:"raw_user_meta,omitempty"`
	CreatedAt   string         `json:"created_at,omitempty"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
}

// AuthSession is an access/refresh token pair issued to an end user.
type AuthSession struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type,omitempty"`
	ExpiresIn    int    `json:"expires_in,omitempty"`
	ExpiresAt    string `json:"expires_at,omitempty"`
}

// AuthResult is returned by SignUp, VerifyOTP, VerifyMagicLink, and RefreshSession.
type AuthResult struct {
	User    *AuthUser    `json:"user"`
	Session *AuthSession `json:"session"`
}

// LoginResult is returned by SignIn. Session is nil and MFARequired is
// true when the account has MFA enabled -- redeem ChallengeToken via a
// future CompleteMFALogin call.
type LoginResult struct {
	User           *AuthUser    `json:"user,omitempty"`
	Session        *AuthSession `json:"session,omitempty"`
	MFARequired    bool         `json:"mfa_required,omitempty"`
	ChallengeToken string       `json:"challenge_token,omitempty"`
	ExpiresIn      int          `json:"expires_in,omitempty"`
}

// OTPRequestResult is returned by RequestOTP.
type OTPRequestResult struct {
	Message string `json:"message"`
	// OTP is only populated in sandbox/dev deployments where the server
	// returns it directly instead of only emailing it.
	OTP       string `json:"otp,omitempty"`
	ExpiresIn int    `json:"expires_in"`
}

// MagicLinkRequestResult is returned by SendMagicLink.
type MagicLinkRequestResult struct {
	Message string `json:"message"`
	// Token is only populated in sandbox/dev deployments where the
	// server returns it directly instead of only emailing it.
	Token     string `json:"token,omitempty"`
	ExpiresIn int    `json:"expires_in"`
}

// AuthClient is the Built-In PGB Auth-as-a-Service client (client.Auth).
//
// Two different bearer tokens are in play: the Client's own apiKey (a
// project Secret/Publishable key) identifies the *project*, sent as
// "Authorization: Bearer <key>" on every call; the *end user's* own
// access token (held in the session set by SignUp/SignIn/SetSession) is
// sent separately as "X-PGB-User-Token" on the routes that need to know
// who's calling (GetUser, SignOut).
type AuthClient struct {
	client  *Client
	session *AuthSession

	// Admin covers Secret-key-only user management.
	Admin *AuthAdminClient
}

// SignUp signs up a new user with email & password. metadata may be nil.
func (a *AuthClient) SignUp(ctx context.Context, email, password string, metadata map[string]any) (*AuthResult, error) {
	body := map[string]any{"email": email, "password": password, "metadata": metadata}
	var result AuthResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/signup", body, nil, &result); err != nil {
		return nil, err
	}
	a.session = result.Session
	return &result, nil
}

// SignIn signs in with email & password.
func (a *AuthClient) SignIn(ctx context.Context, email, password string) (*LoginResult, error) {
	body := map[string]any{"email": email, "password": password}
	var result LoginResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/login", body, nil, &result); err != nil {
		return nil, err
	}
	if result.Session != nil {
		a.session = result.Session
	}
	return &result, nil
}

// SignOut signs out and invalidates the current session. A no-op if
// there is no active session.
func (a *AuthClient) SignOut(ctx context.Context) error {
	if a.session == nil || a.session.AccessToken == "" {
		return nil
	}
	headers := map[string]string{"X-PGB-User-Token": a.session.AccessToken}
	err := a.client.doRequest(ctx, http.MethodPost, "/auth/logout", map[string]any{}, headers, nil)
	a.session = nil
	return err
}

// GetUser returns the current authenticated user's details, or nil
// without a network call if there is no active session.
func (a *AuthClient) GetUser(ctx context.Context) (*AuthUser, error) {
	if a.session == nil || a.session.AccessToken == "" {
		return nil, nil
	}
	headers := map[string]string{"X-PGB-User-Token": a.session.AccessToken}
	var result struct {
		User *AuthUser `json:"user"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/user", nil, headers, &result); err != nil {
		return nil, err
	}
	return result.User, nil
}

// GetSession returns the current active session, or nil.
func (a *AuthClient) GetSession() *AuthSession { return a.session }

// SetSession sets the active session.
func (a *AuthClient) SetSession(s *AuthSession) { a.session = s }

// RefreshSession exchanges a refresh token for a new session, rotating
// the refresh token on every use. Pass "" for refreshToken to use the
// current session's refresh token.
func (a *AuthClient) RefreshSession(ctx context.Context, refreshToken string) (*AuthResult, error) {
	token := refreshToken
	if token == "" && a.session != nil {
		token = a.session.RefreshToken
	}
	if token == "" {
		return nil, fmt.Errorf("RefreshSession requires a refresh token (none provided and no active session)")
	}
	body := map[string]any{"refresh_token": token}
	var result AuthResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/refresh", body, nil, &result); err != nil {
		return nil, err
	}
	a.session = result.Session
	return &result, nil
}

// RequestOTP requests a one-time passcode for passwordless sign-in.
func (a *AuthClient) RequestOTP(ctx context.Context, email string) (*OTPRequestResult, error) {
	var result OTPRequestResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/otp", map[string]any{"email": email}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// VerifyOTP verifies a one-time passcode and establishes a session.
func (a *AuthClient) VerifyOTP(ctx context.Context, email, token string) (*AuthResult, error) {
	body := map[string]any{"email": email, "token": token}
	var result AuthResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/otp/verify", body, nil, &result); err != nil {
		return nil, err
	}
	a.session = result.Session
	return &result, nil
}

// SendMagicLink sends a passwordless magic link.
func (a *AuthClient) SendMagicLink(ctx context.Context, email string) (*MagicLinkRequestResult, error) {
	var result MagicLinkRequestResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/magic", map[string]any{"email": email}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// VerifyMagicLink verifies a magic link token and establishes a session.
func (a *AuthClient) VerifyMagicLink(ctx context.Context, token string) (*AuthResult, error) {
	body := map[string]any{"token": token}
	var result AuthResult
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/magic/verify", body, nil, &result); err != nil {
		return nil, err
	}
	a.session = result.Session
	return &result, nil
}

// AuthAdminClient is Secret-key-only user management (client.Auth.Admin),
// mirroring pgb-cloud's /auth/admin/users CRUD exactly.
type AuthAdminClient struct {
	client *Client
}

// CreateUser creates a user directly (bypasses signup email verification).
func (a *AuthAdminClient) CreateUser(ctx context.Context, email, password string, metadata map[string]any, isVerified bool) (*AuthUser, error) {
	body := map[string]any{"email": email, "password": password, "metadata": metadata, "is_verified": isVerified}
	var result struct {
		User *AuthUser `json:"user"`
	}
	if err := a.client.doRequest(ctx, http.MethodPost, "/auth/admin/users", body, nil, &result); err != nil {
		return nil, err
	}
	return result.User, nil
}

// ListUsers lists users registered in this project.
func (a *AuthAdminClient) ListUsers(ctx context.Context) ([]AuthUser, error) {
	var result struct {
		Users []AuthUser `json:"users"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/admin/users", nil, nil, &result); err != nil {
		return nil, err
	}
	return result.Users, nil
}

// GetUser fetches a single user by ID.
func (a *AuthAdminClient) GetUser(ctx context.Context, userID string) (*AuthUser, error) {
	var result struct {
		User *AuthUser `json:"user"`
	}
	path := "/auth/admin/users/" + url.PathEscape(userID)
	if err := a.client.doRequest(ctx, http.MethodGet, path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result.User, nil
}

// UpdateUser updates a user's password, metadata, or verification
// status. updates may contain "password", "metadata", and/or "is_verified".
func (a *AuthAdminClient) UpdateUser(ctx context.Context, userID string, updates map[string]any) (*AuthUser, error) {
	var result struct {
		User *AuthUser `json:"user"`
	}
	path := "/auth/admin/users/" + url.PathEscape(userID)
	if err := a.client.doRequest(ctx, http.MethodPatch, path, updates, nil, &result); err != nil {
		return nil, err
	}
	return result.User, nil
}

// DeleteUser deletes a user and invalidates their sessions.
func (a *AuthAdminClient) DeleteUser(ctx context.Context, userID string) error {
	path := "/auth/admin/users/" + url.PathEscape(userID)
	return a.client.doRequest(ctx, http.MethodDelete, path, nil, nil, nil)
}

// ─── Storage ─────────────────────────────────────────────────────────────

// StorageFile is a downloaded file's raw bytes and metadata.
type StorageFile struct {
	Data        []byte
	ContentType string
	Path        string
	ETag        string
}

// MediaTransformOptions is accepted by GetMedia for forward
// compatibility only -- see GetMedia's doc comment.
type MediaTransformOptions struct {
	Width  int
	Height int
	Format string
	Fit    string
}

// StorageClient covers file/media download (client.Storage). Upload/
// list/delete are not yet exposed in the Go SDK -- tracked as follow-up
// breadth work, matching the TS SDK's existing coverage.
type StorageClient struct {
	client *Client
}

// GetFile downloads a stored file's raw bytes.
func (s *StorageClient) GetFile(ctx context.Context, filePath string) (*StorageFile, error) {
	cleanPath := strings.TrimLeft(filePath, "/")
	reqURL := fmt.Sprintf("%s/storage/%s", s.client.endpoint, cleanPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")
	if s.client.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.client.apiKey)
	}

	resp, err := s.client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{StatusCode: resp.StatusCode, Message: string(data)}
	}

	return &StorageFile{
		Data:        data,
		ContentType: resp.Header.Get("Content-Type"),
		Path:        cleanPath,
		ETag:        resp.Header.Get("ETag"),
	}, nil
}

// GetMedia downloads a stored image.
//
// The server does not implement on-the-fly image transforms yet (no
// width/height/format handling exists in its storage delivery route) --
// opts is accepted here for forward compatibility only and currently
// has no effect. This is a thin alias over GetFile; do not rely on
// resizing/format conversion actually happening until PGBHub ships real
// transform support.
func (s *StorageClient) GetMedia(ctx context.Context, filePath string, _ *MediaTransformOptions) (*StorageFile, error) {
	return s.GetFile(ctx, filePath)
}

// ─── Functions ───────────────────────────────────────────────────────────

// CloudFunction is a serverless cloud function's stored configuration.
type CloudFunction struct {
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Runtime     string `json:"runtime,omitempty"`
	Entrypoint  string `json:"entrypoint,omitempty"`
	TriggerType string `json:"trigger_type,omitempty"`
	IsEnabled   bool   `json:"is_enabled,omitempty"`
	TimeoutMS   int    `json:"timeout_ms,omitempty"`
	MemoryMB    int    `json:"memory_mb,omitempty"`
}

// DeployFunctionOptions are the optional fields Deploy accepts beyond
// name/code. Every field is optional; an empty DeployFunctionOptions{}
// (or nil) deploys with the server's own defaults, or keeps the
// existing function's values for any field left unset on an update.
type DeployFunctionOptions struct {
	Title         string
	Description   string
	Runtime       string
	Entrypoint    string
	TriggerType   string
	TriggerConfig map[string]any
	EnvVars       map[string]string
	IsEnabled     *bool
	TimeoutMS     int
	MemoryMB      int
}

// FunctionsClient covers deploy/delete/invoke/list of serverless cloud
// functions (client.Functions).
type FunctionsClient struct {
	client *Client
}

// Deploy creates or updates a function's code and configuration.
//
// The server's save-function logic is an upsert regardless of which
// route calls it, so this always POSTs to the functions collection
// endpoint (name in the body, not the URL) rather than needing to check
// existence first and switch between POST/PUT.
func (f *FunctionsClient) Deploy(ctx context.Context, name, code string, opts *DeployFunctionOptions) (*CloudFunction, error) {
	body := map[string]any{"name": name, "code": code}
	if opts != nil {
		if opts.Title != "" {
			body["title"] = opts.Title
		}
		if opts.Description != "" {
			body["description"] = opts.Description
		}
		if opts.Runtime != "" {
			body["runtime"] = opts.Runtime
		}
		if opts.Entrypoint != "" {
			body["entrypoint"] = opts.Entrypoint
		}
		if opts.TriggerType != "" {
			body["trigger_type"] = opts.TriggerType
		}
		if opts.TriggerConfig != nil {
			body["trigger_config"] = opts.TriggerConfig
		}
		if opts.EnvVars != nil {
			body["env_vars"] = opts.EnvVars
		}
		if opts.IsEnabled != nil {
			body["is_enabled"] = *opts.IsEnabled
		}
		if opts.TimeoutMS != 0 {
			body["timeout_ms"] = opts.TimeoutMS
		}
		if opts.MemoryMB != 0 {
			body["memory_mb"] = opts.MemoryMB
		}
	}

	path := fmt.Sprintf("/api/v1/databases/%s/functions", f.client.database)
	var result struct {
		Success  bool          `json:"success"`
		Function CloudFunction `json:"function"`
	}
	if err := f.client.doRequest(ctx, http.MethodPost, path, body, nil, &result); err != nil {
		return nil, err
	}
	return &result.Function, nil
}

// Delete deletes a cloud function.
func (f *FunctionsClient) Delete(ctx context.Context, name string) error {
	path := fmt.Sprintf("/api/v1/databases/%s/functions/%s", f.client.database, url.PathEscape(name))
	return f.client.doRequest(ctx, http.MethodDelete, path, nil, nil, nil)
}

// List lists all cloud functions configured for the target database.
func (f *FunctionsClient) List(ctx context.Context) ([]CloudFunction, error) {
	path := fmt.Sprintf("/api/v1/databases/%s/functions", f.client.database)
	var result struct {
		Functions []CloudFunction `json:"functions"`
	}
	if err := f.client.doRequest(ctx, http.MethodGet, path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result.Functions, nil
}

// Invoke invokes a serverless cloud function via its database-scoped
// route, returning the raw JSON response.
func (f *FunctionsClient) Invoke(ctx context.Context, name string, payload any) (json.RawMessage, error) {
	path := fmt.Sprintf("/api/v1/databases/%s/functions/%s/invoke", f.client.database, url.PathEscape(name))
	var result json.RawMessage
	if err := f.client.doRequest(ctx, http.MethodPost, path, payload, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// ─── Database provisioning ─────────────────────────────────────────────

// CreateDatabaseOptions are the optional fields CreateDatabase accepts.
type CreateDatabaseOptions struct {
	PgVersion  string
	Extensions []string
}

// CreateDatabaseResult is returned by CreateDatabase.
type CreateDatabaseResult struct {
	Success    bool     `json:"success"`
	Database   string   `json:"database"`
	PgVersion  string   `json:"pg_version"`
	Extensions []string `json:"extensions"`
	User       string   `json:"user"`
	Password   string   `json:"password"`
	PublicHost string   `json:"public_host"`
	PublicPort int      `json:"public_port"`
	URL        string   `json:"url"`
	Message    string   `json:"message"`
}

// CreateDatabase provisions a new PGBHub-managed PostgreSQL database.
//
// Requires an account-level API key (scope_type "account", not a
// project-scoped pgb_sec_/pgb_pub_ key) -- a project key is permanently
// bound to the one database it was issued for and the server rejects
// (403) any attempt to use it to provision an unrelated new one.
func (c *Client) CreateDatabase(ctx context.Context, name string, opts *CreateDatabaseOptions) (*CreateDatabaseResult, error) {
	pgVersion := "17"
	var extensions []string
	if opts != nil {
		if opts.PgVersion != "" {
			pgVersion = opts.PgVersion
		}
		extensions = opts.Extensions
	}
	body := map[string]any{"name": name, "pg_version": pgVersion, "extensions": extensions}
	var result CreateDatabaseResult
	if err := c.doRequest(ctx, http.MethodPost, "/api/v1/databases", body, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ─── Cron ──────────────────────────────────────────────────────────────

// CronJob is a scheduled cron job's stored configuration.
type CronJob struct {
	ID                string         `json:"id"`
	Name              string         `json:"name"`
	Description       string         `json:"description,omitempty"`
	Expression        string         `json:"expression"`
	Timezone          string         `json:"timezone,omitempty"`
	TargetType        string         `json:"target_type"`
	TargetConfig      map[string]any `json:"target_config,omitempty"`
	IsEnabled         bool           `json:"is_enabled"`
	ConcurrencyPolicy string         `json:"concurrency_policy,omitempty"`
	TimeoutSeconds    int            `json:"timeout_seconds,omitempty"`
	MaxRetries        int            `json:"max_retries,omitempty"`
	RetryDelaySeconds int            `json:"retry_delay_seconds,omitempty"`
	LastRunAt         *string        `json:"last_run_at,omitempty"`
	LastStatus        *string        `json:"last_status,omitempty"`
	NextRunAt         *string        `json:"next_run_at,omitempty"`
}

// CronRun is one execution record for a cron job.
type CronRun struct {
	ID         string  `json:"id"`
	CronID     string  `json:"cron_id"`
	Status     string  `json:"status"`
	StartedAt  *string `json:"started_at,omitempty"`
	FinishedAt *string `json:"finished_at,omitempty"`
	DurationMS *int    `json:"duration_ms,omitempty"`
	Error      *string `json:"error,omitempty"`
}

// CreateCronJobOptions are the optional fields CronClient.Create accepts.
type CreateCronJobOptions struct {
	Description       string
	Timezone          string
	IsEnabled         *bool
	ConcurrencyPolicy string
	TimeoutSeconds    int
	MaxRetries        int
	RetryDelaySeconds int
}

// UpdateCronJobFields are the optional fields CronClient.Update accepts.
type UpdateCronJobFields struct {
	Name              string
	Description       string
	Expression        string
	Timezone          string
	ConcurrencyPolicy string
	TimeoutSeconds    int
	MaxRetries        int
	TargetType        string
	TargetConfig      map[string]any
}

// CronClient covers scheduled (cron) job management (client.Cron),
// addressed by database name. TargetConfig's shape depends on
// TargetType: function -> {function_name, payload}, sql -> {sql},
// action -> {action_name, params}, http -> {url, method, body?},
// mail -> {to, subject, body}.
type CronClient struct {
	client *Client
}

func (cr *CronClient) base() string {
	return fmt.Sprintf("/api/v1/databases/%s/crons", cr.client.database)
}

// List lists scheduled cron jobs.
func (cr *CronClient) List(ctx context.Context) ([]CronJob, error) {
	var result struct {
		Crons []CronJob `json:"crons"`
	}
	if err := cr.client.doRequest(ctx, http.MethodGet, cr.base(), nil, nil, &result); err != nil {
		return nil, err
	}
	return result.Crons, nil
}

// Create creates a scheduled cron job.
func (cr *CronClient) Create(ctx context.Context, name, expression, targetType string, targetConfig map[string]any, opts *CreateCronJobOptions) (*CronJob, error) {
	body := map[string]any{"name": name, "expression": expression, "target_type": targetType, "target_config": targetConfig}
	if opts != nil {
		if opts.Description != "" {
			body["description"] = opts.Description
		}
		if opts.Timezone != "" {
			body["timezone"] = opts.Timezone
		}
		if opts.IsEnabled != nil {
			body["is_enabled"] = *opts.IsEnabled
		}
		if opts.ConcurrencyPolicy != "" {
			body["concurrency_policy"] = opts.ConcurrencyPolicy
		}
		if opts.TimeoutSeconds != 0 {
			body["timeout_seconds"] = opts.TimeoutSeconds
		}
		if opts.MaxRetries != 0 {
			body["max_retries"] = opts.MaxRetries
		}
		if opts.RetryDelaySeconds != 0 {
			body["retry_delay_seconds"] = opts.RetryDelaySeconds
		}
	}
	var job CronJob
	if err := cr.client.doRequest(ctx, http.MethodPost, cr.base(), body, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// Toggle enables or disables a cron job.
func (cr *CronClient) Toggle(ctx context.Context, cronID string, enabled bool) (*CronJob, error) {
	path := fmt.Sprintf("%s/%s/toggle", cr.base(), url.PathEscape(cronID))
	var job CronJob
	if err := cr.client.doRequest(ctx, http.MethodPost, path, map[string]any{"enabled": enabled}, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// Run manually triggers a cron job to run immediately.
func (cr *CronClient) Run(ctx context.Context, cronID string) (map[string]any, error) {
	path := fmt.Sprintf("%s/%s/run", cr.base(), url.PathEscape(cronID))
	var result map[string]any
	if err := cr.client.doRequest(ctx, http.MethodPost, path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Delete deletes a cron job.
func (cr *CronClient) Delete(ctx context.Context, cronID string) error {
	path := fmt.Sprintf("%s/%s", cr.base(), url.PathEscape(cronID))
	return cr.client.doRequest(ctx, http.MethodDelete, path, nil, nil, nil)
}

// ListRuns lists recent run history for a cron job.
func (cr *CronClient) ListRuns(ctx context.Context, cronID string, limit int) ([]CronRun, error) {
	if limit <= 0 {
		limit = 50
	}
	path := fmt.Sprintf("%s/%s/runs?limit=%d", cr.base(), url.PathEscape(cronID), limit)
	var result struct {
		Runs []CronRun `json:"runs"`
	}
	if err := cr.client.doRequest(ctx, http.MethodGet, path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result.Runs, nil
}

// Update updates a cron job's configuration.
func (cr *CronClient) Update(ctx context.Context, cronID string, fields UpdateCronJobFields) (*CronJob, error) {
	body := map[string]any{}
	if fields.Name != "" {
		body["name"] = fields.Name
	}
	if fields.Description != "" {
		body["description"] = fields.Description
	}
	if fields.Expression != "" {
		body["expression"] = fields.Expression
	}
	if fields.Timezone != "" {
		body["timezone"] = fields.Timezone
	}
	if fields.ConcurrencyPolicy != "" {
		body["concurrency_policy"] = fields.ConcurrencyPolicy
	}
	if fields.TimeoutSeconds != 0 {
		body["timeout_seconds"] = fields.TimeoutSeconds
	}
	if fields.MaxRetries != 0 {
		body["max_retries"] = fields.MaxRetries
	}
	if fields.TargetType != "" {
		body["target_type"] = fields.TargetType
	}
	if fields.TargetConfig != nil {
		body["target_config"] = fields.TargetConfig
	}
	path := fmt.Sprintf("%s/%s", cr.base(), url.PathEscape(cronID))
	var job CronJob
	if err := cr.client.doRequest(ctx, http.MethodPatch, path, body, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// ─── Jobs ──────────────────────────────────────────────────────────────

// BackgroundJob is an on-demand asynchronous background job.
type BackgroundJob struct {
	ID             int            `json:"id"`
	JobType        string         `json:"job_type"`
	TargetType     string         `json:"target_type"`
	TargetConfig   map[string]any `json:"target_config,omitempty"`
	Payload        any            `json:"payload,omitempty"`
	Status         string         `json:"status"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	LastError      *string        `json:"last_error,omitempty"`
}

// EnqueueJobOptions are the optional fields JobsClient.Enqueue accepts.
type EnqueueJobOptions struct {
	Payload        any
	IdempotencyKey string
	MaxAttempts    int
}

// JobsClient covers the on-demand asynchronous background job queue
// (client.Jobs), addressed by database name. Execution happens
// out-of-band in the server's own jobs scheduler -- these methods only
// enqueue/inspect/cancel.
type JobsClient struct {
	client *Client
}

func (j *JobsClient) base() string {
	return fmt.Sprintf("/api/v1/databases/%s/jobs", j.client.database)
}

// Enqueue enqueues a new background job.
func (j *JobsClient) Enqueue(ctx context.Context, jobType, targetType string, targetConfig map[string]any, opts *EnqueueJobOptions) (*BackgroundJob, error) {
	body := map[string]any{"job_type": jobType, "target_type": targetType, "target_config": targetConfig}
	if opts != nil {
		if opts.Payload != nil {
			body["payload"] = opts.Payload
		}
		if opts.IdempotencyKey != "" {
			body["idempotency_key"] = opts.IdempotencyKey
		}
		if opts.MaxAttempts != 0 {
			body["max_attempts"] = opts.MaxAttempts
		}
	}
	var result struct {
		Success bool          `json:"success"`
		Job     BackgroundJob `json:"job"`
	}
	if err := j.client.doRequest(ctx, http.MethodPost, j.base(), body, nil, &result); err != nil {
		return nil, err
	}
	return &result.Job, nil
}

// Get fetches a single job by ID, returning (nil, nil) if not found.
func (j *JobsClient) Get(ctx context.Context, jobID int) (*BackgroundJob, error) {
	path := fmt.Sprintf("%s/%d", j.base(), jobID)
	var result struct {
		Success bool          `json:"success"`
		Job     BackgroundJob `json:"job"`
	}
	err := j.client.doRequest(ctx, http.MethodGet, path, nil, nil, &result)
	if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &result.Job, nil
}

// ListJobsOptions filters JobsClient.List.
type ListJobsOptions struct {
	Status string
	Limit  int
}

// List lists jobs, optionally filtered by status.
func (j *JobsClient) List(ctx context.Context, opts *ListJobsOptions) ([]BackgroundJob, error) {
	q := url.Values{}
	if opts != nil {
		if opts.Status != "" {
			q.Set("status", opts.Status)
		}
		if opts.Limit != 0 {
			q.Set("limit", fmt.Sprintf("%d", opts.Limit))
		}
	}
	path := j.base()
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var result struct {
		Success bool            `json:"success"`
		Jobs    []BackgroundJob `json:"jobs"`
	}
	if err := j.client.doRequest(ctx, http.MethodGet, path, nil, nil, &result); err != nil {
		return nil, err
	}
	return result.Jobs, nil
}

// Cancel cancels a pending/running job. Returns false if it could not
// be cancelled (already running or finished) instead of an error.
func (j *JobsClient) Cancel(ctx context.Context, jobID int) (bool, error) {
	path := fmt.Sprintf("%s/%d/cancel", j.base(), jobID)
	var result struct {
		Success   bool `json:"success"`
		Cancelled bool `json:"cancelled"`
	}
	err := j.client.doRequest(ctx, http.MethodPost, path, nil, nil, &result)
	if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == http.StatusConflict {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return result.Cancelled, nil
}

// ─── Queues ──────────────────────────────────────────────────────────────

// SentMessage is returned by QueuesClient.Send.
type SentMessage struct {
	MessageID    string `json:"message_id"`
	QueueName    string `json:"queue_name"`
	Status       string `json:"status"`
	ScheduledAt  string `json:"scheduled_at,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	Deduplicated bool   `json:"deduplicated"`
}

// ReceivedMessage is one message returned by QueuesClient.Receive.
type ReceivedMessage struct {
	MessageID string `json:"message_id"`
	Payload   any    `json:"payload"`
}

// SendMessageOptions are the optional fields QueuesClient.Send accepts.
type SendMessageOptions struct {
	DelaySeconds    int
	DeduplicationID string
}

// ReceiveMessagesOptions are the optional fields QueuesClient.Receive accepts.
type ReceiveMessagesOptions struct {
	MaxMessages       int
	VisibilityTimeout int
	ConsumerID        string
}

// QueuesClient covers durable message queues (client.Queues). Unlike
// Cron/Jobs/Functions, queue routes are addressed by project ID, not
// database name (/api/v1/projects/{id}/queues/...) -- set Config.ProjectID
// or PGBHUB_PROJECT_ID. Queue creation/deletion is dashboard-only today
// (no API route); this client covers send/receive/ack/nack against an
// already-created queue.
type QueuesClient struct {
	client *Client
}

func (q *QueuesClient) url(queueName, action string) (string, error) {
	if q.client.projectID == "" {
		return "", fmt.Errorf("QueuesClient requires a ProjectID — set Config.ProjectID or PGBHUB_PROJECT_ID")
	}
	return fmt.Sprintf("/api/v1/projects/%s/queues/%s/%s", url.PathEscape(q.client.projectID), url.PathEscape(queueName), action), nil
}

// Send sends a message to a queue.
func (q *QueuesClient) Send(ctx context.Context, queueName string, payload any, opts *SendMessageOptions) (*SentMessage, error) {
	path, err := q.url(queueName, "send")
	if err != nil {
		return nil, err
	}
	body := map[string]any{"payload": payload}
	if opts != nil {
		if opts.DelaySeconds != 0 {
			body["delay_seconds"] = opts.DelaySeconds
		}
		if opts.DeduplicationID != "" {
			body["deduplication_id"] = opts.DeduplicationID
		}
	}
	var result struct {
		OK      bool        `json:"ok"`
		Message SentMessage `json:"message"`
	}
	if err := q.client.doRequest(ctx, http.MethodPost, path, body, nil, &result); err != nil {
		return nil, err
	}
	return &result.Message, nil
}

// Receive receives (and leases) messages from a queue.
func (q *QueuesClient) Receive(ctx context.Context, queueName string, opts *ReceiveMessagesOptions) ([]ReceivedMessage, error) {
	path, err := q.url(queueName, "receive")
	if err != nil {
		return nil, err
	}
	body := map[string]any{}
	if opts != nil {
		if opts.MaxMessages != 0 {
			body["max_messages"] = opts.MaxMessages
		}
		if opts.VisibilityTimeout != 0 {
			body["visibility_timeout"] = opts.VisibilityTimeout
		}
		if opts.ConsumerID != "" {
			body["consumer_id"] = opts.ConsumerID
		}
	}
	var result struct {
		OK       bool              `json:"ok"`
		Count    int               `json:"count"`
		Messages []ReceivedMessage `json:"messages"`
	}
	if err := q.client.doRequest(ctx, http.MethodPost, path, body, nil, &result); err != nil {
		return nil, err
	}
	return result.Messages, nil
}

// Ack acknowledges (deletes) a successfully processed message.
func (q *QueuesClient) Ack(ctx context.Context, queueName, messageID string) (bool, error) {
	path, err := q.url(queueName, "ack")
	if err != nil {
		return false, err
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if err := q.client.doRequest(ctx, http.MethodPost, path, map[string]any{"message_id": messageID}, nil, &result); err != nil {
		return false, err
	}
	return result.OK, nil
}

// Nack negatively acknowledges a message -- returns it to the queue (or
// to the DLQ past max retries).
func (q *QueuesClient) Nack(ctx context.Context, queueName, messageID, errorMessage string) (bool, error) {
	path, err := q.url(queueName, "nack")
	if err != nil {
		return false, err
	}
	var result struct {
		OK bool `json:"ok"`
	}
	body := map[string]any{"message_id": messageID}
	if errorMessage != "" {
		body["error"] = errorMessage
	}
	if err := q.client.doRequest(ctx, http.MethodPost, path, body, nil, &result); err != nil {
		return false, err
	}
	return result.OK, nil
}

// ─── Realtime ────────────────────────────────────────────────────────────

// BroadcastResult is returned by RealtimeClient.Broadcast.
type BroadcastResult struct {
	OK        bool           `json:"ok"`
	Broadcast map[string]any `json:"broadcast"`
}

// RealtimeMsgEvent is one event delivered by RealtimeClient.Subscribe.
type RealtimeMsgEvent struct {
	Event string
	Data  []byte
}

// RealtimeClient covers broadcast/subscribe channels (client.Realtime).
// Channel names are namespaced server-side by project, so two different
// projects' channels of the same name never collide.
type RealtimeClient struct {
	client *Client
}

// Broadcast broadcasts a message to a realtime channel.
func (r *RealtimeClient) Broadcast(ctx context.Context, channel string, payload any, event string) (*BroadcastResult, error) {
	if event == "" {
		event = "message"
	}
	path := fmt.Sprintf("/api/v1/realtime/channels/%s/broadcast", url.PathEscape(channel))
	body := map[string]any{"channel": channel, "event": event, "payload": payload}
	var result BroadcastResult
	if err := r.client.doRequest(ctx, http.MethodPost, path, body, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Subscribe connects to a realtime channel via Server-Sent Events and
// streams events on the returned channel until ctx is cancelled or the
// connection ends, at which point the channel is closed. Errors
// (connection failures, non-2xx responses) are sent once and the
// channel is then closed.
func (r *RealtimeClient) Subscribe(ctx context.Context, channel string) (<-chan RealtimeMsgEvent, <-chan error, error) {
	reqURL := fmt.Sprintf("%s/api/v1/realtime/stream?channel=%s", r.client.endpoint, url.QueryEscape(channel))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if r.client.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.client.apiKey)
	}

	resp, err := r.client.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, nil, &APIError{StatusCode: resp.StatusCode, Message: string(b)}
	}

	events := make(chan RealtimeMsgEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errs)
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		currentEvent := "message"
		var dataLines []string

		flush := func() {
			if len(dataLines) == 0 {
				return
			}
			events <- RealtimeMsgEvent{Event: currentEvent, Data: []byte(strings.Join(dataLines, "\n"))}
			currentEvent = "message"
			dataLines = nil
		}

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line := scanner.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, "event:"):
				currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			errs <- err
		}
	}()

	return events, errs, nil
}
