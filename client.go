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
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Endpoint overrides the address. Otherwise PGBHUB_ENDPOINT or
	// PGBHUB_PROJECT_URL, then https://<ProjectID>.pgbhub.app, then the
	// shared cloud address.
	Endpoint string
	APIKey   string
	// Database is only sent when set; without it the server uses the one
	// the key belongs to.
	Database string
	// ProjectID selects the project's own address and is required by
	// QueuesClient, whose routes are addressed by project.
	ProjectID  string
	HTTPClient *http.Client
	// Timeout per request when HTTPClient is not given (default 30s).
	Timeout time.Duration
	// MaxRetries is how many times a read (GET) is retried on 502/503/504 or
	// a connection error (default 2; set -1 to disable). Writes never retry.
	MaxRetries int
	// RetryDelay is the first backoff step, doubled each retry (default 300ms).
	RetryDelay time.Duration
}

type Client struct {
	endpoint         string
	apiKey           string
	database         string
	databaseExplicit bool
	projectID        string
	httpClient       *http.Client
	maxRetries       int
	retryDelay       time.Duration

	// Auth is the Built-In PGB Auth-as-a-Service client.
	Auth *AuthClient
	// Actions invokes and manages Actions and their effects.
	Actions *ActionsClient
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
	// Code is the server's machine-readable error code, when it sent one.
	Code string
	// RetryAfter is how long a rate-limited (429) caller was told to wait.
	RetryAfter time.Duration
	// Body is the raw response body.
	Body string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("PGBHub API error [%d]: %s", e.StatusCode, e.Message)
}

// errNotSignedIn is returned by calls that act as the signed-in user.
var errNotSignedIn = &APIError{StatusCode: 401, Code: "not_signed_in", Message: "PGBHub Auth: no signed-in user (sign in first, or call SetSession)"}

// doRequest sends an authenticated JSON request to the PGBHub control
// plane and decodes the response into out (if non-nil). extraHeaders
// lets callers add e.g. X-PGB-User-Token without a bespoke request
// builder at every call site. GETs retry transient failures; writes never do.
func (c *Client) doRequest(ctx context.Context, method, path string, body any, extraHeaders map[string]string, out any) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = b
	}
	attempts := 1
	if method == http.MethodGet && c.maxRetries > 0 {
		attempts += c.maxRetries
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		last := i == attempts-1
		var reader io.Reader
		if payload != nil {
			reader = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reader)
		if err != nil {
			return err
		}
		if payload != nil {
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
			lastErr = err
			if !last && ctx.Err() == nil && c.sleep(ctx, i) {
				continue
			}
			return err
		}
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(respBody) > 0 {
				return json.Unmarshal(respBody, out)
			}
			return nil
		}
		apiErr := newAPIError(resp, respBody)
		if (resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504) && !last && c.sleep(ctx, i) {
			lastErr = apiErr
			continue
		}
		return apiErr
	}
	return lastErr
}

// sleep waits out the backoff for retry i; false means the context ended.
func (c *Client) sleep(ctx context.Context, i int) bool {
	t := time.NewTimer(c.retryDelay << uint(i))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func newAPIError(resp *http.Response, body []byte) *APIError {
	var parsed struct {
		Error   string `json:"error"`
		Detail  string `json:"detail"`
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	_ = json.Unmarshal(body, &parsed)
	msg := parsed.Error
	if msg == "" {
		msg = parsed.Detail
	}
	if msg == "" {
		msg = parsed.Message
	}
	if msg == "" {
		msg = string(body)
	}
	e := &APIError{StatusCode: resp.StatusCode, Message: msg, Code: parsed.Code, Body: string(body)}
	if secs, err := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); err == nil && secs > 0 {
		e.RetryAfter = time.Duration(secs * float64(time.Second))
	}
	return e
}

// withQuery appends query parameters; the database is added only when one
// was configured.
func (c *Client) withQuery(path string, q url.Values) string {
	if q == nil {
		q = url.Values{}
	}
	if c.databaseExplicit {
		q.Set("dbname", c.database)
	}
	if len(q) == 0 {
		return path
	}
	return path + "?" + q.Encode()
}

type ActionResponse struct {
	Action     string          `json:"action"`
	Status     string          `json:"status"`
	DryRun     bool            `json:"dry_run"`
	DurationMS float64         `json:"duration_ms"`
	Results    json.RawMessage `json:"results"`
	OutboxID   *int64          `json:"outbox_id,omitempty"`
	// EffectsQueued is how many effects were queued to run after the SQL committed.
	EffectsQueued int `json:"effects_queued,omitempty"`
	// EffectsPreview, on a dry run, is what each effect would have done.
	EffectsPreview []EffectPreview `json:"effects_preview,omitempty"`
	Message        string          `json:"message,omitempty"`
}

// EffectPreview describes one effect a dry run would have performed.
type EffectPreview struct {
	Type     string         `json:"type"`
	WouldRun map[string]any `json:"would_run"`
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
	projectID := cfg.ProjectID
	if projectID == "" {
		projectID = os.Getenv("PGBHUB_PROJECT_ID")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("PGBHUB_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = os.Getenv("PGBHUB_PROJECT_URL")
	}
	if endpoint == "" {
		domain := os.Getenv("PGBHUB_PROJECT_DOMAIN")
		if domain == "" {
			domain = "pgbhub.app"
		}
		if projectID != "" {
			endpoint = "https://" + projectID + "." + domain
		} else {
			endpoint = "https://cloud.pgbhub.com"
		}
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("PGBHUB_API_KEY")
	}
	if apiKey == "" {
		apiKey = os.Getenv("PGBHUB_PUBLISHABLE_KEY")
	}
	db := cfg.Database
	if db == "" {
		db = os.Getenv("PGBHUB_DATABASE")
	}
	explicit := db != ""
	if db == "" {
		db = "postgres"
	}
	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		hc = &http.Client{Timeout: timeout}
	}
	retries := cfg.MaxRetries
	if retries == 0 {
		retries = 2
	}
	delay := cfg.RetryDelay
	if delay <= 0 {
		delay = 300 * time.Millisecond
	}

	c := &Client{
		endpoint:         strings.TrimRight(endpoint, "/"),
		apiKey:           apiKey,
		database:         db,
		databaseExplicit: explicit,
		projectID:        projectID,
		httpClient:       hc,
		maxRetries:       retries,
		retryDelay:       delay,
	}
	c.Auth = &AuthClient{client: c, Admin: &AuthAdminClient{client: c}}
	c.Actions = &ActionsClient{client: c}
	c.Storage = &StorageClient{client: c}
	c.Functions = &FunctionsClient{client: c}
	c.Cron = &CronClient{client: c}
	c.Jobs = &JobsClient{client: c}
	c.Queues = &QueuesClient{client: c}
	c.Realtime = &RealtimeClient{client: c}
	return c
}

func (c *Client) GetSchema(ctx context.Context) (*Schema, error) {
	var schema Schema
	path := fmt.Sprintf("/api/v1/databases/%s/schema.json", url.PathEscape(c.database))
	if err := c.doRequest(ctx, http.MethodGet, path, nil, nil, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// InvokeAction runs an action by name. With dryRun the SQL is simulated and
// the response lists what each effect would have done (nothing is run).
func (c *Client) InvokeAction(ctx context.Context, action string, params map[string]any, dryRun bool) (*ActionResponse, error) {
	body := map[string]any{"action": action, "params": params, "dry_run": dryRun}
	if c.databaseExplicit {
		body["dbname"] = c.database
	}
	var result ActionResponse
	if err := c.doRequest(ctx, http.MethodPost, "/v1/actions/invoke", body, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ─── Actions ────────────────────────────────────────────────────────────

// ActionEffect is something an action does after its SQL commits: a map with
// a "type" (function, queue, mail, event, job, realtime, container, action,
// webhook) and that type's fields. Values may use {{params.x}} and
// {{results.0.id}}.
type ActionEffect = map[string]any

// ActionDefinition is a registered action.
type ActionDefinition struct {
	Name         string         `json:"name"`
	Title        string         `json:"title,omitempty"`
	Description  string         `json:"description,omitempty"`
	SQLTemplate  string         `json:"sql_template,omitempty"`
	InputSchema  map[string]any `json:"input_schema,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
	SampleParams map[string]any `json:"sample_params,omitempty"`
	Effects      []ActionEffect `json:"effects,omitempty"`
	WebhookURL   string         `json:"webhook_url,omitempty"`
}

// RegisterActionOptions describes an action to create or replace.
type RegisterActionOptions struct {
	Name string
	// SQL uses %(param)s placeholders.
	SQL          string
	Title        string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any
	SampleParams map[string]any
	Effects      []ActionEffect
	WebhookURL   string
}

// ActionDelivery is one effect's outcome: delivered, waiting, or failed.
type ActionDelivery struct {
	ID       int64  `json:"id"`
	Action   string `json:"action"`
	Effect   string `json:"effect"`
	Status   string `json:"status"` // pending | dispatched | failed
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
	// CreatedAt is when the effect was queued.
	CreatedAt string `json:"created_at,omitempty"`
}

// DeliveryFilter narrows Deliveries; zero values mean "any".
type DeliveryFilter struct {
	Action string
	Status string
	Limit  int
}

// ActionsClient manages actions and their effects (client.Actions).
type ActionsClient struct{ client *Client }

// Run invokes an action (same as Client.InvokeAction).
func (a *ActionsClient) Run(ctx context.Context, name string, params map[string]any, dryRun bool) (*ActionResponse, error) {
	return a.client.InvokeAction(ctx, name, params, dryRun)
}

// List returns every registered action.
func (a *ActionsClient) List(ctx context.Context) ([]ActionDefinition, error) {
	var out struct {
		Actions []ActionDefinition `json:"actions"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, a.client.withQuery("/v1/actions", nil), nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Actions, nil
}

// Get returns one action.
func (a *ActionsClient) Get(ctx context.Context, name string) (*ActionDefinition, error) {
	var out ActionDefinition
	if err := a.client.doRequest(ctx, http.MethodGet, a.client.withQuery("/v1/actions/"+url.PathEscape(name), nil), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Register creates or replaces an action and its effects.
func (a *ActionsClient) Register(ctx context.Context, opts RegisterActionOptions) error {
	if opts.Name == "" || opts.SQL == "" {
		return fmt.Errorf("Register needs Name and SQL (use %%(param)s placeholders in the SQL)")
	}
	body := map[string]any{"name": opts.Name, "sql_template": opts.SQL, "description": opts.Description}
	set := func(k string, v any, present bool) {
		if present {
			body[k] = v
		}
	}
	set("title", opts.Title, opts.Title != "")
	set("input_schema", opts.InputSchema, opts.InputSchema != nil)
	set("output_schema", opts.OutputSchema, opts.OutputSchema != nil)
	set("sample_params", opts.SampleParams, opts.SampleParams != nil)
	set("effects", opts.Effects, opts.Effects != nil)
	set("webhook_url", opts.WebhookURL, opts.WebhookURL != "")
	return a.client.doRequest(ctx, http.MethodPost, a.client.withQuery("/v1/actions/register", nil), body, nil, nil)
}

// Delete removes an action.
func (a *ActionsClient) Delete(ctx context.Context, name string) error {
	return a.client.doRequest(ctx, http.MethodDelete, a.client.withQuery("/v1/actions/"+url.PathEscape(name), nil), nil, nil, nil)
}

// Deliveries lists what the actions' effects did.
func (a *ActionsClient) Deliveries(ctx context.Context, f DeliveryFilter) ([]ActionDelivery, error) {
	q := url.Values{}
	if f.Action != "" {
		q.Set("action", f.Action)
	}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	var out struct {
		Deliveries []ActionDelivery `json:"deliveries"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, a.client.withQuery("/v1/actions/deliveries", q), nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Deliveries, nil
}

// RetryDelivery puts a failed delivery back in the queue.
func (a *ActionsClient) RetryDelivery(ctx context.Context, id int64) error {
	path := fmt.Sprintf("/v1/actions/deliveries/%d/retry", id)
	return a.client.doRequest(ctx, http.MethodPost, a.client.withQuery(path, nil), map[string]any{}, nil, nil)
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

// AuthResult is returned by SignUp, VerifyOTP, VerifyMagicLink, VerifyMFA,
// OAuthCallback and RefreshSession. After SignUp, Session is nil and
// VerificationRequired is true when the project wants the email confirmed first.
type AuthResult struct {
	User                 *AuthUser    `json:"user"`
	Session              *AuthSession `json:"session"`
	VerificationRequired bool         `json:"verification_required,omitempty"`
	Message              string       `json:"message,omitempty"`
}

// LoginResult is returned by SignIn. Session is nil and MFARequired is
// true when the account has two-factor on: finish with VerifyMFA.
type LoginResult struct {
	User           *AuthUser    `json:"user,omitempty"`
	Session        *AuthSession `json:"session,omitempty"`
	MFARequired    bool         `json:"mfa_required,omitempty"`
	ChallengeToken string       `json:"challenge_token,omitempty"`
	ExpiresIn      int          `json:"expires_in,omitempty"`
}

// OTPRequestResult is returned by RequestOTP. The code itself is only ever
// emailed; the API never returns it.
type OTPRequestResult struct {
	Message   string `json:"message"`
	ExpiresIn int    `json:"expires_in"`
}

// MagicLinkRequestResult is returned by SendMagicLink. The link is only emailed.
type MagicLinkRequestResult struct {
	Message   string `json:"message"`
	ExpiresIn int    `json:"expires_in"`
}

// MessageResult is the plain acknowledgement many auth calls return.
type MessageResult struct {
	Message         string `json:"message"`
	SessionsRevoked int    `json:"sessions_revoked,omitempty"`
}

// AuthSettings is the project's sign-in rules, so a UI can adapt to them.
// Its shape follows the project's settings; fields are kept open.
type AuthSettings map[string]any

// AuthEvent is one entry in a user's sign-in and security history.
type AuthEvent struct {
	Type      string         `json:"type"`
	CreatedAt string         `json:"created_at,omitempty"`
	IP        string         `json:"ip,omitempty"`
	UserAgent string         `json:"user_agent,omitempty"`
	Detail    map[string]any `json:"detail,omitempty"`
}

// AuthSessionInfo is one signed-in device. IsCurrent marks the caller's own.
type AuthSessionInfo struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at,omitempty"`
	LastSeen  string `json:"last_seen_at,omitempty"`
	IP        string `json:"ip,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	IsCurrent bool   `json:"is_current"`
}

// MFAEnrollment is the result of EnrollMFA: show ProvisioningURI as a QR code.
type MFAEnrollment struct {
	Secret          string `json:"secret"`
	ProvisioningURI string `json:"provisioning_uri"`
}

// MFAConfirmation is returned by ConfirmMFA. Show the backup codes once.
type MFAConfirmation struct {
	Enabled     bool     `json:"enabled"`
	BackupCodes []string `json:"backup_codes"`
}

// OAuthStart is the provider's authorize URL and the state to check on return.
type OAuthStart struct {
	AuthorizeURL string `json:"authorize_url"`
	State        string `json:"state"`
}

// SignUpParams are the sign-up inputs. RedirectTo must be allowed in the
// project's auth settings.
type SignUpParams struct {
	Email      string
	Password   string
	Metadata   map[string]any
	RedirectTo string
}

// AuthClient is the Built-In PGB Auth-as-a-Service client (client.Auth).
//
// Two different bearer tokens are in play: the Client's own apiKey (a
// project Secret/Publishable key) identifies the *project*, sent as
// "Authorization: Bearer <key>" on every call; the *end user's* own
// access token (held in the session set by SignUp/SignIn/SetSession) is
// sent separately as "X-PGB-User-Token" on the calls that act as that user.
type AuthClient struct {
	client  *Client
	session *AuthSession

	// Admin covers Secret-key-only user management.
	Admin *AuthAdminClient
}

func (a *AuthClient) post(ctx context.Context, path string, body map[string]any, headers map[string]string, out any) error {
	return a.client.doRequest(ctx, http.MethodPost, path, body, headers, out)
}

// asUser returns the headers for acting as the signed-in user.
func (a *AuthClient) asUser() (map[string]string, error) {
	if a.session == nil || a.session.AccessToken == "" {
		return nil, errNotSignedIn
	}
	return map[string]string{"X-PGB-User-Token": a.session.AccessToken}, nil
}

func withRedirect(body map[string]any, redirectTo string) map[string]any {
	if redirectTo != "" {
		body["redirect_to"] = redirectTo
	}
	return body
}

// keep stores the session from a result that carries one.
func (a *AuthClient) keep(s *AuthSession) {
	if s != nil {
		a.session = s
	}
}

// Settings returns the project's sign-in rules. No sign-in needed.
func (a *AuthClient) Settings(ctx context.Context) (AuthSettings, error) {
	var out AuthSettings
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/settings", nil, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SignUp signs up a new user with email & password. metadata may be nil.
func (a *AuthClient) SignUp(ctx context.Context, email, password string, metadata map[string]any) (*AuthResult, error) {
	return a.SignUpWith(ctx, SignUpParams{Email: email, Password: password, Metadata: metadata})
}

// SignUpWith is SignUp with a redirect for the verification email. When the
// project requires verification the result has no Session.
func (a *AuthClient) SignUpWith(ctx context.Context, p SignUpParams) (*AuthResult, error) {
	meta := p.Metadata
	if meta == nil {
		meta = map[string]any{}
	}
	body := withRedirect(map[string]any{"email": p.Email, "password": p.Password, "metadata": meta}, p.RedirectTo)
	var result AuthResult
	if err := a.post(ctx, "/auth/signup", body, nil, &result); err != nil {
		return nil, err
	}
	a.keep(result.Session)
	return &result, nil
}

// SignIn signs in with email & password.
func (a *AuthClient) SignIn(ctx context.Context, email, password string) (*LoginResult, error) {
	var result LoginResult
	if err := a.post(ctx, "/auth/login", map[string]any{"email": email, "password": password}, nil, &result); err != nil {
		return nil, err
	}
	a.keep(result.Session)
	return &result, nil
}

// SignOut signs out and invalidates the current session. A no-op if
// there is no active session.
func (a *AuthClient) SignOut(ctx context.Context) error {
	if a.session == nil || a.session.AccessToken == "" {
		return nil
	}
	headers := map[string]string{"X-PGB-User-Token": a.session.AccessToken}
	err := a.post(ctx, "/auth/logout", map[string]any{}, headers, nil)
	a.session = nil
	return err
}

// GetUser returns the current user, or nil without a network call when
// signed out. An expired session (401/403) also returns nil; other failures
// are returned as errors.
func (a *AuthClient) GetUser(ctx context.Context) (*AuthUser, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, nil
	}
	var result struct {
		User *AuthUser `json:"user"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/user", nil, headers, &result); err != nil {
		if apiErr, ok := err.(*APIError); ok && (apiErr.StatusCode == 401 || apiErr.StatusCode == 403) {
			return nil, nil
		}
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
	var result AuthResult
	if err := a.post(ctx, "/auth/refresh", map[string]any{"refresh_token": token}, nil, &result); err != nil {
		return nil, err
	}
	a.keep(result.Session)
	return &result, nil
}

// RequestOTP emails a one-time passcode for passwordless sign-in.
func (a *AuthClient) RequestOTP(ctx context.Context, email string) (*OTPRequestResult, error) {
	var result OTPRequestResult
	if err := a.post(ctx, "/auth/otp", map[string]any{"email": email}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// VerifyOTP verifies a one-time passcode and establishes a session.
func (a *AuthClient) VerifyOTP(ctx context.Context, email, token string) (*AuthResult, error) {
	var result AuthResult
	if err := a.post(ctx, "/auth/otp/verify", map[string]any{"email": email, "token": token}, nil, &result); err != nil {
		return nil, err
	}
	a.keep(result.Session)
	return &result, nil
}

// SendMagicLink emails a passwordless magic link.
func (a *AuthClient) SendMagicLink(ctx context.Context, email string) (*MagicLinkRequestResult, error) {
	return a.SendMagicLinkTo(ctx, email, "")
}

// SendMagicLinkTo is SendMagicLink with a redirect allowed in the project's settings.
func (a *AuthClient) SendMagicLinkTo(ctx context.Context, email, redirectTo string) (*MagicLinkRequestResult, error) {
	var result MagicLinkRequestResult
	if err := a.post(ctx, "/auth/magic", withRedirect(map[string]any{"email": email}, redirectTo), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// VerifyMagicLink verifies a magic link token and establishes a session.
func (a *AuthClient) VerifyMagicLink(ctx context.Context, token string) (*AuthResult, error) {
	var result AuthResult
	if err := a.post(ctx, "/auth/magic/verify", map[string]any{"token": token}, nil, &result); err != nil {
		return nil, err
	}
	a.keep(result.Session)
	return &result, nil
}

// ResendVerification sends the email-verification message again.
func (a *AuthClient) ResendVerification(ctx context.Context, email, redirectTo string) (*MessageResult, error) {
	var out MessageResult
	if err := a.post(ctx, "/auth/resend-verification", withRedirect(map[string]any{"email": email}, redirectTo), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ForgotPassword emails a reset link. It always succeeds, so it never reveals
// which emails have accounts.
func (a *AuthClient) ForgotPassword(ctx context.Context, email, redirectTo string) (*MessageResult, error) {
	var out MessageResult
	if err := a.post(ctx, "/auth/forgot-password", withRedirect(map[string]any{"email": email}, redirectTo), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ResetPassword sets a new password using the token from the reset email.
func (a *AuthClient) ResetPassword(ctx context.Context, token, password string) (*MessageResult, error) {
	var out MessageResult
	if err := a.post(ctx, "/auth/reset-password", map[string]any{"token": token, "password": password}, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ChangePassword changes the signed-in user's password. Other sessions are
// signed out; this one stays.
func (a *AuthClient) ChangePassword(ctx context.Context, currentPassword, newPassword string) (*MessageResult, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out MessageResult
	body := map[string]any{"current_password": currentPassword, "new_password": newPassword}
	if err := a.post(ctx, "/auth/user/password", body, headers, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateUser merges values into the user's profile. A key set to nil is removed.
func (a *AuthClient) UpdateUser(ctx context.Context, metadata map[string]any) (*AuthUser, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out struct {
		User *AuthUser `json:"user"`
	}
	if err := a.post(ctx, "/auth/user/update", map[string]any{"user_metadata": metadata}, headers, &out); err != nil {
		return nil, err
	}
	return out.User, nil
}

// DeleteAccount permanently deletes the signed-in user's own account and
// clears the local session.
func (a *AuthClient) DeleteAccount(ctx context.Context, password string) error {
	headers, err := a.asUser()
	if err != nil {
		return err
	}
	if err := a.post(ctx, "/auth/user/delete", map[string]any{"password": password}, headers, nil); err != nil {
		return err
	}
	a.session = nil
	return nil
}

// Events returns the user's recent sign-ins and security events.
func (a *AuthClient) Events(ctx context.Context) ([]AuthEvent, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out struct {
		Events []AuthEvent `json:"events"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/user/events", nil, headers, &out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

// ListSessions returns the user's active sessions (devices).
func (a *AuthClient) ListSessions(ctx context.Context) ([]AuthSessionInfo, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out struct {
		Sessions []AuthSessionInfo `json:"sessions"`
	}
	if err := a.client.doRequest(ctx, http.MethodGet, "/auth/sessions", nil, headers, &out); err != nil {
		return nil, err
	}
	return out.Sessions, nil
}

// RevokeSession signs out one session (for example an old phone).
func (a *AuthClient) RevokeSession(ctx context.Context, sessionID string) error {
	headers, err := a.asUser()
	if err != nil {
		return err
	}
	return a.client.doRequest(ctx, http.MethodDelete, "/auth/sessions/"+url.PathEscape(sessionID), nil, headers, nil)
}

// RevokeOtherSessions signs out every session except this one and returns
// how many were ended.
func (a *AuthClient) RevokeOtherSessions(ctx context.Context) (int, error) {
	headers, err := a.asUser()
	if err != nil {
		return 0, err
	}
	var out struct {
		Revoked int `json:"revoked"`
	}
	if err := a.post(ctx, "/auth/sessions/revoke-all", map[string]any{}, headers, &out); err != nil {
		return 0, err
	}
	return out.Revoked, nil
}

// EnrollMFA starts two-factor enrolment; finish with ConfirmMFA.
func (a *AuthClient) EnrollMFA(ctx context.Context) (*MFAEnrollment, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out MFAEnrollment
	if err := a.post(ctx, "/auth/mfa/enroll", map[string]any{}, headers, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ConfirmMFA confirms enrolment with a current code and returns one-time backup codes.
func (a *AuthClient) ConfirmMFA(ctx context.Context, code string) (*MFAConfirmation, error) {
	headers, err := a.asUser()
	if err != nil {
		return nil, err
	}
	var out MFAConfirmation
	if err := a.post(ctx, "/auth/mfa/confirm", map[string]any{"code": code}, headers, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DisableMFA turns two-factor off (needs a current code).
func (a *AuthClient) DisableMFA(ctx context.Context, code string) error {
	headers, err := a.asUser()
	if err != nil {
		return err
	}
	return a.post(ctx, "/auth/mfa/disable", map[string]any{"code": code}, headers, nil)
}

// VerifyMFA finishes a sign-in that returned MFARequired.
func (a *AuthClient) VerifyMFA(ctx context.Context, challengeToken, code string) (*AuthResult, error) {
	var out AuthResult
	if err := a.post(ctx, "/auth/mfa/verify", map[string]any{"challenge_token": challengeToken, "code": code}, nil, &out); err != nil {
		return nil, err
	}
	a.keep(out.Session)
	return &out, nil
}

// OAuthStart returns the provider's authorize URL and the state to check on return.
func (a *AuthClient) OAuthStart(ctx context.Context, provider, redirectURI string) (*OAuthStart, error) {
	var out OAuthStart
	path := "/auth/oauth/" + url.PathEscape(provider) + "/start?" + url.Values{"redirect_uri": {redirectURI}}.Encode()
	if err := a.client.doRequest(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// OAuthCallback finishes a social sign-in with the code and state the provider sent back.
func (a *AuthClient) OAuthCallback(ctx context.Context, provider, code, state, redirectURI string) (*AuthResult, error) {
	var out AuthResult
	body := map[string]any{"code": code, "state": state, "redirect_uri": redirectURI}
	if err := a.post(ctx, "/auth/oauth/"+url.PathEscape(provider)+"/callback", body, nil, &out); err != nil {
		return nil, err
	}
	a.keep(out.Session)
	return &out, nil
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
