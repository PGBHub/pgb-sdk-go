// 1.1.0: project address, APIError details, GET retries, the full auth
// surface and Actions management, against a real httptest.Server.
package pgbhub

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func fastClient(t *testing.T, h http.HandlerFunc, cfg Config) *Client {
	t.Helper()
	_, srv := newTestClient(t, h)
	cfg.Endpoint, cfg.APIKey = srv.URL, "k"
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = time.Millisecond
	}
	return NewClient(cfg)
}

func TestEndpointResolution(t *testing.T) {
	t.Setenv("PGBHUB_ENDPOINT", "")
	t.Setenv("PGBHUB_PROJECT_URL", "")
	t.Setenv("PGBHUB_PROJECT_DOMAIN", "")
	t.Setenv("PGBHUB_DATABASE", "")
	if got := NewClient(Config{ProjectID: "shop"}).endpoint; got != "https://shop.pgbhub.app" {
		t.Fatalf("project address: %s", got)
	}
	if got := NewClient(Config{}).endpoint; got != "https://cloud.pgbhub.com" {
		t.Fatalf("default: %s", got)
	}
	t.Setenv("PGBHUB_PROJECT_DOMAIN", "example.test")
	if got := NewClient(Config{ProjectID: "shop"}).endpoint; got != "https://shop.example.test" {
		t.Fatalf("domain override: %s", got)
	}
	if NewClient(Config{}).databaseExplicit || !NewClient(Config{Database: "x"}).databaseExplicit {
		t.Fatal("database should only be explicit when configured")
	}
}

func TestAPIErrorCarriesCodeAndRetryAfter(t *testing.T) {
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		respond(w, 429, `{"error":"Too many","code":"rate_limited"}`)
	}, Config{})
	_, err := c.Auth.SignIn(context.Background(), "a@b.c", "pw")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.StatusCode != 429 || apiErr.Code != "rate_limited" || apiErr.Message != "Too many" || apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("unexpected: %+v", apiErr)
	}
}

func TestGetsRetryButWritesDoNot(t *testing.T) {
	var calls int32
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			respond(w, 503, `{"error":"busy"}`)
			return
		}
		respond(w, 200, `{"actions":[]}`)
	}, Config{})
	if _, err := c.Actions.List(context.Background()); err != nil || calls != 2 {
		t.Fatalf("GET should retry once: err=%v calls=%d", err, calls)
	}

	calls = 0
	c = fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		respond(w, 503, `{"error":"busy"}`)
	}, Config{})
	if _, err := c.Auth.SignIn(context.Background(), "a", "b"); err == nil || calls != 1 {
		t.Fatalf("POST must not retry: err=%v calls=%d", err, calls)
	}
}

func TestSignUpWithVerificationKeepsNoSession(t *testing.T) {
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		b := decodeBody(t, r)
		if r.URL.Path != "/auth/signup" || b["redirect_to"] != "https://app/verified" {
			t.Errorf("bad request %s %v", r.URL.Path, b)
		}
		respond(w, 201, `{"user":{"id":"u"},"session":null,"verification_required":true}`)
	}, Config{})
	res, err := c.Auth.SignUpWith(context.Background(), SignUpParams{Email: "a@b.c", Password: "pw", RedirectTo: "https://app/verified"})
	if err != nil || !res.VerificationRequired || c.Auth.GetSession() != nil {
		t.Fatalf("unexpected: %+v %v", res, err)
	}
}

func TestUserCallsNeedASessionAndSendTheToken(t *testing.T) {
	var seen string
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("X-PGB-User-Token")
		switch r.URL.Path {
		case "/auth/user/password":
			b := decodeBody(t, r)
			if b["current_password"] != "old" || b["new_password"] != "new" {
				t.Errorf("body %v", b)
			}
			respond(w, 200, `{"message":"ok","sessions_revoked":2}`)
		case "/auth/sessions":
			respond(w, 200, `{"sessions":[{"id":"s1","is_current":true}]}`)
		case "/auth/sessions/revoke-all":
			respond(w, 200, `{"revoked":3}`)
		case "/auth/user/events":
			respond(w, 200, `{"events":[{"type":"login"}]}`)
		case "/auth/user/delete":
			respond(w, 200, `{"message":"deleted"}`)
		default:
			respond(w, 200, `{}`)
		}
	}, Config{})
	ctx := context.Background()

	var apiErr *APIError
	if _, err := c.Auth.ChangePassword(ctx, "old", "new"); !errors.As(err, &apiErr) || apiErr.Code != "not_signed_in" {
		t.Fatalf("want not_signed_in, got %v", err)
	}
	c.Auth.SetSession(&AuthSession{AccessToken: "at"})
	if r, err := c.Auth.ChangePassword(ctx, "old", "new"); err != nil || r.SessionsRevoked != 2 || seen != "at" {
		t.Fatalf("change password: %+v %v token=%s", r, err, seen)
	}
	if s, _ := c.Auth.ListSessions(ctx); len(s) != 1 || !s[0].IsCurrent {
		t.Fatalf("sessions %+v", s)
	}
	if n, _ := c.Auth.RevokeOtherSessions(ctx); n != 3 {
		t.Fatalf("revoked %d", n)
	}
	if e, _ := c.Auth.Events(ctx); len(e) != 1 {
		t.Fatalf("events %+v", e)
	}
	if err := c.Auth.DeleteAccount(ctx, "pw"); err != nil || c.Auth.GetSession() != nil {
		t.Fatalf("delete account should clear the session: %v", err)
	}
}

func TestMFALoginThenVerify(t *testing.T) {
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/login" {
			respond(w, 200, `{"mfa_required":true,"challenge_token":"ch"}`)
			return
		}
		respond(w, 200, `{"session":{"access_token":"at2"}}`)
	}, Config{})
	ctx := context.Background()
	login, err := c.Auth.SignIn(ctx, "a", "b")
	if err != nil || !login.MFARequired || c.Auth.GetSession() != nil {
		t.Fatalf("login: %+v %v", login, err)
	}
	if _, err := c.Auth.VerifyMFA(ctx, login.ChallengeToken, "123456"); err != nil || c.Auth.GetSession().AccessToken != "at2" {
		t.Fatalf("verify: %v", err)
	}
}

func TestPasswordFlowsAndOAuthPaths(t *testing.T) {
	var path, query string
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		path, query = r.URL.Path, r.URL.RawQuery
		respond(w, 200, `{"message":"ok","authorize_url":"https://g","state":"s"}`)
	}, Config{})
	ctx := context.Background()
	for _, tc := range []struct {
		want string
		call func() error
	}{
		{"/auth/forgot-password", func() error { _, e := c.Auth.ForgotPassword(ctx, "a@b.c", "https://app/reset"); return e }},
		{"/auth/reset-password", func() error { _, e := c.Auth.ResetPassword(ctx, "tok", "pw"); return e }},
		{"/auth/resend-verification", func() error { _, e := c.Auth.ResendVerification(ctx, "a@b.c", ""); return e }},
		{"/auth/magic", func() error { _, e := c.Auth.SendMagicLinkTo(ctx, "a@b.c", "https://app/in"); return e }},
	} {
		if err := tc.call(); err != nil || path != tc.want {
			t.Fatalf("%s: err=%v got %s", tc.want, err, path)
		}
	}
	o, err := c.Auth.OAuthStart(ctx, "google", "https://app/cb")
	if err != nil || o.AuthorizeURL != "https://g" || path != "/auth/oauth/google/start" || query != "redirect_uri=https%3A%2F%2Fapp%2Fcb" {
		t.Fatalf("oauth start: %+v %v %s?%s", o, err, path, query)
	}
}

func TestActionsManagement(t *testing.T) {
	var method, path, query string
	var body map[string]any
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, path, query = r.Method, r.URL.Path, r.URL.RawQuery
		body = decodeBody(t, r)
		switch {
		case path == "/v1/actions/invoke":
			respond(w, 200, `{"action":"a","status":"simulated","dry_run":true,"effects_preview":[{"type":"mail","would_run":{"to":"x"}}]}`)
		case path == "/v1/actions/deliveries":
			respond(w, 200, `{"deliveries":[{"id":4,"status":"failed","effect":"mail"}]}`)
		case path == "/v1/actions":
			respond(w, 200, `{"actions":[{"name":"a"}]}`)
		default:
			respond(w, 200, `{}`)
		}
	}, Config{})
	ctx := context.Background()

	res, err := c.Actions.Run(ctx, "a", map[string]any{"x": 1}, true)
	if err != nil || len(res.EffectsPreview) != 1 || res.EffectsPreview[0].Type != "mail" {
		t.Fatalf("run: %+v %v", res, err)
	}
	if _, has := body["dbname"]; has {
		t.Fatal("dbname must be left out unless configured")
	}
	if err := c.Actions.Register(ctx, RegisterActionOptions{Name: "signup", SQL: "SELECT 1", Effects: []ActionEffect{{"type": "mail"}}}); err != nil || path != "/v1/actions/register" || body["sql_template"] != "SELECT 1" {
		t.Fatalf("register: %v %s %v", err, path, body)
	}
	if err := c.Actions.Register(ctx, RegisterActionOptions{Name: "x"}); err == nil {
		t.Fatal("register without SQL should fail")
	}
	if l, _ := c.Actions.List(ctx); len(l) != 1 {
		t.Fatalf("list %+v", l)
	}
	if err := c.Actions.Delete(ctx, "a b"); err != nil || method != http.MethodDelete || path != "/v1/actions/a b" {
		t.Fatalf("delete: %v %s %s", err, method, path)
	}
	if d, _ := c.Actions.Deliveries(ctx, DeliveryFilter{Status: "failed", Limit: 5}); len(d) != 1 || query != "limit=5&status=failed" {
		t.Fatalf("deliveries %+v ?%s", d, query)
	}
	if err := c.Actions.RetryDelivery(ctx, 4); err != nil || path != "/v1/actions/deliveries/4/retry" {
		t.Fatalf("retry: %v %s", err, path)
	}
}

func TestConfiguredDatabaseIsSentAsDbname(t *testing.T) {
	var query string
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		respond(w, 200, `{"actions":[]}`)
	}, Config{Database: "app"})
	if _, err := c.Actions.List(context.Background()); err != nil || query != "dbname=app" {
		t.Fatalf("err=%v query=%s", err, query)
	}
}
