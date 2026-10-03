// Regression tests for the Round 1 additions: AuthClient (+ Admin),
// StorageClient.GetFile/GetMedia, FunctionsClient.Deploy/Delete, and
// Client.CreateDatabase.
//
// Runs the real client against an httptest.Server (not a
// reimplementation), pinning the exact HTTP method/path/body/headers
// each method sends.
package pgbhub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c := NewClient(Config{
		Endpoint: server.URL,
		APIKey:   "pgb_sec_test",
		Database: "appdb",
	})
	return c, server
}

func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("failed to read request body: %v", err)
	}
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
	return m
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func TestAuthSignUpStoresSession(t *testing.T) {
	var gotPath, gotMethod string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		writeJSON(w, 201, map[string]any{
			"user":    map[string]any{"id": "usr_1", "email": "a@test.com"},
			"session": map[string]any{"access_token": "at1", "refresh_token": "rt1"},
		})
	})

	result, err := client.Auth.SignUp(context.Background(), "a@test.com", "pw", nil)
	if err != nil {
		t.Fatalf("SignUp failed: %v", err)
	}
	if gotPath != "/auth/signup" || gotMethod != http.MethodPost {
		t.Fatalf("expected POST /auth/signup, got %s %s", gotMethod, gotPath)
	}
	if result.Session == nil || result.Session.AccessToken != "at1" {
		t.Fatalf("expected session with access_token=at1, got %+v", result.Session)
	}
	if session := client.Auth.GetSession(); session == nil || session.AccessToken != "at1" {
		t.Fatalf("expected GetSession() to return the stored session, got %+v", session)
	}
}

func TestAuthSignInFailureSurfacesServerMessage(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, map[string]any{"error": "Invalid email or password."})
	})

	_, err := client.Auth.SignIn(context.Background(), "a@test.com", "wrong")
	if err == nil {
		t.Fatal("expected an error")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 400 || !strings.Contains(apiErr.Message, "Invalid email or password") {
		t.Fatalf("unexpected APIError: %+v", apiErr)
	}
	if client.Auth.GetSession() != nil {
		t.Fatal("expected no session to be stored after a failed sign-in")
	}
}

func TestAuthSignOutSendsUserTokenHeader(t *testing.T) {
	var gotHeader, gotPath string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-PGB-User-Token")
		gotPath = r.URL.Path
		writeJSON(w, 200, map[string]any{"success": true})
	})
	client.Auth.SetSession(&AuthSession{AccessToken: "at1", RefreshToken: "rt1"})

	if err := client.Auth.SignOut(context.Background()); err != nil {
		t.Fatalf("SignOut failed: %v", err)
	}
	if gotPath != "/auth/logout" {
		t.Fatalf("expected /auth/logout, got %s", gotPath)
	}
	if gotHeader != "at1" {
		t.Fatalf("expected X-PGB-User-Token=at1, got %q", gotHeader)
	}
	if client.Auth.GetSession() != nil {
		t.Fatal("expected session to be cleared after sign-out")
	}
}

func TestAuthGetUserWithoutSessionMakesNoNetworkCall(t *testing.T) {
	called := false
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		writeJSON(w, 200, map[string]any{})
	})

	user, err := client.Auth.GetUser(context.Background())
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if user != nil {
		t.Fatalf("expected nil user, got %+v", user)
	}
	if called {
		t.Fatal("expected no network call when there is no active session")
	}
}

func TestAuthAdminCreateListDeleteUser(t *testing.T) {
	step := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch step {
		case 0:
			if r.Method != http.MethodPost || r.URL.Path != "/auth/admin/users" {
				t.Fatalf("expected POST /auth/admin/users, got %s %s", r.Method, r.URL.Path)
			}
			writeJSON(w, 201, map[string]any{"user": map[string]any{"id": "usr_2", "email": "b@test.com"}})
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/auth/admin/users" {
				t.Fatalf("expected GET /auth/admin/users, got %s %s", r.Method, r.URL.Path)
			}
			writeJSON(w, 200, map[string]any{"users": []map[string]any{{"id": "usr_2", "email": "b@test.com"}}})
		case 2:
			if r.Method != http.MethodDelete || r.URL.Path != "/auth/admin/users/usr_2" {
				t.Fatalf("expected DELETE /auth/admin/users/usr_2, got %s %s", r.Method, r.URL.Path)
			}
			writeJSON(w, 200, map[string]any{"success": true})
		}
		step++
	})

	created, err := client.Auth.Admin.CreateUser(context.Background(), "b@test.com", "", nil, true)
	if err != nil || created.Email != "b@test.com" {
		t.Fatalf("CreateUser failed: %v, %+v", err, created)
	}
	users, err := client.Auth.Admin.ListUsers(context.Background())
	if err != nil || len(users) != 1 {
		t.Fatalf("ListUsers failed: %v, %+v", err, users)
	}
	if err := client.Auth.Admin.DeleteUser(context.Background(), "usr_2"); err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}
}

func TestStorageGetFileReturnsBytesAndContentType(t *testing.T) {
	var gotPath string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	})

	file, err := client.Storage.GetFile(context.Background(), "/notes/hello.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if gotPath != "/storage/notes/hello.txt" {
		t.Fatalf("expected /storage/notes/hello.txt, got %s", gotPath)
	}
	if string(file.Data) != "hello" || file.ContentType != "text/plain" || file.ETag != `"abc"` {
		t.Fatalf("unexpected file: %+v", file)
	}
}

func TestStorageGetMediaIsAliasOverGetFile(t *testing.T) {
	calls := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.URL.RawQuery, "width") {
			t.Fatal("transform params must not be sent — the server does not support them yet")
		}
		writeJSON(w, 200, map[string]any{})
	})

	_, err := client.Storage.GetMedia(context.Background(), "/avatars/pic.png", &MediaTransformOptions{Width: 200, Height: 200})
	if err != nil {
		t.Fatalf("GetMedia failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly one request, got %d", calls)
	}
}

func TestFunctionsDeployPostsNameInBody(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = decodeBody(t, r)
		writeJSON(w, 201, map[string]any{"success": true, "function": map[string]any{"name": "send-welcome"}})
	})

	isEnabled := true
	fn, err := client.Functions.Deploy(context.Background(), "send-welcome", "export default () => {}", &DeployFunctionOptions{
		TimeoutMS: 8000,
		IsEnabled: &isEnabled,
	})
	if err != nil {
		t.Fatalf("Deploy failed: %v", err)
	}
	if gotPath != "/api/v1/databases/appdb/functions" {
		t.Fatalf("expected /api/v1/databases/appdb/functions, got %s", gotPath)
	}
	if gotBody["name"] != "send-welcome" {
		t.Fatalf("expected name in body, got %+v", gotBody)
	}
	if fn.Name != "send-welcome" {
		t.Fatalf("expected function name 'send-welcome', got %+v", fn)
	}
}

func TestFunctionsDeleteSendsNameInURL(t *testing.T) {
	var gotPath, gotMethod string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		writeJSON(w, 200, map[string]any{"success": true})
	})

	if err := client.Functions.Delete(context.Background(), "send-welcome"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/api/v1/databases/appdb/functions/send-welcome" {
		t.Fatalf("expected DELETE /api/v1/databases/appdb/functions/send-welcome, got %s %s", gotMethod, gotPath)
	}
}

func TestCreateDatabasePostsNameAndDefaultsPgVersion(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody = decodeBody(t, r)
		writeJSON(w, 201, map[string]any{
			"success": true, "database": "newdb", "pg_version": "17",
			"user": "u", "password": "p", "public_host": "h", "public_port": 5433, "url": "postgres://...", "message": "ready",
		})
	})

	result, err := client.CreateDatabase(context.Background(), "newdb", nil)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if gotPath != "/api/v1/databases" {
		t.Fatalf("expected /api/v1/databases, got %s", gotPath)
	}
	if gotBody["name"] != "newdb" || gotBody["pg_version"] != "17" {
		t.Fatalf("unexpected body: %+v", gotBody)
	}
	if result.Database != "newdb" {
		t.Fatalf("expected database=newdb, got %+v", result)
	}
}

func TestCreateDatabaseSurfaces403ForProjectScopedKey(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 403, map[string]any{"error": "This API key is scoped to a single project and cannot provision new databases."})
	})

	_, err := client.CreateDatabase(context.Background(), "newdb", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 403 || !strings.Contains(apiErr.Message, "scoped to a single project") {
		t.Fatalf("unexpected APIError: %+v", apiErr)
	}
}
