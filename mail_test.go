// Mail: reply_to on the wire, MessageID (with the mail_id fallback) and
// the typed 503 MAIL_PROVIDER_UNAVAILABLE error, against httptest.Server.
package pgbhub

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestMailSendSendsReplyTo(t *testing.T) {
	var got map[string]any
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/mail/send" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("missing auth header")
		}
		got = decodeBody(t, r)
		respond(w, 200, `{"success":true,"message_id":"m1","mail_id":"m1","provider":"ses","status":"sent"}`)
	}, Config{})
	res, err := c.Mail.Send(context.Background(), SendMailOptions{To: "a@b.co", Subject: "Hi", Text: "Body", ReplyTo: "support@acme.dev"})
	if err != nil {
		t.Fatal(err)
	}
	if got["reply_to"] != "support@acme.dev" || got["to"] != "a@b.co" || got["subject"] != "Hi" || got["text"] != "Body" {
		t.Fatalf("body: %v", got)
	}
	if _, ok := got["replyTo"]; ok {
		t.Fatal("replyTo must not be sent")
	}
	if res.MessageID != "m1" || !res.Success || res.Provider != "ses" {
		t.Fatalf("result: %+v", res)
	}
}

func TestMailSendWithoutReplyToOmitsIt(t *testing.T) {
	var got map[string]any
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = decodeBody(t, r)
		respond(w, 200, `{"success":true,"message_id":"m1"}`)
	}, Config{})
	if _, err := c.Mail.Send(context.Background(), SendMailOptions{To: "a@b.co", Subject: "Hi", Text: "Body", HTML: "<p>Body</p>", From: "no-reply@acme.dev"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["reply_to"]; ok {
		t.Fatalf("reply_to sent without ReplyTo: %v", got)
	}
	if got["html"] != "<p>Body</p>" || got["from"] != "no-reply@acme.dev" {
		t.Fatalf("body: %v", got)
	}
}

func TestMailSendMessageIDFallsBackToMailID(t *testing.T) {
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, `{"success":true,"mail_id":"old1"}`)
	}, Config{})
	res, err := c.Mail.Send(context.Background(), SendMailOptions{To: "a@b.co", Subject: "Hi", Text: "Body"})
	if err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "old1" || res.MailID != "old1" {
		t.Fatalf("result: %+v", res)
	}
}

func TestMailSendProviderUnavailableIsTyped(t *testing.T) {
	var hits int32
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		respond(w, 503, `{"error":"Email dispatch failed: no provider","code":"MAIL_PROVIDER_UNAVAILABLE"}`)
	}, Config{})
	_, err := c.Mail.Send(context.Background(), SendMailOptions{To: "a@b.co", Subject: "Hi", Text: "Body"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.StatusCode != 503 || apiErr.Code != ErrCodeMailProviderUnavailable || apiErr.Message != "Email dispatch failed: no provider" {
		t.Fatalf("error: %+v", apiErr)
	}
	if !IsMailProviderUnavailable(err) {
		t.Fatal("IsMailProviderUnavailable should be true")
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Fatalf("a send must not be retried, got %d requests", n)
	}
}

func TestMailSendBadReplyToKeepsServerMessage(t *testing.T) {
	c := fastClient(t, func(w http.ResponseWriter, r *http.Request) {
		respond(w, 400, `{"error":"Invalid reply_to email address: 'a@b.co, c@d.co'"}`)
	}, Config{})
	_, err := c.Mail.Send(context.Background(), SendMailOptions{To: "a@b.co", Subject: "Hi", Text: "Body", ReplyTo: "a@b.co, c@d.co"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 400 || apiErr.Code != "" {
		t.Fatalf("error: %v", err)
	}
	if IsMailProviderUnavailable(err) || IsMailProviderUnavailable(nil) {
		t.Fatal("only the 503 code is MAIL_PROVIDER_UNAVAILABLE")
	}
}
