# pgb-sdk-go

Official Go client SDK for **PGB Hub**.

## Installation

```bash
go get github.com/PGBHub/pgb-sdk-go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/PGBHub/pgb-sdk-go"
)

func main() {
	// ProjectID alone targets https://my-project.pgbhub.app. The key picks the
	// database; set Database only to name one explicitly.
	client := pgbhub.NewClient(pgbhub.Config{
		APIKey:    "your_api_key",
		ProjectID: "my-project",
	})

	ctx := context.Background()

	// Invoke a declarative action
	res, err := client.InvokeAction(ctx, "approveOrder", map[string]any{
		"orderId": "ord_102",
		"userId":  "usr_99",
	}, false)
	if err != nil {
		log.Fatalf("Action failed: %v", err)
	}

	fmt.Printf("Status: %s (took %.2f ms)\n", res.Status, res.DurationMS)
	fmt.Printf("Results: %s\n", string(res.Results))
}
```

## Configuration

Every field can come from the environment: `PGBHUB_API_KEY` (or `PGBHUB_PUBLISHABLE_KEY`), `PGBHUB_PROJECT_ID`, `PGBHUB_ENDPOINT` / `PGBHUB_PROJECT_URL`, `PGBHUB_PROJECT_DOMAIN` (default `pgbhub.app`), `PGBHUB_DATABASE`.
`Config` also takes `Timeout` (30s), `MaxRetries` (2) and `RetryDelay` (300ms). Reads retry on 502/503/504 and connection errors; writes never retry.

## Errors

Failures are `*pgbhub.APIError` with the server's message, `StatusCode`, `Code`, `RetryAfter` (rate limits) and the raw `Body`:

```go
var apiErr *pgbhub.APIError
if errors.As(err, &apiErr) && apiErr.StatusCode == 429 {
	time.Sleep(apiErr.RetryAfter)
}
```

## Auth

```go
auth := client.Auth

res, _ := auth.SignUpWith(ctx, pgbhub.SignUpParams{Email: "a@example.com", Password: "a-long-password", RedirectTo: "https://app.example.com/welcome"})
if res.VerificationRequired { /* no session yet: the user must click the emailed link */ }

login, _ := auth.SignIn(ctx, "a@example.com", "a-long-password")
if login.MFARequired {
	auth.VerifyMFA(ctx, login.ChallengeToken, "123456")
}

auth.ForgotPassword(ctx, "a@example.com", "https://app.example.com/reset")
auth.ResetPassword(ctx, tokenFromTheLink, "a-new-long-password")
auth.RequestOTP(ctx, email);  auth.VerifyOTP(ctx, email, "123456")
auth.SendMagicLinkTo(ctx, email, redirect); auth.VerifyMagicLink(ctx, token)

auth.ChangePassword(ctx, "old", "new")
auth.UpdateUser(ctx, map[string]any{"plan": "pro"})
auth.Events(ctx); auth.ListSessions(ctx); auth.RevokeOtherSessions(ctx)
auth.DeleteAccount(ctx, "password")

auth.OAuthStart(ctx, "google", "https://app.example.com/callback")
auth.Settings(ctx) // which methods are on, password policy, providers
```

One-time codes and links are only ever emailed; the API never returns them.

## Actions

```go
res, _ := client.Actions.Run(ctx, "saveUser", map[string]any{"email": "a@example.com"}, true) // dry run
fmt.Println(res.EffectsPreview)

client.Actions.Register(ctx, pgbhub.RegisterActionOptions{
	Name: "signup",
	SQL:  "INSERT INTO users (email) VALUES (%(email)s) RETURNING id",
	Effects: []pgbhub.ActionEffect{
		{"type": "mail", "to": "{{params.email}}", "subject": "Welcome", "body": "Hi!"},
		{"type": "queue", "queue": "onboarding", "payload": map[string]any{"user": "{{results.0.id}}"}},
	},
})
client.Actions.List(ctx); client.Actions.Get(ctx, "signup"); client.Actions.Delete(ctx, "signup")
client.Actions.Deliveries(ctx, pgbhub.DeliveryFilter{Status: "failed"}); client.Actions.RetryDelivery(ctx, 12)
```

Effects run after the SQL commits, retry on their own, and can target a function, queue, mail, event, job, realtime channel, container, another action, or a webhook.

## License

MIT © [PGBHub](https://github.com/PGBHub)
