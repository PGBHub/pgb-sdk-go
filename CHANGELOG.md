# Changelog

## 1.2.0

- Mail: `client.Mail.Send(ctx, pgbhub.SendMailOptions{...})` posts to `/mail/send`. `ReplyTo` is sent as `reply_to`, only when set. `MailSendResult.MessageID` is filled from `MailID` when an older server returns only that.
- Mail: a failed send is an `*APIError` with `StatusCode` and `Code`; with no mail provider configured that is 503 and `ErrCodeMailProviderUnavailable` (`"MAIL_PROVIDER_UNAVAILABLE"`). `pgbhub.IsMailProviderUnavailable(err)` checks for it. Sends are never retried.

## 1.1.0

**Fixes (these affect existing apps)**
- `ProjectID` alone now targets `https://<project>.pgbhub.app`; docs pointed at `console.pgbhub.com`. Override with `PGBHUB_PROJECT_DOMAIN`.
- A database name is only sent when you configure one (project keys cannot use the default `postgres`).
- `InvokeAction` and `GetSchema` return `*APIError` like every other call, with the server's message instead of a raw body.
- Default request timeout is 30s (was 15s).
- `OTPRequestResult.OTP` and `MagicLinkRequestResult.Token` are removed: the platform never returns them, they are only emailed.
- `GetUser` returns nil only for a signed-out or expired session; other failures are errors.

**New**
- `APIError` gains `Code`, `RetryAfter`, `Body`.
- `Config.Timeout`, `MaxRetries`, `RetryDelay`; reads retry transient failures, writes never do.
- Auth: `Settings`, `SignUpWith`, `SendMagicLinkTo`, `ResendVerification`, `ForgotPassword`, `ResetPassword`, `ChangePassword`, `UpdateUser`, `DeleteAccount`, `Events`, `ListSessions`, `RevokeSession`, `RevokeOtherSessions`, `EnrollMFA`, `ConfirmMFA`, `DisableMFA`, `VerifyMFA`, `OAuthStart`, `OAuthCallback`.
- `client.Actions`: `Run`, `List`, `Get`, `Register` (with effects), `Delete`, `Deliveries`, `RetryDelivery`; `EffectsQueued` / `EffectsPreview` on responses.
