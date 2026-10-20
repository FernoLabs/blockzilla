# Foundation monitor: Megaloadon

This Worker checks the Foundation machine from outside the machine. The display
name is **Megaloadon**. The host name remains `blockzilla`. The host sends a
heartbeat every 30 seconds. A heartbeat that is missing for 90 seconds creates a
failure event at the next external check. Other checks cover capture, compaction,
cloud upload, local storage, required services, and the free public blockhash
stream.

The deployed host and pipeline record is in
[the Foundation deployment record](../../hivezilla/deploy/foundation-watch/README.md).

## Telegram delivery

Store these two values as Worker secrets:

| Secret | Purpose |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | The token for the new alert bot from BotFather. |
| `TELEGRAM_CHAT_ID` | The numeric chat ID that must receive alerts. A channel username in `@username` form is also supported. |

The recipient must start a private chat with the bot before the bot can send
messages. Group and channel delivery requires the bot to have the applicable
access. Keep the token and chat ID out of source, command arguments, reports,
and logs. Use private file input with `wrangler secret bulk` or the secure input
for `wrangler secret put`. `.dev.vars` contains fake values for local checks.

When either Telegram secret is set, both must be valid. A partial or invalid
Telegram configuration prevents delivery. The Worker does not send the alert to
another destination. With both secrets valid, Telegram takes priority over
`ALERT_WEBHOOK_URL`. When neither is set, the existing generic webhook path is
available.

The Worker calls only the fixed Telegram `sendMessage` API over HTTPS. It does
not follow redirects. Requests have a 10-second timeout. Replies are limited to
64 KiB. A successful HTTP status alone does not prove delivery: the JSON reply
must have `ok: true`, a valid message ID, and the specified destination chat.
Raw Telegram replies, request URLs, tokens, and chat IDs are not logged or saved.
Errors use fixed codes.

Messages use plain text, have audible notifications enabled, disable link
previews, and stay within 4096 characters. Each message includes its stable alert
event ID. Telegram has no documented idempotency key for `sendMessage`. If
Telegram accepts a message but its reply is lost, a retry can create a duplicate
with the same event ID. The durable outbox retries the oldest pending event and
preserves failure/recovery order.

Use the private operator token to call `POST /test-alert` after the bot and chat
are configured. This sends a clearly marked test without stopping capture. Then
check `GET /status`: `alert_delivery_channel` must be `telegram`,
`alert_delivery_configured` must be true, `pending_alerts` must be zero, and
`lastDeliveryMs` must advance. Confirm the test in the recipient's Telegram chat
before reporting that notifications are active.

## Private alert history

`GET /history` uses the same operator Bearer token as `GET /status`. It reads
stored alerts without running checks, changing alarms or state, or sending a
message. Other methods return 405 after authentication; missing or wrong
credentials return 401.

The response uses schema `blockzilla-foundation-alert-history-v1`. It contains
up to 100 retained alerts, newest insertion first. Each alert contains only
`id`, `kind`, `issues`, `createdMs`, `status`, and `attempts`. The fixed query
reads at most 101 rows; `truncated` is true when a 101st row exists. Query
parameters cannot change this limit or order.

Existing retention keeps the newest 100 delivered alerts and all pending or
sending alerts. This endpoint is not a complete lifetime history. Corrupt
visible records produce a fixed `alert_history_unavailable` response with HTTP
500 and no partial history. Responses use `Cache-Control: no-store`.

The [18 October deployment check](../../hivezilla/deploy/foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/HISTORY-DEPLOY-VALIDATION.json)
verified the deployed bundle, preserved monitor state, and both authentication
paths with GET requests only.

## Private Telegram commands

The owner can send `/status` or `/help` to `@megaloadon_alerts_bot`. Commands
are read-only. `/status` shows the last machine sample, capture, live
compaction, live cloud upload, native storage, disk space, and current faults.
When the heartbeat contains archive telemetry, the reply also shows the current
archive epoch and stage, recent completed work, queued epochs, and available ETA.
The stage ETA includes only that stage. Epoch and plan estimates use recent runs;
live work can pause background archive work. Missing heartbeats, old reports,
paused jobs, and unavailable data do not produce a current ETA.
Command dates use Europe/Paris time and carry a Paris label. The stored
timestamps remain UTC numbers. Recent-run ETA ranges round outward to five
minutes for one epoch and fifteen minutes for the full plan. Stage ETA keeps
the engine's reported precision.
Progress percent, transfer rate, and stage ETA require a matching progress
timestamp no older than 120 seconds. A fresh heartbeat alone does not make old
log progress current. The stage-start time can be old during a long stage.

Before commands can receive updates, generate a **new** random webhook token
(32 to 256 characters from `A-Z`, `a-z`, `0-9`, `_`, and `-`) and store it as
the Worker secret `TELEGRAM_WEBHOOK_SECRET`. Use the bot API `setWebhook` with:

- `url`: the deployed Worker URL plus `/telegram`.
- `secret_token`: the same new webhook token.
- `allowed_updates`: `["message"]`.
- `max_connections`: `1`.
- `drop_pending_updates`: `false`.

Keep the bot token and webhook token in private file input. Do not put them in
command arguments, source, URLs, or reports. Check the reply from `setWebhook`
and `getWebhookInfo`, then verify a real `/status` command from the owner's
private chat. `getUpdates` cannot be used while a webhook is configured.

`POST /telegram` requires the `X-Telegram-Bot-Api-Secret-Token` header. This
token is separate from the heartbeat and operator tokens. The body is limited
to 64 KiB. Both `message.chat.id` and `message.from.id` must equal the configured
**positive numeric** `TELEGRAM_CHAT_ID`; the chat must be private and the sender
must be a human account. Username destinations, groups, other senders, bot
messages, and unknown commands receive no information or outbound reply.

Accepted commands enter a SQLite outbox before the webhook returns HTTP 200.
Duplicate update IDs cannot queue another reply. The background task, alarm,
and cron retry the oldest pending command. A 30-second lease prevents parallel
delivery of the same command. The prepared reply and request ID survive a
restart. Replies use the fixed Telegram API, a 10-second timeout, no redirects,
and the same validated chat receipt as alerts. The response date is included
because a delayed retry can contain an older sample. A lost Telegram reply can
cause a duplicate with the same request ID. Command replies are silent; failure
alerts keep audible notifications.

The private operator status includes `telegram_commands_configured`,
`pending_commands`, `commandDeliveryError`, and `lastCommandDeliveryMs`.
It never includes the bot token, webhook token, or private recipient ID.

## Checks

`npm run check` verifies TypeScript and creates a deployment bundle without
deploying. `npm test` runs model and Worker tests. All outbound calls in the
Worker tests use local mocks. The tests cover authentication, machine silence,
restart recovery, per-fault confirmation, durable retry order, redirect safety,
and Telegram success, rejection, bad configuration, wrong-chat replies, bounded
reply size, and message length.
They also cover private command identities, webhook authentication, duplicate
updates across restart, reply retry and order, old reports, paused archive work,
and the scope of stage and plan estimates.

References: [Telegram Bot API](https://core.telegram.org/bots/api#sendmessage)
and [Cloudflare Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/).
