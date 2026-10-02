# SMS → GigaChat Gateway

A personal FastAPI service that lets you talk to an AI assistant over plain
SMS — no mobile internet required. Incoming SMS messages are forwarded as
webhooks by the [android-sms-gateway](https://sms-gate.app) app running on an
old Android phone at home; the gateway verifies and queues them on a VPS,
asks GigaChat for an answer, trims it to the SMS reply budget (201 UTF-16
code units) and sends the answer back as an SMS through the gateway's REST
API.

The pipeline is deliberately asynchronous: the webhook receiver only verifies
the request, deduplicates it and appends it to a FIFO queue (SQLite, WAL
mode), answering `2xx` within the gateway's 30-second deadline. All the slow
work — session management, the LLM call, the reply delivery — is done by a
single background worker, which also serializes access to the GigaChat
freemium tier (one request at a time) and avoids SQLite write contention.

Privacy is a first-class constraint (Russian personal data law 152-FZ):
message texts are never logged — only metadata (timestamps, sender numbers,
lengths, session numbers, latencies, counters). The service is intended for
personal, non-commercial use; VPS and GigaChat processing both stay in Russia.

## Features

- **Webhook receiver** (step 2): HMAC-SHA256 verification (`X-Signature` /
  `X-Timestamp`, ±5 min window, constant-time compare), configurable dedup
  window (`webhook.dedup_ttl_hours`, default 48 h), 64 KB body limit,
  instant `2xx` after enqueue.
- **Single asyncio worker**: FIFO batches of ≤10, `pending → processing`
  claiming, crash recovery (stuck rows are requeued on startup), graceful
  shutdown finishing the current batch. A row that fails 5 attempts is
  dead-lettered (`failed` status) so a poisoned row cannot block the FIFO.
- **Deferred re-send (best-effort)**: a reply whose gateway send fails with a
  retryable transport/5xx error is requeued ONCE (the send_id persists, so
  the retry cannot double-send); the trimmed reply text is stored in
  `queue.deferred_reply` (schema v6) and replayed verbatim on the next claim
  — no second LLM call, no command re-execution. A second final failure —
  and any non-retryable 4xx — is absorbed (`sms_send_failed`): see
  "Known limitations".
- **Numbered sessions & commands** (step 3): `#N text` routes to session N,
  no prefix → default session; `RESET`, `LIST`, `TEMP x`, `PROMPT`, `HELP`;
  per-sender LRU cap and 24 h TTL.
- **GigaChat client** (step 4): OAuth with a lazily cached access token,
  retries only on 429/5xx, 422 → history halving retry, final failure →
  "try later" SMS; `MOCK_LLM` echo mode for channel testing.
- **Reply pipeline** (step 5): UTF-16-safe trimming at sentence boundaries,
  SMS sending via the gateway REST API, delivery dedup, heartbeat watchdog
  with Telegram alerts.
- **Operations**: `GET /healthz` (status, uptime, version, counters; `503`
  with machine-readable `degraded` reasons when the DB is unreachable or the
  worker loop has stalled), JSON logs (rotating file + stdout), YAML
  configuration with `${ENV}` substitution and hot reload with last-good
  semantics, SQLite migrations via `PRAGMA user_version`, Docker Compose
  deployment with Caddy TLS.

## Quick start: from zero to first SMS

Everything below is the complete path from a bare VPS to your first SMS
conversation with the bot. The detailed sections later in this README
("Deployment (Ubuntu VPS)", "Phone setup", "Configuration reference",
"First live run checklist") are deep-dives; this section is self-sufficient
— every command you need is right here.

### Step 0 — What you need (shopping list)

| Item | Requirements |
|---|---|
| **VPS** | 2 vCPU / 2 GB RAM / 20+ GB disk, **Ubuntu 24.04 LTS**, **KVM virtualization** (Docker does not run on OpenVZ/LXC containers), inbound ports 80/443. Any provider works. |
| **Domain** | Any registrar. You will create two A-records later (Step 3). |
| **Android phone** | Android 5.0+ (8–11 recommended), stays at home on Wi-Fi permanently, SIM slot for the gateway SIM. |
| **Second SIM** | A non-bank SIM with an SMS-capable tariff. Mobile data on it is NOT needed — the phone uses Wi-Fi; the SIM only receives SMS. |
| **GigaChat account** | Russian service (RF users); individual accounts work on freemium limits — enough for a personal bot. |
| **Optional** | Telegram bot (dead-device alerts), UptimeRobot monitor (Step 10). |

### Step 1 — Server prep (Ubuntu 24.04)

All commands in this step run **as root** (the common VPS default); if you
log in as a regular user instead, prefix each command with `sudo` (the
deep-dive "Deployment" section below shows the sudo variant).

```bash
ssh root@<YOUR_VPS_IP>

apt update && apt upgrade -y

# Docker (official convenience script)
curl -fsSL https://get.docker.com | sh
usermod -aG docker $USER
# Log out and back in, then verify:
docker run --rm hello-world
```

**Firewall** — ORDER MATTERS: allow SSH **before** enabling ufw, or you
will lock yourself out:

```bash
ufw allow ssh               # port 22 - FIRST, before enable
ufw default deny incoming
# Outgoing stays ALLOWED: apt install, docker pull, NTP, the GigaChat API
# and Telegram all need outbound access. Do NOT set "deny outgoing".
ufw default allow outgoing
ufw allow 80/tcp            # Let's Encrypt HTTP-01 challenge
ufw allow 443/tcp           # HTTPS
ufw --force enable
```

**Server clock** — webhook signatures carry a Unix timestamp checked with a
±5 min window; a drifting clock rejects EVERY webhook with
`401 rejected_timestamp`:

```bash
timedatectl   # expect "System clock synchronized: yes", "NTP service: active"
# If NTP is inactive:
timedatectl set-ntp true
```

### Step 2 — Get the code and configure

```bash
git clone https://github.com/<your-github-username>/sms-ai-gateway.git /opt/sms-ai-gateway
cd /opt/sms-ai-gateway

cp .env.example .env
nano .env

cp config.example.yaml config.yaml
nano config.yaml
```

**`.env` variables — where to get each value:**

| Variable | Value / how to get it |
|---|---|
| `DOMAIN` | Your domain, e.g. `example.com` (subdomains `sms.` / `app.` are derived automatically) |
| `MYSQL_ROOT_PASSWORD` | Your own secret: `openssl rand -hex 16` |
| `MYSQL_DB_USER` | Leave the default `gateway` |
| `MYSQL_DB_PASSWORD` | Your own secret, `openssl rand -hex 16` |
| `LOG_LEVEL` | Leave `INFO` |
| `GATEWAY_HMAC_SECRET` | `openssl rand -hex 32` — **the SAME value** must later be entered on the phone as the webhook **Signing Key** (Step 6) |
| `GATEWAY_PRIVATE_TOKEN` | Your own secret, `openssl rand -hex 16` — **the SAME value** must be entered in the phone app (Settings → Cloud Server → Private Token) |
| `GIGACHAT_AUTH_KEY` | Register at [developers.sber.ru](https://developers.sber.ru) (RF only, freemium), create a personal project, copy the key from **Settings → API keys → Authorization data** |
| `GIGACHAT_API_KEY` | Leave empty (only needed for some account types) |
| `GATEWAY_USERNAME` / `GATEWAY_PASSWORD` | **Leave EMPTY** — the Go server auto-generates them when the phone first connects (Step 6); you paste them into `.env` in Step 7 |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Optional: create a bot with [@BotFather](https://t.me/BotFather), get `chat_id` via `getUpdates`. Empty = log-only alerts |
| `MOCK_LLM` / `MOCK_GATEWAY` | Leave empty (production uses the real LLM and real sends) |

In `config.yaml` the only must-edit is the **whitelist** — the phone
numbers allowed to talk to the bot, strict E.164 (`+` + country code):

```yaml
whitelist:
  - "+79001234567"   # <- YOUR number (keep the quotes)
```

Everything else has working defaults (`system_prompt`, limits, sessions);
see "Configuration reference" below for every key.

### Step 3 — DNS

Create two A-records at your registrar **before** starting Docker (Caddy
needs them to issue Let's Encrypt certificates):

```
sms.<your-domain>.   A  <YOUR_VPS_IP>
app.<your-domain>.   A  <YOUR_VPS_IP>
```

If you use Cloudflare, the records must be **DNS-only** (grey cloud) —
the proxy interferes with Let's Encrypt and the phone's SSE keep-alive
connection to the Go server (per docs.sms-gate.app the phone holds an SSE
stream, so buffering/proxying breaks the live connection).

Wait a few minutes, then verify (expect your VPS IP twice):

```bash
dig +short sms.<your-domain> A
dig +short app.<your-domain> A
```

### Step 4 — Start the stack

```bash
cd /opt/sms-ai-gateway
docker compose up -d --build
docker compose ps
```

Expected at this point: `db`, `sms-gateway-server`, `sms-gateway-worker`
**healthy**, `caddy` running — and **`app` restarting in a loop. That is
NORMAL**: the app fail-fasts on empty `GATEWAY_USERNAME`/`GATEWAY_PASSWORD`
(they are generated only after the phone connects, Step 6). Let's Encrypt
certificate issuance in the Caddy logs takes 1–2 minutes after DNS
propagates.

### Step 5 — Verify the server side

```bash
curl https://sms.<your-domain>/api/3rdparty/v1/health
# expected: {"status":"ok",...}   (Go server, phone-facing)

timedatectl   # "System clock synchronized: yes", "NTP service: active"
```

`curl https://app.<your-domain>/healthz` starts answering only after
Step 7 (the app needs the phone credentials) — check it there.

### Step 6 — Phone setup (details: "Phone setup" section below)

1. Install the android-sms-gateway APK on the phone — full instructions in
   **"Phone setup" → 1. Install** (repo `capcom6/android-sms-gateway`,
   `app-release.apk`, installed from Chrome without adb).
2. In the app: **Settings → Cloud Server** → enable → **API URL**:
   `https://sms.<your-domain>/api/mobile/v1` (the path is **required**) →
   **Private Token**: the `GATEWAY_PRIVATE_TOKEN` from `.env` → tap
   **"Offline"** to connect. Status becomes online.
3. The app now shows the auto-generated **Cloud Server credentials**
   (username + password) on the Home screen — you need them in Step 7.
4. **Settings → Webhooks → Signing Key** → paste `GATEWAY_HMAC_SECRET`
   (the same value as in `.env`).
5. **Settings → Ping**: enable, interval **14 minutes**.
6. Reliability settings (exact Android 7.x paths in "Phone setup"):
   battery optimisation OFF for the app, background data allowed, Wi-Fi
   sleep → Always, RCS/Chat features off, SIM PIN removed, optionally
   GSM-only network mode.

### Step 7 — Phone credentials into `.env`, app up

```bash
nano .env        # GATEWAY_USERNAME=... / GATEWAY_PASSWORD=... from the phone app
docker compose up -d app
docker compose ps
# expected: all 5 services healthy/running, app no longer restarting

curl https://app.<your-domain>/healthz
# expected: {"status":"ok", ...}
```

### Step 8 — Register webhooks

Webhooks are registered via the Go server's API (NOT via config files) —
one curl per event, using the phone-generated credentials:

```bash
cd /opt/sms-ai-gateway
set -a; source .env; set +a   # export GATEWAY_USERNAME/PASSWORD for curl

# Register all five events we consume (one curl per event):
for EVENT in "sms:received" "sms:delivered" "sms:failed" "system:ping" "app:started"; do
  curl -X POST -u "$GATEWAY_USERNAME:$GATEWAY_PASSWORD" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"https://app.<your-domain>/webhook\", \"event\": \"$EVENT\"}" \
    https://sms.<your-domain>/api/3rdparty/v1/webhooks
done

# Sanity check - the list must show all five events:
curl -u "$GATEWAY_USERNAME:$GATEWAY_PASSWORD" \
  https://sms.<your-domain>/api/3rdparty/v1/webhooks
```

### Step 9 — First SMS

Send any SMS (e.g. `Hello`) **from a number listed in `config.yaml`
whitelist** to the phone's number. A GigaChat reply arrives within
~10–30 seconds. Nothing arrived? Work through the mandatory
**"First live run checklist"** below — every failure mode there has a
specific diagnostic (webhook registration, Signing Key, clock drift,
`messages_path` calibration, monitoring).

### Step 10 — Optional hardening

- **Telegram alerts** (dead-device watchdog): create a bot with @BotFather,
  put `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` into `.env`,
  `docker compose up -d app` — see "Telegram alerts (step 5)".
- **UptimeRobot** on `https://app.<your-domain>/healthz` — see
  "External monitoring (UptimeRobot / healthz)".
- **SQLite backup cron** — see "Back up SQLite" in the Deployment section.

Deep-dive sections below: **Deployment (Ubuntu VPS)** covers every command
in detail, **Phone setup** walks every Android setting, **Configuration
reference** documents every `.env` and `config.yaml` key.

## Webhook receiver (step 2)

`POST /webhook` accepts events sent by the android-sms-gateway app. The
pipeline is: **per-IP rate limit → size limit → signature verification →
payload validation → dedup → atomic enqueue → immediate `2xx`**. The rate
limit runs BEFORE the body is read (streaming is the most expensive step, so
the limit gates it). No business logic happens here; the background worker
(steps 3–5) consumes the queue.

### Signature scheme (Android SMS Gateway protocol)

Verified against the official sources (docs.sms-gate.app — Webhooks /
"Payload signing", the reference example
[android-sms-gateway/example-webhooks-fastapi](https://github.com/android-sms-gateway/example-webhooks-fastapi),
and the gateway's signing plugin `PayloadSingingPlugin.kt`):

| Aspect | Value |
|---|---|
| Headers | `X-Signature` (HMAC as **lowercase hex**), `X-Timestamp` (Unix time **in seconds**, ASCII) |
| Signed input | `raw_request_body_bytes + ASCII_timestamp_bytes` — plain concatenation, **no separator**, no method/path |
| Algorithm | `HMAC-SHA256(key=webhook.secret, msg=body + timestamp)`, hex lowercase |
| Comparison | constant-time (`hmac.compare_digest`), input header trimmed and lowercased |
| Replay window | timestamp must be within ±5 min of the server clock (docs recommendation) |
| Signing key | the phone app's "Signing Key" (Settings → Webhooks → Signing Key), provided via `webhook.secret` / `GATEWAY_HMAC_SECRET` |

```python
expected = hmac.new(secret.encode(), body + timestamp.encode(), hashlib.sha256).hexdigest()
ok = hmac.compare_digest(expected, header_value.strip().lower())
```

### Request handling and status codes

| Condition | Status |
|---|---|
| Body larger than `webhook.max_body_bytes` (64 KB) | `413` (rejected before JSON parsing, never echoed back) |
| More than `webhook.rate_limit_per_min` requests/min from one IP | `429` (soft, in-memory; the gateway retries with backoff) |
| Missing/garbled/out-of-window timestamp, or bad signature | `401` |
| Broken JSON or schema-invalid envelope/payload | `400` |
| New event | `200 {"status": "ok"}` (queued; also 200 for a deduplicated repeat) |

The gateway only requires *some* `2xx` within 30 seconds; `200` with a small
JSON body follows the official FastAPI reference example. Error responses
never echo request data (`{"status": "rejected", "reason": "<machine tag>"}`).

### Memory safety for large bodies

The body is read as a stream with a hard byte cap (`webhook.max_body_bytes`):
a chunked upload or a lying `Content-Length` is cut off with `413` as soon as
the cap is crossed instead of buffering the whole body. The same limit is
enforced at the edge by Caddy (`request_body max_size 65536` — explicit
bytes, same value as `webhook.max_body_bytes`): the two values must be
changed together if `webhook.max_body_bytes` ever changes.

### Rate limit trust model

The per-IP limit trusts `X-Forwarded-For`. This is only valid in the
deployment topology where the app is reachable exclusively through Caddy
inside the Docker network. The proxy REPLACES the header with the real
client address (`header_up X-Forwarded-For "{remote_host}"` on every
`reverse_proxy` in the Caddyfile — Caddy appends by default, which would
let a caller prepend a fake entry); the app trusts only the FIRST XFF
entry. **Do not publish port 8000 directly**: a direct client could spoof
`X-Forwarded-For` and rotate fake IPs to evade the limit — the Caddy
`header_up` above is what makes the header trustworthy, so the app must
stay reachable only through the proxy.

Hardening note (spoofed-XFF floods): the limiter caps the memory its
per-IP windows can occupy. Even an attacker rotating thousands of fake
XFF values cannot grow the tracked-keys dictionary without bound: once it
exceeds 8192 keys, an amortized sweep drops every key whose 60 s window
has fully drained, while windows with live hits are kept (a flood in
progress never gets a fresh budget).

### Validation and dedup

The envelope (`deviceId`, `event`, `id`, `payload`, `webhookId`, camelCase)
is validated with Pydantic; unknown fields are **tolerated** (`extra="allow"`)
so that new gateway versions cannot break the receiver — semantic payload
parsing is the worker's job. For `sms:received` the payload must carry
`messageId` (the gateway's content-based message id); the deprecated
`phoneNumber` field is accepted alongside `sender`.

Dedup keys on `messageId` (`envelope.id` for non-SMS events) and is stored in
`processed_events` with a TTL of `webhook.dedup_ttl_hours` (default **48 h**)
— it must stay comfortably beyond the gateway's retry horizon (exponential
backoff from 10 s, 14 retries by default ≈ 45.5 h; a shorter TTL risks
accepting a retried delivery as new). Expired tombstones are purged lazily on
each accepted delivery. The queue row and the dedup tombstone are committed
in a single transaction, so a duplicate can never be half-recorded.

### Counters

`/healthz` exposes the counters: `received`, `accepted`, `dedup_skipped`,
`rejected_signature`, `rejected_timestamp`, `rejected_size`,
`rejected_payload`, `rate_limited`. In a steady state `received` equals the
sum of the others.

Privacy: request bodies and message texts are **never logged** — only
metadata (event, ids, sender number, body length, verdict).

## Worker pipeline (step 3)

The single worker claims queue rows and runs them through
`app.pipeline.MessagePipeline`:

1. **Envelope parsing** — broken JSON or a schema violation increments
   `dropped_parse`; the row is removed (never retried forever).
2. **Non-SMS events** — `sms:delivered`/`sms:failed`/`system:ping`/
   `app:started` are handled in step 5 (delivery tracking, heartbeat,
   watchdog, alerts); any other non-SMS event name is counted as
   `skipped_event` (INFO for known names, WARNING for unknown) and removed.
3. **Strict `sms:received` parsing** (`app.events.SmsReceivedPayload`).
4. **Whitelist** — the sender is normalized to E.164 (`8-916-123-45-67`,
   `8 916 123 45 67`, `+7 (916) 123-45-67` → `+7916…`; alphanumeric senders
   like `MTS`/`MCHS-moskva` are unparseable) and checked against the config
   whitelist. Misses are **silently dropped**: no reply, no error log —
   only the `dropped_whitelist` counter and a metadata line.
5. **Routing** — `#N <text>` selects session N (created on demand); no
   prefix → the default session 1. An invalid prefix (`#0`, a bare `#2`)
   is treated as ordinary text for the default session.
6. **Commands** (case-insensitive, before any LLM call): `RESET [N]`,
   `LIST`, `TEMP x` (0.0–2.0), `PROMPT <text>`, `HELP`. Commands are not
   stored in the history; replies are SMS-sized (≤ 201 UTF-16 code units).
7. **Sessions** (`app.sessions.store`) — history capped at
   `limits.history_messages`, inactivity TTL `limits.session_ttl_h`
   (lazy cleanup on touch + a daily sweep in the worker loop), at most
   `limits.max_sessions` sessions per sender with LRU eviction.
   `TEMP`/`PROMPT` store per-session overrides that take precedence over
   the YAML defaults; `RESET` clears them.

Worker counters added in step 3: `processed`, `dropped_whitelist`,
`dropped_parse`, `skipped_event`. Commands are deliberately not counted
separately: `processed` already covers the volume and command activity is
visible in the logs.

Step-5 worker counters: `sms_sent`, `sms_send_failed` (reply transport),
`sms_send_requeued` (deferred re-sends, see "Reply sending"), 
`delivered_parts` (per-part delivery confirmations), `failed_reports`
(delivery-failure events), `heartbeats`, `watchdog_alerts`,
`app_restarts`.

Maintenance cadence (worker loop): the **watchdog tick runs every 5 minutes**
(`DEFAULT_WATCHDOG_INTERVAL_S`) so a dead device is detected within ~1 hour
at the default heartbeat; the heavier **session/expiry sweep runs once a
day** (`DEFAULT_SWEEP_INTERVAL_S`).

## GigaChat client (step 4)

Non-command messages are completed by `app.llm.gigachat.GigaChatClient`:

- **Messages layout** — the server keeps no context, so the client sends
  the full array every time: `[system, ...history ≤ limits.history_messages]`.
  The system message is the session `PROMPT` override when set, the YAML
  `system_prompt` otherwise; the temperature likewise (`TEMP` override wins).
  A successful reply is appended to the session history as an `assistant`
  turn.
- **Lazy OAuth token** — `POST https://ngw.devices.sberbank.ru:9443/api/v2/oauth`
  with `Authorization: Basic <llm.auth_key>`, `RqUID: <uuid4>` and
  `scope=GIGACHAT_API_PERS`. The token is fetched right before the first
  completion, cached for at most 29 of the nominal 30 minutes and refetched
  on expiry or a 401 from the chat API. No background refresh timer: the
  single worker makes one request at a time, so a lazy fetch is always
  timely.
- **Retries (bounded)** — 429/5xx/transport errors: up to 3 attempts with
  exponential backoff (1 s, 2 s); a numeric `Retry-After` header is honored.
  401: one token refresh + retry. 422 (context overflow): the history is
  truncated in half (most recent kept) and the request retried once.
  400/404/other 4xx are never retried. When the budget is exhausted the
  pipeline replies to the user with a fixed Russian message
  (`Ошибка, попробуйте позже`) and counts `llm_errors` — the queue row is
  still processed, nothing retries forever.
- **Mock mode** — `llm.mock: true` in the config or `MOCK_LLM=true` in the
  environment (the env var wins) replaces the client with an echo stub:
  the reply is `(mock) <your message>`, zero network calls, no key needed
  (the startup fail-fast for an empty `auth_key` is skipped in mock mode).

Privacy: prompts, message texts, model replies and tokens are **never
logged** — only metadata (status, latency, sizes, attempt counts).

### RF specifics and other LLM providers

GigaChat (Sber) is a **Russian service**: it is intended for users in the
Russian Federation, individual accounts work on freemium limits, and the
Russian trusted root CA (НУЦ Минцифры) is required **only** for the
GigaChat endpoints (`api.giga.chat`, `ngw.devices.sberbank.ru`) — it is
already installed in the Docker image (see "Certificate and TLS notes" in
the Deployment section). Nothing else in the stack depends on that CA:
public TLS (Caddy/Let's Encrypt) uses the standard trust store.

**Other LLM providers:** the client is isolated in `app/llm/`. The
GigaChat chat API follows the OpenAI-compatible `chat/completions` format,
but its OAuth token flow and the НУЦ TLS setup are Sber-specific. Adding
another OpenAI-compatible provider (OpenAI, Groq, OpenRouter, a local
Ollama) is a small separate feature circle (an adapter plus an
`llm.provider` config key) — **not implemented at the moment**; the code
and config are GigaChat-only today.

## Reply sending (step 5)

The pipeline finishes every SMS flow by sending the reply through the
android-sms-gateway **Private Server** REST API (`app.gateway.client`):

- **Trimming** — the reply is first trimmed to `limits.max_reply_units`
  (201 UTF-16 code units): sentence boundaries (`.!?…`) are preferred; a
  dot **not followed by whitespace** is never a cut point (covers
  abbreviations like «т.д.» and decimals like `3.14`), a dot right after a
  digit is excluded as well; without a fitting boundary the reply is hard
  cut — never inside a surrogate pair (emoji stay whole). The FULL reply
  (not the trimmed one) is kept in the session history.
- **Send** — `POST {gateway.base_url}{gateway.messages_path}` (default
  `/api/3rdparty/v1/messages`), Basic auth from the phone app's web-server
  credentials (`gateway.username`/`gateway.password` via
  `GATEWAY_USERNAME`/`GATEWAY_PASSWORD`), body
  `{"message", "phoneNumbers": [...], "withDeliveryReport": true, "id":
  <uuid>}`. A 2xx means *accepted into the server's queue* — actual
  delivery is asynchronous (the phone pulls the queue), confirmed later by
  `sms:delivered` webhooks. The gateway's message id from the response is
  logged (metadata only).
- **Idempotency** — the request carries our own `id` (uuid4), constant
  across the retries of one send: the gateway deduplicates on it, so a
  retried 5xx cannot double-send.
- **Retries (bounded)** — 5xx/transport: up to 3 attempts, small backoff
  (0.5 s, 1 s); 4xx (auth/body errors) are never retried. If all in-request
  attempts fail with a *retryable* error, the queue row is requeued ONCE
  (deferred re-send): the row keeps its `send_id`, so the gateway
  deduplicates and the retry cannot double-send, and the trimmed reply text
  is stored on the row itself (`queue.deferred_reply`, schema v6) and
  replayed verbatim on the next claim — no second LLM call, no command
  re-execution. If the deferred re-send also fails finally (or the first
  failure was non-retryable), the failure is absorbed: counted
  (`sms_send_failed`) and logged with metadata only — the user gets nothing
  that round, exactly like the previous best-effort behaviour.
- **Mock mode** — `gateway.mock: true` or `MOCK_GATEWAY=true` (env wins)
  logs `Gateway mock send` instead of any HTTP call; no credentials needed
  (the startup fail-fast for empty credentials is skipped in mock mode).

### Known limitations (reply delivery)

- The deferred re-send is a **single** extra attempt, not an unbounded
  retry queue: a message whose reply could not be delivered twice is lost
  from the delivery perspective (counted in `sms_send_failed`). This is a
  deliberate trade-off — SMS from a whitelisted sender always produces a
  fresh LLM answer on the next message, so the user is never stuck, and the
  queue can never grow unboundedly from undeliverable replies.

### Known limitations (inbound dedup, content-based `messageId`)

- The gateway derives `messageId` from the SMS CONTENT, and the receiver
  deduplicates on it for `webhook.dedup_ttl_hours` (48 h). Two **identical**
  SMS texts from the same sender inside that window are therefore treated
  as one delivery: the second is answered `200` + `dedup_skipped` and
  **silently produces no reply** (no second LLM call). This is the
  gateway's keying, not a receiver bug — if you send the same "test" SMS
  twice and get only one answer, check the `dedup_skipped` counter in
  `/healthz` before debugging. Any differing character (even a trailing
  space) produces a different id and is processed normally.

### Device events and the watchdog (step 5)

- `sms:delivered` arrives **per multipart part**; per-part dedup is the
  webhook receiver's envelope-id tombstone. Counted as `delivered_parts`.
- `sms:failed` is **log-only** (the device handles its own retries; the
  user is never re-notified); counted as `failed_reports`. The device's
  free-text `reason` is never logged — only its length.
- `system:ping` (device heartbeat, pull cycles) refreshes
  `device_state['last_ping']` and counts `heartbeats`. Only a
  non-identifying summary (health key count) is stored — health values are
  never logged.
- `app:started` (the phone app booted) stores
  `device_state['last_app_started']`, counts `app_restarts` and sends a
  Telegram alert "phone app (re)started" — SIM numbers/ICCID are never
  logged or sent.
- **Watchdog** — runs on the worker's fast tick (every 5 minutes): if the
  last ping is older than `3 x notify.heartbeat_interval_min` (default 3 x
  15 = 45 minutes), Telegram is alerted **once** (`watchdog_alerts` counter,
  a `watchdog_alerted` flag prevents re-alerting); the next ping sends a
  one-line "recovered" alert and clears the flag. No ping ever recorded →
  no alert (fresh-install baseline).

### Telegram alerts (step 5)

`app.notify.telegram.TelegramNotifier` posts short metadata-only messages
via the Bot API (`sendMessage`). Unconfigured (`TELEGRAM_BOT_TOKEN` /
`TELEGRAM_CHAT_ID` empty) it degrades to **log-only** alerts — the
pipeline keeps working without Telegram. A Telegram failure is always
swallowed (logged, metadata only): alerting must never break SMS
processing. No special CA work is needed (`api.telegram.org` uses a
publicly trusted chain).

Privacy: reply texts, delivery reasons, SIM numbers/ICCID and health
values are **never logged**; phone numbers appear in metadata logs only as
the (whitelisted) sender, per the step-3 convention.

### TLS setup (Russian trusted root CA)

Both GigaChat endpoints present certificates signed by the Russian trusted
root CA (НУЦ Минцифры); TLS verification is always on — it is never
disabled in the code.

- **Docker** — the image vendors `docker/russian_trusted_root_ca_pem.crt`
  and installs it into both the system store (`update-ca-certificates`) and
  the **certifi bundle** (`httpx` trusts certifi, not the system store) —
  see the Dockerfile.
- **Local Windows/Linux run** — append the same file to the venv's certifi
  bundle:

```bash
# from the activated venv (PowerShell example)
$cert = "docker/russian_trusted_root_ca_pem.crt"
$bundle = python -c "import certifi; print(certifi.where())"
Get-Content $cert -Raw | Add-Content $bundle
```

## Local run (Python)

Requires Python 3.14 (the runtime the project is developed and tested
against; the code uses modern built-in generics and `asyncio.to_thread`,
so 3.10+ is the practical minimum).

```bash
python -m venv .venv
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
# for running the tests (M5: pytest lives in the dev requirements, the
# runtime image stays lean):
pip install -r requirements-dev.txt

copy config.example.yaml config.yaml   # Windows
# cp config.example.yaml config.yaml   # Linux/macOS
# edit config.yaml as needed

# from the project root
uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
```

Then check `http://127.0.0.1:8000/healthz` (returns `{"status": "ok", ...}`).
The webhook endpoint requires a valid signature — see "Webhook receiver"
above and `config.example.yaml` (`webhook.secret: ${GATEWAY_HMAC_SECRET}`).

Tests:

```bash
pytest
```

> **Editing `config.yaml` in Docker (W4b)**: the file is bind-mounted into
> the container, so hot-reload sees ordinary in-place edits. But an editor
> that *replaces* the file (new inode — VS Code default, `sed -i`) is
> invisible to the mount; after editing run `docker compose restart app`.
> (Alternative: mount a `./config` directory instead of the single file.)

## Project structure

```
app/
  main.py            # FastAPI factory: config, DB, worker, routers wiring
  config.py          # YAML + ${ENV} substitution, Pydantic v2 validation, hot reload
  db.py              # SQLite (WAL), user_version migrations, typed helpers
  logging_setup.py   # JSON logs: rotating file (10 MB x 14) + stdout
  signing.py         # webhook HMAC-SHA256 verification (gateway protocol)
  rate_limit.py      # in-memory sliding-window per-IP limiter
  worker.py          # single asyncio queue consumer + daily maintenance sweep
  pipeline.py        # message pipeline: parse -> whitelist -> route -> command/session
  events.py          # gateway event models (strict SMS + device-event payloads)
  routes/
    webhook.py       # POST /webhook: verify -> dedup -> enqueue -> 2xx
    health.py        # GET /healthz
  llm/               # GigaChat client (step 4): OAuth, retries, mock mode
  gateway/           # reply sending (step 5): REST client, trimmer, mock mode
  sessions/          # sender normalization, numbered sessions, TTL/LRU (step 3)
  commands/          # RESET/LIST/TEMP/PROMPT/HELP (step 3)
  notify/            # Telegram alerts + heartbeat watchdog (step 5)
tests/               # pytest suite (config, DB, health, webhook, rate limit, worker,
                     # events, sessions, commands, pipeline, LLM client, gateway, notify)
docker/
  russian_trusted_root_ca_pem.crt  # Russian trusted root CA (НУЦ), vendored
Dockerfile           # non-root app image (uvicorn) with the НУЦ CA installed
Caddyfile            # reverse proxy with automatic TLS (body cap 65536 bytes)
docker-compose.yml   # 5 services: db, sms-gateway-server, sms-gateway-worker,
                     # app, caddy
config.example.yaml  # annotated configuration template
.env.example         # environment template (DOMAIN, secrets placeholders)
requirements-dev.txt # test-only dependencies (pytest)
```

## Configuration reference

All settings live in `config.yaml` (see `config.example.yaml` for annotated
defaults). Secrets are passed via environment variables from `.env`, not
stored in the YAML file.

| Section | Purpose |
|---|---|
| `whitelist` | Allowed senders, strict E.164; everyone else is silently dropped |
| `system_prompt` | GigaChat persona; enforces brevity and no emoji/markdown |
| `llm` | `model`, `temperature`, `max_tokens`, `timeout_s`, `auth_key`, `base_url`, `oauth_url`, `scope`, `mock` (step 4) |
| `limits` | `max_sessions`, `history_messages`, `session_ttl_h`, `max_reply_units` |
| `sessions_named` | Mapping of `#N` session numbers to human-readable names |
| `gateway` | Reply sending (step 5): `base_url`, `messages_path`, Basic `username`/`password`, `timeout_s`, `mock` |
| `webhook` | Receiver: `secret`, `rate_limit_per_min`, `max_body_bytes`, `timestamp_window_s`, `dedup_ttl_hours` (default 48, must cover the gateway retry horizon) |
| `notify` | Telegram `telegram_token` / `telegram_chat_id` — injected as `${TELEGRAM_BOT_TOKEN}` / `${TELEGRAM_CHAT_ID}` (empty = log-only), `heartbeat_interval_min` (step 5) |
| `rag` | `enabled` flag for the Phase 2 retrieval subsystem |
| `config` | `reload` — hot-reload the file on change (last-good on errors) |
| `db_path` | SQLite file location; relative paths resolve against the config file |
| `log_level` | Default log level; `LOG_LEVEL` env var takes precedence |

### Complete environment variables

| Variable | Required | Description |
|---|---|---|
| `DOMAIN` | **Yes** | Root domain for Caddy (e.g. `example.com`). Two A-records needed: `sms.DOMAIN` and `app.DOMAIN` → VPS IP. |
| `MYSQL_ROOT_PASSWORD` | **Yes** | MySQL root password (used for the Go server's DB). |
| `MYSQL_DB_USER` | No | MySQL user for the Go server. Default: `gateway`. |
| `MYSQL_DB_PASSWORD` | **Yes** | Password for `MYSQL_DB_USER`. |
| `LOG_LEVEL` | No | App log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default: `INFO`. |
| `GATEWAY_HMAC_SECRET` | **Yes** | HMAC-SHA256 signing key for webhook verification. Also the "Signing Key" in the phone app. |
| `GATEWAY_PRIVATE_TOKEN` | **Yes** | Private token for device registration in private mode. Must match the value entered in the phone app (Settings -> Cloud Server -> Private Token). |
| `GIGACHAT_AUTH_KEY` | **Yes** (unless `MOCK_LLM`) | GigaChat OAuth key from developers.sber.ru. |
| `GIGACHAT_API_KEY` | No | GigaChat API key (account-type dependent). |
| `MOCK_LLM` | No | Set `true` to replace GigaChat with an echo stub (no keys, no network). |
| `GATEWAY_USERNAME` | **Yes** (unless `MOCK_GATEWAY`) | Per-device credentials generated by the Go server on first phone connection (shown in the phone app's Home screen under "Cloud Server" credentials). Used for our app's 3rd-party API calls to send SMS. |
| `GATEWAY_PASSWORD` | **Yes** (unless `MOCK_GATEWAY`) | Same requirements as `GATEWAY_USERNAME`. |
| `MOCK_GATEWAY` | No | Set `true` to replace the SMS gateway with a log-only stub (no network). |
| `TELEGRAM_BOT_TOKEN` | No | Telegram bot token. Empty = log-only alerts (the pipeline keeps working). |
| `TELEGRAM_CHAT_ID` | No | Target chat ID for Telegram alerts. Empty = log-only. |
| `GATEWAY_CONFIG` | No | Override config file path. Default: `/app/config.yaml`. |
| `LOG_FILE` | No | Override app log file path. Default: `/app/data/logs/app.log` (inside the `app_data` volume; logs survive restarts). |

## Deployment (Ubuntu VPS)

### Overview

A complete production deployment uses five Docker services on a single VPS:

| Service | Image | Purpose |
|---|---|---|
| `db` | `mariadb:11.8` | Go server's persistence (MariaDB LTS recommended by server docs) |
| `sms-gateway-server` | `ghcr.io/android-sms-gateway/server:v1.47.4` | Receives SMS from the phone, sends via REST |
| `sms-gateway-worker` | `ghcr.io/android-sms-gateway/server:v1.47.4` | Go server's background cleanup worker (removes stored SMS texts, stale devices — privacy compliance) |
| `app` | `sms-ai-gateway:latest` (local build) | Webhook receiver, LLM pipeline, reply sending |
| `caddy` | `caddy:2.10.2` | Reverse proxy, automatic HTTPS (Let's Encrypt), 64 KB body cap |

Two public HTTPS subdomains (both via Caddy, Let's Encrypt TLS):

- **sms.DOMAIN** — the Go server; the phone connects here (initiates the connection, no port forwarding on the phone).
- **app.DOMAIN** — our FastAPI app; the Go server POSTs webhooks to `app.DOMAIN/webhook`.

No other ports are exposed to the public internet.

### Prerequisites

- VPS: Ubuntu 24.04 LTS, at least 2 GB RAM, 20 GB disk, ports 80/443 open.
- Domain: an A record pointing `sms.DOMAIN` and `app.DOMAIN` at your VPS IP.
- Docker and Docker Compose v2 (installed below).

### 1. Install Docker

```bash
# Official convenience script (Ubuntu 24.04)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in, then verify:
docker run --rm hello-world
```

### 2. Firewall

```bash
# ORDER MATTERS: allow SSH before enabling ufw, or you will lock
# yourself out of the VPS.
sudo ufw allow ssh          # port 22
sudo ufw default deny incoming
# Outgoing stays ALLOWED (the default policy is already "allow", we pin it
# explicitly). Do NOT set "deny outgoing": it silently breaks the whole
# deployment - apt install, docker pull, NTP time sync, the GigaChat API
# calls and Telegram alerts all need outbound access. The real barrier is
# the strict INBOUND policy; outbound stays open.
sudo ufw default allow outgoing
sudo ufw allow 80/tcp       # Let's Encrypt HTTP-01 challenge
sudo ufw allow 443/tcp      # HTTPS
sudo ufw --force enable
sudo ufw status
```

> **Production lesson (real-world deploy, 2026-09)**: `deny outgoing` broke
> `apt install`, `docker pull` and would have broken the GigaChat calls. If
> you already enabled it, restore with `sudo ufw default allow outgoing`.

### Go server `/metrics` endpoint (not exposed)

The Go server (android-sms-gateway) serves a `/metrics` endpoint
(operational data: version, device/connection state) protected only by the
server's own auth, which was never designed as the outer wall. The Caddyfile
blocks it at the proxy (`@metrics` → `respond 403`) so it is unreachable
from the Internet. To read the metrics legitimately, go over the Docker
network from inside the stack, or over SSH:

```bash
# From the VPS itself (no Internet exposure at all):
docker compose exec caddy wget -qO- http://sms-gateway-server:3000/metrics

# From your workstation over SSH (one-liner, no tunnel needed):
ssh user@<YOUR_VPS_IP> "cd /opt/sms-ai-gateway && docker compose exec caddy wget -qO- http://sms-gateway-server:3000/metrics"
```

### 3. Get the code and configure

**Primary path — clone the public repository:**

```bash
git clone https://github.com/<your-github-username>/sms-ai-gateway.git /opt/sms-ai-gateway
cd /opt/sms-ai-gateway
```

**Alternative — copy a prepared folder** (e.g. when you keep a verified
`.env`/`config.yaml` on your workstation and do not want to retype secrets):

```bash
# From your workstation (Windows PowerShell — tar.exe is bsdtar there;
# substitute <path-to-project-parent> with the folder that CONTAINS the
# project directory):
tar -czf sms-ai-gateway.tar.gz --exclude=.venv --exclude=.idea --exclude=.pytest_cache --exclude=__pycache__ --exclude=data --exclude=logs -C <path-to-project-parent> sms-ai-gateway
scp sms-ai-gateway.tar.gz root@<YOUR_VPS_IP>:/opt/

# On the VPS (the archive unpacks to /opt/sms-ai-gateway):
ssh root@<YOUR_VPS_IP>
cd /opt && tar -xzf sms-ai-gateway.tar.gz
cd /opt/sms-ai-gateway
```

**Configure** (both paths):

```bash
cp .env.example .env
nano .env      # fill in every value - see the table in "Quick start", step 2
cp config.example.yaml config.yaml
nano config.yaml   # at minimum: whitelist (your number, strict E.164)
```

> **Go server config** — `sms-gateway-config.yml` is already present, secrets
> injected via compose env vars (`DATABASE__PASSWORD`,
> `GATEWAY__PRIVATE_TOKEN`). Edit only if you need non-default settings
> (e.g. non-standard DB port).

> **`app` will restart-loop until the phone credentials are in `.env`**:
> the app fail-fasts on empty `GATEWAY_USERNAME`/`GATEWAY_PASSWORD`
> (RuntimeError in the startup log). This is EXPECTED at this point — they
> are generated by the Go server only after the phone first connects.
> Finish "Phone setup" step 2, put the credentials into `.env`, then
> `docker compose up -d` again (see "First live run checklist", item 2).

### 4. DNS A-records

Create **before** starting Docker (Caddy needs them for Let's Encrypt):

```
sms.example.com.   A  <YOUR_VPS_IP>
app.example.com.  A  <YOUR_VPS_IP>
```

Wait a few minutes for DNS propagation, then verify:
```bash
dig +short sms.example.com A
dig +short app.example.com A
```

### 5. Deploy

```bash
cd /opt/sms-ai-gateway
docker compose up -d --build
docker compose ps   # all services should be "healthy" or "running"
```

> **`app` restarting right after the first `up -d` is EXPECTED** while
> `GATEWAY_USERNAME`/`GATEWAY_PASSWORD` are still empty in `.env` (fail-fast
> at startup). They are generated by the Go server only after the phone
> first connects — do "Phone setup" step 2 (item 6: copy the auto-generated
> credentials into `.env`), then `docker compose up -d` again. The other
> four services (db, sms-gateway-server, sms-gateway-worker, caddy) come up
> healthy regardless.

Check the app is up:
```bash
curl https://app.example.com/healthz
# expected: {"status":"ok",...}
```

### 6. Verify the Go server health

```bash
curl https://sms.example.com/api/3rdparty/v1/health
# expected: {"status":"ok",...}
```

### 7. Back up SQLite

The app's SQLite database is stored in a named Docker volume (`app_data`).
Back it up regularly:

```bash
# One-shot backup (uses Python inside the container — no extra tools needed)
docker compose exec app python -c "
import sqlite3, shutil, sys, os
db = '/app/data/gateway.db'
backup = '/tmp/gateway_backup.db'
if os.path.exists(db):
    sqlite3.connect(db).backup(sqlite3.connect(backup))
    print('BACKUP_OK')
else:
    print('DB_NOT_FOUND', file=sys.stderr)
    sys.exit(1)
" && docker compose cp app:/tmp/gateway_backup.db ./gateway-$(date +%Y%m%d).db

# Or add to crontab (daily at 03:00).  Escape % with backslash in crontab:
# 0 3 * * * cd /opt/sms-ai-gateway && docker compose exec app python -c "import sqlite3,os; sqlite3.connect('/app/data/gateway.db').backup(sqlite3.connect('/tmp/gw.db'))" && docker compose cp app:/tmp/gw.db ./gateway-$(date +\%Y\%m\%d).db
```

### 8. Log rotation

**Container stdout (all services):** every service in docker-compose sets
`logging: json-file` with `max-size: 10m`, `max-file: 3` — the Docker daemon
rotates automatically (`docker compose logs` always shows the tail).

**App file logs:** the app also mirrors JSON logs to
`/app/data/logs/app.log` (env `LOG_FILE`, inside the `app_data` volume, so
files survive restarts).  Rotate them on the host if needed:

```bash
# In /etc/logrotate.d/sms-ai-gateway on the host (weekly, keep 4):
/var/lib/docker/volumes/sms-ai-gateway_app_data/_data/logs/*.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
    copytruncate
}
```

### 9. Update the image

```bash
cd /opt/sms-ai-gateway
# If you cloned the repo (Quick start / section 3 primary path):
git pull
# If you deployed by copying the folder instead: re-copy the changed
# project files from your workstation (scp/tar).
docker compose build app
docker compose up -d --build app
docker compose ps
```

### Calibrating `gateway.messages_path`

The Go server's REST API endpoint for sending SMS is checked via a live curl:

```bash
# Replace with your real credentials.  (GW_USER, not USER — $USER is the
# shell's own "current user" variable and must not be clobbered.)
GW_USER="$GATEWAY_USERNAME"
GW_PASS="$GATEWAY_PASSWORD"
GW_URL="https://sms.example.com"   # or app.DOMAIN:8000 if testing internally

# The id must be unique per send (the gateway deduplicates on it); a static
# id is fine for a one-shot probe — repeating the curl will be deduplicated,
# which is exactly the safe behaviour.
curl -s -o /dev/null -w "%{http_code}" \
  -u "$GW_USER:$GW_PASS" \
  -X POST "$GW_URL/api/3rdparty/v1/messages" \
  -H "Content-Type: application/json" \
  -d '{"message":"test","phoneNumbers":["+79001234567"],"id":"calibration-probe-1"}'
# 2xx = endpoint found
# 404 = wrong path
# 401 = credentials correct but path wrong
```

If the response is 404, list the routes the running build actually serves
(the Go server has no OpenAPI endpoint; the route table is in its logs):
```bash
docker compose logs sms-gateway-server | grep -o '"/api[^"]*"' | sort -u
```

Update `gateway.messages_path` in `config.yaml` to match (the default is `/api/3rdparty/v1/messages`; `gateway.base_url` stays `http://sms-gateway-server:3000`).

### External monitoring (UptimeRobot / healthz)

`GET /healthz` returns `200` when the app is alive and `503` (with a
machine-readable `degraded` reasons list) when the database is unreachable
or the worker loop has stalled.  Use it as an UptimeRobot (or similar)
monitor:

```
URL: https://app.example.com/healthz
Interval: 5 minutes
HTTP status: Expected 200
Alert: when down for 5 minutes
```

`https://sms.example.com/api/3rdparty/v1/health` (the Go server's own health check)
can be monitored separately.

### Certificate and TLS notes

- **Phone ↔ Go server ↔ our app**: all traffic is HTTPS via Caddy and Let's
  Encrypt. The Russian trusted root CA (НУЦ Минцифры) is **only** needed for
  GigaChat endpoints (`api.giga.chat`, `ngw.devices.sberbank.ru`), already
  installed in the Dockerfile.
- Caddy automatically provisions and renews TLS certificates for both
  subdomains.  No manual certificate management needed.

## Phone setup

### Prerequisites

- Any Android 5.0+ device (the app requires minSdk 21). Recommend stock
  Android **without aggressive battery managers**; Android 8–11 are the
  most stable for background SMS apps; avoid aggressive OEM skins (MIUI,
  EMUI and similar power savers kill background apps by default).
  **Do not upgrade Android 7.1.1 to Android 9** — many community reports
  indicate degraded SMS reliability.
  (Verified on: 2 vCPU/2 GB VPS, Android 7.1.1 device.)
- Wi-Fi or mobile data (the app only needs to reach your VPS; no special
  port forwarding required because the phone initiates all connections).

### 1. Install the android-sms-gateway app

> **Verified 2026-09-18**: the app lives in the **`capcom6/android-sms-gateway`**
> repository (the `android-sms-gateway` org on GitHub does NOT host the app).
> Release page: <https://github.com/capcom6/android-sms-gateway/releases>

- **Last stable at the time of writing: v1.75.0** (2026-09-03) — when newer
  releases appear, check the releases page and prefer the latest stable.
- File to download: **`app-release.apk`** (~12.6 MB) — a single universal
  APK, no ABI splits (installs as-is on any arm64 or armv7 device).
- Do **NOT** take `app-insecure.apk` — that build allows plain HTTP.
- minSdk 21 → requires **Android 5.0+** (any Android 5.0+ device is
  compatible).

**Install on the phone WITHOUT adb** (simplest way):

1. Open Chrome **on the phone** and download the APK via the direct link:
   `https://github.com/capcom6/android-sms-gateway/releases/download/v1.75.0/app-release.apk`
   (evergreen "latest stable" link:
   `https://github.com/capcom6/android-sms-gateway/releases/latest/download/app-release.apk`).
2. On Android 7: **Settings → Security → Unknown sources** → enable
   (one-time permission for the browser/manager you install from).
3. Tap the download notification (or open the APK with any file manager) →
   **Install**.

Alternative (if you prefer adb from a workstation):

```bash
adb install app-release.apk
```

### 2. Configure Private Server mode

1. Open the app → **Settings → Cloud Server**.
2. Enable **Cloud server** switch.
3. **API URL**: `https://sms.example.com/api/mobile/v1`
   (The path `/api/mobile/v1` is **required** — omitting it causes connection failures.
   Caddy forwards everything from `sms.DOMAIN` to the Go server on port 3000.)
4. **Private Token**: enter `GATEWAY_PRIVATE_TOKEN` from `.env`.
5. Tap the **"Offline" button** to initiate the connection.
6. After a successful connection, the app **auto-generates** credentials
   (username + password) and displays them in the **Cloud Server** section
   of the Home screen.  Copy them — these are the
   `GATEWAY_USERNAME` / `GATEWAY_PASSWORD` values for your `.env` and
   `config.yaml` (our app uses them for the 3rd-party API to send SMS).
7. Save.

> **Webhooks are registered via the API after connection** (they are NOT
> set via Go-server env vars — the server config v1.47.4 has no webhook
> keys).  After the phone connects, register our endpoint once per event
> with the auto-generated credentials — see step 3 below.

### 3. Register webhooks (required after first connection)

Webhooks tell the Go server where to POST events.  Each webhook is
registered for a **single** event, so one curl per event:

```bash
# List registered webhooks (sanity check)
curl -u "$GATEWAY_USERNAME:$GATEWAY_PASSWORD" \
  https://sms.example.com/api/3rdparty/v1/webhooks

# Register webhooks for all events we consume (one curl per event)
for EVENT in "sms:received" "sms:delivered" "sms:failed" "system:ping" "app:started"; do
  curl -X POST -u "$GATEWAY_USERNAME:$GATEWAY_PASSWORD" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"https://app.example.com/webhook\", \"event\": \"$EVENT\"}" \
    https://sms.example.com/api/3rdparty/v1/webhooks
done
```

The **Signing Key** used in `X-Signature` is set on the phone:
**Settings → Webhooks → Signing Key** → enter `GATEWAY_HMAC_SECRET` from
`.env` (the same key our webhook receiver verifies with).

### 4. Set the ping interval

1. **Settings → Ping**: enable and set interval to **14 minutes** or less
   (the watchdog alerts after 3 × `heartbeat_interval_min`, default 45 min;
   with 15 min interval a missed ping triggers at 45 min).
   Shorter = faster dead-device detection, at the cost of slightly more
   battery / network.

### 5. Battery optimisation

The app must stay alive in the background to receive SMS and maintain the
Go server connection. The goal is the same on every Android version: the
app must be exempt from battery optimisation and allowed background data.
Exact paths differ between versions; the **Android 7.x example** below is
verified, the 8–13 alternatives are indicative (menu names vary slightly
across OEM skins).

1. **Battery optimisation**
   - Android 7.x example: **Settings → Battery → ⋮ (three-dot menu) →
     Battery optimization → All apps → SMS Gateway → Don't optimize**
     (Android 7 has no "Unrestricted" battery switch — the ⋮ menu is the
     7.x way).
   - Android 8–9: **Settings → Apps & notifications → Advanced → Special
     app access → Battery optimization → SMS Gateway → Don't optimize**.
   - Android 10–13: **Settings → Apps → SMS Gateway → Battery (or App
     battery usage) → Unrestricted**.
2. **Background data**
   - Android 7.x example: **Settings → Data usage → ⋮ (menu) → Auto-sync
     data** → ON, and the app's own **Data usage** page must allow
     background data.
   - Android 10–13: **Settings → Network & internet → Data saver** → OFF
     (or add the app to exceptions), and
     **Settings → Apps → SMS Gateway → Mobile data & Wi-Fi → Background
     data** → ON.
3. **Wi-Fi sleep**
   - Android 7.x example: **Settings → Wi-Fi → ⋮ → Advanced → Keep Wi-Fi
     on during sleep → Always**.
   - Android 9+ usually keeps Wi-Fi on during sleep with no setting; on
     some builds the option lives under **Settings → Network & internet →
     Wi-Fi → Network preferences** ("Turn on Wi-Fi automatically").
4. **Disable any battery saver / OEM power manager** that might kill
   background apps (**Settings → Battery → Battery saver** on stock
   Android; on OEM skins also the per-app autostart/launcher manager).

### 6. RCS / Chat Features

RCS (Rich Communication Services) can interfere with SMS delivery. There is
no universal system-level toggle (Android 7 has none at all; on 10+ it
appears only in some builds/carriers) — disable it on the messaging-app
level:

1. In the Messages app (Google Messages): **Settings → Chat features →
   toggle OFF** (if the feature is provisioned by the carrier).
2. Or disable RCS on the carrier's side / in the carrier app.
3. Fallback: force the network mode to GSM only (see step 8 below) — SMS
   then bypasses the RCS path entirely.

### 7. SIM PIN

If the SIM has a PIN set, the phone may not receive SMS after a reboot until
the PIN is entered (the phone sits on the lock screen asking for the PIN —
unattended reboots then silently lose SMS). Path (Android 7.x example; on
8–13 it is under **Settings → Security → Advanced** on most builds):
**Settings → Security → Set up SIM card lock → Lock SIM card: OFF**.
Either remove the SIM PIN or ensure the phone boots unattended (no PIN on
that SIM slot).

### 8. Test SMS network mode (optional)

Dial `*#*#4636#*#*` (Android secret code) → **Phone information → Set
preferred network type → GSM only**.  Using 2G/3G for SMS testing reduces
variables compared to LTE/5G.  Return to your preferred mode afterwards.

### 9. Verify the end-to-end path

Send an SMS from a whitelisted number to your gateway phone:
```
Hello
```

You should receive a reply within ~10–30 seconds:
```
(mock) Hello
```
(or a GigaChat reply if `MOCK_LLM=false`).

Check the app logs if nothing arrives:
```bash
docker compose logs app --tail=50
```

### Troubleshooting: silent when the screen is locked

**Symptom:** while the phone screen is on, SMS are forwarded within
seconds; after the screen locks for a while, replies stop arriving
immediately (or stop entirely until the device is touched). This is the
classic background-Wi-Fi problem: either the Wi-Fi driver enters low-power
mode or the router's NAT/idle-timeout silently drops the long-lived
connection the app holds to the Go server (Server-Sent Events stream).

**IMPORTANT — events are NOT lost.** The android-sms-gateway app queues
webhook deliveries and retries them with exponential backoff (10 s → 20 s
→ 40 s …, **14 retries by default ≈ 45.5 hours**; configurable in
**Settings → Webhooks**). When connectivity returns, the queued events are
delivered **late, not never**. This is exactly why the server-side dedup
TTL is 48 h (`webhook.dedup_ttl_hours`, default) — it intentionally covers
the gateway's full retry horizon, so a late retry is recognised as an
already-processed event rather than being handled twice.

**Root-cause notes.** The app already holds a `WifiLock` (upstream issue
#99 in `capcom6/android-sms-gateway`, implemented), and apps exempt from
battery optimisation are not network-restricted by Doze. So the remaining
suspects are the **Wi-Fi driver's power-save** and **router-side
NAT/keep-alive timeouts** — not the app itself.

**Fixes, ranked (do the first one first):**

1. **Developer options → "Stay awake while charging"** (recommended). The
   device sits on a charger 24/7 anyway; with the screen never sleeping,
   neither light nor deep Doze can engage and Wi-Fi stays up. Set screen
   brightness to minimum. Enable it via **Settings → About phone → tap
   "Build number" 7× → Developer options → Stay awake**. Cons: constant
   100 % charge accelerates battery wear — include the battery (swelling,
   capacity) in your periodic physical check of the device.
2. **Configure Telegram alerts** — the watchdog alerts you when the phone
   has been silent for more than 3 × the ping interval (see
   "Telegram alerts (step 5)"), so degradation is detected even without
   watching the logs.
3. **Mobile-data fallback** on the gateway SIM: if Wi-Fi drops, the app
   reconnects to your public private-server URL over cellular (the SIM
   tariff then needs a little mobile data — see Step 0 in
   "Quick start: from zero to first SMS").
4. **Router-side hardening**: a static DHCP lease for the phone, and
   disable aggressive Wi-Fi power-save / client idle-timeout for this
   device.
5. **Raise the webhook retry count** in **Settings → Webhooks** if your
   outages can exceed ~45 hours.

**Diagnostics:** a permanent notification in the status bar means the app
is alive; the app's Home screen shows the connection state. Server-side,
the watchdog counters `heartbeats` and `watchdog_alerts` (see "Counters")
show whether pings keep arriving and whether a silence alert fired; the
current retry count is visible in **Settings → Webhooks** on the phone.

*(Facts verified against docs.sms-gate.app — FAQ "Local Server" and
"Webhooks" — and upstream issue #99 in `capcom6/android-sms-gateway`;
retrieved 2026-09-19.)*

## First live run checklist (mandatory)

Do not consider the bot "working" until every item below passes. The items
are ordered: each failure mode has a specific diagnostic.

1. **Go server reachable** (public path, v1.47.4):
   ```bash
   curl https://sms.example.com/api/3rdparty/v1/health
   # expected: {"status":"ok",...}
   ```
2. **Phone connected** — the app's Home screen shows the Cloud Server as
   online ("Offline" button tapped, Private Token matches
   `GATEWAY_PRIVATE_TOKEN`).
3. **Webhooks registered** — `Phone setup` step 3 (one curl per event); the
   list call must show all five events (`sms:received`, `sms:delivered`,
   `sms:failed`, `system:ping`, `app:started`).
4. **Server clock in sync** — webhook signatures carry a Unix timestamp that
   must be within ±`webhook.timestamp_window_s` (default 300 s = 5 min) of
   the server clock. If the clock drifts more than that, EVERY webhook is
   rejected with `401 rejected_timestamp` — this is NOT a Signing Key
   problem, do not regenerate the key; fix the clock:
   ```bash
   timedatectl   # "System clock synchronized: yes" and "NTP service: active"
   ```
   If NTP is inactive: `sudo timedatectl set-ntp true` (or configure
   chrony/ntpsec) and re-check before debugging anything else.
5. **First SMS accepted** — send `Hello` from a whitelisted number, then:
   ```bash
   docker compose logs app --tail=20 | grep "Webhook accepted"
   # expected: Webhook accepted: event=sms:received message_id=... queue_id=...
   ```
   If the log shows **nothing at all**, the webhook never reached the app —
   re-check registration (item 3). If it shows
   `Webhook rejected: reason=signature`, the phone's **Signing Key does not
   match** `GATEWAY_HMAC_SECRET`: set it on the phone under
   **Settings → Webhooks → Signing Key** (the receiver compares
   constant-time against `webhook.secret`). The `rejected_signature`
   counter in `/healthz` confirms the same from the outside. (A
   `rejected_timestamp` spike points to item 4 — the clock, not the key.)
6. **First LLM call** — with `MOCK_LLM=true` the reply must be
   `(mock) Hello`; with a real key it must be a GigaChat answer within
   ~10–30 s. The `llm_calls` counter (and `llm_errors` on failure) in
   `/healthz` reflects it. A `sms_send_failed` at this point points to the
   reply path, not the LLM — go to item 7.
7. **`messages_path` calibration** — run the live curl from
   "Calibrating `gateway.messages_path`" above; it must return `2xx`
   before real replies can be sent. `401` here means
   `GATEWAY_USERNAME`/`GATEWAY_PASSWORD` do not match the phone-generated
   Cloud Server credentials.
8. **Monitoring** — add `https://app.example.com/healthz` to UptimeRobot
   ("External monitoring" above); it reports `503` if the DB or the worker
   degrades.
9. **No "silent screen" degradation** — if SMS start arriving only after
   you touch the phone (delayed by minutes/hours instead of seconds), work
   through "Troubleshooting: silent when the screen is locked" in the
   Phone setup section (queued webhooks are delivered late, not lost).
