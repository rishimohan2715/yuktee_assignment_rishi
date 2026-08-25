# Yuktee lead claim service

Go service for Part 1 (lead claim/release, Redis + Postgres) and Part 2
(vendor notify). Part 3 is a written design doc, no code, per the brief.

## Running it

```bash
# 1. Redis + Postgres
docker compose up -d

# 2. Schema
psql "postgres://yuktee:yuktee@localhost:5432/yuktee?sslmode=disable" -f migrations/001_schema.sql

# 3. Vendor stub
python3 vendor_stub.py &

# 4. Service
go build -o bin/server ./cmd/server
./bin/server   # :8080
```

Env vars (optional, default to the docker-compose values): `DATABASE_URL`,
`REDIS_ADDR`, `VENDOR_URL`, `LISTEN_ADDR`.

Note: I developed against `docker-compose.yml` as given. Partway through, my
disk filled up and Docker got stuck restarting, so I validated the running
service against a local Redis/Postgres instead (same schema, same connection
string shape). `docker-compose.yml` itself is untouched.

### Endpoints

```
POST /leads/{id}/claim     {"lease_seconds": 30}                    -> owner_token, fencing_token
POST /leads/{id}/release   {"owner_token", "fencing_token"}
POST /leads/{id}/notify    {"owner_token", "fencing_token", "message"}
GET  /healthz
```

---

## Part 1 — Lead claim service

### Design

- **Redis** does the actual locking: `SET key owner NX EX ttl`, with a
  monotonic fencing token issued in the same Lua script (`internal/lease`).
  One script does both, so a lock can never exist without a token and two
  workers can never get the same one.
- **Postgres** decides who's allowed to act. Every write after a claim
  (`release`, `notify`) must carry the fencing token it was issued, and the
  write only applies if that token is still the highest one recorded for the
  lead (`internal/store`). Claim doesn't need this check — Redis's `SETNX`
  already makes it exclusive.
- Schema in `migrations/001_schema.sql`: a `leads` table and a
  `notify_attempts` table (one row per vendor call, for debugging).

### The lease, in plain terms

Think of it like a claim ticket that expires. A worker claims a lead and
gets a ticket good for a fixed window; if the worker disappears, the ticket
board tears it up automatically once the window passes, so someone else can
take a fresh one. Every ticket also carries a number that only goes up for
that lead — 1, 2, 3 — issued at the same instant as the ticket, so there's
never any doubt about which one is newest.

The database doesn't trust the ticket board at all. When a worker wants to
release or message a lead, the database checks the number on its ticket
against the highest number it's ever seen for that lead. If the worker's
ticket expired and someone else got a new one in the meantime, the old
number is stale and the action is refused — it doesn't matter that the
worker still believes its ticket is good. That's what stops a worker that
paused too long from acting on a lead someone else has since taken over.

### If the holder pauses past the lease and resumes

Redis will have already expired the lock, and maybe reissued it, by the
time the paused worker wakes up. The worker itself has no way to know this —
a live TTL only tells you whether the lease *was* valid at claim time, not
whether it still is by the time you act. That's the standard problem with
plain lease locks (Kleppmann's argument against relying on Redlock alone).

The fix is the fencing token above. Postgres, not Redis, decides whether a
write is still valid, and it does so by comparing tokens, not by re-checking
liveness. A worker that resumes after expiry presents a token that's now
behind whatever a newer claim wrote, so its write gets `409
stale_fencing_token` instead of silently applying.

I tested this directly: claimed a lead with a 2s lease, slept 3s, let a
second worker claim it (fencing token 2), then had the first worker try to
notify with its old token 1. Rejected before it ever reached the vendor;
only the second worker's message went through.

**Cost:** an extra column and an extra comparison on every write, and every
caller has to carry the fencing token forward — if some future code path
forgets to check it, the protection is gone for that path. It also means a
paused worker gets no warning until it actually tries to write; there's no
way to warn it sooner, because the whole premise is that its own view of
holding the lease can already be wrong. I took that tradeoff because
trusting TTL expiry alone fails silently in exactly this scenario, and a
silent double-message is worse than a 409 the caller has to handle.

One gap I left alone: if the process crashes between the Redis `SET`
succeeding and the Postgres write, Redis ends up holding a lock with no
matching Postgres row. Nothing gets stuck — the TTL still expires on its
own — so I didn't add extra recovery for it. A returned error (as opposed to
a crash) on the Postgres write does roll back the Redis lock explicitly; see
`api.claim`.

### How I'd test this

- Fire N concurrent claims at the same lead ID; assert exactly one succeeds
  and the rest get 409, and the surviving Postgres row's fencing_token
  matches the winner. (Ran this manually with 10 real concurrent requests —
  1 winner, 9× 409.)
- Claim with a short TTL, let it expire, claim again as a new owner, assert
  the first owner's release and notify both get 409 while the second
  owner's succeed.
- Call release with a token that never held the lock; assert 409
  `not_holder`, not a crash.
- Drive notify across the vendor stub's full sequence, including the
  outage window, for many leads; assert `GET /_stats` on the vendor shows
  `leads_messaged_more_than_once: []`.
- Call notify twice with the same fencing token after a successful send;
  assert the vendor's call count doesn't move on the second call.
- Kill the process between the Redis claim and the Postgres write; assert
  the lead self-heals once the TTL expires instead of staying stuck.

### Go background

Before this assignment, my primary hands-on experience with Go was building the central orchestrator and the crop health agent for my final-year capstone project. The project is a distributed multi-agent framework designed as a decision engine for paddy farmers across 29 Karnataka districts. It coordinates five specialized agents (crop health, weather, soil, market price, and pest risk) over gRPC, aggregating their outputs through a Go backend to feed a real-time React dashboard.

What tripped me up initially in Go was shifting away from exception-based error handling to verbose, explicit if err != nil checks across every network boundary, along with managing context cancellation cleanly across concurrent goroutines. In this assignment specifically, the main hurdle was getting the Redis Lua script mechanics and PostgreSQL transaction isolation behavior right to guarantee absolute mutual exclusion during lease contention.


### AI tool use

I used Claude Code for roughly 60% of the build—primarily for generating the initial Go HTTP boilerplate, SQL schema scaffolding, and drafting baseline unit test structures.
The remaining 40% was manual architectural direction, debugging, and verification:
⚬	Designing the distributed lease invariant and writing the atomic Redis Lua release script.
⚬	Enforcing database fencing/versioning in PostgreSQL to catch stale worker writes.
⚬	Configuring strict HTTP client timeouts and backoff jitter to handle the 30-second hangs and sustained 503 outages in vendor_stub.py.
⚬	Verifying state transitions and edge cases against live Postgres and Redis containers rather than accepting generated code on faith.

## Part 2 — Flaky vendor integration

`notify` is gated on the same fencing token as `release`, so a stale caller
never reaches the vendor at all. Once that passes, it calls
`internal/vendorclient`.

**Retries and backoff.** Up to 6 attempts per notify call, exponential
backoff (200ms base, doubling, capped at 3s) with full jitter. A 429's
`Retry-After` overrides the computed backoff. Each HTTP call has a 3s
client-side timeout, so a vendor hang is treated as a failure in ~3s instead
of the vendor's 30s.

**Idempotency.** Every attempt for a given lead reuses the same
`idempotency_key` (`notify:{lead_id}`), so the vendor's own dedup — which
fails 15% of the time by design — has a fighting chance. The real guard
against that 15% is Postgres: `RecordNotifySuccess` only flips
`notified_at` from NULL once, gated on the fencing token, so even if the
vendor silently sent twice, the lead is only ever marked sent once. Before
spending a vendor call, notify checks `notified_at IS NULL` first and
short-circuits to `{"notified": true, "already": true}` if it's already
been sent — which also makes notify itself safe to call again if a caller
lost the response to a network blip.

**Blips vs. sustained outage.** Two different mechanisms. A blip (one
429/503/timeout) is absorbed by the retry loop above, inline. A sustained
outage trips a process-wide circuit breaker (`internal/vendorclient`) after
5 consecutive real failures — 503/timeout, not 429, since a 429 means the
vendor is up and asking us to slow down, which is the opposite of evidence
it's down. Once open, calls fail fast for an 8s cooldown without touching
the vendor, then let one trial call through. 5 was picked as comfortably
below what a real outage looks like and comfortably above ordinary noise —
not read off the stub's specific outage length, since the brief says the
grading seed differs. While the breaker is open, notify returns `503
vendor_unavailable` immediately with a `Retry-After` hint; the lead stays
claimed and un-notified, and because notify is idempotent, the caller can
just call it again later at no risk. I didn't build an automatic retry
sweep for that — see below.

**Logging.** Every vendor round trip goes to stdout as a structured
(`log/slog`, JSON) line — lead ID, attempt, outcome, HTTP status, latency —
and as a row in `notify_attempts` in Postgres. `SELECT * FROM
notify_attempts WHERE lead_id = ? ORDER BY id` gives the full attempt
history for one lead without grepping logs.

### What I actually ran

Against the real `vendor_stub.py`, not mocked: a normal claim → notify →
release cycle, a double-claim, wrong-token release and notify, the
zombie-worker scenario above, and enough notify calls to walk the stub's
guaranteed outage window and back out the other side. I reran the whole
thing from a clean rebuild and wiped state before the final pass, not just
once during development.

- Double-claim: 409. Wrong fencing token: 409. Wrong owner token on
  release: 409.
- Zombie worker (2s lease, paused 3s, second worker claims in the
  meantime): zombie's notify rejected before reaching the vendor; new
  holder's notify succeeded.
- During the outage window: one notify call took ~3-5s (bounded retries,
  not 30s) before the breaker tripped; the next call failed fast in
  ~10-20ms instead of retrying into a dead vendor. First call after the
  outage window ended succeeded on the first attempt.
- Vendor's own `GET /_stats` after the full run: `"leads_messaged_more_than_once": []`.
- A separate test: 10 truly concurrent claims on the same lead ID — exactly
  1 winner, 9× 409.

### What I'd do next

- A scheduled sweep for leads stuck claimed/un-notified past some age, to
  retry notify without a human noticing. Right now that retry has to be
  triggered externally.
- A lease-renewal/heartbeat endpoint for workers whose real work legitimately
  runs longer than one lease window. Not built — the brief only asked for
  claim/release.

---

## Part 3 — Agent Council design

*(Written, no code. Assume this layer is built in Python.)*

Four independent scoring agents — Budget, Timeline, Fit, Authority — each
read the same lead context (transcript excerpt, CRM fields) and produce one
bounded, structured score for their own dimension. A plain-Python
aggregator, not another LLM call, combines the four into a Composite Intent
Score. No agent talks to another, and there's no back-and-forth — it's a
fan-out/fan-in of four independent classifiers, which is what makes the
rest of this boundable.

**Constraining output.** Each agent returns `{dimension, score: 0-100,
confidence: 0-1, evidence: <=200 chars, rationale_tags: [enum]}`, enforced
via structured-output/JSON-schema mode so malformed output is rejected at
the API boundary, not regex-parsed after the fact. A schema violation gets
one repair retry with the validation error appended to context; a second
failure is "agent unusable," not retried further. The aggregator only ever
sees validated objects.

**Disagreement and unusable output.** These four agents don't vote on the
same question, so there's no disagreement to reconcile in the usual sense.
The real failure modes are a low-confidence score, an agent that fails
validation twice, or evidence that contradicts across agents (Authority
says no decision-maker present, Fit assumes an enterprise buyer).
Aggregation is confidence-weighted — `score = Σ(score·weight·confidence) /
Σ(weight·confidence)` — so a low-confidence agent contributes
proportionally less instead of being trusted outright or blocking the
pipeline. An unusable agent gets a neutral score at zero confidence, which
drops it from the weighted average, and the lead is flagged
`partial_score: true` for review rather than presenting a falsely precise
number. Cross-agent contradictions aren't reconciled algorithmically —
there's no natural stopping point for agents arguing with each other —
they're surfaced as a `conflict_flags` list for a human or routing rule.

**Bounding the worst case.** Fixed call graph: four calls, fan-out, fan-in,
no re-planning, nothing that can loop. Each call has a ~4s timeout and
exactly one retry (schema repair only). A timeout is treated the same as an
unusable result. Since all four run concurrently, worst case is roughly one
timeout-plus-retry cycle (~8s), not four of them in series.

**Cost and latency inside the 60s SLA.** Scoring is one step inside a
60-second contact SLA that also includes the actual call/WhatsApp attempt,
so it should take a small slice of that — 5-8s. Running all four
concurrently against a fast/cheap model tier, ~1K input tokens and ~150
output tokens each, lands around 1-2s p50 per agent and 2-4s for the whole
fan-out, ~8s worst case with a retry. Cost is four small calls per lead —
sub-cent to low-single-digit cents at current small-model pricing — not the
binding constraint here; latency and worst-case bounding are.

**Provider-agnostic vs. not.** Worth abstracting: the model-call boundary
(`complete(schema, prompt) -> ValidatedResult`) so the model tier can be
swapped without touching the four agents' logic, and the schema-validation
layer, which shouldn't care which provider produced the JSON. Not worth
abstracting: per-agent prompt wording (tuned per model family, re-tuned on
any swap regardless of what interface sits in front of it), and the
aggregation formula, which is business logic and doesn't belong behind a
provider abstraction at all.

---

## Part 4 — Two short questions

What I'm Proudest of:

 I’m proudest of prioritizing systems with real computational and architectural constraints over generic CRUD dashboards. In my multi-agent edge agricultural system and local ONNX runtime setups, the hard part wasn't writing API endpoints—it was managing edge latency, handling disconnected states, and preventing worker bottlenecks under strict hardware limits. I built them by starting from the failure modes first (network drops, memory pressure, slow inference) rather than stitching together tutorial boilerplates.

What I'd describe differently:

 Earlier on my CV, I described projects by listing libraries and buzzwords—"built scalable full-stack app using React, Node, Express, MongoDB"—focusing on what tools I installed rather than the engineering trade-offs I made. Today, I’d rewrite those bullets to describe the actual invariants: state synchronization rules, database schema indexing, how race conditions were handled, and the specific failure recovery mechanisms implemented when third-party services stalled.

What I touch:

In my first week, my priority is getting the entire system running smoothly end-to-end and locking down our integration boundaries. I’d start by spinning up the local environment to trace how data actually flows from ingestion to database persistence. From there, I’d focus heavily on our external API integrations and worker boundaries making sure our HTTP timeouts, retry backoffs, and idempotency checks are rock solid. Third-party APIs are usually where systems silently bleed money through runaway retry loops or break during traffic spikes. Instrumenting clean, structured logging around these external calls and failure states ensures the system stays cost-efficient, predictable, and maintainable for the long haul.

What I leave alone:

I deliberately leave alone cosmetic cleanups, style debates, and premature architectural rewrites. If a service or database query is already working reliably and handling its edge cases, I’m not going to touch it just because the file layout looks unfamiliar or the Go/Python code feels slightly unidiomatic. Swapping out working libraries or trying to re-architect parts of the 70% that already function just introduces regressions and distracts from shipping. My goal in week one is to stabilize the foundations, verify the integration points, and help deliver the remaining 30% rather than rewriting code that’s already doing its job.


---

## Assumptions

- Lease default 30s, capped at 300s, configurable per claim via
  `{"lease_seconds": N}`.
- One idempotency key per lead for life (`notify:{lead_id}`) — the brief
  describes a single first-contact message, not a general messaging API.
- Circuit-breaker threshold/cooldown (5 failures / 8s) were sized by
  reasoning about the shape of the stub's failure mix, not by reading its
  specific outage constants, since the grading seed differs.
