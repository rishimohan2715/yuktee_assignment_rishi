-- Yuktee lead claim service schema.
--
-- fencing_token is the source of truth for "who is allowed to act on this
-- lead right now". It only ever moves forward (enforced by the app, which
-- always writes it alongside a claim and checks it on every later mutation).

CREATE TABLE IF NOT EXISTS leads (
    id                 TEXT PRIMARY KEY,
    status             TEXT NOT NULL DEFAULT 'new'
                        CHECK (status IN ('new', 'claimed', 'released', 'notified')),
    held_by            TEXT,               -- opaque owner token of current/last holder
    fencing_token      BIGINT NOT NULL DEFAULT 0,
    lease_expires_at   TIMESTAMPTZ,
    notified_at        TIMESTAMPTZ,
    vendor_message_id  TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per vendor call attempt, kept for 2am debugging via GET /_stats
-- comparisons and for reconstructing what happened to a given lead.
CREATE TABLE IF NOT EXISTS notify_attempts (
    id           BIGSERIAL PRIMARY KEY,
    lead_id      TEXT NOT NULL REFERENCES leads(id),
    attempt_no   INT NOT NULL,
    outcome      TEXT NOT NULL,   -- sent | duplicate | rate_limited | unavailable | timeout | circuit_open | error
    http_status  INT,
    detail       TEXT,
    latency_ms   INT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notify_attempts_lead_id ON notify_attempts (lead_id);
