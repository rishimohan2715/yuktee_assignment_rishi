// Package vendorclient talks to the flaky messaging vendor. Two failure
// modes get different handling: an isolated 429/503/timeout is absorbed by
// retries with backoff inside one Send call, while a run of consecutive
// failures trips a process-wide circuit breaker that fails fast for a
// cooldown window instead of continuing to hammer a vendor that's down.
//
// Idempotency: every Send for the same logical notification must reuse the
// same idempotencyKey. The vendor mostly dedupes on it; the guard against
// the cases where it doesn't lives in store.RecordNotifySuccess, not here —
// this package only talks to the vendor, it has no notion of leads.
package vendorclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("vendorclient: circuit open, vendor presumed down")

type SendResult struct {
	Status          string // "sent"
	VendorMessageID string
	Duplicate       bool
}

// AttemptOutcome describes one HTTP round trip, for the caller to log.
type AttemptOutcome struct {
	Outcome    string // sent | duplicate | rate_limited | unavailable | timeout | circuit_open | error
	HTTPStatus int
	Detail     string
	Latency    time.Duration
}

type Config struct {
	BaseURL          string
	RequestTimeout   time.Duration // per-call timeout, well under the vendor's 30s hang
	MaxAttempts      int           // retry bound within one Send call
	BaseBackoff      time.Duration
	MaxBackoff       time.Duration
	FailureThreshold int // consecutive real failures before the breaker opens
	OpenCooldown     time.Duration
}

func DefaultConfig() Config {
	return Config{
		BaseURL:        "http://localhost:9000",
		RequestTimeout: 3 * time.Second,
		MaxAttempts:    6,
		BaseBackoff:    200 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		// 5 consecutive real failures is comfortably below what a genuine
		// outage looks like, and comfortably above ordinary noise, so it
		// trips on a real outage without tripping on a bad-luck streak of
		// 429s/timeouts.
		FailureThreshold: 5,
		OpenCooldown:     8 * time.Second,
	}
}

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

// breaker is a minimal circuit breaker: closed/open/half-open, shared across
// all leads since they hit the same vendor.
type breaker struct {
	mu              sync.Mutex
	state           breakerState
	consecutiveFail int
	openUntil       time.Time
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case closed:
		return true
	case open:
		if time.Now().After(b.openUntil) {
			b.state = halfOpen
			return true
		}
		return false
	default: // halfOpen: let one trial through
		return true
	}
}

func (b *breaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFail = 0
	b.state = closed
}

func (b *breaker) recordFailure(threshold int, cooldown time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFail++
	if b.state == halfOpen || b.consecutiveFail >= threshold {
		b.state = open
		b.openUntil = time.Now().Add(cooldown)
	}
}

type Client struct {
	cfg     Config
	http    *http.Client
	breaker *breaker
}

func New(cfg Config) *Client {
	return &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: cfg.RequestTimeout},
		breaker: &breaker{},
	}
}

type sendRequest struct {
	LeadID         string `json:"lead_id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Message        string `json:"message"`
}

// Send delivers one message with bounded retries and backoff, failing fast
// with ErrCircuitOpen if a sustained outage has already been detected.
// onAttempt fires after every round trip, including failed ones, so the
// caller can persist a debug trail.
func (c *Client) Send(ctx context.Context, leadID, idempotencyKey, message string, onAttempt func(AttemptOutcome)) (*SendResult, error) {
	var lastErr error

	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if !c.breaker.allow() {
			onAttempt(AttemptOutcome{Outcome: "circuit_open", Detail: "sustained outage detected, failing fast"})
			return nil, ErrCircuitOpen
		}

		result, outcome, retryAfter, err := c.doOnce(ctx, leadID, idempotencyKey, message)
		onAttempt(outcome)

		if err == nil {
			c.breaker.recordSuccess()
			return result, nil
		}
		lastErr = err
		// 429 means the vendor is up and asking us to slow down, not that
		// it's down, so it doesn't count toward the breaker.
		if outcome.Outcome != "rate_limited" {
			c.breaker.recordFailure(c.cfg.FailureThreshold, c.cfg.OpenCooldown)
		}

		if attempt == c.cfg.MaxAttempts {
			break
		}

		wait := c.backoffFor(attempt)
		if retryAfter > 0 {
			wait = retryAfter
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("vendorclient: exhausted %d attempts: %w", c.cfg.MaxAttempts, lastErr)
}

// backoffFor returns an exponential delay with full jitter, so concurrent
// retries don't all land on the vendor at once.
func (c *Client) backoffFor(attempt int) time.Duration {
	d := c.cfg.BaseBackoff * time.Duration(1<<uint(attempt-1))
	if d > c.cfg.MaxBackoff {
		d = c.cfg.MaxBackoff
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

func (c *Client) doOnce(ctx context.Context, leadID, idempotencyKey, message string) (*SendResult, AttemptOutcome, time.Duration, error) {
	start := time.Now()
	body, _ := json.Marshal(sendRequest{LeadID: leadID, IdempotencyKey: idempotencyKey, Message: message})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return nil, AttemptOutcome{Outcome: "error", Detail: err.Error()}, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	latency := time.Since(start)
	if err != nil {
		// Covers real network errors and the client timeout firing on a
		// vendor hang; we never wait anywhere near the vendor's 30s.
		return nil, AttemptOutcome{Outcome: "timeout", Detail: err.Error(), Latency: latency}, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		var payload struct {
			Status          string `json:"status"`
			VendorMessageID string `json:"vendor_message_id"`
			Duplicate       bool   `json:"duplicate"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, AttemptOutcome{Outcome: "error", HTTPStatus: 200, Detail: "malformed response body", Latency: latency}, 0, err
		}
		outcomeName := "sent"
		if payload.Duplicate {
			outcomeName = "duplicate"
		}
		return &SendResult{Status: payload.Status, VendorMessageID: payload.VendorMessageID, Duplicate: payload.Duplicate},
			AttemptOutcome{Outcome: outcomeName, HTTPStatus: 200, Detail: string(raw), Latency: latency}, 0, nil

	case http.StatusTooManyRequests:
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		return nil, AttemptOutcome{Outcome: "rate_limited", HTTPStatus: 429, Detail: string(raw), Latency: latency},
			retryAfter, fmt.Errorf("rate_limited")

	case http.StatusServiceUnavailable:
		return nil, AttemptOutcome{Outcome: "unavailable", HTTPStatus: 503, Detail: string(raw), Latency: latency}, 0,
			fmt.Errorf("service_unavailable")

	default:
		return nil, AttemptOutcome{Outcome: "error", HTTPStatus: resp.StatusCode, Detail: string(raw), Latency: latency}, 0,
			fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return time.Duration(secs) * time.Second
}
