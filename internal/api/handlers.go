// Package api wires the claim/release/notify endpoints to the lease, store,
// and vendor client packages. Claim hands out a fencing token; release and
// notify both require it back and are rejected with 409 if it's stale.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"yuktee-assignment/internal/lease"
	"yuktee-assignment/internal/store"
	"yuktee-assignment/internal/vendorclient"
)

const (
	defaultLeaseSeconds = 30
	maxLeaseSeconds     = 300
)

type Handler struct {
	Lease  *lease.Manager
	Store  *store.Store
	Vendor *vendorclient.Client
	Log    *slog.Logger
}

func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /leads/{id}/claim", h.claim)
	mux.HandleFunc("POST /leads/{id}/release", h.release)
	mux.HandleFunc("POST /leads/{id}/notify", h.notify)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
}

// ---------------------------------------------------------------- claim

type claimRequest struct {
	LeaseSeconds int `json:"lease_seconds"`
}

func (h *Handler) claim(w http.ResponseWriter, r *http.Request) {
	leadID := r.PathValue("id")
	log := h.Log.With("endpoint", "claim", "lead_id", leadID)

	var req claimRequest
	_ = json.NewDecoder(r.Body).Decode(&req) // empty body is fine, defaults apply
	leaseSeconds := req.LeaseSeconds
	if leaseSeconds <= 0 {
		leaseSeconds = defaultLeaseSeconds
	}
	if leaseSeconds > maxLeaseSeconds {
		leaseSeconds = maxLeaseSeconds
	}

	claim, err := h.Lease.Claim(r.Context(), leadID, leaseSeconds)
	if errors.Is(err, lease.ErrAlreadyHeld) {
		log.Info("claim rejected: already held")
		writeJSON(w, http.StatusConflict, map[string]any{"claimed": false, "reason": "already_held"})
		return
	}
	if err != nil {
		log.Error("claim: redis error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"claimed": false, "reason": "internal_error"})
		return
	}

	if err := h.Store.RecordClaim(r.Context(), leadID, claim.OwnerToken, claim.FencingToken, claim.ExpiresAt); err != nil {
		log.Error("claim: postgres error, rolling back redis lock", "error", err)
		_ = h.Lease.Release(r.Context(), leadID, claim.OwnerToken)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"claimed": false, "reason": "internal_error"})
		return
	}

	log.Info("claim granted", "owner_token", claim.OwnerToken, "fencing_token", claim.FencingToken, "lease_seconds", leaseSeconds)
	writeJSON(w, http.StatusOK, map[string]any{
		"claimed":       true,
		"owner_token":   claim.OwnerToken,
		"fencing_token": claim.FencingToken,
		"lease_seconds": leaseSeconds,
		"expires_at":    claim.ExpiresAt,
	})
}

// ---------------------------------------------------------------- release

type releaseRequest struct {
	OwnerToken   string `json:"owner_token"`
	FencingToken int64  `json:"fencing_token"`
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	leadID := r.PathValue("id")
	log := h.Log.With("endpoint", "release", "lead_id", leadID)

	var req releaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OwnerToken == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"released": false, "reason": "owner_token_required"})
		return
	}

	// Redis check first: this is what actually blocks a wrong-owner or
	// expired caller from dropping someone else's active lock.
	err := h.Lease.Release(r.Context(), leadID, req.OwnerToken)
	if errors.Is(err, lease.ErrNotHolder) {
		log.Info("release rejected: caller is not current holder", "owner_token", req.OwnerToken)
		writeJSON(w, http.StatusConflict, map[string]any{"released": false, "reason": "not_holder"})
		return
	}
	if err != nil {
		log.Error("release: redis error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"released": false, "reason": "internal_error"})
		return
	}

	// Defense in depth: Redis already confirmed this owner_token holds the
	// lock, but Postgres is what notify also trusts, so check fencing here
	// too for a consistent 409 contract between the two endpoints.
	if err := h.Store.RecordRelease(r.Context(), leadID, req.FencingToken); err != nil {
		log.Info("release rejected: stale fencing token", "fencing_token", req.FencingToken)
		writeJSON(w, http.StatusConflict, map[string]any{"released": false, "reason": "stale_fencing_token"})
		return
	}

	log.Info("release granted", "owner_token", req.OwnerToken, "fencing_token", req.FencingToken)
	writeJSON(w, http.StatusOK, map[string]any{"released": true})
}

// ---------------------------------------------------------------- notify

type notifyRequest struct {
	OwnerToken   string `json:"owner_token"`
	FencingToken int64  `json:"fencing_token"`
	Message      string `json:"message"`
}

func (h *Handler) notify(w http.ResponseWriter, r *http.Request) {
	leadID := r.PathValue("id")
	log := h.Log.With("endpoint", "notify", "lead_id", leadID)

	var req notifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"notified": false, "reason": "message_required"})
		return
	}

	// Cheap check before spending a vendor call: is this fencing token
	// still current, and has the lead already been messaged? This also
	// makes notify itself safe to call again after a lost response.
	if err := h.Store.CheckFencing(r.Context(), leadID, req.FencingToken); err != nil {
		switch {
		case errors.Is(err, store.ErrAlreadyNotified):
			log.Info("notify: already sent, returning idempotent success")
			writeJSON(w, http.StatusOK, map[string]any{"notified": true, "already": true})
		case errors.Is(err, store.ErrStaleFencingToken):
			log.Info("notify rejected: stale fencing token", "fencing_token", req.FencingToken)
			writeJSON(w, http.StatusConflict, map[string]any{"notified": false, "reason": "stale_fencing_token"})
		default:
			log.Error("notify: lead lookup failed", "error", err)
			writeJSON(w, http.StatusNotFound, map[string]any{"notified": false, "reason": "lead_not_found"})
		}
		return
	}

	idempotencyKey := "notify:" + leadID // stable across retries of this send
	attempt := 0
	result, err := h.Vendor.Send(r.Context(), leadID, idempotencyKey, req.Message, func(o vendorclient.AttemptOutcome) {
		attempt++
		h.Store.LogNotifyAttempt(r.Context(), store.NotifyAttempt{
			LeadID: leadID, AttemptNo: attempt, Outcome: o.Outcome,
			HTTPStatus: o.HTTPStatus, Detail: o.Detail, LatencyMS: int(o.Latency.Milliseconds()),
		})
		log.Info("vendor attempt", "attempt", attempt, "outcome", o.Outcome, "http_status", o.HTTPStatus, "latency_ms", o.Latency.Milliseconds())
	})

	if err != nil {
		// Retries exhausted or breaker open — either way, don't hold the
		// request open for minutes. Fail fast, leave the lead claimed and
		// un-notified, let the caller retry notify later.
		retryAfter := 5
		if errors.Is(err, vendorclient.ErrCircuitOpen) {
			retryAfter = 10
		}
		log.Error("notify: vendor send failed after retries", "error", err, "attempts", attempt)
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"notified": false, "reason": "vendor_unavailable", "attempts": attempt, "retry_after_seconds": retryAfter,
		})
		return
	}

	applied, dbErr := h.Store.RecordNotifySuccess(r.Context(), leadID, req.FencingToken, result.VendorMessageID)
	if dbErr != nil {
		// The vendor call already succeeded. Do not retry it here —
		// retrying a send we know went through is the double-message bug
		// this service exists to prevent. Surface the inconsistency
		// instead of silently masking it.
		log.Error("notify: sent by vendor but failed to record locally", "error", dbErr, "vendor_message_id", result.VendorMessageID)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"notified": false, "reason": "sent_but_not_recorded", "vendor_message_id": result.VendorMessageID,
		})
		return
	}
	if !applied {
		log.Info("notify: lost the race to record success, another call already marked this lead notified")
		writeJSON(w, http.StatusOK, map[string]any{"notified": true, "already": true})
		return
	}

	log.Info("notify: message sent", "vendor_message_id", result.VendorMessageID, "duplicate", result.Duplicate, "attempts", attempt)
	writeJSON(w, http.StatusOK, map[string]any{
		"notified": true, "vendor_message_id": result.VendorMessageID, "duplicate": result.Duplicate, "attempts": attempt,
	})
}

// ---------------------------------------------------------------- helpers

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
