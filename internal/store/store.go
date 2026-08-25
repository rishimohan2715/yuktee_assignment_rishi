// Package store persists lead state in PostgreSQL. Postgres is the source of
// truth for who's allowed to act on a lead: every write past the initial
// claim is gated on the fencing token still matching the highest one on
// record, so a stale writer gets rejected even after Redis has forgotten it.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrStaleFencingToken = errors.New("store: stale fencing token")
	ErrNotFound          = errors.New("store: lead not found")
	ErrAlreadyNotified   = errors.New("store: lead already notified")
)

type LeadStatus string

const (
	StatusNew      LeadStatus = "new"
	StatusClaimed  LeadStatus = "claimed"
	StatusReleased LeadStatus = "released"
	StatusNotified LeadStatus = "notified"
)

type Lead struct {
	ID              string
	Status          LeadStatus
	HeldBy          string
	FencingToken    int64
	LeaseExpiresAt  *time.Time
	NotifiedAt      *time.Time
	VendorMessageID string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// RecordClaim upserts the lead row for a freshly issued claim. Not
// fencing-gated: Redis already guaranteed this (owner, fencing) pair is
// exclusive, so there's nothing to race against here.
func (s *Store) RecordClaim(ctx context.Context, leadID, owner string, fencing int64, expiresAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO leads (id, status, held_by, fencing_token, lease_expires_at)
		VALUES ($1, 'claimed', $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			status = 'claimed',
			held_by = EXCLUDED.held_by,
			fencing_token = EXCLUDED.fencing_token,
			lease_expires_at = EXCLUDED.lease_expires_at,
			updated_at = now()
	`, leadID, owner, fencing, expiresAt)
	return err
}

// RecordRelease marks the lead released, gated on fencing matching the
// current row.
func (s *Store) RecordRelease(ctx context.Context, leadID string, fencing int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE leads SET status = 'released', updated_at = now()
		WHERE id = $1 AND fencing_token = $2
	`, leadID, fencing)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleFencingToken
	}
	return nil
}

func (s *Store) GetByID(ctx context.Context, leadID string) (*Lead, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, status, held_by, fencing_token, lease_expires_at, notified_at, COALESCE(vendor_message_id, '')
		FROM leads WHERE id = $1
	`, leadID)
	var l Lead
	var status string
	if err := row.Scan(&l.ID, &status, &l.HeldBy, &l.FencingToken, &l.LeaseExpiresAt, &l.NotifiedAt, &l.VendorMessageID); err != nil {
		return nil, ErrNotFound
	}
	l.Status = LeadStatus(status)
	return &l, nil
}

// CheckFencing verifies fencing is still current for leadID without
// mutating anything, so the notify path can reject a stale caller before
// spending a vendor call.
func (s *Store) CheckFencing(ctx context.Context, leadID string, fencing int64) error {
	lead, err := s.GetByID(ctx, leadID)
	if err != nil {
		return err
	}
	if lead.FencingToken != fencing {
		return ErrStaleFencingToken
	}
	if lead.NotifiedAt != nil {
		return ErrAlreadyNotified
	}
	return nil
}

// RecordNotifySuccess marks the lead notified, gated on fencing and on
// notified_at still being NULL. If two calls race past CheckFencing, only
// the first UPDATE here wins; the second affects zero rows and the caller
// treats that as "already notified" instead of sending twice.
func (s *Store) RecordNotifySuccess(ctx context.Context, leadID string, fencing int64, vendorMessageID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE leads SET status = 'notified', notified_at = now(), vendor_message_id = $3, updated_at = now()
		WHERE id = $1 AND fencing_token = $2 AND notified_at IS NULL
	`, leadID, fencing, vendorMessageID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

type NotifyAttempt struct {
	LeadID     string
	AttemptNo  int
	Outcome    string
	HTTPStatus int
	Detail     string
	LatencyMS  int
}

// LogNotifyAttempt records one vendor call attempt for debugging. Errors are
// swallowed on purpose — a lost debug row shouldn't turn a successful send
// into a failed request.
func (s *Store) LogNotifyAttempt(ctx context.Context, a NotifyAttempt) {
	_, _ = s.pool.Exec(ctx, `
		INSERT INTO notify_attempts (lead_id, attempt_no, outcome, http_status, detail, latency_ms)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, a.LeadID, a.AttemptNo, a.Outcome, a.HTTPStatus, a.Detail, a.LatencyMS)
}
