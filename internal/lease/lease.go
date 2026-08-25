// Package lease implements the Redis-backed exclusive lock used to make sure
// only one worker acts on a lead at a time.
//
// A plain TTL lock isn't enough: a worker can stall past its lease (GC pause,
// frozen container) and resume believing it's still the holder, after Redis
// has already handed the lease to someone else. To make that safe, every
// claim also gets a fencing token — a per-lead counter that only increases.
// Redis issues the lock and the token together, atomically. Callers must
// carry the token into any later write (release, notify), and the store
// package (not Redis) is what actually enforces it.
package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrAlreadyHeld = errors.New("lease: already held")
	ErrNotHolder   = errors.New("lease: caller is not the current holder")
)

const keyPrefix = "yuktee:lease:"
const fencingPrefix = "yuktee:fencing:"

// Only increments the fencing counter if the lock is free, so a token is
// never issued without a matching lock, and two callers can never get the
// same token.
var claimScript = redis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing then
  return {0, existing}
end
local fencing = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return {1, tostring(fencing)}
`)

// Deletes the lock only if it still belongs to the caller.
var releaseScript = redis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

type Manager struct {
	rdb *redis.Client
}

func NewManager(rdb *redis.Client) *Manager {
	return &Manager{rdb: rdb}
}

type Claim struct {
	OwnerToken   string
	FencingToken int64
	LeaseSeconds int
	ExpiresAt    time.Time
}

// Claim acquires the lease for leadID, returning a fresh owner token and a
// fencing token higher than any previously issued for this lead. Returns
// ErrAlreadyHeld if another owner currently holds it.
func (m *Manager) Claim(ctx context.Context, leadID string, leaseSeconds int) (*Claim, error) {
	owner, err := randomToken()
	if err != nil {
		return nil, err
	}

	lockKey := keyPrefix + leadID
	fencingKey := fencingPrefix + leadID

	res, err := claimScript.Run(ctx, m.rdb, []string{lockKey, fencingKey}, owner, leaseSeconds).Slice()
	if err != nil {
		return nil, err
	}
	ok := res[0].(int64)
	if ok == 0 {
		return nil, ErrAlreadyHeld
	}
	fencingStr := res[1].(string)
	fencing, err := strconv.ParseInt(fencingStr, 10, 64)
	if err != nil {
		return nil, err
	}

	return &Claim{
		OwnerToken:   owner,
		FencingToken: fencing,
		LeaseSeconds: leaseSeconds,
		ExpiresAt:    time.Now().Add(time.Duration(leaseSeconds) * time.Second),
	}, nil
}

// Release drops the lock if ownerToken is still the current holder. If the
// lease already expired or was reclaimed by someone else, it returns
// ErrNotHolder — not an error, just a no-op for a caller that showed up late.
func (m *Manager) Release(ctx context.Context, leadID, ownerToken string) error {
	lockKey := keyPrefix + leadID
	res, err := releaseScript.Run(ctx, m.rdb, []string{lockKey}, ownerToken).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrNotHolder
	}
	return nil
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
