package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Delivery struct {
	ID        int64
	EventType string
	TargetURL string
	Payload   json.RawMessage
	Signature string
	Attempts  int
}

type Store interface {
	ClaimDue(ctx context.Context, limit int) ([]Delivery, error)
	MarkDelivered(ctx context.Context, id int64, httpStatus int) error
	ScheduleRetryOrDead(ctx context.Context, id int64, attempts, maxAttempts, httpStatus int, errMsg string, nextAt time.Time) (dead bool, err error)
	ReclaimStuck(ctx context.Context, stuckAfter time.Duration) (int64, error)
}

type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

const claimDueSQL = `
WITH due AS (
  SELECT id
  FROM webhook_deliveries
  WHERE status = 'pending'
    AND next_attempt_at <= NOW()
  ORDER BY next_attempt_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE webhook_deliveries wd
SET
  status = 'processing',
  attempts = wd.attempts + 1,
  updated_at = NOW()
FROM due
WHERE wd.id = due.id
RETURNING wd.id, wd.event_type, wd.target_url, wd.payload, wd.signature, wd.attempts;
`

func (s *PgStore) ClaimDue(ctx context.Context, limit int) ([]Delivery, error) {
	rows, err := s.pool.Query(ctx, claimDueSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.EventType, &d.TargetURL, &d.Payload, &d.Signature, &d.Attempts); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

const markDeliveredSQL = `
UPDATE webhook_deliveries
SET status = 'delivered',
    last_http_status = $2,
    last_error = NULL,
    delivered_at = NOW(),
    updated_at = NOW()
WHERE id = $1;
`

func (s *PgStore) MarkDelivered(ctx context.Context, id int64, httpStatus int) error {
	_, err := s.pool.Exec(ctx, markDeliveredSQL, id, httpStatus)
	return err
}

const scheduleRetryOrDeadSQL = `
UPDATE webhook_deliveries
SET status = $2,
    last_http_status = NULLIF($3, 0),
    last_error = $4,
    next_attempt_at = $5,
    updated_at = NOW()
WHERE id = $1;
`

func (s *PgStore) ScheduleRetryOrDead(ctx context.Context, id int64, attempts, maxAttempts, httpStatus int, errMsg string, nextAt time.Time) (bool, error) {
	dead := attempts >= maxAttempts
	status := "pending"
	if dead {
		status = "dead"
	}
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	_, err := s.pool.Exec(ctx, scheduleRetryOrDeadSQL, id, status, httpStatus, errMsg, nextAt)
	return dead, err
}

const reclaimStuckSQL = `
UPDATE webhook_deliveries
SET status = 'pending',
    next_attempt_at = NOW(),
    updated_at = NOW()
WHERE status = 'processing'
  AND updated_at < NOW() - ($1 * INTERVAL '1 second');
`

func (s *PgStore) ReclaimStuck(ctx context.Context, stuckAfter time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, reclaimStuckSQL, int64(stuckAfter.Seconds()))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
