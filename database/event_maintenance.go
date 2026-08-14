/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// event_maintenance.go provides the singleton claim that decides WHICH replica runs the
// event pipeline's maintenance work.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blnkfinance/blnk/internal/apierror"
	"go.opentelemetry.io/otel"
)

// EventMaintenanceLockKey identifies the event-maintenance advisory lock.
const EventMaintenanceLockKey int64 = 0x626c6e6b65766d31

// ErrEventMaintenanceLeaseHeld reports that another process already owns the
// maintenance lease.
var ErrEventMaintenanceLeaseHeld = errors.New("blnk: the event maintenance lease is held by another process")

// EventMaintenanceLease is a held, session-scoped singleton claim.
type EventMaintenanceLease struct {
	conn *sql.Conn
	key  int64
}

// TryAcquireEventMaintenanceLease attempts to take the event-maintenance lease.
//
// Parameters:
//   - ctx context.Context: bounds acquiring the connection and issuing the lock.
//   - key int64: the advisory lock key.
//
// Returns:
//   - *EventMaintenanceLease: the held lease. Release it to give up leadership.
//   - error: ErrEventMaintenanceLeaseHeld when another process owns it, which is an
//     ordinary outcome; anything else is a real failure to reach the database.
func (d Datasource) TryAcquireEventMaintenanceLease(ctx context.Context, key int64) (*EventMaintenanceLease, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TryAcquireEventMaintenanceLease")
	defer span.End()

	if d.Conn == nil {
		err := apierror.NewAPIError(apierror.ErrInternalServer,
			"Cannot acquire the event maintenance lease without a database connection", nil)
		span.RecordError(err)

		return nil, err
	}

	conn, err := d.Conn.Conn(ctx)
	if err != nil {
		span.RecordError(err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to reserve a connection for the event maintenance lease",
			"acquire_event_maintenance_lease", err)
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		// The connection is released on every failure path, including this one. A leaked
		// pinned connection is worse than a missed lease: it is permanently removed from the
		// pool, so repeated attempts would exhaust it.
		_ = conn.Close()
		span.RecordError(err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to request the event maintenance advisory lock",
			"acquire_event_maintenance_lease", err)
	}

	if !acquired {
		_ = conn.Close()

		return nil, ErrEventMaintenanceLeaseHeld
	}

	return &EventMaintenanceLease{conn: conn, key: key}, nil
}

// Release gives up the lease and returns the pinned connection to the pool.
//
// Parameters:
//   - ctx context.Context: bounds the unlock statement.
//
// Returns:
//   - error: a failed unlock, for the record. Leadership is surrendered regardless.
func (l *EventMaintenanceLease) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}

	conn := l.conn
	l.conn = nil

	_, unlockErr := conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, l.key)
	closeErr := conn.Close()

	if unlockErr != nil {
		return fmt.Errorf("releasing the event maintenance lease: %w", errors.Join(unlockErr, closeErr))
	}

	return closeErr
}

// Held reports whether this lease still owns its connection.
//
// Returns:
//   - bool: true while the lease is held.
func (l *EventMaintenanceLease) Held() bool {
	return l != nil && l.conn != nil
}

// StillHeld verifies with the DATABASE that this lease is intact.
//
// Parameters:
//   - ctx context.Context: bounds the check.
//
// Returns:
//   - bool: true when the session behind the lease is alive and still holds the lock.
func (l *EventMaintenanceLease) StillHeld(ctx context.Context) bool {
	if !l.Held() {
		return false
	}

	var mine bool
	if err := l.conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND pid = pg_backend_pid()
			  AND granted
			  AND (classid::bigint << 32) | (objid::bigint & 4294967295) = $1
		)
	`, l.key).Scan(&mine); err != nil {
		return false
	}

	return mine
}
