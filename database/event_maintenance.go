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
//
// The event metrics collector and the retention sweeper are maintenance, not serving:
// one measures the pipeline and the other deletes from it. Both were started
// unconditionally in the server role, so a deployment scaled to N replicas ran N of
// each.
//
// For the COLLECTOR that is not merely wasteful, it is wrong. Its gauges are
// process-scoped observations of a shared table, so N replicas publish N series for one
// truth and each retires the others' as stale — the reading an alert evaluates then
// depends on which replica scraped last.
//
// The relay is deliberately NOT gated by this. Its claim query is FOR UPDATE SKIP
// LOCKED, which is designed for exactly this concurrency: N relays divide the work and
// none of them duplicates it.
//
// The alternative — a configuration flag naming the maintenance replica — pushes the
// decision to whoever writes the manifest and fails in both directions: set on every
// replica it changes nothing, set on none it silently stops all measurement and all
// deletion, and set on one it stops both the moment that pod is rescheduled.
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
//
//	'b'=0x62 'l'=0x6c 'n'=0x6e 'k'=0x6b 'e'=0x65 'v'=0x76 'm'=0x6d '1'=0x31
const EventMaintenanceLockKey int64 = 0x626c6e6b65766d31

// ErrEventMaintenanceLeaseHeld reports that another process already owns the
// maintenance lease.
//
// A SENTINEL and not a failure. On a deployment of N replicas this is the expected
// answer for N-1 of them on every attempt, so it is something callers branch on, not
// something they log as a problem.
var ErrEventMaintenanceLeaseHeld = errors.New("blnk: the event maintenance lease is held by another process")

// EventMaintenanceLease is a held, session-scoped singleton claim.
type EventMaintenanceLease struct {
	conn *sql.Conn
	key  int64
}

// TryAcquireEventMaintenanceLease attempts to take the event-maintenance lease.
//
// Non-blocking, deliberately. pg_try_advisory_lock returns false rather than waiting,
// so a replica that is not the leader learns so immediately and can go and do its real
// work; the blocking form would park a connection for the life of the process on every
// non-leader.
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
// Used by the maintenance loop to decide whether it is still the leader before it keeps
// running work that only the leader may do.
//
// Returns:
//   - bool: true while the lease is held.
func (l *EventMaintenanceLease) Held() bool {
	return l != nil && l.conn != nil
}

// StillHeld verifies with the DATABASE that this lease is intact.
//
// Held() only reports what this process believes. The lock lives in the database and is
// released by the SESSION ending, which can happen without this process being involved
// at all — a connection reset by a proxy, a failover, an administrator terminating the
// backend.
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
