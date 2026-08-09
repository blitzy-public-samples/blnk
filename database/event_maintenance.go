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
// event pipeline's maintenance work. PERF-P13.
//
// # The problem it solves
//
// The event metrics collector and the retention sweeper are maintenance, not serving: one
// measures the pipeline and the other deletes from it. Both were started unconditionally in
// the server role, so a deployment scaled to N replicas ran N of each.
//
// For the COLLECTOR that is not merely wasteful, it is wrong. Its gauges are process-scoped
// observations of a shared table, so N replicas publish N series for one truth and each
// retires the others' as stale — the reading an alert evaluates then depends on which replica
// scraped last. For the SWEEPER it is contention: N processes issuing bounded DELETEs against
// the same rows, competing for the same locks on the table the relay is claiming from.
//
// The relay is deliberately NOT gated by this. Its claim query is FOR UPDATE SKIP LOCKED,
// which is designed for exactly this concurrency: N relays divide the work and none of them
// duplicates it. Gating the relay would throw away the horizontal scaling the outbox pattern
// exists to provide.
//
// # Why a Postgres advisory lock
//
// The alternative — a configuration flag naming the maintenance replica — pushes the decision
// to whoever writes the manifest and fails in both directions: set on every replica it changes
// nothing, set on none it silently stops all measurement and all deletion, and set on one it
// stops both the moment that pod is rescheduled.
//
// A session-scoped advisory lock needs no operator action, no new table, no migration and no
// new dependency, and its failover is a property of the transport rather than of any code
// here: the lock is released by the DATABASE when the session ends, so a replica that is
// killed, partitioned or OOMed stops being the leader without having to notice or agree.
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
// Advisory lock keys share one namespace per DATABASE, so this value has to be unlikely to
// collide with anything else that ever takes one. It is the ASCII of "blnkevm1" read as a
// big-endian 64-bit integer, which makes it both improbable as an accident and identifiable
// in pg_locks: an operator looking at a held advisory lock can convert it back to the name.
//
//	'b'=0x62 'l'=0x6c 'n'=0x6e 'k'=0x6b 'e'=0x65 'v'=0x76 'm'=0x6d '1'=0x31
const EventMaintenanceLockKey int64 = 0x626c6e6b65766d31

// ErrEventMaintenanceLeaseHeld reports that another process already owns the maintenance
// lease.
//
// A SENTINEL and not a failure. On a deployment of N replicas this is the expected answer for
// N-1 of them on every attempt, so it is something callers branch on, not something they log
// as a problem. It is deliberately distinguishable from a real acquisition error — a database
// that cannot be reached is a fault and must not be mistaken for "someone else is the leader",
// because the two call for opposite responses.
var ErrEventMaintenanceLeaseHeld = errors.New("blnk: the event maintenance lease is held by another process")

// EventMaintenanceLease is a held, session-scoped singleton claim.
//
// It owns the CONNECTION the lock was taken on, and that ownership is the mechanism rather
// than an implementation detail. A Postgres advisory lock taken with pg_try_advisory_lock is
// scoped to its SESSION, so releasing it requires the same session — and database/sql hands
// out pooled connections per statement, which would put the unlock on whichever connection
// happened to be free and silently leave the lock held for the life of the pool. Pinning one
// *sql.Conn is what makes the lock releasable, and what makes it release itself if the process
// dies without unlocking.
type EventMaintenanceLease struct {
	conn *sql.Conn
	key  int64
}

// TryAcquireEventMaintenanceLease attempts to take the event-maintenance lease.
//
// Non-blocking, deliberately. pg_try_advisory_lock returns false rather than waiting, so a
// replica that is not the leader learns so immediately and can go and do its real work; the
// blocking form would park a connection for the life of the process on every non-leader.
//
// Parameters:
//   - ctx context.Context: bounds acquiring the connection and issuing the lock.
//   - key int64: the advisory lock key. Callers pass EventMaintenanceLockKey; the parameter
//     exists so a test can use a key of its own and not contend with a running server.
//
// Returns:
//   - *EventMaintenanceLease: the held lease. Release it to give up leadership.
//   - error: ErrEventMaintenanceLeaseHeld when another process owns it, which is an ordinary
//     outcome; anything else is a real failure to reach the database.
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
// Idempotent and nil-safe, so a caller can release on every exit path without tracking
// whether it already has. The unlock is issued on a caller-supplied context so a shutdown can
// bound it; the connection is closed WHATEVER the unlock reports, because a session that ends
// releases its advisory locks anyway — so the one thing that must not be skipped is returning
// the connection.
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
// # Why asking is necessary
//
// Held() only reports what this process believes. The lock lives in the database and is
// released by the SESSION ending, which can happen without this process being involved at all
// — a connection reset by a proxy, a failover, an administrator terminating the backend. The
// leader would then go on measuring and deleting while another replica has legitimately taken
// over, which is the split brain the lease exists to prevent, in its least visible form.
//
// A round trip on the PINNED connection is what settles it: if that session is gone the query
// fails, and if it is alive the lock it holds is by definition still held.
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
