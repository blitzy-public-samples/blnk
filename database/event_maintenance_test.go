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

// event_maintenance_test.go covers the singleton claim behind leader election.
//
// These are REAL-DATABASE tests and they have to be. The whole mechanism is a Postgres
// advisory lock and the session semantics of the connection it is pinned to — mutual exclusion
// between sessions, release on unlock, release on session death. A mock cannot exhibit any of
// that, so a mocked version of these tests would assert only that the code calls the functions
// it obviously calls.
package database

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMaintenanceLockKey returns a key unique to one test run.
//
// NOT database.EventMaintenanceLockKey, deliberately.
func testMaintenanceLockKey(t *testing.T) int64 {
	t.Helper()

	// Kept well away from the production key's neighbourhood.
	return int64(0x744553540000_0000) | rand.Int63n(1<<31) //nolint:gosec // not security-sensitive
}

func TestTryAcquireEventMaintenanceLease_AdmitsExactlyOneHolder_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	key := testMaintenanceLockKey(t)

	first, err := ds.TryAcquireEventMaintenanceLease(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.True(t, first.Held())

	t.Cleanup(func() { _ = first.Release(context.Background()) })

	// THE POINT OF THE WHOLE MECHANISM: a second attempt on the same key is refused, and
	// refused with the sentinel rather than an error, because "another replica is the
	// leader" is the ordinary answer every replica but one receives on every attempt.
	second, err := ds.TryAcquireEventMaintenanceLease(ctx, key)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEventMaintenanceLeaseHeld,
		"a lease held elsewhere must be reported as the sentinel, so a caller can tell it apart "+
			"from a database it could not reach — the two call for opposite responses")
	assert.Nil(t, second, "no lease may be handed out while another process holds one")

	// And the holder still believes, correctly, that it holds it.
	assert.True(t, first.StillHeld(ctx),
		"the holder must be able to confirm its leadership with the database, not merely assume it")
}

func TestEventMaintenanceLease_ReleaseHandsLeadershipOn_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	key := testMaintenanceLockKey(t)

	first, err := ds.TryAcquireEventMaintenanceLease(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, first)

	require.NoError(t, first.Release(ctx))
	assert.False(t, first.Held(), "a released lease must not claim to be held")
	assert.False(t, first.StillHeld(ctx),
		"a released lease must not confirm leadership: this is what stops a former leader from "+
			"continuing to measure and delete after another replica has taken over")

	// Failover: the next competitor wins immediately.
	second, err := ds.TryAcquireEventMaintenanceLease(ctx, key)
	require.NoError(t, err, "once released, the lease must be available to the next replica")
	require.NotNil(t, second)

	t.Cleanup(func() { _ = second.Release(context.Background()) })

	assert.True(t, second.Held())
}

func TestEventMaintenanceLease_ReleaseIsIdempotentAndNilSafe_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	key := testMaintenanceLockKey(t)

	lease, err := ds.TryAcquireEventMaintenanceLease(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, lease)

	require.NoError(t, lease.Release(ctx))

	// Released twice, because the maintenance loop releases on every exit path and must not
	// have to track whether it already did.
	require.NoError(t, lease.Release(ctx), "releasing an already-released lease must be a no-op")

	var absent *EventMaintenanceLease
	require.NoError(t, absent.Release(ctx), "releasing a nil lease must be a no-op")
	assert.False(t, absent.Held())
	assert.False(t, absent.StillHeld(ctx))
}

func TestEventMaintenanceLease_ReleasedByTheDatabaseWhenTheSessionDies_RealDB(t *testing.T) {
	// THE FAILOVER GUARANTEE, and the reason an advisory lock was chosen over a lease
	// table: leadership is surrendered by the DATABASE when the session ends, so a replica
	// that is killed, OOMed or partitioned stops being the leader without having to notice
	// or agree. Simulated here by terminating the holder's backend, which is what a
	// network partition or a proxy reset looks like from the database's side.
	holder := openRealTestDB(t)
	ctx := context.Background()
	key := testMaintenanceLockKey(t)

	lease, err := holder.TryAcquireEventMaintenanceLease(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, lease)

	// Terminate every other backend holding this advisory key. A separate pool, because the
	// statement cannot run on the session it is about to end.
	killer := openRealTestDB(t)

	_, err = killer.Conn.ExecContext(ctx, `
		SELECT pg_terminate_backend(l.pid)
		FROM pg_locks l
		WHERE l.locktype = 'advisory'
		  AND l.granted
		  AND l.pid <> pg_backend_pid()
		  AND (l.classid::bigint << 32) | (l.objid::bigint & 4294967295) = $1
	`, key)
	require.NoError(t, err)

	// The holder must now discover it is no longer the leader when it asks. This is
	// exactly what holdEventMaintenanceLease checks on its verify tick, and what makes it
	// stop the collector and the sweeper instead of running them beside the new leader's.
	require.Eventually(t, func() bool {
		return !lease.StillHeld(ctx)
	}, 5*time.Second, 100*time.Millisecond,
		"a lease whose session was terminated must stop confirming leadership")

	// And the lock is genuinely free, so another replica takes over.
	require.Eventually(t, func() bool {
		next, acqErr := killer.TryAcquireEventMaintenanceLease(ctx, key)
		if acqErr != nil {
			return false
		}

		_ = next.Release(ctx)

		return true
	}, 5*time.Second, 100*time.Millisecond,
		"the database must have released the dead session's lock so another replica can lead")

	_ = lease.Release(ctx)
}

func TestTryAcquireEventMaintenanceLease_RefusesWithoutAPool(t *testing.T) {
	// No database needed: this is the guard that keeps a misconfigured caller from panicking,
	// and it must be an ERROR rather than the held-elsewhere sentinel — nothing is holding
	// anything, the caller simply cannot ask.
	var ds Datasource

	lease, err := ds.TryAcquireEventMaintenanceLease(context.Background(), EventMaintenanceLockKey)
	require.Error(t, err)
	assert.Nil(t, lease)
	assert.False(t, errors.Is(err, ErrEventMaintenanceLeaseHeld),
		"an absent pool is a fault, not a lost election; conflating them would make every replica "+
			"believe another one was leading and none would maintain the pipeline")
}

func TestEventMaintenanceLockKey_IsTheDocumentedName(t *testing.T) {
	// The constant's documentation claims it is "blnkevm1" read as a big-endian 64-bit
	// integer, and an operator reading pg_locks is told they can convert it back. Asserted so
	// the claim and the value cannot drift apart.
	var reconstructed int64
	for _, b := range []byte("blnkevm1") {
		reconstructed = reconstructed<<8 | int64(b)
	}

	assert.Equal(t, reconstructed, EventMaintenanceLockKey)
	assert.Positive(t, EventMaintenanceLockKey,
		"the key must be positive: pg_try_advisory_lock takes a bigint and a negative value "+
			"would still work but could not be read back as ASCII")
}
