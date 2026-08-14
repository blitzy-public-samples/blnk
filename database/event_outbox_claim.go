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

package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Claim shaping constants. Every one of them exists to keep ONE claim's cost bounded by
// the batch it returns rather than by the population it is chosen from, which is the
// property the previous claim did not have: it tested a head-of-line predicate per
// candidate row while scanning candidates in global occurrence order, so a single key
// carrying 150,000 blocked rows cost 1.5 seconds and 605,415 buffer accesses to return
// ONE row, and a 200,000-row backlog could hold one claim open for minutes.
const (
	// eventClaimKeyScanWindow is how many rows each of the two AGE-ORDERED key sources
	// reads. It bounds their cost outright: the oldest source reads the window forwards
	// along idx_event_outbox_claim_order and the newest source reads it backwards, and
	// neither can read more than this however large the backlog is.
	//
	// It is deliberately larger than a batch. In steady state — the state that decides the
	// publish latency percentile — the whole claimable set fits inside the window, so the
	// oldest source alone offers every claimable key and the claim behaves exactly like a
	// plain oldest-first claim.
	eventClaimKeyScanWindow = 256

	// eventClaimKeyWalkDepth is how many DISTINCT keys the rotation walk visits per claim.
	// The walk is a loose index scan — one index seek per key on
	// idx_event_outbox_effective_key_inflight — so its cost is this number of seeks and is
	// independent of how many rows sit behind each key.
	//
	// "One index seek" is true because each step goes through
	// blnk.event_outbox_next_effective_key, whose plan is PINNED to that index by its own
	// definition; written inline the step is planned per statement and the planner answers
	// it with an age-ordered scan filtered on the key instead, which costs the whole
	// claimable population PER STEP. Measured at 5,000 pending rows, the inline form made
	// this walk 134ms of a 140ms claim; through the pinned lookup the same 32 steps take
	// 1.1ms. sql/1781252500.sql carries the full measurements.
	//
	// Its purpose is anti-starvation, and it is the only source that has one. While a
	// dominant key drains, the age-ordered sources see that key (it owns the oldest rows)
	// and the freshest keys (they own the newest), and a key in between would be reached by
	// neither until the dominant backlog had drained past it. The walk sweeps the key space
	// in rotation so every key is offered a claim within a bounded number of claims.
	eventClaimKeyWalkDepth = 32

	// eventClaimKeyOversampleFactor is how many keys a claim considers per row of its batch.
	// It is what lets several relay replicas share one outbox.
	//
	// A key list the same size as the batch offers every replica the SAME keys — the oldest
	// ones — so the second replica finds their heads locked by the first, and comes back with
	// whatever is left of its batch rather than a full one. Measured on twenty rows across
	// twenty keys, four simultaneous claimants of five each took five rows between them
	// instead of twenty. Considering four keys per batch row means a claimant can skip the
	// heads another holds and still fill its batch from further down the list.
	//
	// It costs one index seek per key considered, and nothing else: a key whose head is not
	// free contributes no rows and is not locked.
	eventClaimKeyOversampleFactor = 4
)

// eventClaimKeyBudget resolves how many distinct effective keys one claim considers.
//
// Parameters:
//   - batchSize int: the batch the claim is bounded by.
//
// Returns:
//   - int: at least batchSize, so a claim can always fill its batch from single-row keys.
func eventClaimKeyBudget(batchSize int) int {
	if batchSize <= 0 {
		return 0
	}

	return batchSize * eventClaimKeyOversampleFactor
}

// eventOutboxClaimableSQL renders the predicate that decides whether one row may be
// claimed RIGHT NOW, for the given alias. It is rendered rather than written out at each
// of its four uses in the claim so the key sources, the per-key run, the lock and the
// re-check cannot drift apart — a row that one of them considered claimable and another
// did not would either be skipped forever or claimed while leased.
func eventOutboxClaimableSQL(alias string) string {
	return `(` + alias + `.locked_until IS NULL OR ` + alias + `.locked_until < NOW())
			  AND ` + alias + `.attempts < ` + alias + `.max_attempts
			  AND ` + alias + `.next_attempt_at <= NOW()`
}

// claimPendingEventOutboxQuery claims a batch of publishable rows, takes a lease on them
// and stamps a fresh claim token, all in one statement. It is a package-level variable
// rather than a local so its text is reachable from tests, which assert that FOR UPDATE
// SKIP LOCKED, the occurred_at ordering and the per-key serialisation are all still
// present — the three properties a well-meaning refactor is most likely to drop. A
// variable and not a constant only because it is composed from the effective-key and
// claimable expressions rather than repeating them; nothing assigns to it after
// initialisation.
//
// THE SHAPE, and why each stage is here:
//
//	oldest_keys / newest_keys  bounded age-ordered key sources (see eventClaimKeyScanWindow).
//	                           oldest_keys also CARRIES each of its keys' heads, which is what
//	                           keeps the claim's cost off the size of the backlog
//	walk                       bounded rotation over the key space (see eventClaimKeyWalkDepth),
//	                           stepped through the pinned key-space lookup
//	keys                       the union, capped, ranked BY AGE first — oldest_keys carries each
//	                           key's position in the oldest window, so the cap can only ever drop
//	                           the newest keys, never the oldest work
//	carried_supply             how many of those keys arrived WITH their head already resolved and
//	                           claimable. It is the guard on the lookups below
//	key_heads                  one row per key: that key's OLDEST unfinished row, whatever state
//	                           it is in. This is the gate every claimant must pass through
//	claimed_heads              the heads that were free, oldest first, bounded by the batch and
//	                           LOCKED — the SKIP LOCKED and the LIMIT are in one node, so a head
//	                           another claimant holds is skipped and the batch is filled from
//	                           further down the key list instead of coming back short
//	webhook_rows               the legacy leg, which carries no ordering constraint, claimed the
//	                           same way
//	run_allowance              how many rows one key may add behind its head: the batch shared
//	                           out over the heads actually claimed, so a lone key takes the whole
//	                           batch and a hundred keys take one row each
//	run_tails                  each claimed head's successors, up to that allowance, kept only as
//	                           far as they are CONTIGUOUSLY claimable — the claimable PREFIX
//	claim_set                  heads, webhook rows and tails, oldest first, cut back to the batch
//	claimed                    the UPDATE, over that set alone
//
// keys, key_heads, claimed_heads and webhook_rows are MATERIALIZED rather than left to the
// planner. Each carries a LIMIT that BOUNDS THE CLAIM, and the two locking ones are read more
// than once — by run_allowance, by run_tails and by claim_set. An inlined version could be
// re-evaluated, and a re-evaluated LIMIT is a claim that leases more rows than the caller asked
// for; a re-evaluated FOR UPDATE would take a second, different set of locks.
//
// WHY oldest_keys CARRIES ITS KEYS' HEADS instead of leaving every head to a lookup in
// key_heads. This is the difference between a claim whose cost is bounded and a claim whose cost
// follows the backlog, and it was added after the second was measured in production shape.
//
// A key's head is its oldest unfinished row. The oldest window is the globally oldest
// unfinished rows in exactly that order, so IF A KEY APPEARS IN THE WINDOW AT ALL, ITS HEAD IS
// IN THE WINDOW TOO — the head sorts before every other row of the key, so it cannot be the one
// that fell outside. DISTINCT ON therefore hands back the true head for free, out of a scan the
// claim was already paying for, with no lookup and no extra index access.
//
// The lookup it replaces is the one thing in this statement whose plan the planner gets wrong.
// Two indexes can serve "this key's oldest unfinished row":
// idx_event_outbox_effective_key_inflight, which seeks it, and idx_event_outbox_claim_order,
// which scans rows in age order and filters on the key. Which one is chosen turns on the
// estimated size of status IN ('pending','processing') — and that estimate is bimodal in an
// outbox: it is zero for long stretches and thousands during a burst, so ANALYZE almost always
// last ran on an empty backlog and the planner values the claimable set at ONE ROW. At one row
// the age-ordered scan looks free, and it is chosen. Measured on this table with 5,000 rows
// pending and statistics gathered while it was empty, the per-key lookups took 198.9ms; a
// single ANALYZE flipped the plan to the seek and the identical statement took 1.798ms. Under
// live load the consequence is a runaway, because the wrong plan's cost rises with the very
// backlog it is failing to drain: claim latency was observed climbing 26ms, 171ms, 654ms,
// 1.9s, 3.2s, 4.5s while pending went 11, 1900, 8386, 16088, 23886 — the drain rate collapsing
// to one batch every two seconds until autoanalyze happened to run and broke the loop.
//
// Carrying the head removes the lookup from the path that matters rather than trying to
// out-argue the planner. Every remedy that tries to is either ineffective or too expensive, and
// each was measured before this was written: lowering random_page_cost moves both paths together
// and does not flip it; ALTER COLUMN status SET STATISTICS 0 leaves the estimate at one row;
// reordering the lateral's ORDER BY and dropping the claimable predicate from it change nothing;
// running ANALYZE often enough to keep the estimate honest costs 1.26s to 1.59s per run on a
// 1,174MB table, which is more than the claims it would be protecting.
//
// The lookups that cannot be carried — the walk's step, and an uncarried key's head — go through
// blnk.event_outbox_next_effective_key and blnk.event_outbox_key_head instead of being written
// inline. Those functions carry SET enable_sort = 'off' in their definitions, so they are planned
// with that setting and never inlined, and both are phrased so the age-ordered alternative would
// have to sort — which removes it from consideration rather than merely making it dearer. It is
// the one place in this statement where a plan is fixed rather than chosen, and it is fixed for
// two lookups whose correct plan is not in doubt. sql/1781252500.sql holds the measurements and
// the rejected alternatives.
//
// "Would have to sort" took a SECOND migration to become true of the head lookup, and the
// reason is worth knowing before either function is edited. Selecting the key with an
// EQUALITY puts it in an equivalence class, the planner folds the leading ORDER BY column to
// a constant, and idx_event_outbox_claim_order then satisfies the whole ordering with no sort
// node — so enable_sort = off excluded nothing and, at the one-row estimate ANALYZE reports
// for an empty claimable set, the age-ordered scan was the CHEAPER plan and won, reading the
// claimable population per key. blnk.event_outbox_key_head therefore selects its key with
// `>= key AND <= key`, which returns the same rows and forms no equivalence class.
// sql/1781252600.sql carries that correction. Keep any future predicate over the effective
// key in that shape.
//
// carried_supply is what makes the removal complete rather than partial. The keys the walk and
// the newest source contribute arrive without a head and still need a lookup — but a key from
// either source has its head OUTSIDE the oldest window by definition, so that head is newer
// than every carried head, and claimed_heads takes the oldest batch-worth. When the window
// already supplies a batch of claimable heads, an uncarried key therefore cannot reach the
// batch, and its lookup cannot change the result: it is skipped. That is the steady state and
// the whole of a high-key-spread backlog however large, so in both the claim performs NO per-key
// lookups at all. They come back only when the window holds fewer than a batch of claimable
// heads, which means a small or heavily leased claimable set — the case where a lookup is cheap
// whichever plan is chosen. The guard counts claimable carried heads rather than carried heads
// because a leased head yields nothing: counting it would let the claim skip the lookups and
// come back short.
//
// WHY PER-KEY ORDERING HOLDS, given that a claim may hold several rows of one key. Four things
// hold it up together, and removing any one of them breaks it:
//
//  1. EVERY CLAIMANT ENTERS A KEY THROUGH ITS HEAD. key_heads is the key's oldest unfinished
//     row and claimed_heads must lock it, so while one claimant holds a key's head no other can
//     take that key at all: its head is either locked (skipped) or leased (not claimable).
//     This is why the oldest window does NOT filter to claimable rows, and it is not an
//     oversight: the window is where carried heads come from, and a window that skipped leased
//     rows would hand back a key's oldest CLAIMABLE row as though it were the head, which is
//     precisely the row that must not be claimed while an older sibling of it is still leased.
//     The window carries each row's claimability instead, and lets claimed_heads reject the
//     leased ones. It offers the same keys either way — a key whose head is leased yields
//     nothing whichever row surfaced it.
//  2. A key's rows are only ever taken FROM THAT HEAD FORWARD, and only as far as they are
//     contiguously claimable. A row is never claimed over the top of an older sibling that is
//     leased, not yet due, or out of budget.
//  3. claim_set is cut back to the batch BY AGE, and a head is always older than its own
//     successors, so the cut can only ever drop a run's tail — never leave a tail without its
//     head.
//  4. The relay publishes one key's rows sequentially and abandons the rest of the group as
//     soon as a row's Kafka leg does not settle. That is enforced in event_relay_publish.go,
//     and it is the half of the guarantee SQL cannot express.
//
// WHY THE UPDATE CANNOT BLOCK, which is what lets a second relay replica add throughput
// instead of removing it. Every id it names is either a head this transaction locked with SKIP
// LOCKED, or a successor of one — and a successor cannot be held by anyone, because holding it
// would have required entering the key through the head this transaction holds. So two
// claimants never wait on each other's row locks; they diverge.
var claimPendingEventOutboxQuery = `
		WITH RECURSIVE oldest_keys AS (
			SELECT DISTINCT ON (window_rows.effective_key)
				window_rows.effective_key,
				window_rows.age_rank AS priority,
				window_rows.id AS head_id,
				window_rows.occurred_at AS head_occurred_at,
				window_rows.claimable AS head_claimable
			FROM (
				SELECT window_row.id, window_row.occurred_at, window_row.claimable,
					` + eventOutboxEffectiveKeySQL("window_row") + ` AS effective_key,
					ROW_NUMBER() OVER (ORDER BY window_row.occurred_at ASC, window_row.id ASC) AS age_rank
				FROM (
					SELECT candidate.id, candidate.occurred_at, candidate.ledger_id, candidate.partition_key,
						(` + eventOutboxClaimableSQL("candidate") + `) AS claimable
					FROM blnk.event_outbox candidate
					WHERE candidate.status IN ('pending', 'processing')
					ORDER BY candidate.occurred_at ASC, candidate.id ASC
					LIMIT $6
				) window_row
			) window_rows
			ORDER BY window_rows.effective_key ASC, window_rows.age_rank ASC
		),
		newest_keys AS (
			SELECT DISTINCT ` + eventOutboxEffectiveKeySQL("window_row") + ` AS effective_key
			FROM (
				SELECT candidate.ledger_id, candidate.partition_key
				FROM blnk.event_outbox candidate
				WHERE candidate.status IN ('pending', 'processing')
				  AND ` + eventOutboxClaimableSQL("candidate") + `
				ORDER BY candidate.occurred_at DESC, candidate.id DESC
				LIMIT $6
			) window_row
		),
		walk AS (
			SELECT blnk.event_outbox_next_effective_key($7) AS effective_key, 1 AS depth
			UNION ALL
			SELECT blnk.event_outbox_next_effective_key(walk.effective_key), walk.depth + 1
			FROM walk
			WHERE walk.effective_key IS NOT NULL AND walk.depth < $8
		),
		keys AS MATERIALIZED (
			SELECT sources.effective_key,
				MAX(sources.head_id) AS head_id,
				MAX(sources.head_occurred_at) AS head_occurred_at,
				BOOL_OR(sources.head_claimable) AS head_claimable
			FROM (
				SELECT effective_key, priority AS source_rank, head_id, head_occurred_at, head_claimable
				FROM oldest_keys WHERE effective_key IS NOT NULL
				UNION ALL
				SELECT effective_key, 1000000 + depth AS source_rank,
					NULL::bigint, NULL::timestamptz, NULL::boolean
				FROM walk WHERE effective_key IS NOT NULL
				UNION ALL
				SELECT effective_key, 2000000 AS source_rank,
					NULL::bigint, NULL::timestamptz, NULL::boolean
				FROM newest_keys WHERE effective_key IS NOT NULL
			) sources
			GROUP BY sources.effective_key
			ORDER BY MIN(sources.source_rank) ASC, sources.effective_key ASC
			LIMIT $5
		),
		carried_supply AS MATERIALIZED (
			SELECT COUNT(*)::int AS ready
			FROM keys
			WHERE keys.head_id IS NOT NULL AND keys.head_claimable
		),
		key_heads AS MATERIALIZED (
			SELECT keys.effective_key, keys.head_id AS id, keys.head_occurred_at AS occurred_at
			FROM keys
			WHERE keys.head_id IS NOT NULL
			UNION ALL
			SELECT keys.effective_key, head.id, head.occurred_at
			FROM keys
			CROSS JOIN LATERAL blnk.event_outbox_key_head(keys.effective_key) head
			WHERE keys.head_id IS NULL
			  AND (SELECT ready FROM carried_supply) < $4
		),
		claimed_heads AS MATERIALIZED (
			SELECT candidate.id, candidate.occurred_at
			FROM blnk.event_outbox candidate
			WHERE candidate.id = ANY (ARRAY(SELECT id FROM key_heads))
			  AND ` + eventOutboxClaimableSQL("candidate") + `
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		),
		webhook_rows AS MATERIALIZED (
			SELECT candidate.id, candidate.occurred_at
			FROM blnk.event_outbox candidate
			WHERE candidate.status = 'webhook_pending'
			  AND ` + eventOutboxClaimableSQL("candidate") + `
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		),
		run_allowance AS (
			SELECT GREATEST(1, $4 / GREATEST(1,
				(SELECT COUNT(*) FROM claimed_heads) + (SELECT COUNT(*) FROM webhook_rows)
			))::int AS allowance
		),
		run_tails AS (
			SELECT tail.id, tail.occurred_at
			FROM key_heads
			JOIN claimed_heads ON claimed_heads.id = key_heads.id
			CROSS JOIN LATERAL (
				SELECT following.id, following.occurred_at,
					BOOL_AND(following.claimable) OVER (
						ORDER BY following.occurred_at ASC, following.id ASC
					) AS runnable
				FROM (
					SELECT candidate.id, candidate.occurred_at,
						(` + eventOutboxClaimableSQL("candidate") + `) AS claimable
					FROM blnk.event_outbox candidate
					WHERE candidate.status IN ('pending', 'processing')
					  AND ` + eventOutboxEffectiveKeySQL("candidate") + ` = key_heads.effective_key
					  AND (candidate.occurred_at, candidate.id) > (key_heads.occurred_at, key_heads.id)
					ORDER BY candidate.occurred_at ASC, candidate.id ASC
					LIMIT (SELECT allowance FROM run_allowance) - 1
				) following
			) tail
			WHERE tail.runnable
		),
		claim_set AS MATERIALIZED (
			SELECT offered.id AS claim_id FROM (
				SELECT id, occurred_at FROM claimed_heads
				UNION ALL
				SELECT id, occurred_at FROM webhook_rows
				UNION ALL
				SELECT id, occurred_at FROM run_tails
			) offered
			ORDER BY offered.occurred_at ASC, offered.id ASC
			LIMIT $4
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET status = $1,
				locked_until = NOW() + $2::interval,
				claim_token = $3,
				first_attempted_at = COALESCE(first_attempted_at, NOW()),
				last_attempted_at = NOW()
			WHERE id = ANY (ARRAY(SELECT claim_id FROM claim_set))
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingEventOutbox claims a batch of pending entries for publishing, takes a
// lease on them for lockDuration and stamps every claimed row with one fresh claim
// token. The returned entries are ordered oldest occurrence first, which is the order
// they must be published in, and each carries the token in its ClaimToken field — every
// transition the caller subsequently performs must present it.
//
// Parameters:
//   - ctx context.Context: cancels the claim. The caller is expected to bound it; see
//     eventClaimBudget for why the relay does.
//   - batchSize int: the maximum number of rows to claim.
//   - lockDuration time.Duration: the lease taken on every claimed row.
//   - keyCursor string: where this claim's ROTATION WALK resumes from. Pass the highest
//     effective key the previous claim returned to sweep the key space, and "" to start
//     from the beginning. It affects WHICH keys a claim offers when the backlog is larger
//     than one batch, and nothing else: any value is correct, including a stale one.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first. Several rows may
//     share one effective key, in which case they are contiguous and in order, and the
//     caller must publish them sequentially and stop at the first that does not settle.
//   - error: a bad-request error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimPendingEventOutbox(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
	keyCursor string,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingEventOutbox")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive event outbox lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()
	keyBudget := eventClaimKeyBudget(batchSize)

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
		attribute.Int("event_outbox.key_budget", keyBudget),
		attribute.Bool("event_outbox.key_cursor_set", strings.TrimSpace(keyCursor) != ""),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, lockDuration.String(), claimToken, batchSize,
		keyBudget, eventClaimKeyScanWindow, keyCursor, eventClaimKeyWalkDepth)
	if err != nil {
		failDatabaseSpan(span, err)

		// A claim that ran out of time is reported as the timeout it is, so an operator
		// reading the log sees a bounded statement giving up rather than a database fault,
		// and so the relay can count it separately from a broken query. Before the claim was
		// bounded this condition had no name: the statement simply stayed open — measured at
		// 504 seconds on a 200,000-row backlog — while the relay published nothing and said
		// nothing.
		if claimTimedOut(ctx, err) {
			logrus.WithFields(logrus.Fields{
				"batch_size":    batchSize,
				"key_budget":    keyBudget,
				"lock_duration": lockDuration.String(),
				"error_class":   databaseErrorClass(err),
			}).Error(
				"event outbox claim did not complete inside its budget and was cancelled; no rows were " +
					"claimed and the relay will claim again on its next poll. A claim that keeps timing " +
					"out means the claimable population has outgrown its indexes",
			)

			return nil, apierror.NewAPIError(apierror.ErrInternalServer,
				"Claiming pending event outbox entries did not complete within its budget", err)
		}

		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending event outbox entries", "claim_pending_event_outbox", err)
	}
	defer func() {
		// A close failure is logged rather than returned: the rows have already
		// been read, and replacing a successful claim with an error here would
		// leave those rows leased but unpublished until the lease expired.
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_event_outbox", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_event_outbox", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimTimedOut reports whether a failed claim failed because it ran out of time rather
// than because the statement or the connection was broken.
//
// It tests the CONTEXT as well as the error because that is where the distinction lives:
// lib/pq reports a cancelled statement as a driver error whose text is
// "pq: canceling statement due to user request", which is indistinguishable from a
// caller-initiated cancellation until the context's own state is consulted. A deadline
// that has passed on the context the query ran under says the budget expired; a context
// merely cancelled says the process is shutting down and is NOT a timeout.
//
// Parameters:
//   - ctx context.Context: the context the claim ran under.
//   - err error: the error the driver returned.
//
// Returns:
//   - bool: true when the claim was stopped by its own budget expiring.
func claimTimedOut(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// claimFailedEventOutboxForDeadLetterQuery claims rows whose retry budget is spent and
// whose dead-letter PRESERVATION has not happened, so it can be attempted again.
const claimFailedEventOutboxForDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status = 'failed'
			  AND candidate.dlt_topic IS NULL
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimFailedEventOutboxForDeadLetter claims up to batchSize rows whose retry budget is
// spent and whose dead-letter write has not yet succeeded, taking a lease on them and
// stamping one fresh claim token, so the preservation can be attempted again.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: the maximum number of rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest first. Empty when nothing needs
//     repair, which is the normal state.
//   - error: a bad-request error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimFailedEventOutboxForDeadLetter(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimFailedEventOutboxForDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox dead-letter recovery batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)

		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter recovery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimFailedEventOutboxForDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to claim event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox entry awaiting dead-letter preservation",
				"claim_failed_event_outbox_for_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))

	return entries, nil
}

// brokerRecordBindings renders a broker coordinate as the three nullable SQL parameters
// the marking statements bind, so all three transitions agree on one definition.
func brokerRecordBindings(record model.BrokerRecord) (topic, partition, offset any) {
	if !record.Confirmed() {
		return nil, nil, nil
	}

	return strings.TrimSpace(record.Topic), record.Partition, record.Offset
}

// requireEventOutboxClaimToken rejects a transition attempted without a claim token.
func requireEventOutboxClaimToken(claimToken, transition string) error {
	if strings.TrimSpace(claimToken) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox transition %q requires the claim token held for the row", transition), nil)
	}
	return nil
}

// normalizeDeadLetterHandoffLease resolves the lease a terminal transition holds the
// row under while its dead-letter write is owed.
func normalizeDeadLetterHandoffLease(lease time.Duration, transition string) time.Duration {
	if lease > 0 {
		return lease
	}

	logrus.WithFields(logrus.Fields{
		"transition":      transition,
		"requested_lease": lease.String(),
		"applied_lease":   defaultEventClaimLease.String(),
	}).Warn(
		"Non-positive dead-letter hand-off lease; falling back to the default. A lease that has " +
			"already expired lets the dead-letter repair pass reclaim the row while this worker's " +
			"dead-letter write is still in flight, which is how one event reaches a .dlt topic twice",
	)

	return defaultEventClaimLease
}

// eventOutboxClaimLost is the typed error every conditional transition returns when its
// UPDATE matched no row, and it is a DELIBERATE behaviour change from the previous
// warn-and-continue treatment.
func eventOutboxClaimLost(id int64, transition string) error {
	logrus.WithFields(logrus.Fields{
		"event_outbox_id": id,
		"transition":      transition,
	}).Warn("Event outbox transition matched no row: the claim was lost or the row already moved on")

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event outbox row is no longer claimed for transition %q", transition), nil)
}

// requireEventOutboxRowAffected converts an UPDATE that matched no row into the typed
// lost-claim conflict above.
func requireEventOutboxRowAffected(result sql.Result, id int64, transition string) error {
	if result == nil {
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
			"error_class":     databaseErrorClass(err),
			"sqlstate":        postgresSQLState(err),
		}).Debug("Could not determine rows affected for event outbox transition")
		logDatabaseDiagnostic("require_event_outbox_row_affected", err)
		return nil
	}
	if affected == 0 {
		return eventOutboxClaimLost(id, transition)
	}

	return nil
}

// RenewEventOutboxLease extends the lease on every row still being worked under one
// claim token, and reports how many it extended.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - claimToken string: the token the claim issued. Required; without it the statement
//     could only match rows nobody holds.
//   - lease time.Duration: how long from NOW the extended lease should run.
//
// Returns:
//   - int64: how many rows were extended. Zero is a legitimate answer.
//   - error: a bad-request error for a missing token, or a wrapped driver error.
func (d Datasource) RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RenewEventOutboxLease")
	defer span.End()

	if err := requireEventOutboxClaimToken(claimToken, "renew_lease"); err != nil {
		failDatabaseSpan(span, err)
		return 0, err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive event outbox lease renewal; falling back to %s", defaultEventClaimLease)
		lease = defaultEventClaimLease
	}

	span.SetAttributes(
		attribute.String("event_outbox.claim_token", claimToken),
		attribute.String("event_outbox.lease", lease.String()),
	)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET locked_until = NOW() + $1::interval
		WHERE claim_token = $2 AND status = $3
	`, lease.String(), claimToken, model.EventOutboxStatusProcessing)
	if err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the event outbox lease", "renew_event_outbox_lease", err)
	}

	renewed, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The renewal itself succeeded, so this is
		// reported as zero rather than as a failure: the caller uses the count only to
		// decide whether to keep renewing, and one uncounted round is harmless.
		logrus.WithFields(logrus.Fields{
			"claim_token": claimToken,
			"error_class": databaseErrorClass(err),
			"sqlstate":    postgresSQLState(err),
		}).Debug("Could not determine how many event outbox leases were renewed")
		logDatabaseDiagnostic("renew_event_outbox_lease", err)

		return 0, nil
	}

	span.SetAttributes(attribute.Int64("event_outbox.renewed_count", renewed))

	return renewed, nil
}

// claimPendingWebhookDeliveriesQuery claims rows whose Kafka leg has FINISHED and whose
// LEGACY WEBHOOK leg is still owed, taking a lease and stamping a fresh claim token
// WITHOUT changing the row's status.
const claimPendingWebhookDeliveriesQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('dispatched', 'failed', 'dead_lettered')
			  AND candidate.webhook_dispatched = FALSE
			  AND candidate.webhook_attempts < candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingWebhookDeliveries claims rows whose Kafka leg has finished and whose
// legacy HTTP webhook leg has not been recorded, so the dual-delivery window can finish
// a leg that failed alongside the Kafka publish — whether that publish then succeeded
// or was itself given up on.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first, each carrying the
//     claim token MarkWebhookDispatched requires.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingWebhookDeliveries")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Webhook delivery claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive webhook delivery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingWebhookDeliveriesQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending webhook deliveries", "claim_pending_webhook_deliveries", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_webhook_deliveries", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_webhook_deliveries", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimEventsOwedDeadLetterQuery claims rows whose retry budget is spent and whose
// dead-letter write has NOT been recorded, taking a lease and stamping a fresh claim
// token WITHOUT changing their status.
const claimEventsOwedDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('failed', 'processing')
			  AND candidate.dlt_topic IS NULL
			  AND candidate.attempts >= candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.next_attempt_at ASC, candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimEventsOwedDeadLetter claims events whose retry budget is spent and whose
// dead-letter write is still owed, so the hand-off can be retried instead of the event
// being stranded in the only table that holds it.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimEventsOwedDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventsOwedDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Dead-letter hand-off claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter hand-off lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimEventsOwedDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim events owed a dead-letter write", "claim_events_owed_dead_letter", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_events_owed_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_events_owed_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}
