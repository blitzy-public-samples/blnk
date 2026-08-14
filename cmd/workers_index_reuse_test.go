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

// THE INDEX QUEUE'S SETUP COST MUST BE PAID ONCE PER PROCESS, NOT ONCE PER TASK.
//
// Both index handlers used to build a TypeSense client and call EnsureCollectionsExist on
// every task. That call creates each of the five collections and then upserts the default
// general ledger, so six HTTP round trips ran ahead of the task's own single write, on a
// connection that could not be reused because the client was thrown away with the task.
//
// The index queue receives one task per indexed write, so under the validated 550
// events/second load the arrival rate was 550/s while the drain rate was bounded by that
// setup. The queue grew without bound: 609,673 tasks pending at peak, draining at 276/s
// against 550/s arriving.
//
// These tests count the HTTP requests the schema assurance makes, so the per-task cost is
// asserted as a NUMBER rather than described. A regression that reintroduces per-task
// assurance changes that number and fails here.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/search"
)

// TypeSense is a hard test dependency here, as it is for the search suite: these tests FAIL
// rather than skip when it is unreachable. Start it with `docker compose up -d typesense`.
const (
	indexTestTypesenseHost = "http://localhost:8108"
	indexTestTypesenseKey  = "blnk-api-key"
)

// countingTypesense sits IN FRONT of the real TypeSense and counts what passes through.
//
// A hand-written stub was tried first and rejected: the generated TypeSense client accepts
// only the exact response shapes the real server produces, so a stub either has to
// reimplement them — at which point the test is measuring the stub — or it fails for
// reasons that have nothing to do with what is under test. Proxying keeps the client's real
// behaviour, including which calls it treats as success, and still yields an exact request
// count. The failing mode short-circuits the proxy so an outage can be simulated without
// disturbing the real server.
type countingTypesense struct {
	server   *httptest.Server
	requests atomic.Int64
	failing  atomic.Bool
}

func newCountingTypesense(t *testing.T) *countingTypesense {
	t.Helper()

	upstream, err := url.Parse(indexTestTypesenseHost)
	require.NoError(t, err, "the TypeSense address must parse")

	counter := &countingTypesense{}
	proxy := httputil.NewSingleHostReverseProxy(upstream)

	counter.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.requests.Add(1)

		if counter.failing.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"unavailable"}`))

			return
		}

		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(counter.server.Close)

	// Fail loudly and early if TypeSense is not there, rather than misattributing its
	// absence to the behaviour under test.
	probe := search.NewTypesenseClient(indexTestTypesenseKey, []string{indexTestTypesenseHost})
	_, healthErr := probe.Client.Health(context.Background(), 2*time.Second)
	require.NoErrorf(t, healthErr,
		"TypeSense must be running on %s for these tests (docker compose up -d typesense)",
		indexTestTypesenseHost)

	return counter
}

func TestSearchIndexer_AssuresTheSchemaOncePerProcessRatherThanOncePerTask(t *testing.T) {
	counter := newCountingTypesense(t)
	indexer := &searchIndexer{}
	ctx := context.Background()

	first, err := indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
	require.NoError(t, err, "the first task must establish the schema")
	require.NotNil(t, first)

	afterFirst := counter.requests.Load()
	require.Positive(t, afterFirst,
		"the first call must actually contact TypeSense, or this test proves nothing")

	// Nine more tasks, standing in for the stream the index queue carries.
	for i := 0; i < 9; i++ {
		again, againErr := indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
		require.NoError(t, againErr)
		assert.Samef(t, first, again,
			"every task must receive the SAME client, so the underlying HTTP connections are "+
				"pooled across tasks instead of being dialled and discarded per task")
	}

	assert.Equalf(t, afterFirst, counter.requests.Load(),
		"the schema assurance must not be repeated: %d request(s) were made establishing it, "+
			"and nine further tasks must add NONE. Per-task assurance is what capped the index "+
			"queue's drain rate below its arrival rate.", afterFirst)
}

// A failed assurance must not be latched. sync.Once would have recorded the failure
// permanently, and every later task in that process would then have written into
// collections that were never created — for the life of the process.
func TestSearchIndexer_RetriesTheSchemaAfterAFailureRatherThanLatchingIt(t *testing.T) {
	counter := newCountingTypesense(t)
	counter.failing.Store(true)

	indexer := &searchIndexer{}
	ctx := context.Background()

	_, err := indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
	require.Error(t, err, "an unavailable TypeSense must surface as an error the task can retry")

	counter.failing.Store(false)
	before := counter.requests.Load()

	client, err := indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
	require.NoError(t, err, "the next task must RETRY the assurance, not inherit the failure")
	require.NotNil(t, client)

	assert.Greaterf(t, counter.requests.Load(), before,
		"the retry must actually re-contact TypeSense; %d request(s) before, %d after",
		before, counter.requests.Load())

	// And having finally succeeded, it latches from then on.
	settled := counter.requests.Load()
	_, err = indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
	require.NoError(t, err)
	assert.Equal(t, settled, counter.requests.Load(),
		"once the schema is established the assurance must stop being repeated")
}

// The index queue is served by many goroutines at once, and the first burst of tasks arrives
// together. Exactly one of them may do the schema work.
func TestSearchIndexer_AssuresTheSchemaOnceUnderConcurrentFirstTasks(t *testing.T) {
	counter := newCountingTypesense(t)
	indexer := &searchIndexer{}
	ctx := context.Background()

	const goroutines = 24

	var (
		wait    sync.WaitGroup
		start   = make(chan struct{})
		clients = make([]interface{ EnsureCollectionsExist(context.Context) error }, goroutines)
		errs    = make([]error, goroutines)
	)

	wait.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(slot int) {
			defer wait.Done()
			<-start

			client, err := indexer.clientFor(ctx, indexTestTypesenseKey, []string{counter.server.URL})
			clients[slot], errs[slot] = client, err
		}(i)
	}

	close(start)
	wait.Wait()

	for i := 0; i < goroutines; i++ {
		require.NoErrorf(t, errs[i], "goroutine %d", i)
		assert.Samef(t, clients[0], clients[i], "goroutine %d received a different client", i)
	}

	// Whatever one assurance costs, twenty-four concurrent first tasks must cost the same.
	single := &searchIndexer{}
	solo := newCountingTypesense(t)
	_, err := single.clientFor(ctx, indexTestTypesenseKey, []string{solo.server.URL})
	require.NoError(t, err)

	assert.Equalf(t, solo.requests.Load(), counter.requests.Load(),
		"twenty-four concurrent first tasks made %d request(s); one task alone makes %d. They "+
			"must be equal — the point is that one task does the work while the others wait, "+
			"not that they all race to create the same five collections.",
		counter.requests.Load(), solo.requests.Load())
}
