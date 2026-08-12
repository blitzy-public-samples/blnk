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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards ONE fact about the worker role, and it is the single most
// consequential thing to get wrong when the legacy webhook transport is finally
// deleted:

// sharedQueueTaskProbe reports whether the mux has a handler for a task type.
//
// asynq's ServeMux exposes no lookup, so the only way to ask "is there a handler for
// this type?" is to dispatch a task and interpret the outcome.
//
// Parameters:
//   - t *testing.T: the test, for Helper marking.
//   - mux *asynq.ServeMux: the mux under test.
//   - taskType string: the task type to dispatch.
//
// Returns:
//   - bool: true when a handler was reached, false when asynq reported none.
func sharedQueueTaskProbe(t *testing.T, mux *asynq.ServeMux, taskType string) bool {
	t.Helper()

	// A payload each handler can at least attempt to unmarshal. Its content is irrelevant:
	// reaching the unmarshal at all means the handler was found.
	payload, err := json.Marshal(map[string]string{"probe": "shared-queue-registration"})
	require.NoError(t, err)

	// asynq's own NotFoundHandler message, matched exactly. It is spelled out rather than
	// detected loosely because a loose match ("contains not found") would also swallow a
	// handler's own not-found error and report a registered handler as missing.
	missing := fmt.Sprintf("handler not found for task %q", taskType)

	reached := false
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				// Reached the handler body and it panicked on the probe payload or on
				// infrastructure it does not have here. That is a registration, which is what is
				// being asserted.
				reached = true
			}
		}()

		processErr := mux.ProcessTask(context.Background(), asynq.NewTask(taskType, payload))
		reached = processErr == nil || processErr.Error() != missing
	}()

	return reached
}

// TestWebhookWorkerServer_KeepsEveryHandlerTheSharedQueueCarries pins all four
// registrations.
//
// Each is named with the feature it belongs to and what its absence costs, because the
// point of the test is not that four handlers exist but that THREE OF THEM ARE NOT PART
// OF THE WEBHOOK FEATURE and must survive its deletion.
func TestWebhookWorkerServer_KeepsEveryHandlerTheSharedQueueCarries(t *testing.T) {
	instance := newCmdTestInstance(t)
	cfg := instance.cnf

	mux := asynq.NewServeMux()
	initializeWebhookTaskHandlers(instance, mux)

	for _, registration := range []struct {
		taskType string
		feature  string
		cost     string
	}{
		{
			taskType: cfg.Queue.WebhookQueue,
			feature:  "legacy webhook delivery",
			cost: "no webhook is delivered during the dual-delivery window, and the payload-equivalence " +
				"criterion has no second transport to compare against",
		},
		{
			taskType: "new:hook_execution",
			feature:  "transaction hooks (feature F-016, OUT OF SCOPE for the Kafka migration)",
			cost: "every PRE_TRANSACTION and POST_TRANSACTION callout is archived unexecuted; this " +
				"handler MUST survive the webhook sunset",
		},
		{
			taskType: cfg.Queue.IndexQueue,
			feature:  "TypeSense single-document indexing (OUT OF SCOPE)",
			cost: "search silently stops reflecting new records; this handler MUST survive the " +
				"webhook sunset",
		},
		{
			taskType: "new:index:batch",
			feature:  "TypeSense batch indexing (OUT OF SCOPE)",
			cost:     "bulk reindexing silently stops; this handler MUST survive the webhook sunset",
		},
	} {
		t.Run(registration.feature, func(t *testing.T) {
			assert.True(t, sharedQueueTaskProbe(t, mux, registration.taskType),
				"no handler is registered for task type %q (%s): %s",
				registration.taskType, registration.feature, registration.cost)
		})
	}
}

// TestWebhookWorkerServer_PollsBothQueuesItOwns pins the queue map.
//
// A handler with no queue behind it is as inert as a queue with no handler, and the
// failure looks different: the task is accepted by Redis and simply never dequeued,
// because the asynq server was never told to poll that queue.
func TestWebhookWorkerServer_PollsBothQueuesItOwns(t *testing.T) {
	cfg := realInfraConfig(t)

	queues := initializeWebhookQueues()

	require.Contains(t, queues, cfg.Queue.WebhookQueue,
		"the webhook queue must be polled for as long as the legacy transport exists")
	require.Contains(t, queues, cfg.Queue.IndexQueue,
		"the INDEX queue is polled by this same server: removing it with the webhook transport "+
			"leaves indexing tasks in Redis, accepted and never dequeued")
	assert.Len(t, queues, 2,
		"exactly these two queues ride on the webhook worker server; a third would need its own "+
			"handler registration and its own entry here")

	for queue, priority := range queues {
		assert.Positive(t, priority,
			"queue %q must carry a positive priority, or asynq never allocates it a worker", queue)
	}
}

// TestWebhookWorkerServer_SurvivesTheWebhookSunset states the sunset contract as an
// executable assertion rather than as a comment in the file being deleted.
//
// The deletion removes the ProcessWebhook MAPPING.
func TestWebhookWorkerServer_SurvivesTheWebhookSunset(t *testing.T) {
	instance := newCmdTestInstance(t)
	cfg := instance.cnf

	// The post-sunset mux: exactly initializeWebhookTaskHandlers minus the webhook line.
	mux := asynq.NewServeMux()
	mux.HandleFunc("new:hook_execution", instance.blnk.Hooks.ProcessHookTask)
	mux.HandleFunc(cfg.Queue.IndexQueue, instance.indexData)
	mux.HandleFunc("new:index:batch", instance.indexBatchData)

	assert.False(t, sharedQueueTaskProbe(t, mux, cfg.Queue.WebhookQueue),
		"this mux stands in for the post-sunset server, where the webhook mapping is the ONE thing "+
			"removed; a handler still answering here means the probe cannot tell absence from "+
			"presence and the rest of this test proves nothing")

	for _, taskType := range []string{"new:hook_execution", cfg.Queue.IndexQueue, "new:index:batch"} {
		assert.True(t, sharedQueueTaskProbe(t, mux, taskType),
			"%q must still be handled after the webhook sunset: it belongs to transaction hooks or "+
				"to search indexing, neither of which is part of the transport being retired",
			taskType)
	}

	// And the queues are untouched by the sunset: the hooks subsystem enqueues onto the webhook
	// queue BY NAME, so even that queue survives — only its ProcessWebhook mapping goes.
	queues := initializeWebhookQueues()
	assert.Contains(t, queues, cfg.Queue.WebhookQueue,
		"the webhook QUEUE survives the sunset even though the webhook HANDLER does not: "+
			"internal/hooks enqueues onto it by name, so deleting it would break transaction hooks")
	assert.Contains(t, queues, cfg.Queue.IndexQueue)
}
