//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"vod/internal/models"
	"vod/internal/queue"

	"github.com/google/uuid"
)

// TestFanoutDeliversToAllWorkerQueues confirms a single video.uploaded
// publish reaches all three independent worker queues (HLS, thumbnail,
// caption) — the mechanism that lets each worker download and process the
// raw upload independently with no pipeline dependency between them.
func TestFanoutDeliversToAllWorkerQueues(t *testing.T) {
	q := testQueue(t)
	ctx, _ := withTimeout(t)

	event := models.VideoUploadedEvent{
		VideoID:    uuid.NewString(),
		StorageKey: "raw/fanout-test.mp4",
	}
	if err := q.Publish(ctx, event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for _, queueName := range []string{queue.QueueHLS, queue.QueueThumbnail, queue.QueueCaption} {
		deliveries, err := q.Consume(queueName, "test-"+queueName)
		if err != nil {
			t.Fatalf("consume %s: %v", queueName, err)
		}
		select {
		case d := <-deliveries:
			var got models.VideoUploadedEvent
			if err := json.Unmarshal(d.Body, &got); err != nil {
				t.Fatalf("unmarshal delivery from %s: %v", queueName, err)
			}
			if got.VideoID != event.VideoID {
				t.Fatalf("queue %s: expected video_id %s, got %s", queueName, event.VideoID, got.VideoID)
			}
			_ = d.Ack(false)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for fanout delivery on %s", queueName)
		}
	}
}
