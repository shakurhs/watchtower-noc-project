package screening

import (
	"context"
	"sync"
	"testing"
	"time"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
)

func TestStartWorkerPool(t *testing.T) {
	cfg := &config.Config{}
	cfg.Screening.WorkerCount = 2
	cfg.Screening.DedupTTLSeconds = 2
	cfg.Screening.NoiseWindowSeconds = 5

	ingestionChannel := make(chan models.EventEnvelope, 5)
	screenedChannel := make(chan models.EventEnvelope, 5)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	policyEngine := policy.NewEngine("policy/screening.json")
	StartWorkerPool(ctx, cfg, policyEngine, ingestionChannel, screenedChannel, &wg)

	validEvent := models.EventEnvelope{ID: "evt-1", Timestamp: time.Now().Unix()}
	duplicateEvent := models.EventEnvelope{ID: "evt-1", Timestamp: time.Now().Unix()} // ID sama persis
	noiseEvent := models.EventEnvelope{ID: "evt-2", Timestamp: time.Now().Add(-10 * time.Second).Unix()} // Waktu lampau

	ingestionChannel <- validEvent
	ingestionChannel <- duplicateEvent
	ingestionChannel <- noiseEvent

	time.Sleep(100 * time.Millisecond)

	if len(screenedChannel) != 1 {
		t.Errorf("Gagal: Diharapkan 1 event lolos, tapi terdapat %d event", len(screenedChannel))
	} else {
		result := <-screenedChannel
		if result.ID != "evt-1" {
			t.Errorf("Gagal: Diharapkan event yang lolos adalah evt-1, tapi mendapat %s", result.ID)
		}
	}

	cancel() // Mengirim sinyal ctx.Done()
	wg.Wait() // Menunggu worker benar-benar berhenti
}