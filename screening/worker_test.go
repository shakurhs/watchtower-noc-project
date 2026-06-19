package screening

import (
	"context"
	"sync"
	"testing"
	"time"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/storage"
)

func TestStartWorkerPool(t *testing.T) {
	cfg := &config.Config{}
	cfg.Screening.WorkerCount = 2
	cfg.Screening.DedupTTLSeconds = 2
	cfg.Screening.NoiseWindowSeconds = 5

	// Setup MinIO
	minioClient, err := storage.InitMinio("localhost:9000", "watchtower_admin", "watchtower_password")
	if err != nil {
		t.Fatalf("Gagal inisialisasi MinIO: %v", err)
	}

	ctx := context.Background()
	bucketName := "watchtower-test"
	
	err = storage.EnsureBucketExists(ctx, minioClient, bucketName, "us-east-1")
	if err != nil {
		t.Fatalf("Gagal create bucket: %v", err)
	}

	ingestionChannel := make(chan models.EventEnvelope, 5)
	screenedChannel := make(chan models.EventEnvelope, 5)
	var wg sync.WaitGroup
	ctxTest, cancel := context.WithCancel(ctx)

	policyEngine := policy.NewEngine("policy/screening.json", minioClient, bucketName)

	processor := NewProcessor(cfg, policyEngine)
	StartWorkerPool(ctxTest, cfg, policyEngine, processor, ingestionChannel, screenedChannel, &wg)

	validEvent := models.EventEnvelope{ID: "evt-1", Timestamp: time.Now().Unix()}
	duplicateEvent := models.EventEnvelope{ID: "evt-1", Timestamp: time.Now().Unix()}
	noiseEvent := models.EventEnvelope{ID: "evt-2", Timestamp: time.Now().Add(-10 * time.Second).Unix()}

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

	cancel()
	wg.Wait()
}