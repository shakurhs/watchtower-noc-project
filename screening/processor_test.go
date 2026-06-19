package screening

import (
	"context"
	"testing"
	"time"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/storage"
)

func TestIsDuplicate(t *testing.T) {
	cfg := &config.Config{}
	cfg.Screening.DedupTTLSeconds = 2

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

	policyEngine := policy.NewEngine("policy/screening.json", minioClient, bucketName)
	processor := NewProcessor(cfg, policyEngine)

	if processor.IsDuplicate("event-1") {
		t.Errorf("Gagal: event-1 seharusnya belum menjadi duplikat saat pertama kali diproses")
	}

	if !processor.IsDuplicate("event-1") {
		t.Errorf("Gagal: event-1 seharusnya terdeteksi sebagai duplikat")
	}

	if processor.IsDuplicate("event-2") {
		t.Errorf("Gagal: event-2 adalah ID baru, seharusnya bukan duplikat")
	}
}

func TestFilterNoise(t *testing.T) {
	cfg := &config.Config{}
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

	policyEngine := policy.NewEngine("policy/screening.json", minioClient, bucketName)
	processor := NewProcessor(cfg, policyEngine)

	validEvent := models.EventEnvelope{
		Timestamp: time.Now().Unix(),
	}
	if processor.FilterNoise(validEvent) {
		t.Errorf("Gagal: Event dengan waktu saat ini tidak boleh dianggap sebagai noise")
	}

	oldEvent := models.EventEnvelope{
		Timestamp: time.Now().Add(-10 * time.Second).Unix(),
	}
	if !processor.FilterNoise(oldEvent) {
		t.Errorf("Gagal: Event usang (10 detik lalu) seharusnya dibuang sebagai noise")
	}
}