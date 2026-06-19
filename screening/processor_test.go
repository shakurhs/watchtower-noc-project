package screening

import (
	"testing"
	"time"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
)

func TestIsDuplicate(t *testing.T) {
	cfg := &config.Config{}
	cfg.Screening.DedupTTLSeconds = 2

	policyEngine := policy.NewEngine("policy/screening.json")
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

	policyEngine := policy.NewEngine("policy/screening.json")
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