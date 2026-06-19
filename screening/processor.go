package screening

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
	"crypto/sha256"
    "encoding/hex"

	"github.com/minio/minio-go/v7"
	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/storage"
)

type SignatureWindow struct {
    mu         sync.Mutex
    Timestamps []time.Time
}

type Processor struct {
	processedIDs sync.Map
	signatureTracker sync.Map
	cfg          *config.Config
	policyEngine *policy.Engine 
}

func NewProcessor(cfg *config.Config, engine *policy.Engine) *Processor {
	return &Processor{
		cfg:          cfg,
		policyEngine: engine,
	}
}

func (p *Processor) IsDuplicate(id string) bool {
	now := time.Now()

	if val, ok := p.processedIDs.Load(id); ok {
		timestamp := val.(time.Time)

		if now.Sub(timestamp).Seconds() > float64(p.cfg.Screening.DedupTTLSeconds) {
			p.processedIDs.Store(id, now)
			return false
		}
		return true
	}
	p.processedIDs.Store(id, now)
	return false
}

func (p *Processor) FilterNoise(event models.EventEnvelope) bool {
	evenTime := time.Unix(event.Timestamp, 0)
	timeDiff := time.Since(evenTime).Seconds()

	if timeDiff > float64(p.cfg.Screening.NoiseWindowSeconds) {
		return true
	}
	
	payloadBytes, _ := json.Marshal(event.Payload)
	payloadString := string(payloadBytes)

	if p.policyEngine.ShouldDrop(payloadString) {
		return true
	}

	return false
}

func (p *Processor) ClassifyPriority(event models.EventEnvelope) string {
	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return "P4"
	}
	
	payloadString := string(payloadBytes)
	result := p.policyEngine.ClassifyPriority(event.Source, payloadString)
	
	log.Printf("[DEBUG Processor] Source: %s, Payload: %s, Result: %s", event.Source, payloadString, result)
	
	return result
}



func (p *Processor) SaveDedupState(ctx context.Context, client *minio.Client, bucketName string) error {
	dedupData := make(map[string]time.Time)
	
	p.processedIDs.Range(func(key, value interface{}) bool {
		id := key.(string)
		timestamp := value.(time.Time)
		dedupData[id] = timestamp
		return true
	})

	return storage.SaveDedupWindow(ctx, client, bucketName, dedupData)
}

func (p *Processor) LoadDedupState(ctx context.Context, client *minio.Client, bucketName string) error {
	dedupData, err := storage.LoadDedupWindow(ctx, client, bucketName)
	if err != nil {
		return err
	}

	now := time.Now()
	ttlDuration := time.Duration(p.cfg.Screening.DedupTTLSeconds) * time.Second
	
	loadedCount := 0
	for id, timestamp := range dedupData {
		if now.Sub(timestamp) < ttlDuration {
			p.processedIDs.Store(id, timestamp)
			loadedCount++
		}
	}

	log.Printf("Berhasil memuat %d event IDs yang masih valid (TTL: %ds)", loadedCount, p.cfg.Screening.DedupTTLSeconds)
	return nil
}

func (p *Processor) IsNoisy(event models.EventEnvelope) bool {
    payloadBytes, err := json.Marshal(event.Payload)
    if err != nil {
        return false // Jika gagal marshal, biarkan lewat
    }
    
    hash := sha256.Sum256(payloadBytes)
    signature := hex.EncodeToString(hash[:])

    val, _ := p.signatureTracker.LoadOrStore(signature, &SignatureWindow{})
    tracker := val.(*SignatureWindow)

    tracker.mu.Lock()
    defer tracker.mu.Unlock()

    now := time.Now()
    windowDuration := time.Duration(p.cfg.Screening.NoiseWindowSeconds) * time.Second
    cutoff := now.Add(-windowDuration)

    var recent []time.Time
    for _, t := range tracker.Timestamps {
        if t.After(cutoff) {
            recent = append(recent, t)
        }
    }
    tracker.Timestamps = recent

	threshold := p.cfg.Screening.SignatureThreshold
    if threshold == 0 {
        threshold = 5 // Default fallback jika config tidak diset
    }

    if len(tracker.Timestamps) >= threshold {
        return true // Dibuang karena terlalu sering muncul (noise)
    }

    tracker.Timestamps = append(tracker.Timestamps, now)
    return false
}