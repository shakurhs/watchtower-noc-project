package screening

import (
	"encoding/json"
	"sync"
	"time"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy" // Import package policy baru
)

type Processor struct {
	processedIDs sync.Map
	cfg          *config.Config
	policyEngine *policy.Engine // Tambahkan reference ke engine
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