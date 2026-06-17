package screening

import (
	"sync"
	"time"

	"watchtower/config"
	"watchtower/models"

)

type Processor struct {
	processedIDs sync.Map
	cfg *config.Config
}

func NewProcessor(cfg *config.Config) *Processor {
	return &Processor {
			cfg: cfg,
	}
}

func (p *Processor) IsDuplicate(id string) bool {
	now := time.Now()

	if val, ok := p.processedIDs.Load(id); ok {
		timestamp := val.(time.Time)

		if now.Sub(timestamp).Seconds() >float64(p.cfg.Screening.DedupTTLSeconds) {
			p.processedIDs.Store(id, now)
			return false
		}
		return true
	}
	p.processedIDs.Store(id, now)
	return false

	
}

func (p *Processor) FilterNoise(event models.EventEnvelope) bool {

	evenTime := time.Unix(event.Timestamp,0)
	timeDiff := time.Since(evenTime).Seconds()

	if timeDiff > float64(p.cfg.Screening.NoiseWindowSeconds) {
		return true
	}
	
	
	return false
}