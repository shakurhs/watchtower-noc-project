package mocks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
	"watchtower/models"
)

func GeneratePrometheusData(ctx context.Context, dataPipe chan<- models.EventEnvelope) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Prometheus mock stopped.")
			return
		default:
			event := models.EventEnvelope{
				Version:   "1.0",
				ID:        fmt.Sprintf("prom-%d", time.Now().UnixNano()),
				Source:    "prometheus",
				Timestamp: time.Now().Unix(),
				Payload: map[string]interface{}{
					"cpu_utilization": rand.Float64() * 100,
					"ram_usage_mb":    rand.Intn(16000),
				},
			}

			select {
			case dataPipe <- event:

			default:
				log.Printf("[DROP] Prometheus channel full. Dropping packet ID: %s", event.ID)
			}

			time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)
		}
	}
}