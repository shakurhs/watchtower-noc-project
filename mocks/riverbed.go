package mocks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
	"watchtower/models"
)

func GenerateRiverbedData(ctx context.Context, dataPipe chan<- models.EventEnvelope) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Riverbed mock stopped.")
			return
		default:
			latency := rand.Float64() * 300
			packetLoss := rand.Float64() * 5

			event := models.EventEnvelope{
				Version:   "1.0",
				ID:        fmt.Sprintf("rvb-%d", time.Now().UnixNano()),
				Source:    "riverbed",
				Timestamp: time.Now().Unix(),
				Payload: map[string]interface{}{
					"latency_ms":      latency,
					"packet_loss_pct": packetLoss,
					"target_endpoint": "supplier-api.gateway.internal",
				},
			}

			select {
			case dataPipe <- event:
			default:
				log.Printf("[DROP] Riverbed channel full. Dropping packet ID: %s", event.ID)
			}

			time.Sleep(time.Duration(rand.Intn(800)+200) * time.Millisecond)
		}
	}
}