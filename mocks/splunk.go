package mocks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
	"watchtower/models"
)

func GenerateSplunkData(ctx context.Context, dataPipe chan<- models.EventEnvelope) {
	logLevels := []string{"INFO", "WARN", "ERROR"}
	
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Splunk mock stopped.")
			return
		default:
			level := logLevels[rand.Intn(len(logLevels))]
			
			event := models.EventEnvelope{
				Version:   "1.0",
				ID:        fmt.Sprintf("spl-%d", time.Now().UnixNano()),
				Source:    "splunk",
				Timestamp: time.Now().Unix(),
				Payload: map[string]interface{}{
					"log_level":   level,
					"pos_node_id": fmt.Sprintf("kasir-%d", rand.Intn(5)+1),
					"message":     "Transaksi sinkronisasi inventaris harian",
				},
			}

			select {
			case dataPipe <- event:
			default:
				log.Printf("[DROP] Splunk channel full. Dropping packet ID: %s", event.ID)
			}

			time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
		}
	}
}