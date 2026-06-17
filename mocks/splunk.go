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


	messages := map[string]string{
		"INFO":  "Service health check passed",
		"WARN":  "High memory usage detected on node",
		"ERROR": "Connection timeout to upstream service",
	}

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
					"log_level": level,
					
					"node_id": fmt.Sprintf("node-%d", rand.Intn(5)+1),
					
					"message": messages[level],
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