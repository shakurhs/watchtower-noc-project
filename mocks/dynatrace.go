package mocks

import (
		"context"
		"fmt"
		"math/rand"
		"time"
		"log"

		"watchtower/models"
)

func GenerateDynatraceData(ctx context.Context, dataPipe chan<- models.EventEnvelope) {
	for {
		select {
		case <-ctx.Done():
				fmt.Println("Dynatrace mock stopped.")
				return
		default:

			event := models.EventEnvelope{
			Version:   "1.0",
			ID:        fmt.Sprintf("dt-%d", time.Now().UnixNano()),
			Source:    "dynatrace",
			Timestamp: time.Now().Unix(),
			Payload: map[string]interface{}{
				"cpu_usage":    rand.Float64() * 100,
				"memory_usage": rand.Float64() * 100,
				"status":       []string{"OK", "WARN", "CRITICAL"}[rand.Intn(3)],
						},
					}

					select {
					case dataPipe <- event:
					default:
						log.Printf("[DROP] Dynatrace channel full. Dropping packet ID: %s", event.ID)
					}

			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}
	}
}