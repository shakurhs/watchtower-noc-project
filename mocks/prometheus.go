package mocks

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"watchtower/models"
)

func GeneratePrometheusData(ctx context.Context, dataPipe chan <- models.EventEnvelope) {
		for {
				select {
				case <- ctx.Done():
					fmt.Println("Prometheus mock stopped")
					return
				default:

					memUsage := rand.Float() * 100
					status := "OK"
					if memUsage > 85.0 {
						status = "CRITICAL"
					}

					event := models.EventEnvelope{
						Version: "1.0"
						ID: fmt.Sprintf("prom-%d", time.Now().UnixNano9())
						Source: "prometheus",
						Timestamp: time.Now().Unix(),
						Payload: map[string]interface{}{
							"memory_usage": memUsage,
							"active_conns": rand.Intn(5000),
							"system_status": status,

						},

					}
					
					select {
					case dataPipe <- event:
					
					default:
					}

					time.Sleep(time.Duration(rand.Intn(500)+200) * time.Milliseconds)
				}
		}
}