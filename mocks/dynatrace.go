package mocks

import (
		"context"
		"fmt"
		"math/rand"
		"time"

		"watchtower/models"
)

func GenerateDynatraceData(ctx context.Context, dataPipe chan<- models.EventEnvelope) {
	for {
		select {
		case <-ctx.Done():
				fmt.Println("Dynatrace mock stopped.")
				return
		default:

			event:= models.EventEnvelope{
					Version: 	"1.0",
					ID:			fmt.Sprintf("dt-%d", time.Now().UnixNano()),
					Source: 	"dynatrace",
					Timestamp: 	time.Now().Unix(),
					Payload:	map[string]interface{}{
								"cpu_usage": rand.Float64() * 100,
								"status":	 "OK",

					},
			}

			select {
			case dataPipe <- event:

			default:

			}
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}
	}
}