package sse

import (
	"encoding/json"
	"log"
	"watchtower/models"
)

type SSEMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

func BridgeMLToSSE(ssePipe <-chan interface{}, hub *SSEHub) {
	for result := range ssePipe {
		var msg SSEMessage

		switch v := result.(type) {
		case models.EventEnvelope:
			msg = SSEMessage{
				Type:    "event",
				Payload: v,
			}
		case *models.AnomalyResult:
			msg = SSEMessage{
				Type:    "anomaly",
				Payload: v,
			}
		case *models.ForecastResult:
			msg = SSEMessage{
				Type:    "forecast",
				Payload: v,
			}
		default:
			log.Printf("Unknown type in SSE pipe: %T", result)
			continue
		}

		jsonBytes, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Error marshaling SSE message: %v", err)
			continue
		}

		hub.Broadcast(jsonBytes)
	}
}