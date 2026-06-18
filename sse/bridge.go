package sse

import (
	"encoding/json"
	"log"
)

func BridgeMLToSSE(ssePipe <-chan interface{}, hub *SSEHub) {
	for result := range ssePipe {
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			log.Printf("Error marshaling ML result for SSE: %v", err)
			continue
		}

		hub.Broadcast(jsonBytes)
	}
}