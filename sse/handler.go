package sse

import (
	"fmt"
	"net/http"
)

func SSEHandler(hub *SSEHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		messageChan := make(chan []byte, 10)
		
		hub.register <- messageChan

		defer func() {
			hub.unregister <- messageChan
		}()

		ctx := r.Context()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messageChan:
				if !ok {
					return
				}
				
				fmt.Fprintf(w, "data: %s\n\n", msg)
				
				flusher.Flush()
			}
		}
	}
}