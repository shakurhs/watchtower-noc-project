package sse

import (
	"sync"
)

type SSEHub struct {
	clients    map[chan []byte]bool
	broadcast  chan []byte
	register   chan chan []byte
	unregister chan chan []byte
	mu         sync.RWMutex
}

func NewSSEHub() *SSEHub {
	return &SSEHub{
		clients:    make(map[chan []byte]bool),
		broadcast:  make(chan []byte, 100),
		register:   make(chan chan []byte),
		unregister: make(chan chan []byte),
	}
}

func (h *SSEHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client)
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client <- message:
				default:
					go func(c chan []byte) {
						h.unregister <- c
					}(client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *SSEHub) Broadcast(message []byte) {
	select {
	case h.broadcast <- message:
	default:
	}
}