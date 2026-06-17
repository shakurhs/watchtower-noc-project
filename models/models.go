package models

type EventEnvelope struct {	
	Version    string      			 	`json:"version"`
	ID         string      			 	`json:"id"`
	Source     string      				`json:"source"`
	Timestamp  int64   	 				`json:"timestamp"`
	Payload    map[string]interface{}	`json:"payload"`
}

