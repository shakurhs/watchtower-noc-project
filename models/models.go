package models

import "time"

type EventEnvelope struct {	
	Version    string      			 	`json:"version"`
	ID         string      			 	`json:"id"`
	Source     string      				`json:"source"`
	Timestamp  int64   	 				`json:"timestamp"`
	Payload    map[string]interface{}	`json:"payload"`
}

type AnomalyResult struct {
	Source     string    `json:"source"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`       // Nilai yang diamati saat ini
	Expected   float64   `json:"expected"`    // Nilai yang diharapkan (berdasarkan EMA)
	Confidence float64   `json:"confidence"`  //  (0.0 - 1.0)
	Timestamp  time.Time `json:"timestamp"`
}

type ForecastResult struct {
	Source      string    `json:"source"`
	Metric      string    `json:"metric"`
	Predicted   float64   `json:"predicted"`
	HorizonTime time.Time `json:"horizon_time"` // Waktu prediksi di masa depan 
}