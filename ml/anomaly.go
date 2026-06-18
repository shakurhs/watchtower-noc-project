package ml

import (
		"math"
		"sync"
		"time"
		"watchtower/models"
)

type AnomalyDetector struct {
		mu				sync.RWMutex
		emaState		map[string]float64
		alpha			float64
		sigmaThreshold	float64
}

func NewAnomalyDetector(alpha, sigma float64) *AnomalyDetector {
		return &AnomalyDetector{
			emaState: make(map[string]float64),
			alpha: alpha,
			sigmaThreshold: sigma,
		}
}

func (d *AnomalyDetector) Detect(event models.EventEnvelope, metricName string, currentValue float64) (*models.AnomalyResult, bool) {
		d.mu.Lock()
		defer d.mu.Unlock()

		stateKey := event.Source + "_" + metricName

		expectedValue, exists := d.emaState[stateKey]
		if !exists {
			d.emaState[stateKey] = currentValue
			return nil, false
		}
		
		diff := math.Abs(currentValue - expectedValue)

		isAnomaly := diff > d.sigmaThreshold

		d.emaState[stateKey] = (currentValue * d.alpha) + (expectedValue * (1 - d.alpha))

		if !isAnomaly {
			return nil, false
		}

		confidence := math.Min(1.0, diff / (d.sigmaThreshold * 2))

		result := &models.AnomalyResult{
			Source:     event.Source,
			Metric:     metricName,
			Value:      currentValue,
			Expected:   expectedValue,
			Confidence: confidence,
			Timestamp:  time.Unix(event.Timestamp, 0),
		}

		return result, true
}
