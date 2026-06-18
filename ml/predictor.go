package ml

import (
	"math"
	"sync"
	"time"
	"watchtower/models"
)

type Point struct {
	X float64
	Y float64
}

type TrendPredictor struct {
	mu         sync.RWMutex
	windows    map[string][]Point // Menyimpan data historis (sliding window) per metrik
	windowSize int                // Batas maksimal titik data yang disimpan
	horizonMin float64            // Seberapa jauh ke depan kita memprediksi (dalam menit)
}

func NewTrendPredictor(windowSize int, horizonMin float64) *TrendPredictor {
	return &TrendPredictor{
		windows:    make(map[string][]Point),
		windowSize: windowSize,
		horizonMin: horizonMin,
	}
}

func (p *TrendPredictor) Predict(event models.EventEnvelope, metricName string, value float64) *models.ForecastResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	stateKey := event.Source + "_" + metricName
	now := float64(event.Timestamp)

	p.windows[stateKey] = append(p.windows[stateKey], Point{X: now, Y: value})

	if len(p.windows[stateKey]) > p.windowSize {
		p.windows[stateKey] = p.windows[stateKey][1:]
	}

	points := p.windows[stateKey]
	n := float64(len(points))

	if n < 2 {
		return nil
	}

	var sumX, sumY, sumXY, sumXX float64
	for _, pt := range points {
		sumX += pt.X
		sumY += pt.Y
		sumXY += pt.X * pt.Y
		sumXX += pt.X * pt.X
	}

	denominator := (n * sumXX) - (sumX * sumX)
	if denominator == 0 {
		return nil // Mencegah error division by zero jika semua timestamp sama
	}

	slope := ((n * sumXY) - (sumX * sumY)) / denominator
	intercept := (sumY - (slope * sumX)) / n

	futureTime := time.Unix(event.Timestamp, 0).Add(time.Duration(p.horizonMin) * time.Minute)
	futureX := float64(futureTime.Unix())

	predictedValue := (slope * futureX) + intercept

	var sumSquaredResiduals float64
		for _, pt := range points {
			predictedY := (slope * pt.X) + intercept
			residual := pt.Y - predictedY
			sumSquaredResiduals += residual * residual
		}

		variance := sumSquaredResiduals / (n - 2)
		stdDev := math.Sqrt(variance)

		confidenceInterval := stdDev

	return &models.ForecastResult{
		Source:      event.Source,
		Metric:      metricName,
		Predicted:   predictedValue,
		ConfidenceInterval: confidenceInterval,
		HorizonTime: futureTime,
	}
}