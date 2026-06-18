package state

import (
	"sync"
	"time"
	"watchtower/models"
)

type DashboardState struct {
	mu sync.RWMutex

	RecentEvents []models.EventEnvelope `json:"recent_events"`

	SourceRates map[string]float64 `json:"source_rates"`

	ActiveAnomalies []models.AnomalyResult `json:"active_anomalies"`

	LatestForecasts map[string]models.ForecastResult `json:"latest_forecasts"`

	DropCount int64 `json:"drop_count"`

	LastUpdated time.Time `json:"last_updated"`

	rateWindow map[string][]time.Time
}

func NewDashboardState() *DashboardState {
	return &DashboardState{
		RecentEvents:    make([]models.EventEnvelope, 0, 50),
		SourceRates:     make(map[string]float64),
		ActiveAnomalies: make([]models.AnomalyResult, 0, 20),
		LatestForecasts: make(map[string]models.ForecastResult),
		rateWindow:      make(map[string][]time.Time),
	}
}

func (s *DashboardState) AddEvent(event models.EventEnvelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.RecentEvents = append(s.RecentEvents, event)

	if len(s.RecentEvents) > 50 {
		s.RecentEvents = s.RecentEvents[1:]
	}

	now := time.Now()
	s.rateWindow[event.Source] = append(s.rateWindow[event.Source], now)

	cutoff := now.Add(-10 * time.Second)
	var recent []time.Time
	for _, t := range s.rateWindow[event.Source] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	s.rateWindow[event.Source] = recent

	s.LastUpdated = now
}

func (s *DashboardState) AddAnomaly(anomaly models.AnomalyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ActiveAnomalies = append(s.ActiveAnomalies, anomaly)

	if len(s.ActiveAnomalies) > 20 {
		s.ActiveAnomalies = s.ActiveAnomalies[1:]
	}

	s.LastUpdated = time.Now()
}

func (s *DashboardState) AddForecast(forecast models.ForecastResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := forecast.Source + "_" + forecast.Metric
	s.LatestForecasts[key] = forecast

	s.LastUpdated = time.Now()
}

func (s *DashboardState) IncrementDropCount() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.DropCount++
	s.LastUpdated = time.Now()
}

func (s *DashboardState) CalculateRates() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-10 * time.Second)

	for source, timestamps := range s.rateWindow {
		var count int
		for _, t := range timestamps {
			if t.After(cutoff) {
				count++
			}
		}

		s.SourceRates[source] = float64(count) / 10.0
	}
}

func (s *DashboardState) GetSnapshot() DashboardState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.mu.RUnlock()
	s.CalculateRates()
	s.mu.RLock()

	snapshot := DashboardState{
		RecentEvents:    make([]models.EventEnvelope, len(s.RecentEvents)),
		SourceRates:     make(map[string]float64),
		ActiveAnomalies: make([]models.AnomalyResult, len(s.ActiveAnomalies)),
		LatestForecasts: make(map[string]models.ForecastResult),
		DropCount:       s.DropCount,
		LastUpdated:     s.LastUpdated,
	}

	copy(snapshot.RecentEvents, s.RecentEvents)
	copy(snapshot.ActiveAnomalies, s.ActiveAnomalies)

	for k, v := range s.SourceRates {
		snapshot.SourceRates[k] = v
	}

	for k, v := range s.LatestForecasts {
		snapshot.LatestForecasts[k] = v
	}

	return snapshot
}