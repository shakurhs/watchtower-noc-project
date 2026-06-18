package ml

import (
	"sync"
	"testing"
	"time"
	"watchtower/models"
	
)

func TestTrendPredictor_Logic(t *testing.T) {
	predictor := NewTrendPredictor(5, 5.0)
	
	dummyEvent := models.EventEnvelope{
		Source: "Prometheus",
	}
	metricName := "Disk_Usage"

	timestamps := []int64{10, 20, 30}
	values := []float64{100.0, 200.0, 300.0}

	for i, ts := range timestamps {
		dummyEvent.Timestamp = ts
		res := predictor.Predict(dummyEvent, metricName, values[i])
		
		if i == 0 && res != nil {
			t.Errorf("Titik pertama seharusnya belum bisa menghasilkan prediksi")
		}
		if i > 0 && res == nil {
			t.Errorf("Prediksi seharusnya tersedia setelah 2 titik data")
		}
	}

	dummyEvent.Timestamp = 40
	res := predictor.Predict(dummyEvent, metricName, 400.0)
	
	if res == nil {
		t.Fatalf("Prediksi tidak boleh nil")
	}

	if res.Predicted <= 400.0 {
		t.Errorf("Logika regresi salah. Tren sedang naik, prediksi harus > 400. Didapat: %v", res.Predicted)
	}
}

func TestTrendPredictor_RaceCondition(t *testing.T) {
	predictor := NewTrendPredictor(100, 5.0)
	var wg sync.WaitGroup

	workerCount := 50 // 50 Goroutine bersamaan
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			event := models.EventEnvelope{
				Source:    "Riverbed",
				Timestamp: time.Now().Unix(),
			}
			
			for j := 0; j < 100; j++ {
				event.Timestamp += int64(j)
				predictor.Predict(event, "Bandwidth", float64(j*2))
			}
		}(i)
	}

	wg.Wait()
}