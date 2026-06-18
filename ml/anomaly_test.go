package ml

import (
	"math"
	"sync"
	"testing"
	"time"
	"watchtower/models"
)

func TestAnomalyDetector_Logic(t *testing.T) {
	detector := NewAnomalyDetector(0.15, 2.5)
	
	dummyEvent := models.EventEnvelope{
		Source:    "Dynatrace",
		Timestamp: time.Now().Unix(),
	}
	metricName := "CPU_Usage"

	res, isAnomaly := detector.Detect(dummyEvent, metricName, 50.0)
	if isAnomaly || res != nil {
		t.Errorf("Data pertama seharusnya tidak langsung dianggap anomali")
	}

	normalValues := []float64{51.0, 49.5, 50.2, 52.0}
	for _, val := range normalValues {
		res, isAnomaly = detector.Detect(dummyEvent, metricName, val)
		if isAnomaly {
			t.Errorf("Nilai %v seharusnya dianggap normal, tetapi terdeteksi sebagai anomali", val)
		}
	}

	resAnomaly, isAnomaly := detector.Detect(dummyEvent, metricName, 95.0) // Lonjakan jauh dari ~50
	if !isAnomaly || resAnomaly == nil {
		t.Fatalf("Nilai 95.0 seharusnya terdeteksi sebagai anomali")
	}

	if resAnomaly.Confidence <= 0.0 || resAnomaly.Confidence > 1.0 {
		t.Errorf("Skor confidence harus berada di antara 0.0 dan 1.0, didapat: %v", resAnomaly.Confidence)
	}
	
	resMild, isAnomaly := detector.Detect(dummyEvent, metricName, resAnomaly.Expected + 3.0) // Beda 3.0 dari expected (sigma=2.5)
	if isAnomaly && resMild != nil {
		if resMild.Confidence == resAnomaly.Confidence {
			t.Errorf("Skor confidence terdeteksi statis (%v). Seharusnya bervariasi secara bermakna!", resMild.Confidence)
		}
	}
}

func TestAnomalyDetector_RaceCondition(t *testing.T) {
	detector := NewAnomalyDetector(0.15, 2.5)
	var wg sync.WaitGroup

	workerCount := 50
	
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			event := models.EventEnvelope{
				Source:    "Splunk",
				Timestamp: time.Now().Unix(),
			}
			
			for j := 0; j < 100; j++ {
				val := 50.0 + (10.0 * math.Sin(float64(j)))
				detector.Detect(event, "Memory_Usage", val)
			}
		}(i)
	}

	wg.Wait() // Tunggu semua worker selesai
}