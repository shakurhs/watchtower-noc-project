package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"watchtower/config"
	"watchtower/mocks"
	"watchtower/ml"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/screening"
	"watchtower/sse"
	"watchtower/state"
	"watchtower/storage"
)

func main() {
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Gagal memuat config: %v", err)
	}

	// engine := policy.NewEngine("policy/screening.json")
	// engine.Watch(2 * time.Second)

	minioClient, err := storage.InitMinio(cfg.Storage.Endpoint, "watchtower_admin", "watchtower_password")
	if err != nil {
		log.Fatalf("Gagal Inisialisasi MinIO: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = storage.EnsureBucketExists(ctx, minioClient, cfg.Storage.Bucket, cfg.Storage.Region)
	if err != nil {
		log.Fatalf("Gagal memastikan keberadaan bucket: %v", err)
	}

	localPolicy, err := os.ReadFile("policy/screening.json")
	if err != nil {
		log.Fatalf("Gagal membaca file policy lokal: %v", err)
	}
	err = storage.SavePolicyToMinIO(ctx, minioClient, cfg.Storage.Bucket, "policy/screening.json", localPolicy)
	if err != nil {
		log.Fatalf("Gagal upload policy ke MinIO: %v", err)
	}
	log.Println("Policy berhasil disinkronisasi ke MinIO.")

	engine := policy.NewEngine("policy/screening.json", minioClient, cfg.Storage.Bucket)

	go engine.Watch(ctx, 2*time.Second) 

	processor := screening.NewProcessor(cfg, engine)
	err = processor.LoadDedupState(ctx, minioClient, cfg.Storage.Bucket)
	if err != nil {
		log.Printf("Warning: Gagal memuat dedup state: %v", err)
	}

	dataPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	screenedPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)

	log.Println("Memulai Project Watchtower: Ingestion & Screening Pipeline!")

	var wg sync.WaitGroup

	screening.StartWorkerPool(ctx, cfg, engine, processor, dataPipe, screenedPipe, &wg)

	anomalyDetector := ml.NewAnomalyDetector(cfg.ML.EmaAlpha, cfg.ML.AnomalySigmaThreshold)
	trendPredictor := ml.NewTrendPredictor(cfg.ML.RegressionWindowSize, float64(cfg.ML.ForecastHorizonMins))
	dashboardState := state.NewDashboardState()

	archivePipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	mlPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	ssePipe := make(chan interface{}, 100)
	screenedArchivePipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)

	go storage.ArchiveScreenedEvent(ctx, minioClient, cfg.Storage.Bucket, screenedArchivePipe)

	go func() {
		for event := range screenedPipe {
			dashboardState.AddEvent(event)

			select {
			case archivePipe <- event:
			default:
				dashboardState.IncrementDropCount()
			}

			select {
			case screenedArchivePipe <- event:
			default:
			}

			select {
			case mlPipe <- event:
			default:
				dashboardState.IncrementDropCount()
			}

			select {
			case ssePipe <- event:
			default:
			}
		}
		close(archivePipe)
		close(mlPipe)
		close(screenedArchivePipe)
	}()

	go storage.ArchiveRawEvent(ctx, minioClient, cfg.Storage.Bucket, archivePipe)

	go func() {
		for event := range mlPipe {
			for key, val := range event.Payload {
				var metricVal float64
				switch v := val.(type) {
				case float64:
					metricVal = v
				case int:
					metricVal = float64(v)
				default:
					continue
				}

				if anomalyRes, isAnomaly := anomalyDetector.Detect(event, key, metricVal); isAnomaly {
					log.Printf("ANOMALI TERDETEKSI: %s - %s | Expected: %.2f | Actual: %.2f | Conf: %.2f\n",
						anomalyRes.Source, anomalyRes.Metric, anomalyRes.Expected, anomalyRes.Value, anomalyRes.Confidence)

					dashboardState.AddAnomaly(*anomalyRes)
					go storage.SaveMLResult(ctx, minioClient, cfg.Storage.Bucket, "ml/anomalies", anomalyRes)

					select {
					case ssePipe <- anomalyRes:
					default:
					}
				}

				if forecastRes := trendPredictor.Predict(event, key, metricVal); forecastRes != nil {
					dashboardState.AddForecast(*forecastRes)
					go storage.SaveMLResult(ctx, minioClient, cfg.Storage.Bucket, "ml/forecasts", forecastRes)

					select {
					case ssePipe <- forecastRes:
					default:
					}
				}
			}
		}
		close(ssePipe)
	}()

	go mocks.GenerateDynatraceData(ctx, dataPipe)
	go mocks.GeneratePrometheusData(ctx, dataPipe)
	go mocks.GenerateSplunkData(ctx, dataPipe)
	go mocks.GenerateRiverbedData(ctx, dataPipe)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := processor.SaveDedupState(ctx, minioClient, cfg.Storage.Bucket)
				if err != nil {
					log.Printf("Warning: Gagal menyimpan dedup state: %v", err)
				} else {
					log.Println("Dedup state berhasil disimpan ke MinIO")
				}
			}
		}
	}()

	sseHub := sse.NewSSEHub()
	go sseHub.Run()
	go sse.BridgeMLToSSE(ssePipe, sseHub)

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "dashboard.html")
	})

	mux.HandleFunc("/stream", sse.SSEHandler(sseHub))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshot := dashboardState.GetSnapshot()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		jsonBytes, err := json.Marshal(snapshot)
		if err != nil {
			http.Error(w, "Failed to marshal state", http.StatusInternalServerError)
			return
		}
		w.Write(jsonBytes)
	})

	mux.HandleFunc("/api/policy", func(w http.ResponseWriter, r *http.Request) {
		policyPath := "policy/screening.json"
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			return
		}

		if r.Method == http.MethodGet {
			data, err := os.ReadFile(policyPath)
			if err != nil {
				http.Error(w, "Failed to read policy", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(data)
			return
		}

		if r.Method == http.MethodPut {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read body", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			var js json.RawMessage
			if err := json.Unmarshal(body, &js); err != nil {
				http.Error(w, "Invalid JSON format", http.StatusBadRequest)
				return
			}

			err = storage.SavePolicyToMinIO(context.Background(), minioClient, cfg.Storage.Bucket, "policy/screening.json", body)			
			if err != nil {
				http.Error(w, "Failed to save policy to MinIO", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status": "success", "message": "Policy updated. Hot-reload will trigger in ~2 seconds."}`))
			return
		}

		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})

	port := ":8080"
	log.Printf("Watchtower SSE Server listening on http://localhost%s/stream\n", port)

	go func() {
		if err := http.ListenAndServe(port, mux); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP Server failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	log.Println("Received shutdown signal, initiating graceful shutdown...")
	
	cancel()

	err = processor.SaveDedupState(context.Background(), minioClient, cfg.Storage.Bucket)
	if err != nil {
		log.Printf("Warning: Gagal menyimpan final dedup state: %v", err)
	} else {
		log.Println("Final dedup state berhasil disimpan.")
	}

	err = storage.SaveDashboardSnapshot(context.Background(), minioClient, cfg.Storage.Bucket, dashboardState.GetSnapshot())
	if err != nil {
		log.Printf("Warning: Gagal menyimpan dashboard snapshot: %v", err)
	} else {
		log.Println("Dashboard snapshot berhasil disimpan ke /state/dashboard.json")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers finished gracefully.")
	case <-time.After(5 * time.Second):
		log.Println("Shutdown timeout exceeded, forcing exit.")
	}

	log.Println("Watchtower shutdown complete.")
}