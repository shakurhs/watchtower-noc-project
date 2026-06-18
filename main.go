package main

import (
	"context"
	"log"
	"sync"
	"time"
	"os"
	"io"

	"watchtower/config"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/screening"
	"watchtower/ml"
	"watchtower/storage"
	"watchtower/sse"
	"watchtower/state"

	"net/http"
	"encoding/json"
)

func main() {
		cfg, err := config.LoadConfig("config.json")
		if err != nil {
			log.Fatalf("Gagal memuat config: %v", err)
		}

		engine := policy.NewEngine("policy/screening.json")
		engine.Watch(2 * time.Second)

		minioClient, err:= storage.InitMinio(cfg.Storage.Endpoint,"watchtower_admin", "watchtower_password")
		if err != nil {
			log.Fatalf("Gagal Inisialisasi MinIO: %v", err)
		}
		
		ctx := context.Background()

		err = storage.EnsureBucketExists(ctx, minioClient, cfg.Storage.Bucket, cfg.Storage.Region)
		if err != nil {
			log.Fatalf("Gagal memastikan keberadaan bucket: %v", err)
		}
		
		dataPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)

		screenedPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)

		log.Println("Memulai Project Watchtower: Ingestion & Screening Pipeline!")

		var wg sync.WaitGroup

		screening.StartWorkerPool(ctx, cfg, engine, dataPipe, screenedPipe, &wg)

		anomalyDetector := ml.NewAnomalyDetector(cfg.ML.EmaAlpha, cfg.ML.AnomalySigmaThreshold)
		trendPredictor := ml.NewTrendPredictor(cfg.ML.RegressionWindowSize, float64(cfg.ML.ForecastHorizonMins))
		dashboardState := state.NewDashboardState()

		archivePipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
		mlPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
		ssePipe := make(chan interface{}, 100)


		go func() {
				for event := range screenedPipe {

						dashboardState.AddEvent(event)

						select {
						case archivePipe <- event:
						default:
							dashboardState.IncrementDropCount()
						}

						select {
						case mlPipe <- event:
						default:
							dashboardState.IncrementDropCount()
						}
				}
				close(archivePipe)
				close(mlPipe)

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

										if anomalyRes, isAnomaly := anomalyDetector.Detect(event, key, metricVal); isAnomaly{
												log.Printf("ANOMALI TERDETEKSI: %s - %s | Expected: %.2f | Actual: %.2f | Conf: %.2f\n",
													anomalyRes.Source, anomalyRes.Metric, anomalyRes.Expected, anomalyRes.Value, anomalyRes.Confidence)	
												
												dashboardState.AddAnomaly(*anomalyRes)

												go storage.SaveMLResult(ctx, minioClient, cfg.Storage.Bucket, "ml/anomalies", anomalyRes)

												select {
												case ssePipe <-anomalyRes:
												default:

												}
										}

										if forecastRes := trendPredictor.Predict(event, key, metricVal); forecastRes != nil {
												// log.Printf("📈 PREDIKSI TREN: %s - %s | Future Val: %.2f\n", forecastRes.Source, forecastRes.Metric, forecastRes.Predicted)										

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

				// GET: Membaca policy
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

				// PUT: Menulis policy
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

					err = os.WriteFile(policyPath, body, 0644)
					if err != nil {
						http.Error(w, "Failed to save policy", http.StatusInternalServerError)
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
				if err := http.ListenAndServe(port, mux); err != nil {
					log.Fatalf("HTTP Server failed: %v", err)
				}
			}()
		select {}
}