package main

import (
	"context"
	"log"
	"sync"
	"time"

	"watchtower/config"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/policy"
	"watchtower/screening"
	"watchtower/ml"
	"watchtower/storage"
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

		archivePipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
		mlPipe := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)


		go func() {
				for event := range screenedPipe {
						select {
						case archivePipe <- event:
						default:

						}

						select {
						case mlPipe <- event:
						default:
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

												go storage.SaveMLResult(ctx, minioClient, cfg.Storage.Bucket, "ml/anomalies", anomalyRes)
										}

										if forecastRes := trendPredictor.Predict(event, key, metricVal); forecastRes != nil {
												// log.Printf("📈 PREDIKSI TREN: %s - %s | Future Val: %.2f\n", forecastRes.Source, forecastRes.Metric, forecastRes.Predicted)										
										
												go storage.SaveMLResult(ctx, minioClient, cfg.Storage.Bucket, "ml/forecasts", forecastRes)
										}	

								
						}
				}
		}()

		go mocks.GenerateDynatraceData(ctx, dataPipe)
		go mocks.GeneratePrometheusData(ctx, dataPipe)
		go mocks.GenerateSplunkData(ctx, dataPipe)
		go mocks.GenerateRiverbedData(ctx, dataPipe)
		
		select {}
}