package main

import (
	"context"
	"log"
	"sync"

	"watchtower/config"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/screening"
	"watchtower/storage"
)

func main() {
		cfg, err := config.LoadConfig("config.json")
		if err != nil {
			log.Fatalf("Gagal memuat config: %v", err)
		}

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

		screening.StartWorkerPool(ctx, cfg, dataPipe, screenedPipe, &wg)

		go storage.ArchiveRawEvent(ctx, minioClient, cfg.Storage.Bucket, screenedPipe)
		// go func() {
		// for event := range dataPipe {
			// Mencetak ID log dan sumbernya ke layar
			// log.Printf("Log Diterima -> ID: %s | Source: %s\n", event.ID, event.Source)
		// }

		go mocks.GenerateDynatraceData(ctx, dataPipe)
		go mocks.GeneratePrometheusData(ctx, dataPipe)
		go mocks.GenerateSplunkData(ctx, dataPipe)
		go mocks.GenerateRiverbedData(ctx, dataPipe)
		
		select {}
}