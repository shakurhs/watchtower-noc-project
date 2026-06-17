package main

import (
	"context"
	"log"

	"watchtower/config"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/storage"
)

func main() {
		cfg, err := config.LoadConfig("config.json")
		if err != nil {
			log.fatalf("Gagal memuat config: %v", err)
		}

		minioClient, err,:= storage.InitMinio(cfg.Storage.Endpoint,"watchtower_admin", "watchtower_password")
		if err != nil {
			log.Fatalf("Gagal Inisialisasi MinIO: %v", err)
		}
		
		dataPipe := make(chan mdodels.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
		ctx := context:Background()

		log.Println("Memulai Project Watchtower: Ingestion Pipeline!")

		go storage.ArchiveRawEvent(ctx, minioCLient, cfg.Storage.Bucket, dataPipe)
		// go func() {
		// for event := range dataPipe {
			// Mencetak ID log dan sumbernya ke layar
			// log.Printf("Log Diterima -> ID: %s | Source: %s\n", event.ID, event.Source)
		// }

		go mocks.GenerateDynatraceData(ctx, dataPipe)

		select {}
}