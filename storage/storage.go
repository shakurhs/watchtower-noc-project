package storage

import (
		"bytes"
		"context"
		"encoding/json"
		"fmt"
		"log"
		"time"

		"github.com/minio/minio-go/v7"
		"github.com/minio/minio-go/v7/pkg/credentials"
		"watchtower/models"
)

func InitMinio(endpoint, accessKey, secretKey string) (*minio.Clien, error) {
		client, err := minio.New(endpoint, &minio.Options{
				Creds: credentials.NewStaticV4(accessKey, secretKey, ""),
				Secure: false,
		})
		return client, err
}

func ArchiveRawEvent(ctx context.Context, client *minio.Client, bucketName string, dataPipe <-chan models.EventEnvelope) {
	for {
			select {
			case <-ctx.Done():
				fmt.Println("Archiver stopped.")
				return
			case event:= <-dataPipe:
				
				jsonData, err := json.Marshal(event)
				if err != nil {
					continue
				}

				now := time.Unix(event.Timestamp, 0)
				objectName := fmt.Sprintf("events/raw/%04d/%02d/%02d/%s.json",
						now.Year(), now.Month(), now.Day(), now.Hour(), event.ID)

				reader := bytes.NewReader(jsonData)
				_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.putObjectOptions{
						ContentType: "application/json",
				})

				if err != nil {
					log.Printf("Gagal upload ke MinIO: %v/n", err)
				}
			}
	}
}