package storage

import (
		"bytes"
		"context"
		"encoding/json"
		"fmt"
		"log"
		"time"
		"io"

		"github.com/minio/minio-go/v7"
		"github.com/minio/minio-go/v7/pkg/credentials"
		"watchtower/models"
)

func InitMinio(endpoint, accessKey, secretKey string) (*minio.Client, error) {
		client, err := minio.New(endpoint, &minio.Options{
				Creds: credentials.NewStaticV4(accessKey, secretKey, ""),
				Secure: false,
		})
		return client, err
}

func EnsureBucketExists(ctx context.Context, client *minio.Client, bucketName string, region string) error{
		exists, err := client.BucketExists(ctx , bucketName)
		if err != nil {
			return err
		}

		if !exists {
				err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: region})
				if err != nil {
					return err
				}
				log.Printf("Bucket '%s' berhasil dibuat otomatis!\n", bucketName)
		}
		return nil
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
				objectName := fmt.Sprintf("events/raw/%d/%02d/%02d/%02d/%s.json",
						now.Year(), now.Month(), now.Day(), now.Hour(), event.ID)

				reader := bytes.NewReader(jsonData)
				_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{
						ContentType: "application/json",
				})

				if err != nil {
					log.Printf("Gagal upload ke MinIO: %v/n", err)
				}
			}
	}
}

func SaveMLResult(ctx context.Context, client *minio.Client, bucketName string, prefix string, data interface{}) {
	jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Gagal marshal data ML: %v\n", err)
			return
		}

		now := time.Now()
		fileName := fmt.Sprintf("%d.json", now.UnixNano())
		
		objectName := fmt.Sprintf("%s/%04d/%02d/%02d/%s", 
			prefix, now.Year(), now.Month(), now.Day(), fileName)

		reader := bytes.NewReader(jsonData)
		_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{
			ContentType: "application/json",
		})

		if err != nil {
			log.Printf("Gagal menyimpan %s ke MinIO: %v\n", objectName, err)
	}
}

func SaveDedupWindow(ctx context.Context, client *minio.Client, bucketName string, dedupData map[string]time.Time) error {
	buckets := make(map[string]map[string]time.Time)
	
	for id, timestamp := range dedupData {
		bucketKey := timestamp.Truncate(time.Minute).Format("200601021504")
		if buckets[bucketKey] == nil {
			buckets[bucketKey] = make(map[string]time.Time)
		}
		buckets[bucketKey][id] = timestamp
	}

	for bucketKey, ids := range buckets {
		jsonData, err := json.Marshal(ids)
		if err != nil {
			log.Printf("Gagal marshal dedup bucket %s: %v", bucketKey, err)
			continue
		}

		objectName := fmt.Sprintf("dedup/window/%s.json", bucketKey)
		reader := bytes.NewReader(jsonData)
		
		_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{
			ContentType: "application/json",
		})

		if err != nil {
			log.Printf("Gagal menyimpan dedup bucket %s: %v", bucketKey, err)
		}
	}

	return nil
}

func LoadDedupWindow(ctx context.Context, client *minio.Client, bucketName string) (map[string]time.Time, error) {
	dedupData := make(map[string]time.Time)
	
	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    "dedup/window/",
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			log.Printf("Error listing dedup objects: %v", object.Err)
			continue
		}

		obj, err := client.GetObject(ctx, bucketName, object.Key, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("Gagal membaca object %s: %v", object.Key, err)
			continue
		}

		var bucketData map[string]time.Time
		decoder := json.NewDecoder(obj)
		if err := decoder.Decode(&bucketData); err != nil {
			log.Printf("Gagal decode dedup bucket %s: %v", object.Key, err)
			obj.Close()
			continue
		}
		obj.Close()

		for id, timestamp := range bucketData {
			dedupData[id] = timestamp
		}
	}

	log.Printf("Berhasil memuat %d event IDs dari dedup window", len(dedupData))
	return dedupData, nil
}

func SaveDashboardSnapshot(ctx context.Context, client *minio.Client, bucketName string, snapshot interface{}) error {
	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(jsonData)
	_, err = client.PutObject(ctx, bucketName, "state/dashboard.json", reader, int64(len(jsonData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func SavePolicyToMinIO(ctx context.Context, client *minio.Client, bucketName string, objectName string, policyData []byte) error {
	reader := bytes.NewReader(policyData)
	_, err := client.PutObject(ctx, bucketName, objectName, reader, int64(len(policyData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func LoadPolicyFromMinIO(ctx context.Context, client *minio.Client, bucketName string, objectName string) ([]byte, string, error) {
	obj, err := client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", err
	}

	stat, err := obj.Stat()
	if err != nil {
		return nil, "", err
	}

	return data, stat.ETag, nil
}

func ArchiveScreenedEvent(ctx context.Context, client *minio.Client, bucketName string, events <-chan models.EventEnvelope) {
	for event := range events {
		t := time.Unix(event.Timestamp, 0)
		path := fmt.Sprintf("events/screened/%d/%02d/%02d/%d/%s.json", 
			t.Year(), t.Month(), t.Day(), t.Hour(), event.ID)
		
		jsonData, err := json.Marshal(event)
		if err != nil {
			continue
		}
		
		_, err = client.PutObject(ctx, bucketName, path, bytes.NewReader(jsonData), int64(len(jsonData)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
		if err != nil {
			log.Printf("Gagal arsip screened event %s: %v", event.ID, err)
		}
	}
	fmt.Println("Screened Archiver stopped.")
}