package policy

import (
	"context"
	"testing"
	"time"

	"watchtower/storage"
)

func TestEngineLogic(t *testing.T) {
	minioClient, err := storage.InitMinio("localhost:9000", "watchtower_admin", "watchtower_password")
	if err != nil {
		t.Fatalf("Gagal inisialisasi MinIO: %v", err)
	}

	ctx := context.Background()
	bucketName := "watchtower-test"
	objectName := "policy/test-screening.json"

	err = storage.EnsureBucketExists(ctx, minioClient, bucketName, "us-east-1")
	if err != nil {
		t.Fatalf("Gagal create bucket: %v", err)
	}

	initialJSON := `{"drop_keywords":["DEBUG"], "critical_sources":["splunk"]}`
	err = storage.SavePolicyToMinIO(ctx, minioClient, bucketName, objectName, []byte(initialJSON))
	if err != nil {
		t.Fatalf("Gagal upload policy: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	engine := NewEngine(objectName, minioClient, bucketName)

	if !engine.ShouldDrop("ini log system dengan level DEBUG") {
		t.Errorf("Gagal: Kata DEBUG seharusnya terdeteksi dan dibuang")
	}
	if engine.ShouldDrop("ini log system dengan level INFO") {
		t.Errorf("Gagal: Kata INFO seharusnya aman dan tidak dibuang")
	}
	if !engine.IsCritical("splunk") {
		t.Errorf("Gagal: splunk seharusnya berstatus kritis")
	}
}

func TestHotReload(t *testing.T) {
	minioClient, err := storage.InitMinio("localhost:9000", "watchtower_admin", "watchtower_password")
	if err != nil {
		t.Fatalf("Gagal inisialisasi MinIO: %v", err)
	}

	ctx := context.Background()
	bucketName := "watchtower-test"
	objectName := "policy/test-hotreload.json"

	err = storage.EnsureBucketExists(ctx, minioClient, bucketName, "us-east-1")
	if err != nil {
		t.Fatalf("Gagal create bucket: %v", err)
	}

	initialJSON := `{"drop_keywords":["DEBUG"]}`
	err = storage.SavePolicyToMinIO(ctx, minioClient, bucketName, objectName, []byte(initialJSON))
	if err != nil {
		t.Fatalf("Gagal upload initial policy: %v", err)
	}

	time.Sleep(200 * time.Millisecond)


	engine := NewEngine(objectName, minioClient, bucketName)
	
	ctxWatch, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	go engine.Watch(ctxWatch, 100*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	
	newJSON := `{"drop_keywords":["TRACE"]}`
	err = storage.SavePolicyToMinIO(ctx, minioClient, bucketName, objectName, []byte(newJSON))	
	if err != nil {
		t.Fatalf("Gagal upload new policy: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if engine.ShouldDrop("log DEBUG") {
		t.Errorf("Gagal: DEBUG seharusnya sudah TIDAK dibuang setelah hot-reload")
	}
	if !engine.ShouldDrop("log TRACE") {
		t.Errorf("Gagal: TRACE seharusnya SEKARANG dibuang setelah hot-reload")
	}
}