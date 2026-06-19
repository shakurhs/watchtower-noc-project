package policy

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"watchtower/storage"
)

type Policy struct {
	DropKeywords    []string                `json:"drop_keywords"`
	CriticalSources []string                `json:"critical_sources"`
	PriorityRules   map[string]PriorityRule `json:"priority_rules"`
}

type PriorityRule struct {
	Keywords []string `json:"keywords"`
	Sources  []string `json:"sources"`
}

type Engine struct {
	mu          sync.RWMutex
	policy      Policy
	objectName  string
	minioClient *minio.Client
	bucketName  string
	currentETag string
}

// NewEngine sekarang menerima MinIO client dan bucket, bukan path file lokal
func NewEngine(objectName string, client *minio.Client, bucketName string) *Engine {
	engine := &Engine{
		objectName:  objectName,
		minioClient: client,
		bucketName:  bucketName,
	}
	engine.loadPolicy()
	return engine
}

func (e *Engine) loadPolicy() {
	data, etag, err := storage.LoadPolicyFromMinIO(context.Background(), e.minioClient, e.bucketName, e.objectName)
	if err != nil {
		log.Printf("Gagal membaca policy dari MinIO: %v", err)
		return
	}

	var newPolicy Policy
	if err := json.Unmarshal(data, &newPolicy); err != nil {
		log.Printf("Gagal mem-parsing JSON kebijakan: %v", err)
		return
	}

	e.mu.Lock()
	e.policy = newPolicy
	e.currentETag = etag
	e.mu.Unlock()

	log.Println("Kebijakan berhasil diperbarui dari MinIO!")
}

func (e *Engine) Watch(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.checkForUpdate()
		}
	}
}

func (e *Engine) checkForUpdate() {
	objInfo, err := e.minioClient.StatObject(context.Background(), e.bucketName, e.objectName, minio.StatObjectOptions{})
	if err != nil {
		return
	}

	e.mu.RLock()
	lastETag := e.currentETag
	e.mu.RUnlock()

	if objInfo.ETag != lastETag {
		log.Println("Perubahan policy terdeteksi di MinIO (ETag berubah). Memuat ulang...")
		e.loadPolicy()
	}
}

func (e *Engine) ShouldDrop(payload string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, keyword := range e.policy.DropKeywords {
		if strings.Contains(strings.ToUpper(payload), strings.ToUpper(keyword)) {
			return true
		}
	}
	return false
}

func (e *Engine) IsCritical(source string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, src := range e.policy.CriticalSources {
		if strings.ToLower(src) == strings.ToLower(source) {
			return true
		}
	}
	return false
}

func (e *Engine) ClassifyPriority(source string, payload string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	payloadUpper := strings.ToUpper(payload)
	sourceLower := strings.ToLower(source)

	priorities := []string{"P1", "P2", "P3", "P4"}
	
	for _, priority := range priorities {
		rule, exists := e.policy.PriorityRules[priority]
		if !exists {
			continue
		}

		for _, keyword := range rule.Keywords {
			if strings.Contains(payloadUpper, strings.ToUpper(keyword)) {
				return priority
			}
		}

		for _, src := range rule.Sources {
			if strings.ToLower(src) == sourceLower {
				return priority
			}
		}
	}

	return "P4"
}