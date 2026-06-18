package policy

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Policy struct {
	DropKeywords    []string `json:"drop_keywords"`
	CriticalSources []string `json:"critical_sources"`
}

type Engine struct {
	mu           sync.RWMutex // Mencegah Race Condition saat Hot-Reload
	policy       Policy
	filePath     string
	lastModified time.Time
}

func NewEngine(filePath string) *Engine {
	engine := &Engine{
		filePath: filePath,
	}
	engine.loadPolicy()
	return engine
}

func (e *Engine) loadPolicy() {
	fileInfo, err := os.Stat(e.filePath)
	if err != nil {
		log.Printf("Gagal membaca info file kebijakan: %v", err)
		return
	}

	fileData, err := os.ReadFile(e.filePath)
	if err != nil {
		log.Printf("Gagal membaca file kebijakan: %v", err)
		return
	}

	var newPolicy Policy
	if err := json.Unmarshal(fileData, &newPolicy); err != nil {
		log.Printf("Gagal mem-parsing JSON kebijakan: %v", err)
		return
	}

	e.mu.Lock()
	e.policy = newPolicy
	e.lastModified = fileInfo.ModTime()
	e.mu.Unlock()

	log.Println("Kebijakan berhasil diperbarui!")
}

func (e *Engine) Watch(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			fileInfo, err := os.Stat(e.filePath)
			if err != nil {
				continue
			}

			e.mu.RLock()
			lastMod := e.lastModified
			e.mu.RUnlock()

			if fileInfo.ModTime().After(lastMod) {
				log.Println("Perubahan file kebijakan terdeteksi. Memuat ulang...")
				e.loadPolicy()
			}
		}
	}()
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