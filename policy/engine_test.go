package policy

import (
	"os"
	"testing"
	"time"
)

func TestEngineLogic(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "screening_*.json")
	if err != nil {
		t.Fatalf("Gagal membuat file temp: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Pastikan file dihapus setelah test selesai

	initialJSON := `{"drop_keywords":["DEBUG"], "critical_sources":["splunk"]}`
	os.WriteFile(tmpFile.Name(), []byte(initialJSON), 0644)

	engine := NewEngine(tmpFile.Name())

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
	tmpFile, _ := os.CreateTemp("", "screening_*.json")
	defer os.Remove(tmpFile.Name())

	initialJSON := `{"drop_keywords":["DEBUG"]}`
	os.WriteFile(tmpFile.Name(), []byte(initialJSON), 0644)

	engine := NewEngine(tmpFile.Name())
	engine.Watch(100 * time.Millisecond) // Cek perubahan sangat cepat (tiap 0.1 detik)

	time.Sleep(50 * time.Millisecond)
	newJSON := `{"drop_keywords":["TRACE"]}`
	os.WriteFile(tmpFile.Name(), []byte(newJSON), 0644)

	time.Sleep(200 * time.Millisecond)

	if engine.ShouldDrop("log DEBUG") {
		t.Errorf("Gagal: DEBUG seharusnya sudah TIDAK dibuang setelah hot-reload")
	}
	if !engine.ShouldDrop("log TRACE") {
		t.Errorf("Gagal: TRACE seharusnya SEKARANG dibuang setelah hot-reload")
	}
}