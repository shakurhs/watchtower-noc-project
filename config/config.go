package config

import (
		"encoding/json"
		"os"

)
type Config struct {
		Ingestion struct {
				ChannelBufferSize int `json:"channel_buffer_size"`
				DropLoginIntervalMs int `json:"drop_log_interval_ms"`
		} `json:"ingestion"`

		Screening struct {
				WorkerCount int `json:"worker_count"`
				DedupTTLSeconds int `json:"dedup_ttl_seconds"`
				NoiseWindowSeconds int `json:"noise_window_seconds"`
				SignatureThreshold int `json:"noise_signature_threshold"`
		} `json:"screening"`

		ML struct {
				AnomalySigmaThreshold float64 `json:"anomaly_sigma_threshold"`
				EmaAlpha float64 `json:"ema_alpha"`
				ForecastHorizonMins int `json:"forecast_horizon_minutes"`
				RegressionWindowSize int `json:"regression_window_size"`
		} `json:"ml"`

		Storage struct {
				Endpoint string `json:"endpoint"`
				Bucket string `json:"bucket"`
				Region string `json:"region"`
		} `json:"storage"`
		
		Server struct {
				Port int `json:"port"`
				ShutdownTimeoutSeconds int `json:"shutdown_timeout_seconds"`
		} `json:"server"`
}
	func LoadConfig(filename string) (*Config, error) {
		file, err := os.Open(filename)
		if err != nil {
				return nil, err
		}
		defer file.Close()

		var cfg Config

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&cfg); err != nil {
				return nil, err
		}
		return &cfg, nil
}