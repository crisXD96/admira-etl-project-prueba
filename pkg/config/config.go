package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port        string
	AdsURL      string
	CrmURL      string
	SinkURL     string
	SinkSecret  string
	Timeout     time.Duration
	MaxRetries  int
	BackoffTime time.Duration
}

func LoadConfig() (*Config, error) {
	// Cargar .env si existe
	godotenv.Load()

	timeout, _ := strconv.Atoi(getEnv("TIMEOUT_SECONDS", "30"))
	maxRetries, _ := strconv.Atoi(getEnv("MAX_RETRIES", "3"))
	backoff, _ := strconv.Atoi(getEnv("BACKOFF_MS", "1000"))

	return &Config{
		Port:        getEnv("PORT", "8080"),
		AdsURL:      getEnv("ADS_API_URL", "https://mocki.io/v1/9dcc2981-2bc8-465a-bce3-47767e1278e6"),
		CrmURL:      getEnv("CRM_API_URL", "https://mocki.io/v1/6a064f10-829d-432c-9f0d-24d5b8cb71c7"),
		SinkURL:     getEnv("SINK_URL", ""),
		SinkSecret:  getEnv("SINK_SECRET", "admira_secret_example"),
		Timeout:     time.Duration(timeout) * time.Second,
		MaxRetries:  maxRetries,
		BackoffTime: time.Duration(backoff) * time.Millisecond,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}