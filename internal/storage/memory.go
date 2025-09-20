package storage

import (
    "sync"
    "time"
    "admira-etl/internal/models"
)

type MemoryStorage struct {
    mu      sync.RWMutex
    metrics []models.Metrics
}

func NewMemoryStorage() *MemoryStorage {
    return &MemoryStorage{
        metrics: make([]models.Metrics, 0),
    }
}

func (s *MemoryStorage) StoreMetrics(metrics []models.Metrics) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    s.metrics = append(s.metrics, metrics...)
    return nil
}

func (s *MemoryStorage) GetMetrics(filter func(models.Metrics) bool) []models.Metrics {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var result []models.Metrics
    for _, metric := range s.metrics {
        if filter(metric) {
            result = append(result, metric)
        }
    }
    return result
}

func (s *MemoryStorage) GetMetricsByChannel(channel string, from, to time.Time) []models.Metrics {
    return s.GetMetrics(func(m models.Metrics) bool {
        metricDate, err := time.Parse("2006-01-02", m.Date)
        if err != nil {
            return false
        }
        return m.Channel == channel && 
            (metricDate.After(from) || metricDate.Equal(from)) && 
            (metricDate.Before(to) || metricDate.Equal(to))
    })
}

func (s *MemoryStorage) GetMetricsByCampaign(campaign string, from, to time.Time) []models.Metrics {
    return s.GetMetrics(func(m models.Metrics) bool {
        metricDate, err := time.Parse("2006-01-02", m.Date)
        if err != nil {
            return false
        }
        return m.UTMCampaign == campaign && 
            (metricDate.After(from) || metricDate.Equal(from)) && 
            (metricDate.Before(to) || metricDate.Equal(to))
    })
}

// Nueva función para exportación
func (s *MemoryStorage) GetMetricsByDate(date time.Time) []models.Metrics {
    return s.GetMetrics(func(m models.Metrics) bool {
        metricDate, err := time.Parse("2006-01-02", m.Date)
        if err != nil {
            return false
        }
        return metricDate.Equal(date)
    })
}
