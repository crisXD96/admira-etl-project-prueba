package test

import (
    "testing"
    "time"
    "admira-etl/internal/etl"
    "admira-etl/internal/models"
)

func TestTransformer_Transform(t *testing.T) {
    transformer := etl.NewTransformer()
    
    // Datos de prueba Ads
    adsData := []models.AdsPerformance{
        {
            Date:        "2024-01-01",
            CampaignID:  "C-1001",
            Channel:     "google_ads",
            Clicks:      100,
            Impressions: 5000,
            Cost:        50.0,
            UTMCampaign: "test_campaign",
            UTMSource:   "google",
            UTMMedium:   "cpc",
        },
    }
    
    // Datos de prueba CRM
    crmData := []models.CRMOpportunity{
        {
            OpportunityID: "O-9001",
            Stage:         "lead",
            Amount:        0,
            CreatedAt:     time.Now(),
            UTMCampaign:   "test_campaign",
            UTMSource:     "google",
            UTMMedium:     "cpc",
        },
        {
            OpportunityID: "O-9002",
            Stage:         "closed_won",
            Amount:        1000.0,
            CreatedAt:     time.Now(),
            UTMCampaign:   "test_campaign",
            UTMSource:     "google",
            UTMMedium:     "cpc",
        },
    }
    
    // Ejecutar transformación
    metrics, err := transformer.Transform(adsData, crmData)
    if err != nil {
        t.Fatalf("Transform failed: %v", err)
    }
    
    // Verificar resultados
    if len(metrics) == 0 {
        t.Error("No metrics generated")
    }
    
    // Verificar cálculos de métricas
    for _, metric := range metrics {
        if metric.Clicks > 0 && metric.Cost > 0 {
            expectedCPC := metric.Cost / float64(metric.Clicks)
            if metric.CPC != expectedCPC {
                t.Errorf("CPC calculation wrong: got %.2f, expected %.2f", metric.CPC, expectedCPC)
            }
        }
        
        if metric.Leads > 0 {
            expectedCPA := metric.Cost / float64(metric.Leads)
            if metric.CPA != expectedCPA {
                t.Errorf("CPA calculation wrong: got %.2f, expected %.2f", metric.CPA, expectedCPA)
            }
        }
    }
}

func TestTransformer_FilterByDate(t *testing.T) {
    transformer := etl.NewTransformer()
    
    metrics := []models.Metrics{
        {Date: "2024-01-01"},
        {Date: "2024-01-02"},
        {Date: "2024-01-03"},
    }
    
    // Filtrar desde 2024-01-02
    filterDate, _ := time.Parse("2006-01-02", "2024-01-02")
    filtered := transformer.FilterByDate(metrics, filterDate)
    
    if len(filtered) != 2 {
        t.Errorf("Expected 2 metrics after filtering, got %d", len(filtered))
    }
}
