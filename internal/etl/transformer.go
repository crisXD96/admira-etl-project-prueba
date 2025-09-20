package etl

import (
    "fmt"
    "time"

    "admira-etl/internal/models"
)

type Transformer struct{}

func NewTransformer() *Transformer {
    return &Transformer{}
}

func inferChannelFromUTM(utmSource, utmMedium string) string {
    switch utmSource {
    case "google":
        return "google_ads"
    case "facebook":
        return "facebook_ads" 
    case "tiktok":
        return "tiktok_ads"
    case "linkedin":
        return "linkedin_ads"
    default:
        return fmt.Sprintf("%s_%s", utmSource, utmMedium)
    }
}

// Clave única para agrupar métricas
type MetricKey struct {
    Date        string
    Channel     string
    CampaignID  string
    UTMCampaign string
    UTMSource   string
    UTMMedium   string
}

func (t *Transformer) Transform(adsData []models.AdsPerformance, crmData []models.CRMOpportunity) ([]models.Metrics, error) {
    fmt.Printf("Debug: Transformando %d registros Ads y %d registros CRM\n", len(adsData), len(crmData))
    
    // Map para consolidar métricas por clave única
    metricsMap := make(map[MetricKey]*models.Metrics)

    // Procesar datos de Ads
    for _, ad := range adsData {
        key := MetricKey{
            Date:        ad.Date,
            Channel:     ad.Channel,
            CampaignID:  ad.CampaignID,
            UTMCampaign: ad.UTMCampaign,
            UTMSource:   ad.UTMSource,
            UTMMedium:   ad.UTMMedium,
        }
        
        if existing, exists := metricsMap[key]; exists {
            // Consolidar datos de Ads
            existing.Clicks += ad.Clicks
            existing.Impressions += ad.Impressions
            existing.Cost += ad.Cost
        } else {
            // Crear nueva métrica
            metricsMap[key] = &models.Metrics{
                Date:        ad.Date,
                Channel:     ad.Channel,
                CampaignID:  ad.CampaignID,
                Clicks:      ad.Clicks,
                Impressions: ad.Impressions,
                Cost:        ad.Cost,
                UTMCampaign: ad.UTMCampaign,
                UTMSource:   ad.UTMSource,
                UTMMedium:   ad.UTMMedium,
            }
        }
        fmt.Printf("Debug: Procesado Ads - Date: %s, Channel: %s, Clicks: %d, Cost: %.2f\n", ad.Date, ad.Channel, ad.Clicks, ad.Cost)
    }

    // Procesar datos de CRM - INFERIR channel desde UTM
    for _, crm := range crmData {
        date := crm.CreatedAt.Format("2006-01-02")
        channel := inferChannelFromUTM(crm.UTMSource, crm.UTMMedium)
        
        key := MetricKey{
            Date:        date,
            Channel:     channel,
            CampaignID:  "", // CRM no tiene campaign_id
            UTMCampaign: crm.UTMCampaign,
            UTMSource:   crm.UTMSource,
            UTMMedium:   crm.UTMMedium,
        }
        
        if existing, exists := metricsMap[key]; exists {
            // Consolidar datos de CRM
            switch crm.Stage {
            case "lead":
                existing.Leads += 1
            case "opportunity":
                existing.Opportunities += 1
            case "closed_won":
                existing.ClosedWon += 1
                existing.Revenue += crm.Amount
            }
        } else {
            // Crear nueva métrica
            metric := &models.Metrics{
                Date:        date,
                Channel:     channel,
                CampaignID:  "",
                UTMCampaign: crm.UTMCampaign,
                UTMSource:   crm.UTMSource,
                UTMMedium:   crm.UTMMedium,
            }
            
            switch crm.Stage {
            case "lead":
                metric.Leads = 1
            case "opportunity":
                metric.Opportunities = 1
            case "closed_won":
                metric.ClosedWon = 1
                metric.Revenue = crm.Amount
            }
            
            metricsMap[key] = metric
        }
        fmt.Printf("Debug: Procesado CRM - Date: %s, Channel: %s, Stage: %s, Amount: %.2f\n", date, channel, crm.Stage, crm.Amount)
    }

    // Convertir map a slice y calcular métricas
    var metrics []models.Metrics
    for _, metric := range metricsMap {
        // Calcular métricas derivadas
        t.calculateDerivedMetrics(metric)
        metrics = append(metrics, *metric)
    }

    fmt.Printf("Debug: Total métricas consolidadas generadas: %d\n", len(metrics))
    return metrics, nil
}

// Calcular métricas derivadas (CPC, CPA, CVR, ROAS)
func (t *Transformer) calculateDerivedMetrics(metric *models.Metrics) {
    // CPC = cost / clicks (proteger división por cero)
    if metric.Clicks > 0 {
        metric.CPC = metric.Cost / float64(metric.Clicks)
    } else {
        metric.CPC = 0
    }
    
    // CPA = cost / leads (proteger división por cero)
    if metric.Leads > 0 {
        metric.CPA = metric.Cost / float64(metric.Leads)
    } else {
        metric.CPA = 0
    }
    
    // CVR lead→opportunity = opportunities / leads (proteger división por cero)
    if metric.Leads > 0 {
        metric.CVRLeadToOpp = float64(metric.Opportunities) / float64(metric.Leads)
    } else {
        metric.CVRLeadToOpp = 0
    }
    
    // CVR opportunity→won = won / opportunities (proteger división por cero)
    if metric.Opportunities > 0 {
        metric.CVROppToWon = float64(metric.ClosedWon) / float64(metric.Opportunities)
    } else {
        metric.CVROppToWon = 0
    }
    
    // ROAS = revenue / cost (proteger división por cero)
    if metric.Cost > 0 {
        metric.ROAS = metric.Revenue / metric.Cost
    } else {
        metric.ROAS = 0
    }
    
    fmt.Printf("Debug: Métricas calculadas - Date: %s, Channel: %s, CPC: %.3f, CPA: %.2f, CVR_Lead_Opp: %.3f, CVR_Opp_Won: %.3f, ROAS: %.2f\n", 
        metric.Date, metric.Channel, metric.CPC, metric.CPA, metric.CVRLeadToOpp, metric.CVROppToWon, metric.ROAS)
}

func (t *Transformer) FilterByDate(metrics []models.Metrics, since time.Time) []models.Metrics {
    var filtered []models.Metrics
    
    for _, metric := range metrics {
        metricDate, err := time.Parse("2006-01-02", metric.Date)
        if err != nil {
            continue
        }
        
        if metricDate.After(since) || metricDate.Equal(since) {
            filtered = append(filtered, metric)
        }
    }
    
    fmt.Printf("Debug: Métricas después de filtrar por fecha: %d\n", len(filtered))
    return filtered
}
