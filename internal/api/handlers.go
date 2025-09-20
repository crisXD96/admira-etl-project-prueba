package api

import (
    "bytes"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "strconv"
    "time"

    "admira-etl/internal/etl"
    "admira-etl/internal/models"
    "admira-etl/internal/storage"
    "admira-etl/pkg/config"

    "github.com/gin-gonic/gin"
)

type Server struct {
    cfg       *config.Config
    router    *gin.Engine
    storage   *storage.MemoryStorage
    etl       *etl.Transformer
    extractor *etl.Extractor
}

func NewServer(cfg *config.Config) *Server {
    server := &Server{
        cfg:      cfg,
        storage:  storage.NewMemoryStorage(),
        etl:      etl.NewTransformer(),
        extractor: etl.NewExtractor(cfg),
    }
    server.setupRouter()
    return server
}

func (s *Server) setupRouter() {
    router := gin.Default()
    
    router.Use(s.requestIDMiddleware())
    router.Use(s.loggingMiddleware())
    
    router.GET("/healthz", s.healthCheck)
    router.GET("/readyz", s.readyCheck)
    
    router.POST("/ingest/run", s.runIngest)
    router.POST("/export/run", s.runExport)
    
    router.GET("/metrics/channel", s.getChannelMetrics)
    router.GET("/metrics/funnel", s.getFunnelMetrics)
    
    // Endpoints de debug
    router.GET("/debug/ads", s.debugAds)
    router.GET("/debug/crm", s.debugCRM)
    router.GET("/debug/matches", s.debugMatches)
    
    s.router = router
}

func (s *Server) Start() error {
    return s.router.Run(":" + s.cfg.Port)
}

func (s *Server) healthCheck(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func (s *Server) readyCheck(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (s *Server) runIngest(c *gin.Context) {
    ctx := c.Request.Context()
    
    sinceStr := c.Query("since")
    var since time.Time
    if sinceStr != "" {
        parsedSince, err := time.Parse("2006-01-02", sinceStr)
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format. Use YYYY-MM-DD"})
            return
        }
        since = parsedSince
    } else {
        since = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
    }
    
    adsData, err := s.extractor.ExtractAdsData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract ads data: %v", err)})
        return
    }
    
    crmData, err := s.extractor.ExtractCRMData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract CRM data: %v", err)})
        return
    }
    
    metrics, err := s.etl.Transform(adsData, crmData)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to transform data: %v", err)})
        return
    }
    
    filteredMetrics := s.etl.FilterByDate(metrics, since)
    
    if err := s.storage.StoreMetrics(filteredMetrics); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to store metrics: %v", err)})
        return
    }
    
    c.JSON(http.StatusOK, gin.H{
        "message": "Ingestion completed successfully",
        "metrics_processed": len(filteredMetrics),
        "since": since.Format("2006-01-02"),
    })
}

func (s *Server) getChannelMetrics(c *gin.Context) {
    channel := c.Query("channel")
    fromStr := c.Query("from")
    toStr := c.Query("to")
    limitStr := c.Query("limit")
    offsetStr := c.Query("offset")
    
    if channel == "" || fromStr == "" || toStr == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "channel, from, and to parameters are required"})
        return
    }
    
    from, err := time.Parse("2006-01-02", fromStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid from date format. Use YYYY-MM-DD"})
        return
    }
    
    to, err := time.Parse("2006-01-02", toStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid to date format. Use YYYY-MM-DD"})
        return
    }
    
    limit := 100
    if limitStr != "" {
        if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
            limit = parsedLimit
        }
    }
    
    offset := 0
    if offsetStr != "" {
        if parsedOffset, err := strconv.Atoi(offsetStr); err == nil && parsedOffset >= 0 {
            offset = parsedOffset
        }
    }
    
    metrics := s.storage.GetMetricsByChannel(channel, from, to)
    
    end := offset + limit
    if end > len(metrics) {
        end = len(metrics)
    }
    
    if offset > len(metrics) {
        offset = len(metrics)
    }
    
    paginatedMetrics := metrics[offset:end]
    
    c.JSON(http.StatusOK, gin.H{
        "data": paginatedMetrics,
        "pagination": gin.H{
            "total": len(metrics),
            "limit": limit,
            "offset": offset,
            "has_more": end < len(metrics),
        },
    })
}

func (s *Server) getFunnelMetrics(c *gin.Context) {
    campaign := c.Query("utm_campaign")
    fromStr := c.Query("from")
    toStr := c.Query("to")
    
    if campaign == "" || fromStr == "" || toStr == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "utm_campaign, from, and to parameters are required"})
        return
    }
    
    from, err := time.Parse("2006-01-02", fromStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid from date format"})
        return
    }
    
    to, err := time.Parse("2006-01-02", toStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid to date format"})
        return
    }
    
    metrics := s.storage.GetMetricsByCampaign(campaign, from, to)
    c.JSON(http.StatusOK, metrics)
}

func (s *Server) runExport(c *gin.Context) {
    dateStr := c.Query("date")
    if dateStr == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "date parameter is required"})
        return
    }
    
    date, err := time.Parse("2006-01-02", dateStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format. Use YYYY-MM-DD"})
        return
    }
    
    // Obtener métricas del día
    metrics := s.storage.GetMetricsByDate(date)
    if len(metrics) == 0 {
        c.JSON(http.StatusNotFound, gin.H{"error": "No metrics found for the specified date"})
        return
    }
    
    // Consolidar métricas del día
    consolidatedMetrics := s.consolidateMetricsByDate(metrics, dateStr)
    
    // Verificar si hay SINK_URL configurado
    if s.cfg.SinkURL == "" {
        c.JSON(http.StatusOK, gin.H{
            "message": "Export data prepared (no SINK_URL configured)",
            "date": dateStr,
            "metrics": consolidatedMetrics,
            "total_records": len(consolidatedMetrics),
        })
        return
    }
    
    // Exportar al sink
    err = s.exportToSink(consolidatedMetrics, dateStr)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to export to sink: %v", err)})
        return
    }
    
    c.JSON(http.StatusOK, gin.H{
        "message": "Export completed successfully",
        "date": dateStr,
        "total_records": len(consolidatedMetrics),
        "sink_url": s.cfg.SinkURL,
    })
}

// consolidateMetricsByDate agrupa las métricas por canal y campaña para el día
func (s *Server) consolidateMetricsByDate(metrics []models.Metrics, date string) []models.Metrics {
    // Crear un mapa para consolidar por canal y campaña
    consolidated := make(map[string]*models.Metrics)
    
    for _, metric := range metrics {
        key := fmt.Sprintf("%s_%s_%s", metric.Channel, metric.CampaignID, metric.UTMCampaign)
        
        if existing, exists := consolidated[key]; exists {
            // Consolidar métricas existentes
            existing.Clicks += metric.Clicks
            existing.Impressions += metric.Impressions
            existing.Cost += metric.Cost
            existing.Leads += metric.Leads
            existing.Opportunities += metric.Opportunities
            existing.ClosedWon += metric.ClosedWon
            existing.Revenue += metric.Revenue
            
            // Recalcular métricas derivadas
            s.calculateDerivedMetrics(existing)
        } else {
            // Crear nueva métrica consolidada
            newMetric := metric
            consolidated[key] = &newMetric
        }
    }
    
    // Convertir mapa a slice
    var result []models.Metrics
    for _, metric := range consolidated {
        result = append(result, *metric)
    }
    
    return result
}

// calculateDerivedMetrics recalcula las métricas derivadas
func (s *Server) calculateDerivedMetrics(metric *models.Metrics) {
    if metric.Clicks > 0 {
        metric.CPC = metric.Cost / float64(metric.Clicks)
    } else {
        metric.CPC = 0
    }
    
    if metric.Leads > 0 {
        metric.CPA = metric.Cost / float64(metric.Leads)
    } else {
        metric.CPA = 0
    }
    
    if metric.Leads > 0 {
        metric.CVRLeadToOpp = float64(metric.Opportunities) / float64(metric.Leads)
    } else {
        metric.CVRLeadToOpp = 0
    }
    
    if metric.Opportunities > 0 {
        metric.CVROppToWon = float64(metric.ClosedWon) / float64(metric.Opportunities)
    } else {
        metric.CVROppToWon = 0
    }
    
    if metric.Cost > 0 {
        metric.ROAS = metric.Revenue / metric.Cost
    } else {
        metric.ROAS = 0
    }
}

// exportToSink envía los datos al sink con HMAC signature
func (s *Server) exportToSink(metrics []models.Metrics, date string) error {
    // Preparar el payload
    payload := map[string]interface{}{
        "date": date,
        "metrics": metrics,
        "exported_at": time.Now().UTC().Format(time.RFC3339),
    }
    
    // Serializar a JSON
    jsonData, err := json.Marshal(payload)
    if err != nil {
        return fmt.Errorf("failed to marshal payload: %v", err)
    }
    
    // Generar HMAC signature
    signature := s.generateHMACSignature(jsonData)
    
    // Crear request HTTP
    req, err := http.NewRequest("POST", s.cfg.SinkURL, bytes.NewBuffer(jsonData))
    if err != nil {
        return fmt.Errorf("failed to create request: %v", err)
    }
    
    // Configurar headers
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-Signature", signature)
    req.Header.Set("User-Agent", "Admira-ETL-Service/1.0")
    
    // Enviar request
    client := &http.Client{
        Timeout: s.cfg.Timeout,
    }
    
    resp, err := client.Do(req)
    if err != nil {
        return fmt.Errorf("failed to send request: %v", err)
    }
    defer resp.Body.Close()
    
    // Verificar respuesta
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        return fmt.Errorf("sink returned status %d", resp.StatusCode)
    }
    
    return nil
}

// generateHMACSignature genera la firma HMAC-SHA256
func (s *Server) generateHMACSignature(data []byte) string {
    h := hmac.New(sha256.New, []byte(s.cfg.SinkSecret))
    h.Write(data)
    return hex.EncodeToString(h.Sum(nil))
}

// Endpoints de debug
func (s *Server) debugAds(c *gin.Context) {
    dateStr := c.Query("date")
    if dateStr == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "date parameter is required"})
        return
    }
    
    date, err := time.Parse("2006-01-02", dateStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format. Use YYYY-MM-DD"})
        return
    }
    
    ctx := c.Request.Context()
    adsData, err := s.extractor.ExtractAdsData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract ads data: %v", err)})
        return
    }
    
    // Filtrar por fecha
    var filteredAds []models.AdsPerformance
    for _, ad := range adsData {
        adDate, err := time.Parse("2006-01-02", ad.Date)
        if err == nil && adDate.Equal(date) {
            filteredAds = append(filteredAds, ad)
        }
    }
    
    c.JSON(http.StatusOK, gin.H{
        "date": dateStr,
        "ads_data": filteredAds,
        "total_records": len(filteredAds),
    })
}

func (s *Server) debugCRM(c *gin.Context) {
    dateStr := c.Query("date")
    if dateStr == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "date parameter is required"})
        return
    }
    
    _, err := time.Parse("2006-01-02", dateStr)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format. Use YYYY-MM-DD"})
        return
    }
    
    ctx := c.Request.Context()
    crmData, err := s.extractor.ExtractCRMData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract CRM data: %v", err)})
        return
    }
    
    // Filtrar por fecha
    var filteredCRM []models.CRMOpportunity
    for _, crm := range crmData {
        crmDate := crm.CreatedAt.Format("2006-01-02")
        if crmDate == dateStr {
            filteredCRM = append(filteredCRM, crm)
        }
    }
    
    c.JSON(http.StatusOK, gin.H{
        "date": dateStr,
        "crm_data": filteredCRM,
        "total_records": len(filteredCRM),
    })
}

func (s *Server) debugMatches(c *gin.Context) {
    campaign := c.Query("utm_campaign")
    if campaign == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "utm_campaign parameter is required"})
        return
    }
    
    ctx := c.Request.Context()
    adsData, err := s.extractor.ExtractAdsData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract ads data: %v", err)})
        return
    }
    
    crmData, err := s.extractor.ExtractCRMData(ctx)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to extract CRM data: %v", err)})
        return
    }
    
    // Filtrar por campaña
    var matchingAds []models.AdsPerformance
    var matchingCRM []models.CRMOpportunity
    
    for _, ad := range adsData {
        if ad.UTMCampaign == campaign {
            matchingAds = append(matchingAds, ad)
        }
    }
    
    for _, crm := range crmData {
        if crm.UTMCampaign == campaign {
            matchingCRM = append(matchingCRM, crm)
        }
    }
    
    c.JSON(http.StatusOK, gin.H{
        "utm_campaign": campaign,
        "ads_matches": matchingAds,
        "crm_matches": matchingCRM,
        "ads_count": len(matchingAds),
        "crm_count": len(matchingCRM),
    })
}
