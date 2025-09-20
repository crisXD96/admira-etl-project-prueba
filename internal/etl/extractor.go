package etl

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "time"

    "admira-etl/internal/models"
    "admira-etl/pkg/config"
)

type Extractor struct {
    cfg *config.Config
}

func NewExtractor(cfg *config.Config) *Extractor {
    return &Extractor{cfg: cfg}
}

func (e *Extractor) fetchWithRetry(ctx context.Context, url string, maxRetries int) ([]byte, error) {
    var lastErr error
    
    for i := 0; i < maxRetries; i++ {
        req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
        if err != nil {
            return nil, err
        }

        client := &http.Client{Timeout: e.cfg.Timeout}
        resp, err := client.Do(req)
        if err != nil {
            lastErr = err
            time.Sleep(e.cfg.BackoffTime * time.Duration(i+1))
            continue
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            lastErr = fmt.Errorf("HTTP error: %s", resp.Status)
            time.Sleep(e.cfg.BackoffTime * time.Duration(i+1))
            continue
        }

        body, err := io.ReadAll(resp.Body)
        if err != nil {
            return nil, err
        }

        return body, nil
    }

    return nil, fmt.Errorf("failed after %d retries: %v", maxRetries, lastErr)
}

func (e *Extractor) ExtractAdsData(ctx context.Context) ([]models.AdsPerformance, error) {
    body, err := e.fetchWithRetry(ctx, e.cfg.AdsURL, e.cfg.MaxRetries)
    if err != nil {
        return nil, err
    }

    var response models.AdsResponse
    if err := json.Unmarshal(body, &response); err != nil {
        return nil, err
    }

    for i := range response.External.Ads.Performance {
        response.External.Ads.Performance[i].IngestedAt = time.Now()
    }

    return response.External.Ads.Performance, nil
}

func (e *Extractor) ExtractCRMData(ctx context.Context) ([]models.CRMOpportunity, error) {
    body, err := e.fetchWithRetry(ctx, e.cfg.CrmURL, e.cfg.MaxRetries)
    if err != nil {
        return nil, err
    }

    var response models.CRMResponse
    if err := json.Unmarshal(body, &response); err != nil {
        return nil, err
    }

    for i := range response.External.CRM.Opportunities {
        response.External.CRM.Opportunities[i].IngestedAt = time.Now()
    }

    return response.External.CRM.Opportunities, nil
}
