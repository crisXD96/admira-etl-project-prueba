package models

import "time"

type AdsPerformance struct {
    Date         string    `json:"date"`
    CampaignID   string    `json:"campaign_id"`
    Channel      string    `json:"channel"`
    Clicks       int       `json:"clicks"`
    Impressions  int       `json:"impressions"`
    Cost         float64   `json:"cost"`
    UTMCampaign  string    `json:"utm_campaign"`
    UTMSource    string    `json:"utm_source"`
    UTMMedium    string    `json:"utm_medium"`
    IngestedAt   time.Time `json:"ingested_at"`
}

type AdsResponse struct {
    External struct {
        Ads struct {
            Performance []AdsPerformance `json:"performance"`
        } `json:"ads"`
    } `json:"external"`
}
