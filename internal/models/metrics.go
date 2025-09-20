package models

type Metrics struct {
    Date           string  `json:"date"`
    Channel        string  `json:"channel"`
    CampaignID     string  `json:"campaign_id"`
    Clicks         int     `json:"clicks"`
    Impressions    int     `json:"impressions"`
    Cost           float64 `json:"cost"`
    Leads          int     `json:"leads"`
    Opportunities  int     `json:"opportunities"`
    ClosedWon      int     `json:"closed_won"`
    Revenue        float64 `json:"revenue"`
    CPC            float64 `json:"cpc"`
    CPA            float64 `json:"cpa"`
    CVRLeadToOpp   float64 `json:"cvr_lead_to_opp"`
    CVROppToWon    float64 `json:"cvr_opp_to_won"`
    ROAS           float64 `json:"roas"`
    UTMCampaign    string  `json:"utm_campaign"`
    UTMSource      string  `json:"utm_source"`
    UTMMedium      string  `json:"utm_medium"`
}
