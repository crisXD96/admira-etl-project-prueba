package models

import (
    "encoding/json"
    "fmt"
    "strings"
    "time"
)

type CRMOpportunity struct {
    OpportunityID string    `json:"opportunity_id"`
    ContactEmail  string    `json:"contact_email"`
    Stage         string    `json:"stage"`
    Amount        float64   `json:"amount"`
    CreatedAt     time.Time `json:"created_at"`
    UTMCampaign   string    `json:"utm_campaign"`
    UTMSource     string    `json:"utm_source"`
    UTMMedium     string    `json:"utm_medium"`
    IngestedAt    time.Time `json:"ingested_at"`
}

// UnmarshalJSON maneja el parsing flexible de fechas
func (c *CRMOpportunity) UnmarshalJSON(data []byte) error {
    type Alias CRMOpportunity
    aux := &struct {
        CreatedAtString string `json:"created_at"`
        *Alias
    }{
        Alias: (*Alias)(c),
    }
    
    if err := json.Unmarshal(data, &aux); err != nil {
        return err
    }
    
    // Parsear la fecha si está como string
    if aux.CreatedAtString != "" {
        parsedTime, err := parseDateTime(aux.CreatedAtString)
        if err != nil {
            // Si falla el parsing, usar fecha actual en lugar de fallar
            c.CreatedAt = time.Now()
        } else {
            c.CreatedAt = parsedTime
        }
    }
    
    return nil
}

func parseDateTime(dateStr string) (time.Time, error) {
    // Limpiar y normalizar el string de fecha
    dateStr = strings.TrimSpace(dateStr)
    
    // Reemplazar slashes por guiones para normalizar
    normalized := strings.Replace(dateStr, "/", "-", -1)
    
    // Intentar diferentes formatos de fecha
    formats := []string{
        "2006-01-02T15:04:05Z",
        "2006-01-02 15:04:05",
        "2006-01-02",
        time.RFC3339,
        "2006-01-02T15:04:05-07:00",
        "2006-01-02 15:04:05 -0700",
        "2006-01-02 15:04:05 MST",
        "2006-01-02 15:04:05.000",
    }
    
    for _, format := range formats {
        if t, err := time.Parse(format, normalized); err == nil {
            return t, nil
        }
    }
    
    return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

type CRMResponse struct {
    External struct {
        CRM struct {
            Opportunities []CRMOpportunity `json:"opportunities"`
        } `json:"crm"`
    } `json:"external"`
}
