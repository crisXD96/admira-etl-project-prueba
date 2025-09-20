package api

import (
    "fmt"
    "math/rand"
    "time"

    "github.com/gin-gonic/gin"
)

func (s *Server) requestIDMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        requestID := generateRequestID()
        c.Set("requestID", requestID)
        c.Header("X-Request-ID", requestID)
        c.Next()
    }
}

func (s *Server) loggingMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next()
        duration := time.Since(start)
        fmt.Printf("Request: %s %s - Status: %d - Duration: %v\n", 
            c.Request.Method, c.Request.URL.Path, c.Writer.Status(), duration)
    }
}

func generateRequestID() string {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    b := make([]byte, 16)
    for i := range b {
        b[i] = charset[rand.Intn(len(charset))]
    }
    return string(b)
}
