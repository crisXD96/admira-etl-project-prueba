package main

import (
	"log"
	"os"

	"admira-etl/internal/api"
	"admira-etl/pkg/config"
)

func main() {
	// Cargar configuración
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Error loading config:", err)
	}

	// Inicializar servidor
	server := api.NewServer(cfg)
	
	// Iniciar servidor
	if err := server.Start(); err != nil {
		log.Fatal("Error starting server:", err)
		os.Exit(1)
	}
}