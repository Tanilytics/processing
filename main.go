package main

import (
	"fmt"
	"os"

	"github.com/gofiber/fiber/v3"
)

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	app := fiber.New(fiber.Config{
		Immutable: true,
	})

	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	port := getEnv("PORT", "3000")
	fmt.Printf("Server starting on port %s...\n", port)
	app.Listen(":" + port)
}
