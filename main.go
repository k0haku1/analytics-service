package main

import (
	"context"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/k0haku1/analytics-service/database"
	"github.com/k0haku1/analytics-service/internal/handlers"
	"github.com/k0haku1/analytics-service/internal/kafka"
	"github.com/k0haku1/analytics-service/internal/repository"
	"github.com/k0haku1/analytics-service/internal/service"
	"log"
	"os"
	"os/signal"
)

func main() {
	conn, err := database.NewClickhouseConn()
	if err != nil {
		fmt.Println("ClickHouse connection error:", err)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println("ClickHouse connection close error:", err)
		}
	}()

	repo := repository.NewClickHouseRepository(conn)
	service := service.NewAnalyticsService(repo)
	handler := handlers.NewOrderHandler(service)

	app := fiber.New()
	app.Get("/get-popular", handler.GetPopularProduct)

	brokers := []string{"localhost:9092"}
	groupID := "analytics_group"
	topics := []string{"orders"}

	consumer, err := kafka.NewConsumer(brokers, groupID, topics, service)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, os.Interrupt)
		<-sigterm
		log.Println("Shutting down consumer...")
		cancel()
	}()

	go func() {
		if err := app.Listen(":8082"); err != nil {
			log.Fatal(err)
		}
	}()

	consumer.Start(ctx)

	<-ctx.Done()
	log.Println("Consumer stopped")

	if err := conn.Close(); err != nil {
		log.Println("Error closing ClickHouse connection:", err)
	}

}
