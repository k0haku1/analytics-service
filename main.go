package main

import (
	"context"
	"fmt"
	"github.com/k0haku1/analytics-service/internal/database"
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

	consumer.Start(ctx)

	<-ctx.Done()
	log.Println("Consumer stopped")

	if err := conn.Close(); err != nil {
		log.Println("Error closing ClickHouse connection:", err)
	}
}
