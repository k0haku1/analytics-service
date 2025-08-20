package main

import (
	"context"
	"github.com/k0haku1/analytics-service/internal/kafka"
	"github.com/k0haku1/analytics-service/internal/service"
	"log"
	"os"
	"os/signal"
)

func main() {
	service := service.NewAnalyticsService()
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

}
