package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k0haku1/analytics-service/internal/models"
	"github.com/k0haku1/analytics-service/internal/repository"
	"github.com/k0haku1/analytics-service/redis"
	"log"
)

type AnalyticsService struct {
	clickHouseRepository *repository.ClickHouseRepository
	redisClient          *redis.RedisClient
}

func NewAnalyticsService(clickHouseRepository *repository.ClickHouseRepository, redisClient *redis.RedisClient) *AnalyticsService {
	return &AnalyticsService{
		clickHouseRepository: clickHouseRepository,
		redisClient:          redisClient,
	}
}

func (s *AnalyticsService) InsertOrders(ctx context.Context, key, value []byte) {
	var event models.OrderEvent
	log.Printf("Inserting order: key=%s, value=%s", string(key), string(value))
	if err := json.Unmarshal(value, &event); err != nil {
		fmt.Println(err)
		return
	}

	redisKey := fmt.Sprintf("order:%s:%s", event.OrderID, event.EventID)

	status, _ := s.redisClient.IsDuplicate(ctx, redisKey)

	if status {
		log.Printf("Duplicate key found: key=%s", redisKey)
		return
	}

	if err := s.redisClient.SetPendingStatus(ctx, redisKey); err != nil {
		log.Printf("Redis error setting pending: %v", err)
		return
	}

	for _, p := range event.Products {
		log.Printf("Sending to ClickHouse: orderID=%s, productID=%s, eventKey=%s", event.OrderID, p.ID, key)
		if err := s.clickHouseRepository.Insert(ctx, event.OrderID, event.CustomerID, p.ID, p.Name, int64(p.Quantity), string(key)); err != nil {
			log.Printf("Error inserting item orderID=%s, productID=%s, eventKey=%s: %v", event.OrderID, p.ID, key, err)
		}
	}

	if err := s.redisClient.SetCompleteStatus(ctx, redisKey); err != nil {
		log.Printf("Redis error setting complete: %v", err)
	}
}

func (s *AnalyticsService) GetMostPopularProduct(ctx context.Context) (*models.Product, error) {
	return s.clickHouseRepository.GetMostPopular(ctx)
}
