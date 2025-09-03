package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k0haku1/analytics-service/internal/models"
	"github.com/k0haku1/analytics-service/internal/repository"
	"log"
)

type AnalyticsService struct {
	clickHouseRepository *repository.ClickHouseRepository
}

func NewAnalyticsService(clickHouseRepository *repository.ClickHouseRepository) *AnalyticsService {
	return &AnalyticsService{
		clickHouseRepository: clickHouseRepository,
	}
}

func (s *AnalyticsService) InsertOrders(ctx context.Context, key, value []byte) {
	var event models.OrderEvent
	log.Printf("Inserting order: key=%s, value=%s", string(key), string(value))
	if err := json.Unmarshal(value, &event); err != nil {
		fmt.Println(err)
		return
	}

	for _, p := range event.Products {
		log.Printf("Sending to ClickHouse: orderID=%s, productID=%s, eventKey=%s", event.OrderID, p.ID, key)
		if err := s.clickHouseRepository.Insert(ctx, event.OrderID, event.CustomerID, p.ID, p.Name, int64(p.Quantity), string(key)); err != nil {
			log.Printf("Error inserting item orderID=%s, productID=%s, eventKey=%s: %v", event.OrderID, p.ID, key, err)
		}
	}
}

func (s *AnalyticsService) GetMostPopularProduct(ctx context.Context) (*models.Product, error) {
	return s.clickHouseRepository.GetMostPopular(ctx)
}
