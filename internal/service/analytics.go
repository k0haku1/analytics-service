package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k0haku1/analytics-service/internal/models"
	"github.com/k0haku1/analytics-service/internal/repository"
)

type AnalyticsService struct {
	clickHouseRepository *repository.ClickHouseRepository
}

func NewAnalyticsService(clickHouseRepository *repository.ClickHouseRepository) *AnalyticsService {
	return &AnalyticsService{
		clickHouseRepository: clickHouseRepository,
	}
}

func (s *AnalyticsService) GetTopics(ctx context.Context, key, value []byte) {
	var event models.OrderEvent
	if err := json.Unmarshal(value, &event); err != nil {
		fmt.Println(err)
		return
	}

	for _, p := range event.Products {
		if err := s.clickHouseRepository.Insert(ctx, event.OrderID, event.CustomerID, p.ID, p.Name, p.Quantity, string(key)); err != nil {
			fmt.Println("Error inserting item:", err)
		}
	}
}
