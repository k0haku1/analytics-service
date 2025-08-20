package service

import "log"

type AnalyticsService struct{}

func NewAnalyticsService() *AnalyticsService {
	return &AnalyticsService{}
}

func (s *AnalyticsService) GetTopics(key, value []byte) {
	log.Printf("Received message: key=%s, value=%s\n", string(key), string(value))
}
