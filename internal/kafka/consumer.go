package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/k0haku1/analytics-service/internal/service"
	"log"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
	service       *service.AnalyticsService
}

func NewConsumer(brokers []string, groupID string, topics []string, service *service.AnalyticsService) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_0_0_0

	cg, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumerGroup: cg,
		topics:        topics,
		service:       service,
	}, nil
}
func (c *Consumer) Start(ctx context.Context) {
	go func() {
		if err := c.consumerGroup.Consume(ctx, c.topics, c); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}()
}
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		c.service.GetTopics(msg.Key, msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}
