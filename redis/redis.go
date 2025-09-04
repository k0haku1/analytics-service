package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"os"
	"strconv"
	"time"
)

type RedisClient struct {
	client *redis.Client
}

func init() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("Warning: .env file not loaded")
	}
}

func NewRedisClient() *RedisClient {
	addr := fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT"))
	db, _ := strconv.Atoi(os.Getenv("REDIS_DB"))

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASS"),
		DB:       db,
	})

	return &RedisClient{
		client: rdb,
	}
}

func (r *RedisClient) IsDuplicate(ctx context.Context, key string) (bool, error) {
	status, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return status == "complete", nil
}

func (r *RedisClient) SetPendingStatus(ctx context.Context, key string) error {
	return r.client.Set(ctx, key, "pending", 10*time.Minute).Err()
}

func (r *RedisClient) SetCompleteStatus(ctx context.Context, key string) error {
	return r.client.Set(ctx, key, "complete", 0).Err()
}
