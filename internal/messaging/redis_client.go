// messaging/redis_client.go
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultStreamName    = "tasks-stream"
	DefaultConsumerGroup = "tasks-workers"
)

type MessageClient interface {
	PublishTask(ctx context.Context, taskID string) error
	SubscribeToTasks(ctx context.Context, handler func(taskID string)) error
	HealthCheck() error
	Close() error
}

type redisClient struct {
	client        *redis.Client
	streamName    string
	consumerGroup string
	consumerName  string
}

type TaskMessage struct {
	TaskID    string    `json:"task_id"`
	Timestamp time.Time `json:"timestamp"`
}

func NewRedisClient(url, password string, db int, streamName, consumerGroup string) (MessageClient, error) {
	opts := &redis.Options{
		Addr:         url,
		Password:     password,
		DB:           db,
		PoolSize:     20,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	client := redis.NewClient(opts)

	// Проверяем соединение
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Создаем consumer group если не существует
	if err := createConsumerGroup(ctx, client, streamName, consumerGroup); err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	log.Printf("Redis client initialized. Stream: %s, Group: %s", streamName, consumerGroup)

	return &redisClient{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		consumerName:  fmt.Sprintf("consumer-%d", time.Now().UnixNano()),
	}, nil
}

func createConsumerGroup(ctx context.Context, client *redis.Client, streamName, consumerGroup string) error {
	// Пытаемся создать consumer group
	err := client.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	if err == nil {
		log.Printf("Created consumer group '%s' for stream '%s'", consumerGroup, streamName)
	} else {
		log.Printf("Consumer group '%s' already exists", consumerGroup)
	}

	return nil
}

func (c *redisClient) PublishTask(ctx context.Context, taskID string) error {
	message := TaskMessage{
		TaskID:    taskID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Публикуем в Redis Stream
	result, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: c.streamName,
		Values: map[string]interface{}{
			"task_id": taskID,
			"data":    string(data),
			"created": time.Now().UnixNano(),
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to publish to Redis Stream: %w", err)
	}

	log.Printf("Task %s published to Redis Stream (ID: %s)", taskID, result)
	return nil
}

func (c *redisClient) SubscribeToTasks(ctx context.Context, handler func(taskID string)) error {
	log.Printf("Consumer %s started listening for tasks", c.consumerName)

	// Запускаем обработку сообщений в отдельной горутине
	go c.processMessages(ctx, handler)

	return nil
}

func (c *redisClient) processMessages(ctx context.Context, handler func(taskID string)) {
	// Block время для чтения из потока
	blockTime := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer %s stopped", c.consumerName)
			return
		default:
			// Читаем сообщения из потока
			messages, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.consumerGroup,
				Consumer: c.consumerName,
				Streams:  []string{c.streamName, ">"},
				Count:    1,
				Block:    blockTime,
				NoAck:    false,
			}).Result()

			if err != nil {
				if err == redis.Nil || err == context.Canceled {
					continue
				}
				log.Printf("Error reading from Redis Stream: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Обрабатываем полученные сообщения
			for _, stream := range messages {
				for _, message := range stream.Messages {
					c.processMessage(ctx, message, handler)
				}
			}
		}
	}
}

func (c *redisClient) processMessage(ctx context.Context, message redis.XMessage, handler func(taskID string)) {
	taskID := message.Values["task_id"].(string)

	log.Printf("Consumer %s processing task %s (ID: %s)",
		c.consumerName, taskID, message.ID)

	// Вызываем обработчик
	handler(taskID)

	// Подтверждаем обработку (ACK)
	if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, message.ID).Err(); err != nil {
		log.Printf("Failed to ACK message %s: %v", message.ID, err)
	}
}

func (c *redisClient) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis ping failed: %w", err)
	}

	// Проверяем доступность stream
	_, err := c.client.XInfoStream(ctx, c.streamName).Result()
	if err != nil && !strings.Contains(err.Error(), "no such key") {
		return fmt.Errorf("Redis stream check failed: %w", err)
	}

	return nil
}

func (c *redisClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
