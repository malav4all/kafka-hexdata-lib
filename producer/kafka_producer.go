package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka" // Alias for confluent-kafka-go
	"github.com/malav4all/kafka-hexdata-lib/config"           // Adjust the import path as needed
	"github.com/segmentio/kafka-go"                           // No alias needed for kafka-go
)

type KafkaClient struct {
	writer *kafka.Writer
}

// NewKafkaClient initializes a new KafkaClient with the provided topic.
// If the topic does not exist, it will create it.
func NewKafkaClient(topic string) *KafkaClient {
	// Ensure the topic exists or create it dynamically
	err := ensureTopicExists(topic)
	if err != nil {
		log.Fatalf("Error ensuring topic exists: %v", err)
	}

	// Create a Kafka writer
	return &KafkaClient{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  config.KafkaBrokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),
	}
}

// ensureTopicExists checks if the topic exists and creates it if it doesn't.
func ensureTopicExists(topic string) error {
	// Create an admin client
	adminClient, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": config.KafkaBrokers[0]})
	if err != nil {
		return fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer adminClient.Close()

	// Check if the context has a deadline
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context does not have a deadline")
	}

	// Get metadata with the deadline
	metadata, err := adminClient.GetMetadata(nil, false, int(deadline.Unix()))
	if err != nil {
		return fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	if _, exists := metadata.Topics[topic]; exists {
		log.Printf("Topic '%s' already exists.\n", topic)
		return nil
	}

	// Create the topic if it doesn't exist
	topicSpec := ckafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1, // Adjust as needed
		ReplicationFactor: 1, // Adjust as needed
	}

	results, err := adminClient.CreateTopics(ctx, []ckafka.TopicSpecification{topicSpec})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != ckafka.ErrNoError {
			return fmt.Errorf("failed to create topic '%s': %v", result.Topic, result.Error)
		}
	}

	log.Printf("Topic '%s' created successfully.\n", topic)
	return nil
}

// Close closes the Kafka writer, releasing any resources.
func (kc *KafkaClient) Close() {
	if err := kc.writer.Close(); err != nil {
		log.Fatalf("Error closing Kafka writer: %v", err)
	}
}

// SendMessage sends a JSON-encoded payload to Kafka.
func (kc *KafkaClient) SendMessage(payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload to JSON: %w", err)
	}

	// Send the message to Kafka
	err = kc.writer.WriteMessages(context.Background(), kafka.Message{
		Value: body,
	})
	if err != nil {
		return fmt.Errorf("error sending message to Kafka: %w", err)
	}
	return nil
}
