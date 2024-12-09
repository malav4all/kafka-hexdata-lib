package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer    *kafka.Writer
	brokerURL string
}

func NewKafkaProducer(brokerURL string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokerURL),
			Balancer: &kafka.LeastBytes{},
		},
		brokerURL: brokerURL,
	}
}

func (kp *KafkaProducer) TopicExists(topic string) (bool, error) {
	conn, err := kafka.Dial("tcp", kp.brokerURL)
	if err != nil {
		return false, fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	topics, err := conn.ReadPartitions()
	if err != nil {
		return false, fmt.Errorf("failed to read partitions: %w", err)
	}

	for _, t := range topics {
		if t.Topic == topic {
			return true, nil
		}
	}

	return false, nil
}

func (kp *KafkaProducer) CreateTopic(topic string, numPartitions, replicationFactor int) error {
	controller, err := kp.getKafkaController()
	if err != nil {
		return fmt.Errorf("failed to get Kafka controller: %v", err)
	}

	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	log.Printf("Topic '%s' created successfully", topic)
	return nil
}

func (kp *KafkaProducer) SendMessage(topic string, key string, message interface{}) error {
	kp.writer.Topic = topic

	// Serialize the message to JSON format
	serializedMessage, err := serializeMessage(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err = kp.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(key),
			Value: serializedMessage,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	log.Printf("Message sent successfully to topic '%s': %s\n", topic, string(serializedMessage))
	return nil
}

func (kp *KafkaProducer) getKafkaController() (kafka.Broker, error) {
	conn, err := kafka.Dial("tcp", kp.brokerURL)
	if err != nil {
		return kafka.Broker{}, fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return kafka.Broker{}, fmt.Errorf("failed to get controller: %w", err)
	}

	return controller, nil
}

// serializeMessage converts a message into a byte array. If the message is not a []byte, it serializes it as JSON.
func serializeMessage(message interface{}) ([]byte, error) {
	switch v := message.(type) {
	case []byte:
		// Message is already in byte format
		return v, nil
	case string:
		// Convert string to byte array
		return []byte(v), nil
	default:
		// Serialize as JSON
		return json.Marshal(v)
	}
}

func (kp *KafkaProducer) Close() {
	if err := kp.writer.Close(); err != nil {
		log.Fatalf("Failed to close Kafka writer: %v", err)
	}
}
