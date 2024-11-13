package producer

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer is the struct representing the producer.
type KafkaProducer struct {
	writer    *kafka.Writer
	brokerURL string
}

// NewKafkaProducer initializes a new KafkaProducer with the given Kafka broker URL.
func NewKafkaProducer(brokerURL string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokerURL),
			Balancer: &kafka.LeastBytes{},
		},
		brokerURL: brokerURL,
	}
}

// CreateTopic creates a Kafka topic if it doesn't already exist.
func (kp *KafkaProducer) CreateTopic(topic string, numPartitions, replicationFactor int) error {
	controller, err := kp.getKafkaController()
	if err != nil {
		return fmt.Errorf("failed to get Kafka controller: %w", err)
	}

	conn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	log.Println("Kafka topic created successfully:", topic)
	return nil
}

// SendMessage sends a message to the specified Kafka topic.
func (kp *KafkaProducer) SendMessage(topic string, message []byte) error {
	// Ensure the topic is set in the writer
	kp.writer.Topic = topic

	// Write the message
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := kp.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
			Value: message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	log.Printf("Message sent successfully to topic '%s': %s\n", topic, string(message))
	return nil
}

// getKafkaController retrieves the Kafka controller node.
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

// Close closes the Kafka writer.
func (kp *KafkaProducer) Close() {
	if err := kp.writer.Close(); err != nil {
		log.Fatalf("Failed to close Kafka writer: %v", err)
	}
}
