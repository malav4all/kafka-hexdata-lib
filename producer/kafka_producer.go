package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

type KafkaProducer struct {
	writer  *kafka.Writer
	brokers []string
}

// NewKafkaProducer creates a new Kafka producer
// Accepts either:
// - Single broker: "192.168.77.202:9092"
// - Multiple brokers (comma-separated): "192.168.77.202:9092,192.168.77.203:9092,192.168.77.204:9092"
// - Slice of brokers: []string{"192.168.77.202:9092", "192.168.77.203:9092"}
func NewKafkaProducer(brokers interface{}) *KafkaProducer {
	var brokerList []string

	switch v := brokers.(type) {
	case string:
		// Split comma-separated string into slice
		brokerList = strings.Split(v, ",")
		// Trim whitespace from each broker
		for i, broker := range brokerList {
			brokerList[i] = strings.TrimSpace(broker)
		}
	case []string:
		brokerList = v
	default:
		log.Fatalf("Invalid broker type. Expected string or []string")
	}

	if len(brokerList) == 0 {
		log.Fatalf("No brokers provided")
	}

	log.Printf("Initializing Kafka producer with brokers: %v", brokerList)

	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokerList...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  3,
			Compression:  compress.Snappy,
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			ReadTimeout:  10 * time.Second,
		},
		brokers: brokerList,
	}
}

// NewKafkaProducerWithConfig creates a Kafka producer with custom Writer configuration
func NewKafkaProducerWithConfig(brokers interface{}, customWriter *kafka.Writer) *KafkaProducer {
	var brokerList []string

	switch v := brokers.(type) {
	case string:
		brokerList = strings.Split(v, ",")
		for i, broker := range brokerList {
			brokerList[i] = strings.TrimSpace(broker)
		}
	case []string:
		brokerList = v
	default:
		log.Fatalf("Invalid broker type. Expected string or []string")
	}

	if customWriter == nil {
		customWriter = &kafka.Writer{
			Addr:         kafka.TCP(brokerList...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  3,
			Compression:  compress.Snappy,
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			ReadTimeout:  10 * time.Second,
		}
	} else {
		// Override the Addr with the provided brokers
		customWriter.Addr = kafka.TCP(brokerList...)
	}

	return &KafkaProducer{
		writer:  customWriter,
		brokers: brokerList,
	}
}

// NewKafkaProducerAdvanced creates a producer with detailed configuration options
func NewKafkaProducerAdvanced(brokers interface{}, opts ProducerOptions) *KafkaProducer {
	var brokerList []string

	switch v := brokers.(type) {
	case string:
		brokerList = strings.Split(v, ",")
		for i, broker := range brokerList {
			brokerList[i] = strings.TrimSpace(broker)
		}
	case []string:
		brokerList = v
	default:
		log.Fatalf("Invalid broker type. Expected string or []string")
	}

	// Apply default options if not set
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.BatchTimeout == 0 {
		opts.BatchTimeout = 10 * time.Millisecond
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 10 * time.Second
	}
	if opts.Balancer == nil {
		opts.Balancer = &kafka.LeastBytes{}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Balancer:     opts.Balancer,
		RequiredAcks: opts.RequiredAcks,
		MaxAttempts:  opts.MaxAttempts,
		// Compression:  opts.Compression,
		BatchSize:    opts.BatchSize,
		BatchTimeout: opts.BatchTimeout,
		WriteTimeout: opts.WriteTimeout,
		ReadTimeout:  opts.ReadTimeout,
		Async:        opts.Async,
	}

	return &KafkaProducer{
		writer:  writer,
		brokers: brokerList,
	}
}

// ProducerOptions holds configuration options for Kafka producer
type ProducerOptions struct {
	RequiredAcks kafka.RequiredAcks
	MaxAttempts  int
	Compression  compress.Codec
	BatchSize    int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
	Balancer     kafka.Balancer
	Async        bool
}

// DefaultProducerOptions returns default producer options
func DefaultProducerOptions() ProducerOptions {
	return ProducerOptions{
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
		// Compression:  compress.Snappy,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
	}
}

// TopicExists checks if a topic exists in the Kafka cluster
func (kp *KafkaProducer) TopicExists(topic string) (bool, error) {
	var conn *kafka.Conn
	var err error

	for _, broker := range kp.brokers {
		conn, err = kafka.Dial("tcp", broker)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to broker %s: %v", broker, err)
	}

	if conn == nil {
		return false, fmt.Errorf("failed to connect to any broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return false, fmt.Errorf("failed to read partitions: %w", err)
	}

	for _, partition := range partitions {
		if partition.Topic == topic {
			return true, nil
		}
	}

	return false, nil
}

// CreateTopic creates a new topic in the Kafka cluster
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

// SendMessage sends a message to the specified Kafka topic
func (kp *KafkaProducer) SendMessage(topic string, key string, message interface{}) error {
	kp.writer.Topic = topic

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
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	log.Printf("Message sent successfully to topic '%s' with key '%s'", topic, key)
	return nil
}

// SendMessages sends multiple messages to the specified Kafka topic
func (kp *KafkaProducer) SendMessages(topic string, messages []kafka.Message) error {
	kp.writer.Topic = topic

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := kp.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	log.Printf("Sent %d messages successfully to topic '%s'", len(messages), topic)
	return nil
}

// getKafkaController gets the Kafka controller broker
func (kp *KafkaProducer) getKafkaController() (kafka.Broker, error) {
	var conn *kafka.Conn
	var err error

	for _, broker := range kp.brokers {
		conn, err = kafka.Dial("tcp", broker)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to broker %s: %v", broker, err)
	}

	if conn == nil {
		return kafka.Broker{}, fmt.Errorf("failed to connect to any broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return kafka.Broker{}, fmt.Errorf("failed to get controller: %w", err)
	}

	return controller, nil
}

// GetBrokers returns the list of brokers
func (kp *KafkaProducer) GetBrokers() []string {
	return kp.brokers
}

// serializeMessage converts a message into a byte array
func serializeMessage(message interface{}) ([]byte, error) {
	switch v := message.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// Close closes the Kafka writer
func (kp *KafkaProducer) Close() error {
	if kp.writer != nil {
		if err := kp.writer.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
			return err
		}
		log.Println("Kafka writer closed successfully")
	}
	return nil
}
