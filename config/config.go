package config

// KafkaConfig holds configuration details for Kafka.
type KafkaConfig struct {
	BrokerURL string // Kafka broker URL
}

// DefaultKafkaConfig returns the default Kafka configuration.
func DefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		BrokerURL: "localhost:9092", // Default Kafka broker address
	}
}
