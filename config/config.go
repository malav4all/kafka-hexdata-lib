package config

import "strings"

// KafkaConfig holds configuration details for Kafka.
type KafkaConfig struct {
	Brokers []string // Kafka broker addresses (supports multiple brokers)
}

// NewKafkaConfig creates a new KafkaConfig from a comma-separated string or slice
func NewKafkaConfig(brokers interface{}) *KafkaConfig {
	switch v := brokers.(type) {
	case string:
		// Split comma-separated string into slice
		brokerList := strings.Split(v, ",")
		// Trim whitespace from each broker
		for i, broker := range brokerList {
			brokerList[i] = strings.TrimSpace(broker)
		}
		return &KafkaConfig{Brokers: brokerList}
	case []string:
		return &KafkaConfig{Brokers: v}
	default:
		return DefaultKafkaConfig()
	}
}

// DefaultKafkaConfig returns the default Kafka configuration.
func DefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Brokers: []string{"localhost:9092"}, // Default Kafka broker address
	}
}

// GetBrokersString returns brokers as a comma-separated string
func (c *KafkaConfig) GetBrokersString() string {
	return strings.Join(c.Brokers, ",")
}
