package config

var KafkaBrokers = []string{"103.20.214.201:9092"} // Specify your Kafka broker addresses here

// SetKafkaBrokers allows updating the broker list dynamically.
func SetKafkaBrokers(brokers []string) {
	KafkaBrokers = brokers
}
