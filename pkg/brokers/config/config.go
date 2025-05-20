package config

type Kafka struct {
	Brokers     []string `envconfig:"KAFKA_BROKERS" required:"true"`
	TopicInput  string   `envconfig:"KAFKA_TOPIC_INPUT" required:"true"`
	TopicOutput string   `envconfig:"KAFKA_TOPIC_OUTPUT" required:"true"`
	GroupID     string   `envconfig:"KAFKA_GROUP_ID" required:"true"`
}
