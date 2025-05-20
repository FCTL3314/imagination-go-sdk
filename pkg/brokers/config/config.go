package config

type Kafka struct {
	Brokers     []string
	TopicInput  string
	TopicOutput string
	GroupID     string
}
