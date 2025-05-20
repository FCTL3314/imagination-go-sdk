package config

type Kafka struct {
	Brokers      []string
	InputTopics  []string
	OutputTopics []string
	GroupID      string
}
