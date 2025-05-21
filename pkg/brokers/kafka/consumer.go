package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

type HandlerFunc func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader   *kafka.Reader
	handlers map[string]HandlerFunc
}

func NewRouter(reader *kafka.Reader) *Consumer {
	return &Consumer{reader: reader, handlers: make(map[string]HandlerFunc)}
}

func (r *Consumer) RegisterHandler(topic string, handler HandlerFunc) {
	r.handlers[topic] = handler
}

func (r *Consumer) getHandler(topic string) (HandlerFunc, bool) {
	handler, ok := r.handlers[topic]
	return handler, ok
}

func (r *Consumer) Consume(ctx context.Context) {
	for {
		m, err := r.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[kafka] consumer shutdown")
				return
			}
			log.Printf("[kafka] read error: %v", err)
			continue
		}

		handler, ok := r.getHandler(m.Topic)
		if !ok {
			log.Printf("[kafka] no handler for topic %s, skipping offset=%d", m.Topic, m.Offset)
			continue
		}

		go func(msg kafka.Message) {
			if err := handler(ctx, msg); err != nil {
				log.Printf("[kafka] handler error for topic %s: %v", msg.Topic, err)
			}
		}(m)
	}
}
