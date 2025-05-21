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

func NewConsumer(reader *kafka.Reader) *Consumer {
	return &Consumer{reader: reader, handlers: make(map[string]HandlerFunc)}
}

func (c *Consumer) RegisterHandler(topic string, handler HandlerFunc) {
	c.handlers[topic] = handler
}

func (c *Consumer) getHandler(topic string) (HandlerFunc, bool) {
	handler, ok := c.handlers[topic]
	return handler, ok
}

func (c *Consumer) Consume(ctx context.Context, workerCount int) {
	jobs := make(chan kafka.Message)

	for i := 0; i < workerCount; i++ {
		go func() {
			for msg := range jobs {
				handlerFunc, ok := c.getHandler(msg.Topic)
				if !ok {
					log.Printf("no handler for topic: %s", msg.Topic)
					continue
				}
				if err := handlerFunc(ctx, msg); err != nil {
					log.Printf("handler error: %v", err)
				} else {
					if err := c.reader.CommitMessages(ctx, msg); err != nil {
						log.Printf("commit error: %v", err)
					}
				}
			}
		}()
	}

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return
		}
		jobs <- msg
	}
}
