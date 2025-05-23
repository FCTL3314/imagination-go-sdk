package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type MessageMetadata struct {
	CorrelationID string
}

type MessageHandler interface {
	Handle(
		ctx context.Context,
		logger *zap.Logger,
		metadata *MessageMetadata,
		msg kafka.Message,
	) error
}

type HandlerFunc func(
	ctx context.Context,
	logger *zap.Logger,
	metadata *MessageMetadata,
	msg kafka.Message,
) error

func (f HandlerFunc) Handle(
	ctx context.Context,
	logger *zap.Logger,
	metadata *MessageMetadata,
	msg kafka.Message,
) error {
	return f(ctx, logger, metadata, msg)
}

type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	logger  *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	CommitInterval time.Duration
	Logger         *zap.Logger
}

func NewConsumer(cfg ConsumerConfig, handler MessageHandler) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
	})

	return &Consumer{
		reader:  reader,
		handler: WithCorrelationID(handler),
		logger:  cfg.Logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *Consumer) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		backoff := time.Second

		for {
			m, err := c.reader.FetchMessage(c.ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					c.logger.Info("consumer context canceled, exiting")
					return
				}
				c.logger.Error("fetch message error", zap.Error(err))
				c.logger.Info("sleeping before retry", zap.Duration("backoff", backoff))
				time.Sleep(backoff)
				backoff = backoff * 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				continue
			}

			backoff = time.Second

			correlationID := c.getCorrelationID(&m)
			c.logger.Info("received kafka message",
				zap.String("correlation_id", correlationID),
				zap.String("topic", m.Topic),
				zap.Int("partition", m.Partition),
				zap.Int64("offset", m.Offset),
				zap.Time("time", m.Time),
			)
			c.logger.Debug("message payload", zap.String("value", string(m.Value)))

			if err := c.handler.Handle(
				c.ctx,
				c.logger,
				&MessageMetadata{CorrelationID: correlationID},
				m,
			); err != nil {
				c.logger.Error(
					"message handling failed", zap.Error(err),
					zap.String("topic", m.Topic), zap.Int64("offset", m.Offset),
				)
			} else {
				if err := c.reader.CommitMessages(c.ctx, m); err != nil {
					c.logger.Warn("commit message failed", zap.Error(err))
				}
			}
		}
	}()
}

func (c *Consumer) Close() error {
	c.cancel()

	c.wg.Wait()

	if err := c.reader.Close(); err != nil {
		c.logger.Error("failed to close kafka reader", zap.Error(err))
		return err
	}
	c.logger.Info("consumer closed")
	return nil
}

func (c *Consumer) getCorrelationID(m *kafka.Message) string {
	for _, h := range m.Headers {
		if string(h.Key) == "correlation_id" {
			return string(h.Value)
		}
	}
	return ""
}
