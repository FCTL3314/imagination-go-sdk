package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func WithCorrelationID(h MessageHandler) HandlerFunc {
	return func(
		ctx context.Context,
		logger *zap.Logger,
		metadata *MessageMetadata,
		msg kafka.Message,
	) error {
		logger = logger.With(zap.String("correlation_id", metadata.CorrelationID))
		return h.Handle(ctx, logger, metadata, msg)
	}
}
