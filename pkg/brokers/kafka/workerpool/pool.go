package workerpool

import (
	"context"
	kafkasdk "github.com/FCTL3314/imagination-go-sdk/pkg/brokers/kafka"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"sync"
)

type job struct {
	ctx      context.Context
	logger   *zap.Logger
	metadata *kafkasdk.MessageMetadata
	msg      kafka.Message
}

type PoolHandler struct {
	jobs    chan job
	handler kafkasdk.MessageHandler
	logger  *zap.Logger
	wg      sync.WaitGroup
}

func NewPoolHandler(concurrency int, handler kafkasdk.MessageHandler, logger *zap.Logger) *PoolHandler {
	p := &PoolHandler{
		jobs:    make(chan job, concurrency*2),
		handler: handler,
		logger:  logger,
	}
	p.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go p.worker()
	}
	return p
}

func (p *PoolHandler) worker() {
	defer p.wg.Done()
	for j := range p.jobs {
		if err := p.handler.Handle(j.ctx, j.logger, j.metadata, j.msg); err != nil {
			j.logger.Warn("handle message failed", zap.Error(err))
		}
	}
}

func (p *PoolHandler) Handle(
	ctx context.Context,
	logger *zap.Logger,
	metadata *kafkasdk.MessageMetadata,
	msg kafka.Message,
) error {
	select {
	case p.jobs <- job{ctx: ctx, logger: logger, metadata: metadata, msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PoolHandler) Close() {
	close(p.jobs)
	p.wg.Wait()
}
