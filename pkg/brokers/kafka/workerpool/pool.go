package workerpool

import (
	"context"
	kafkasdk "github.com/FCTL3314/imagination-go-sdk/pkg/brokers/kafka"
	"github.com/segmentio/kafka-go"
	"sync"
)

type PoolHandler struct {
	jobs    chan kafka.Message
	handler kafkasdk.MessageHandler
	wg      sync.WaitGroup
}

func NewPoolHandler(concurrency int, handler kafkasdk.MessageHandler) *PoolHandler {
	p := &PoolHandler{
		jobs:    make(chan kafka.Message, concurrency*2),
		handler: handler,
	}
	p.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go p.worker()
	}
	return p
}

func (p *PoolHandler) worker() {
	defer p.wg.Done()
	for msg := range p.jobs {
		if err := p.handler.Handle(context.Background(), msg); err != nil {
			// логика ошибок (DLQ, retry, метрики…)
		}
	}
}

func (p *PoolHandler) Handle(ctx context.Context, msg kafka.Message) error {
	select {
	case p.jobs <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PoolHandler) Close() {
	close(p.jobs)
	p.wg.Wait()
}
