package ydbjobs

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.uber.org/zap"
)

type Consumer interface {
	Start() <-chan *topicreader.Message
	Stop()
}

type consumer struct {
	reader *topicreader.Reader
	logger *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}
}

func NewConsumer(
	reader *topicreader.Reader,
	logger *zap.Logger,
) Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &consumer{
		reader: reader,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
		doneCh: make(chan struct{}),
	}
}
func (c *consumer) Start() <-chan *topicreader.Message {
	output := make(chan *topicreader.Message)

	c.logger.Debug("Consumer started")

	go func() {
		defer close(c.doneCh)
		defer close(output)

		var commitMessage *topicreader.Message

		for {
			select {
			case <-c.ctx.Done():
				goto shutdown
			default:
			}

			batch, err := c.reader.ReadMessagesBatch(c.ctx)

			if err != nil {
				if errors.Is(err, context.Canceled) {
					continue
				}

				c.logger.Error("Failed to read messages", zap.Error(err))

				goto shutdown
			}

			for _, message := range batch.Messages {
				select {
				case output <- message:
					commitMessage = message
				case <-c.ctx.Done():
					goto shutdown
				}
			}

			if err := c.reader.Commit(context.Background(), batch); err != nil {
				c.logger.Error("Failed to commit offsets", zap.Error(err))
			}

			commitMessage = nil
		}

	shutdown:
		if commitMessage != nil {
			if err := c.reader.Commit(context.Background(), commitMessage); err != nil {
				c.logger.Error("Failed to commit offsets during shutdown", zap.Error(err))
			}
		}

		err := c.reader.Close(context.Background())

		if err != nil {
			c.logger.Error("Failed to close reader", zap.Error(err))
		}
	}()

	return output
}

func (c *consumer) Stop() {
	c.logger.Debug("Stopping consumer")

	c.cancel()

	<-c.doneCh

	c.logger.Debug("Consumer stopped successfully")
}
