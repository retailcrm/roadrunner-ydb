package ydbjobs

import (
	"bytes"
	"context"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"go.uber.org/zap"
	"time"
)

type Producer interface {
	Produce(ctx context.Context, msg jobs.Message) error
	Stop(ctx context.Context) error
}

type producer struct {
	writer *topicwriter.Writer
	logger *zap.Logger
}

func NewProducer(
	writer *topicwriter.Writer,
	logger *zap.Logger,
) Producer {
	return &producer{
		writer: writer,
		logger: logger,
	}
}

func (p producer) Produce(ctx context.Context, msg jobs.Message) error {
	writeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	meta := make(map[string][]byte, len(msg.Headers()))
	for key, v := range msg.Headers() {
		meta[key] = []byte(v[0])
	}

	err := p.writer.Write(writeCtx, topicwriter.Message{
		Data:     bytes.NewReader(msg.Payload()),
		Metadata: meta,
	})

	if err != nil {
		p.logger.Error(err.Error())

		return err
	}

	p.logger.Debug("pushing message")

	return nil
}

func (p producer) Stop(ctx context.Context) error {
	if err := p.writer.Flush(ctx); err != nil {
		p.logger.Error(err.Error())
	}

	return p.writer.Close(ctx)
}
