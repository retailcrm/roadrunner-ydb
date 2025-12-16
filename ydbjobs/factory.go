package ydbjobs

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.uber.org/zap"
	"time"
)

func BuildConsumer(
	client topic.Client,
	logger *zap.Logger,
	topic string,
	consumerName string,
	handler func(*topicreader.Message) error,
) (Consumer, error) {
	reader, err := client.StartReader(
		consumerName,
		topicoptions.ReadSelectors{
			topicoptions.ReadSelector{
				Path: topic,
			},
		},
		topicoptions.WithReaderCommitMode(topicoptions.CommitModeSync),
	)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := reader.WaitInit(ctx); err != nil {
		logger.Error("failed to wait for reader initialization", zap.Error(err))

		return nil, err
	}

	c := NewConsumer(reader, logger)

	logger.Info("consumer ready",
		zap.String("consumer_name", consumerName),
		zap.String("topic", topic),
	)

	go func() {
		for record := range c.Start() {
			err := handler(record)

			if err != nil {
				logger.Error("failed to handle record", zap.Error(err))
			}
		}
	}()

	return c, nil
}

func BuildProducer(
	client topic.Client,
	logger *zap.Logger,
	topic string,
	producerId string,
) (Producer, error) {
	writer, err := client.StartWriter(topic,
		topicoptions.WithWriterCodec(topictypes.CodecGzip),
		topicoptions.WithWriterWaitServerAck(false),
		topicoptions.WithWriterProducerID(producerId),
	)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := writer.WaitInit(ctx); err != nil {
		logger.Error("failed to wait for writer initialization", zap.Error(err))

		return nil, err
	}

	p := NewProducer(writer, logger)

	logger.Info("producer ready",
		zap.String("producer_id", producerId),
		zap.String("topic", topic),
	)

	return p, nil
}
