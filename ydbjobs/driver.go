package ydbjobs

import (
	"context"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.uber.org/zap"
	"sync/atomic"
)

const (
	pluginName string = "ydb"
)

type Driver struct {
	Cfg      Config
	Driver   *ydb.Driver
	Client   topic.Client
	Queue    jobs.Queue
	Pipeline atomic.Pointer[jobs.Pipeline]
	Logger   *zap.Logger
	consumer Consumer
	producer Producer
	ready    uint32
}

func (d *Driver) Push(ctx context.Context, msg jobs.Message) error {
	return d.producer.Produce(ctx, msg)
}

func (d *Driver) Run(ctx context.Context, pipeline jobs.Pipeline) error {
	d.Logger.Debug("starting driver")
	defer d.Logger.Debug("driver started")

	pipe := *d.Pipeline.Load()
	if pipe.Name() != pipeline.Name() {
		return errors.Errorf("no such pipeline registered: %s", pipe.Name())
	}

	defer atomic.StoreUint32(&d.ready, 1)

	var err error

	d.consumer, err = BuildConsumer(
		d.Client,
		d.Logger,
		d.Cfg.Topic,
		d.Cfg.ConsumerOpts.Name,
		func(record *topicreader.Message) error {
			d.Queue.Insert(fromMessage(record))

			return nil
		},
	)
	if err != nil {
		return err
	}

	d.producer, err = BuildProducer(
		d.Client,
		d.Logger,
		d.Cfg.Topic,
		d.Cfg.ProducerOpts.Id,
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	d.Logger.Debug("stopping driver")

	defer atomic.StoreUint32(&d.ready, 0)

	err := d.producer.Stop(ctx)
	if err != nil {
		return err
	}

	d.consumer.Stop()

	err = d.Driver.Close(ctx)
	if err != nil {
		return err
	}

	d.Logger.Debug("driver stopped")

	return nil
}

func (d *Driver) Pause(ctx context.Context, pipeline string) error {
	pipe := *d.Pipeline.Load()

	if pipe.Name() != pipeline {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if atomic.LoadUint32(&d.ready) == 0 {
		return errors.Errorf("driver is not running")
	}

	d.consumer.Stop()
	d.consumer = nil

	atomic.StoreUint32(&d.ready, 0)
	d.Logger.Debug("pipeline was paused")

	return nil
}

func (d *Driver) Resume(ctx context.Context, pipeline string) error {
	pipe := *d.Pipeline.Load()

	if pipe.Name() != pipeline {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if atomic.LoadUint32(&d.ready) == 1 {
		return errors.Errorf("driver is already running")
	}

	var err error

	d.consumer, err = BuildConsumer(
		d.Client,
		d.Logger,
		d.Cfg.Topic,
		d.Cfg.ConsumerOpts.Name,
		func(record *topicreader.Message) error {
			d.Queue.Insert(fromMessage(record))

			return nil
		},
	)

	if err != nil {
		return err
	}

	atomic.StoreUint32(&d.ready, 1)
	d.Logger.Debug("pipeline was resumed")

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	pipe := *d.Pipeline.Load()

	return &jobs.State{
		Priority: uint64(pipe.Priority()),
		Pipeline: pipe.Name(),
		Driver:   pipe.Driver(),
		Queue:    d.Cfg.Topic,
		Ready:    atomic.LoadUint32(&d.ready) > 0,
	}, nil
}
