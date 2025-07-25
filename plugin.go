package ydb

import (
	"context"
	"encoding/json"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/vragovr/roadrunner-ydb/ydbjobs"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"go.uber.org/zap"
	"time"
)

const (
	pluginName string = "ydb"
)

const (
	priorityKey        string = "priority"
	topicKey           string = "topic"
	producerOptionsKey string = "producer_options"
	consumerOptionsKey string = "consumer_options"
)

type Plugin struct {
	logger *zap.Logger
	cfg    Configurer
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) {
		return errors.E(errors.Disabled)
	}

	p.logger = log.NamedLogger(pluginName)
	p.cfg = cfg

	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) DriverFromConfig(configKey string, queue jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	p.logger.Debug("start driver from config")

	if !p.cfg.Has(pluginName) {
		return nil, errors.E("no global configuration found")
	}

	if !p.cfg.Has(configKey) {
		return nil, errors.Errorf("no configuration by provided key: %s", configKey)
	}

	var cfg ydbjobs.Config
	err := p.cfg.UnmarshalKey(pluginName, &cfg)
	if err != nil {
		return nil, err
	}

	err = p.cfg.UnmarshalKey(configKey, &cfg)
	if err != nil {
		return nil, err
	}

	return open(cfg, queue, pipeline, p.logger)
}

func (p *Plugin) DriverFromPipeline(pipeline jobs.Pipeline, queue jobs.Queue) (jobs.Driver, error) {
	p.logger.Debug("start driver from config")

	if !p.cfg.Has(pluginName) {
		return nil, errors.E("no global configuration found")
	}

	var cfg ydbjobs.Config
	err := p.cfg.UnmarshalKey(pluginName, &cfg)
	if err != nil {
		return nil, err
	}

	cfg.Priority = pipeline.Int(priorityKey, 10)
	cfg.Topic = pipeline.String(topicKey, "")
	if cfg.Topic == "" {
		return nil, errors.E("no topic specified")
	}

	var pOpt *ydbjobs.ProducerOpts
	producerOpts := pipeline.String(producerOptionsKey, "")
	if producerOpts != "" {
		err = json.Unmarshal([]byte(producerOpts), &pOpt)
		if err != nil {
			return nil, err
		}

		cfg.ProducerOpts = pOpt
	}

	var cOpt *ydbjobs.ConsumerOpts
	consumerOpts := pipeline.String(consumerOptionsKey, "")
	if consumerOpts != "" {
		err = json.Unmarshal([]byte(consumerOpts), &cOpt)
		if err != nil {
			return nil, err
		}

		cfg.ConsumerOpts = cOpt
	}

	return open(cfg, queue, pipeline, p.logger)
}

func open(cfg ydbjobs.Config, queue jobs.Queue, pipeline jobs.Pipeline, logger *zap.Logger) (jobs.Driver, error) {
	options := []ydb.Option{
		ydb.With(config.WithNoAutoRetry()),
		ydb.WithDialTimeout(time.Second * 5),
		ydb.WithConnectionTTL(time.Second * 30),
	}

	if cfg.StaticCredentials != nil {
		options = append(
			options,
			ydb.WithStaticCredentials(cfg.StaticCredentials.User, cfg.StaticCredentials.Password),
		)
	}

	if cfg.TLS != nil && cfg.TLS.Ca != "" {
		options = append(options, ydb.WithCertificatesFromFile(cfg.TLS.Ca))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	driver, err := ydb.Open(ctx, cfg.Endpoint, options...)
	if err != nil {
		return nil, err
	}

	d := &ydbjobs.Driver{
		Cfg:    cfg,
		Driver: driver,
		Client: driver.Topic(),
		Queue:  queue,
		Logger: logger,
	}

	d.Pipeline.Store(&pipeline)

	return d, nil
}
