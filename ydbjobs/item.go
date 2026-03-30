package ydbjobs

import (
	"encoding/json"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"io"
	"strconv"
)

const (
	defaultJobName      = "deduced_by_rr"
	defaultPipelineName = "deduced_by_rr"
	defaultPriority     = int64(10)
)

type Item struct {
	Job     string `json:"job"`
	Ident   string `json:"id"`
	Payload []byte `json:"payload"`
	headers map[string][]string
	Options *Options `json:"options,omitempty"`
}

type Options struct {
	Priority  int64  `json:"priority"`
	Pipeline  string `json:"pipeline,omitempty"`
	Delay     int64  `json:"delay,omitempty"`
	AutoAck   bool   `json:"auto_ack"`
	Queue     string
	Metadata  string
	Partition int32
	Offset    int64
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

func (i *Item) Headers() map[string][]string {
	return i.headers
}

func (i *Item) Body() []byte {
	return i.Payload
}

func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID        string              `json:"id"`
			Job       string              `json:"job"`
			Driver    string              `json:"driver"`
			Headers   map[string][]string `json:"headers"`
			Pipeline  string              `json:"pipeline"`
			Queue     string              `json:"queue"`
			Topic     string              `json:"topic"`
			Partition int32               `json:"partition"`
			Offset    int64               `json:"offset"`
		}{
			ID:        i.ID(),
			Job:       i.Job,
			Driver:    pluginName,
			Headers:   i.headers,
			Pipeline:  i.Options.Pipeline,
			Queue:     i.Options.Queue,
			Topic:     i.Options.Queue,
			Partition: i.Options.Partition,
			Offset:    i.Options.Offset,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	return nil
}

func (i *Item) Nack() error {
	return nil
}

func (i *Item) NackWithOptions(_ bool, _ int) error {
	return nil
}

func (i *Item) Copy() *Item {
	item := new(Item)
	*item = *i

	*item.Options = Options{
		Priority:  i.Options.Priority,
		Pipeline:  i.Options.Pipeline,
		Delay:     i.Options.Delay,
		AutoAck:   i.Options.AutoAck,
		Queue:     i.Options.Queue,
		Partition: i.Options.Partition,
		Metadata:  i.Options.Metadata,
		Offset:    i.Options.Offset,
	}

	return item
}

func (i *Item) Requeue(headers map[string][]string, _ int) error {
	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromMessage(msg *topicreader.Message, pipeline string) *Item {
	headers := make(map[string][]string, len(msg.Metadata))
	for key, value := range msg.Metadata {
		headers[key] = []string{string(value)}
	}

	if pipeline == "" {
		pipeline = defaultPipelineName
	}

	data, _ := io.ReadAll(msg)

	item := &Item{
		Job:     defaultJobName,
		Ident:   strconv.FormatInt(msg.SeqNo, 10),
		Payload: data,
		headers: headers,

		Options: &Options{
			Priority:  defaultPriority,
			Pipeline:  pipeline,
			Partition: int32(msg.PartitionID()),
			Queue:     msg.Topic(),
			Offset:    msg.Offset,
		},
	}

	return item
}
