package tests

import (
	"fmt"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v5"
	"github.com/roadrunner-server/jobs/v5"
	_ "github.com/roadrunner-server/kafka/v5"
	"github.com/roadrunner-server/resetter/v5"
	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/roadrunner-server/server/v5"
	"github.com/stretchr/testify/assert"
	ydb "github.com/vragovr/roadrunner-ydb"
	"go.uber.org/zap"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"tests/helpers"
	mocklogger "tests/mock"
	"time"
)

func TestFoo(t *testing.T) {
	address := "127.0.0.1:6001"
	pipeline := "test-ydb"

	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "v2024.2.0",
		Path:    "configs/.rr-init.yaml",
	}

	l, oLogger := mocklogger.ZapTestLogger(zap.DebugLevel)
	err := cont.RegisterAll(
		cfg,
		l,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&ydb.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	for i := range 10 {
		helpers.PushToPipe(t, address, &jobsProto.Job{
			Id:      uuid.NewString(),
			Payload: []byte(fmt.Sprintf("test_%d", i)),
			Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test_1", "test_2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Topic:    "test-topic",
			},
		})
	}

	time.Sleep(1 * time.Second)

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 10)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 10)

	helpers.PausePipelines(t, address, pipeline)

	for i := range 10 {
		helpers.PushToPipe(t, address, &jobsProto.Job{
			Id:      uuid.NewString(),
			Payload: []byte(fmt.Sprintf("test_%d", i)),
			Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test_1", "test_2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Topic:    "test-topic",
			},
		})
	}

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 20)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 10)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("pipeline was paused").Len(), 1)

	time.Sleep(1 * time.Second)

	state := helpers.Stats(t, address)
	assert.False(t, state.Stats[0].Ready)

	time.Sleep(1 * time.Second)

	helpers.ResumePipes(t, address, pipeline)

	time.Sleep(1 * time.Second)

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 20)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 20)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("pipeline was resumed").Len(), 1)

	time.Sleep(1 * time.Second)

	state = helpers.Stats(t, address)
	assert.True(t, state.Stats[0].Ready)

	time.Sleep(1 * time.Second)

	helpers.DestroyPipelines(t, address, pipeline)

	stopCh <- struct{}{}

	wg.Wait()

	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was pushed successfully").Len(), 20)
	assert.GreaterOrEqual(t, oLogger.FilterMessageSnippet("job was processed successfully").Len(), 20)
}
