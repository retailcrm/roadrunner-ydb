package helpers

import (
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"net/rpc"
	"testing"
)

const (
	push    string = "jobs.Push"
	pause   string = "jobs.Pause"
	destroy string = "jobs.Destroy"
	resume  string = "jobs.Resume"
	stat    string = "jobs.Stat"
)

func PushToPipe(t *testing.T, address string, job *jobsProto.Job) {
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	req := &jobsProto.PushRequest{Job: job}

	er := &jobsProto.Empty{}
	err = client.Call(push, req, er)
	require.NoError(t, err)
}

func ResumePipes(t *testing.T, address string, pipes ...string) {
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

	for i := 0; i < len(pipes); i++ {
		pipe.GetPipelines()[i] = pipes[i]
	}

	er := &jobsProto.Empty{}
	err = client.Call(resume, pipe, er)
	require.NoError(t, err)
}

func PausePipelines(t *testing.T, address string, pipes ...string) {
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

	for i := 0; i < len(pipes); i++ {
		pipe.GetPipelines()[i] = pipes[i]
	}

	er := &jobsProto.Empty{}
	err = client.Call(pause, pipe, er)
	assert.NoError(t, err)
}

func DestroyPipelines(t *testing.T, address string, pipes ...string) {
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

	for i := 0; i < len(pipes); i++ {
		pipe.GetPipelines()[i] = pipes[i]
	}

	er := &jobsProto.Empty{}
	err = client.Call(destroy, pipe, er)

	assert.NoError(t, err)
}

func Stats(t *testing.T, address string) *jobsProto.Stats {
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	st := &jobsProto.Stats{}
	er := &jobsProto.Empty{}

	err = client.Call(stat, er, st)
	require.NoError(t, err)
	require.NotNil(t, st)

	return st
}
