// 职责: 断言失败执行落库时 failure_reason / error_code 两列被写入。
// 关联文件:
//   internal/server/biz/request.go
//   internal/ent/schema/request_execution.go
package biz

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/internal/pkg/xcache"
)

// TestRequestExecutionFailureReason persists a failed execution through the metrics
// update path with a populated ExecutionErrorInfo and reads the two columns back.
// The two columns are written only when UpdateRequestExecutionStatusWithMetrics
// receives a non-empty reason and code.
func TestRequestExecutionFailureReason(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:failure-reason?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))

	system := NewSystemService(SystemServiceParams{Ent: client, CacheConfig: xcache.Config{Mode: xcache.ModeMemory}})
	storage := NewDataStorageService(DataStorageServiceParams{Client: client, SystemService: system, CacheConfig: xcache.Config{Mode: xcache.ModeMemory}})
	svc := NewRequestService(client, system.CacheConfig, system, nil, storage, NewLiveStreamRegistry())

	req, err := client.Request.Create().
		SetModelID("model").
		SetRequestBody([]byte(`{}`)).
		SetStatus(request.StatusProcessing).
		Save(ctx)
	require.NoError(t, err)

	execution, err := client.RequestExecution.Create().
		SetRequestID(req.ID).
		SetProjectID(req.ProjectID).
		SetModelID("model").
		SetRequestBody([]byte(`{}`)).
		SetStatus(requestexecution.StatusProcessing).
		Save(ctx)
	require.NoError(t, err)

	statusCode := 502
	err = svc.UpdateRequestExecutionStatusWithMetrics(
		ctx,
		execution,
		requestexecution.StatusFailed,
		"upstream closed the stream",
		&ExecutionErrorInfo{
			StatusCode:    &statusCode,
			FailureReason: "upstream_stream_interrupted",
			ErrorCode:     "upstream_stream_interrupted",
		},
		nil, "", nil,
	)
	require.NoError(t, err)

	saved := client.RequestExecution.GetX(ctx, execution.ID)
	require.Equal(t, requestexecution.StatusFailed, saved.Status)
	require.NotEmpty(t, saved.FailureReason, "failure_reason column must be written for a failed execution")
	require.Equal(t, "upstream_stream_interrupted", saved.FailureReason)
	require.Equal(t, "upstream_stream_interrupted", saved.ErrorCode)
}

// TestRequestExecutionFailureReasonCanceledBranch locks the biz-layer contract
// that a non-empty reason is persisted for a canceled execution regardless of
// which caller supplied it.
func TestRequestExecutionFailureReasonCanceledBranch(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:failure-reason-canceled?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))

	system := NewSystemService(SystemServiceParams{Ent: client, CacheConfig: xcache.Config{Mode: xcache.ModeMemory}})
	storage := NewDataStorageService(DataStorageServiceParams{Client: client, SystemService: system, CacheConfig: xcache.Config{Mode: xcache.ModeMemory}})
	svc := NewRequestService(client, system.CacheConfig, system, nil, storage, NewLiveStreamRegistry())

	req, err := client.Request.Create().
		SetModelID("model").
		SetRequestBody([]byte(`{}`)).
		SetStatus(request.StatusProcessing).
		Save(ctx)
	require.NoError(t, err)

	execution, err := client.RequestExecution.Create().
		SetRequestID(req.ID).
		SetProjectID(req.ProjectID).
		SetModelID("model").
		SetRequestBody([]byte(`{}`)).
		SetStatus(requestexecution.StatusProcessing).
		Save(ctx)
	require.NoError(t, err)

	err = svc.UpdateRequestExecutionStatusWithMetrics(
		ctx,
		execution,
		requestexecution.StatusCanceled,
		"client canceled",
		&ExecutionErrorInfo{FailureReason: "client_cancel"},
		nil, "", nil,
	)
	require.NoError(t, err)

	saved := client.RequestExecution.GetX(ctx, execution.ID)
	require.Equal(t, requestexecution.StatusCanceled, saved.Status)
	require.Equal(t, "client_cancel", saved.FailureReason)
	require.Empty(t, saved.ErrorCode, "a cancellation carries no classified upstream code")
}
