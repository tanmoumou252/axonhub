// 职责: ErrorScopeFor 分类器真值表——上游失败 → 重试粒度（Model/Channel/Client）。
// 关联文件:
//   internal/server/orchestrator/error_scope.go
//   internal/server/orchestrator/outbound.go
package orchestrator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
)

func errorScopeChannelCandidate(optIn429 bool) *ChannelModelsCandidate {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 1, Name: "scope"}}
	if optIn429 {
		ch.Channel.Settings = &objects.ChannelSettings{RetryableStatusCodes: []int{http.StatusTooManyRequests}}
	}
	return &ChannelModelsCandidate{Channel: ch}
}

func newScopeTransformer(optIn429 bool) *PersistentOutboundTransformer {
	return &PersistentOutboundTransformer{
		state: &PersistenceState{CurrentCandidate: errorScopeChannelCandidate(optIn429)},
	}
}

func TestErrorScopeFor(t *testing.T) {
	// Client cancellation: a real downstream cancel stops every retry.
	require.Equal(t, errorScopeClient, newScopeTransformer(false).ErrorScopeFor(context.Canceled))

	// The idle-timeout watchdog cancels its context with a sentinel cause that can
	// present as a cancellation; it must NOT be classified as a client cancel.
	idleDisguise := fmtWrap(httpclient.ErrStreamIdleTimeout, context.Canceled)
	require.NotEqual(t, errorScopeClient, newScopeTransformer(false).ErrorScopeFor(idleDisguise))

	// Channel-level fatal HTTP statuses: the account/edge cannot serve any model.
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(&httpclient.Error{StatusCode: http.StatusUnauthorized}))
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(&httpclient.Error{StatusCode: http.StatusForbidden}))
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(&httpclient.Error{StatusCode: http.StatusPaymentRequired}))
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(&httpclient.Error{StatusCode: 520}))

	// 429: not opted in → channel-level (switch); opted in → model-level (busy endpoint).
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(&httpclient.Error{StatusCode: http.StatusTooManyRequests}))
	require.Equal(t, errorScopeModel, newScopeTransformer(true).ErrorScopeFor(&httpclient.Error{StatusCode: http.StatusTooManyRequests}))

	// Channel left service mid-request → channel-level.
	require.Equal(t, errorScopeChannel, newScopeTransformer(false).ErrorScopeFor(errChannelUnavailableForRetry))

	// Model-level: transport EOF, first-token/non-stream timeouts, empty response, nil.
	require.Equal(t, errorScopeModel, newScopeTransformer(false).ErrorScopeFor(io.EOF))
	require.Equal(t, errorScopeModel, newScopeTransformer(false).ErrorScopeFor(pipeline.ErrStreamFirstEventTimeout))
	require.Equal(t, errorScopeModel, newScopeTransformer(false).ErrorScopeFor(pipeline.ErrNonStreamResponseTimeout))
	require.Equal(t, errorScopeModel, newScopeTransformer(false).ErrorScopeFor(pipeline.ErrEmptyResponse))
	require.Equal(t, errorScopeModel, newScopeTransformer(false).ErrorScopeFor(nil))
}

// fmtWrap builds an error whose chain contains both sentinels, mirroring how a
// watchdog-canceled context surfaces ErrStreamIdleTimeout wrapped in a canceled
// error.
func fmtWrap(idle, canceled error) error {
	return errors.Join(canceled, idle)
}
