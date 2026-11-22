// 职责: 验证首字超时(ErrStreamFirstEventTimeout)是同渠道兄弟模型级失败, 必须先逐个尝试
//       同渠道其余模型, 再跨渠道切换; 杜绝单模型首字超时连坐整条渠道。
// 关联文件:
//   llm/pipeline/pipeline.go
//   internal/server/orchestrator/outbound.go
// 注意事项:
//   超时由 firstEventTimeoutGuard 取消 streamCtx 触发, 测试通过捕获该 ctx 确定性阻塞,
//   不依赖真实计时竞态。
package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

// blockingLLMStream blocks Next() until its context is cancelled, then reports
// exhaustion so the first-event timeout guard surfaces ErrStreamFirstEventTimeout.
type blockingLLMStream struct {
	ctx    context.Context
	closed bool
}

func (s *blockingLLMStream) Next() bool {
	<-s.ctx.Done()

	return false
}

func (s *blockingLLMStream) Current() *llm.Response { return nil }
func (s *blockingLLMStream) Err() error             { return nil }
func (s *blockingLLMStream) Close() error           { s.closed = true; return nil }

// retryingOutbound is a mock that also reports whether it still holds untried
// sibling models, so the pipeline can decide to iterate models before switching
// channels on a first-token timeout.
type retryingOutbound struct {
	mockOutbound
	moreModels bool
}

func (m *retryingOutbound) HasMoreModels() bool { return m.moreModels }

func TestProcess_FirstTokenTimeout_IteratesSiblingModelsBeforeSwitchingChannel(t *testing.T) {
	streamFlag := true
	inbound := &mockInbound{
		transformRequest: func(context.Context, *httpclient.Request) (*llm.Request, error) {
			return &llm.Request{Stream: &streamFlag}, nil
		},
		transformStream: transformLlmContentToEvents,
	}

	var capturedCtx context.Context
	attempts := 0
	executor := &mockExecutor{
		doStream: func(ctx context.Context, _ *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
			attempts++
			capturedCtx = ctx

			return streams.SliceStream([]*httpclient.StreamEvent{{Data: []byte("raw")}}), nil
		},
	}

	prepareCalls := 0
	nextChannelCalls := 0
	out := &retryingOutbound{
		moreModels: true,
	}
	out.canRetry = func(error) bool { return true }
	out.prepareForRetry = func(context.Context) error {
		prepareCalls++

		return nil
	}
	out.hasMoreChannels = func() bool { return true }
	out.nextChannel = func(context.Context) error {
		nextChannelCalls++

		return nil
	}
	out.transformStream = func(context.Context, *httpclient.Request, streams.Stream[*httpclient.StreamEvent]) (streams.Stream[*llm.Response], error) {
		// Attempt 1: block until the first-event guard times out.
		if attempts == 1 {
			return &blockingLLMStream{ctx: capturedCtx}, nil
		}
		// Later attempts (after sibling-model advance): return real content.
		return streams.SliceStream([]*llm.Response{
			llmContentChunk("ok"),
			llm.DoneResponse,
		}), nil
	}

	p := NewFactory(executor).Pipeline(inbound, out,
		WithRetry(2, 2, 0),
		WithResponseTimeouts(30*time.Millisecond, 0),
	)

	res, err := p.Process(context.Background(), &httpclient.Request{})
	require.NoError(t, err)
	require.NotNil(t, res)

	// The bug: pre-fix, `if !timeoutRetry` skips same-channel retry, so on a first
	// token timeout the pipeline jumps straight to NextChannel and abandons the
	// channel's sibling models. prepareCalls would be 0 and nextChannelCalls >= 1.
	require.Equal(t, 1, prepareCalls, "first-token timeout must advance to the sibling model via PrepareForRetry")
	require.Equal(t, 0, nextChannelCalls, "must not switch channel while untried sibling models remain on the current channel")

	events, err := streams.All(res.EventStream)
	require.NoError(t, err)
	require.Equal(t, []string{"ok", "[DONE]"}, dataPayloads(events))
}

func TestProcess_FirstTokenTimeout_SingleModelChannelStillSwitches(t *testing.T) {
	streamFlag := true
	inbound := &mockInbound{
		transformRequest: func(context.Context, *httpclient.Request) (*llm.Request, error) {
			return &llm.Request{Stream: &streamFlag}, nil
		},
		transformStream: transformLlmContentToEvents,
	}

	var capturedCtx context.Context
	attempts := 0
	executor := &mockExecutor{
		doStream: func(ctx context.Context, _ *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
			attempts++
			capturedCtx = ctx

			return streams.SliceStream([]*httpclient.StreamEvent{{Data: []byte("raw")}}), nil
		},
	}

	prepareCalls := 0
	nextChannelCalls := 0
	// moreModels=false: single-model channel, so a first-token timeout is genuinely
	// channel-scoped and must fall through to the cross-channel switch.
	out := &retryingOutbound{
		moreModels: false,
	}
	out.canRetry = func(error) bool { return true }
	out.prepareForRetry = func(context.Context) error {
		prepareCalls++

		return nil
	}
	out.hasMoreChannels = func() bool { return attempts == 1 }
	out.nextChannel = func(context.Context) error {
		nextChannelCalls++

		return nil
	}
	out.transformStream = func(context.Context, *httpclient.Request, streams.Stream[*httpclient.StreamEvent]) (streams.Stream[*llm.Response], error) {
		if attempts == 1 {
			return &blockingLLMStream{ctx: capturedCtx}, nil
		}

		return streams.SliceStream([]*llm.Response{
			llmContentChunk("ok"),
			llm.DoneResponse,
		}), nil
	}

	p := NewFactory(executor).Pipeline(inbound, out,
		WithRetry(2, 2, 0),
		WithResponseTimeouts(30*time.Millisecond, 0),
	)

	res, err := p.Process(context.Background(), &httpclient.Request{})
	require.NoError(t, err)
	require.NotNil(t, res)

	// No sibling model to try, so the timeout switches channel without iterating
	// models on the exhausted single-model candidate.
	require.Equal(t, 0, prepareCalls, "single-model channel must not pretend to iterate models")
	require.Equal(t, 1, nextChannelCalls, "single-model channel timeout must switch channels")
}

func dataPayloads(events []*httpclient.StreamEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, string(e.Data))
	}

	return out
}
