package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer"
)

// responseTimeoutOutbound implements transformer.Outbound plus
// ResponseTimeoutProvider so the pipeline can resolve per-attempt timeouts.
type responseTimeoutOutbound struct {
	*mockOutbound

	streamOverride    *time.Duration
	nonStreamOverride *time.Duration
}

func (o *responseTimeoutOutbound) ResponseTimeouts(context.Context) (*time.Duration, *time.Duration) {
	return o.streamOverride, o.nonStreamOverride
}

func newResponseTimeoutOutbound() *responseTimeoutOutbound {
	return &responseTimeoutOutbound{mockOutbound: &mockOutbound{apiFormat: llm.APIFormatOpenAIChatCompletion}}
}

func TestEffectiveResponseTimeouts(t *testing.T) {
	tests := []struct {
		name              string
		outbound          transformer.Outbound
		streamOverride    *time.Duration
		nonStreamOverride *time.Duration
		wantStream        time.Duration
		wantNonStream     time.Duration
	}{
		{
			name:          "no provider keeps pipeline defaults",
			outbound:      &testOutbound{},
			wantStream:    30 * time.Second,
			wantNonStream: 20 * time.Second,
		},
		{
			name:          "provider with no override keeps pipeline defaults",
			outbound:      newResponseTimeoutOutbound(),
			wantStream:    30 * time.Second,
			wantNonStream: 20 * time.Second,
		},
		{
			name:           "provider overrides only the stream timeout",
			outbound:       newResponseTimeoutOutbound(),
			streamOverride: lo.ToPtr(5 * time.Second),
			wantStream:     5 * time.Second,
			wantNonStream:  20 * time.Second,
		},
		{
			name:              "provider overrides only the non-stream timeout",
			outbound:          newResponseTimeoutOutbound(),
			nonStreamOverride: lo.ToPtr(7 * time.Second),
			wantStream:        30 * time.Second,
			wantNonStream:     7 * time.Second,
		},
		{
			name:              "provider overrides both timeouts",
			outbound:          newResponseTimeoutOutbound(),
			streamOverride:    lo.ToPtr(5 * time.Second),
			nonStreamOverride: lo.ToPtr(7 * time.Second),
			wantStream:        5 * time.Second,
			wantNonStream:     7 * time.Second,
		},
		{
			name:           "provider can disable the stream timeout",
			outbound:       newResponseTimeoutOutbound(),
			streamOverride: lo.ToPtr(time.Duration(0)),
			wantStream:     0,
			wantNonStream:  20 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if provider, ok := tt.outbound.(*responseTimeoutOutbound); ok {
				provider.streamOverride = tt.streamOverride
				provider.nonStreamOverride = tt.nonStreamOverride
			}

			p := NewFactory(nil).Pipeline(nil, tt.outbound, WithResponseTimeouts(30*time.Second, 20*time.Second))

			streamTimeout, nonStreamTimeout := p.effectiveResponseTimeouts(context.Background())
			require.Equal(t, tt.wantStream, streamTimeout)
			require.Equal(t, tt.wantNonStream, nonStreamTimeout)
		})
	}
}

// TestProcess_NonStreamTimeoutUsesChannelOverride proves the per-attempt
// override is wired into Process, not just the resolver.
func TestProcess_NonStreamTimeoutUsesChannelOverride(t *testing.T) {
	outbound := newResponseTimeoutOutbound()
	inbound := &mockInbound{}
	executor := &mockExecutor{
		do: func(ctx context.Context, _ *httpclient.Request) (*httpclient.Response, error) {
			select {
			case <-time.After(150 * time.Millisecond):
				return &httpclient.Response{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	// The channel override is shorter than the executor's latency, so the
	// attempt fails with the pipeline's non-stream timeout error.
	outbound.nonStreamOverride = lo.ToPtr(30 * time.Millisecond)
	p := NewFactory(executor).Pipeline(inbound, outbound, WithResponseTimeouts(0, 0))

	_, err := p.Process(context.Background(), &httpclient.Request{})
	require.ErrorIs(t, err, ErrNonStreamResponseTimeout)

	// Disabling the timeout on the channel wins over the (short) global default,
	// so the slow but successful request is not reported as a timeout.
	outbound.nonStreamOverride = lo.ToPtr(time.Duration(0))
	p = NewFactory(executor).Pipeline(inbound, outbound, WithResponseTimeouts(0, 10*time.Millisecond))

	_, err = p.Process(context.Background(), &httpclient.Request{})
	require.NoError(t, err)
}

// blockingExecutor blocks Do until its context is cancelled, then returns ctx.Err().
type blockingExecutor struct{}

func (e *blockingExecutor) Do(ctx context.Context, _ *httpclient.Request) (*httpclient.Response, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (e *blockingExecutor) DoStream(ctx context.Context, _ *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// fakeStreamOutboundWrapper wraps a mockOutbound and reports a FakeStreamMode,
// so the pipeline's Mode B synthesis branch is selected.
type fakeStreamOutboundWrapper struct {
	*mockOutbound
	mode FakeStreamMode
}

func (w *fakeStreamOutboundWrapper) FakeStreamMode() FakeStreamMode { return w.mode }

// TestSynthesizeStreamFromNonStreamSurfacesNonStreamTimeout red: the synthesis
// path (Mode B) must surface ErrNonStreamResponseTimeout like its sibling
// branches (pipeline.go:506-515/:523-532). Without the isNonStreamTimeout check
// in synthesizeStreamFromNonStream, the deadline error is wrapped and the
// errors.Is contract is broken.
func TestSynthesizeStreamFromNonStreamSurfacesNonStreamTimeout(t *testing.T) {
	inbound := &mockInbound{
		transformRequest: func(context.Context, *httpclient.Request) (*llm.Request, error) {
			return &llm.Request{Stream: lo.ToPtr(true)}, nil
		},
	}
	outbound := &fakeStreamOutboundWrapper{
		mockOutbound: &mockOutbound{apiFormat: llm.APIFormatOpenAIChatCompletion},
		mode:         FakeStreamModeFake,
	}
	p := NewFactory(&blockingExecutor{}).Pipeline(inbound, outbound, WithResponseTimeouts(0, 30*time.Millisecond))

	_, err := p.Process(context.Background(), &httpclient.Request{Body: []byte(`{"stream":true}`)})
	require.ErrorIs(t, err, ErrNonStreamResponseTimeout, "the synthesized path must preserve the non-stream timeout marker like its sibling branches")
}
