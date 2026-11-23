package orchestrator

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
)

type streamCapturingTransformer struct {
	*mockTransformer
	seen *bool
}

func (s *streamCapturingTransformer) TransformRequest(ctx context.Context, req *llm.Request) (*httpclient.Request, error) {
	s.seen = req.Stream
	return s.mockTransformer.TransformRequest(ctx, req)
}

func TestPerformanceModeBStreamFlag(t *testing.T) {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 7, Name: "fake"}}
	ch.Policies.FakeStream = objects.FakeStreamModeFake
	candidate := &ChannelModelsCandidate{Channel: ch, Models: []biz.ChannelModelEntry{{ActualModel: "m"}}}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		CurrentCandidate: candidate,
		Perf:             &biz.PerformanceRecord{Stream: true},
	}}
	require.Equal(t, pipeline.FakeStreamModeFake, outbound.FakeStreamMode())

	recorder := &performanceRecording{outbound: outbound}
	_, err := recorder.OnOutboundRawRequest(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, outbound.state.Perf.Stream, "Mode B must record upstream as non-stream")
}

func TestPerformanceModeAKeepsClientStreamFlag(t *testing.T) {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 8, Name: "placeholder"}}
	ch.Policies.FakeStream = objects.FakeStreamModePlaceholder
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		CurrentCandidate: &ChannelModelsCandidate{Channel: ch},
		Perf:             &biz.PerformanceRecord{Stream: true},
	}}
	require.Equal(t, pipeline.FakeStreamModePlaceholder, outbound.FakeStreamMode())

	recorder := &performanceRecording{outbound: outbound}
	_, err := recorder.OnOutboundRawRequest(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, outbound.state.Perf.Stream, "Mode A keeps real upstream streaming semantics")
}

func TestModeBForcesUpstreamNonStream(t *testing.T) {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 9, Name: "synthetic", Settings: &objects.ChannelSettings{}}}
	ch.Policies.FakeStream = objects.FakeStreamModeFake
	capture := &streamCapturingTransformer{mockTransformer: &mockTransformer{}}
	ch.Outbound = capture
	candidate := &ChannelModelsCandidate{Channel: ch, Models: []biz.ChannelModelEntry{{ActualModel: "m"}}}
	llmReq := &llm.Request{Stream: lo.ToPtr(true), Model: "m"}
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			ChannelModelsCandidates: []*ChannelModelsCandidate{candidate},
			LlmRequest:              llmReq,
		},
	}

	_, err := outbound.TransformRequest(context.Background(), llmReq)
	require.NoError(t, err)
	require.NotNil(t, capture.seen)
	require.False(t, *capture.seen, "upstream must receive stream:false under fake_stream")
	require.NotNil(t, llmReq.Stream)
	require.False(t, *llmReq.Stream)
}

func TestFakeStreamModeWSBoundIsPlaceholder(t *testing.T) {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 21, Name: "ws", Settings: &objects.ChannelSettings{}}}
	ch.Policies.FakeStream = objects.FakeStreamModeFake
	candidate := &ChannelModelsCandidate{
		Channel:         ch,
		Models:          []biz.ChannelModelEntry{{ActualModel: "m"}},
		APIFormat:       "openai/responses-ws",
		modelAPIFormats: []string{"openai/responses-ws"},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		CurrentCandidate:  candidate,
		CurrentModelIndex: 0,
	}}
	require.Equal(t, pipeline.FakeStreamModePlaceholder, outbound.FakeStreamMode(),
		"a fake_stream model bound to a stream-only ws endpoint must degrade to placeholder, not fake_stream")
}

func TestModeBNotForcedForWSBound(t *testing.T) {
	ch := &biz.Channel{Channel: &ent.Channel{ID: 22, Name: "ws2", Settings: &objects.ChannelSettings{}}}
	ch.Policies.FakeStream = objects.FakeStreamModeFake
	capture := &streamCapturingTransformer{mockTransformer: &mockTransformer{}}
	ch.Outbound = capture
	candidate := &ChannelModelsCandidate{
		Channel:         ch,
		Models:          []biz.ChannelModelEntry{{ActualModel: "m"}},
		APIFormat:       "openai/responses-ws",
		modelAPIFormats: []string{"openai/responses-ws"},
	}
	llmReq := &llm.Request{Stream: lo.ToPtr(true), Model: "m"}
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			ChannelModelsCandidates: []*ChannelModelsCandidate{candidate},
			LlmRequest:              llmReq,
		},
	}

	_, err := outbound.TransformRequest(context.Background(), llmReq)
	require.NoError(t, err)
	require.NotNil(t, capture.seen)
	require.True(t, *capture.seen, "ws-bound model must keep stream:true (Mode B force must not fire)")
}
