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
		CurrentCandidate:         candidate,
		FakeStreamHandlerEnabled: true,
		Perf:                     &biz.PerformanceRecord{Stream: true},
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
		CurrentCandidate:         &ChannelModelsCandidate{Channel: ch},
		FakeStreamHandlerEnabled: true,
		Perf:                     &biz.PerformanceRecord{Stream: true},
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
			ChannelModelsCandidates:  []*ChannelModelsCandidate{candidate},
			FakeStreamHandlerEnabled: true,
			LlmRequest:               llmReq,
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
		CurrentCandidate:         candidate,
		CurrentModelIndex:        0,
		FakeStreamHandlerEnabled: true,
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
			ChannelModelsCandidates:  []*ChannelModelsCandidate{candidate},
			FakeStreamHandlerEnabled: true,
			LlmRequest:               llmReq,
		},
	}

	_, err := outbound.TransformRequest(context.Background(), llmReq)
	require.NoError(t, err)
	require.NotNil(t, capture.seen)
	require.True(t, *capture.seen, "ws-bound model must keep stream:true (Mode B force must not fire)")
}

func TestPerformanceStreamFlagSurvivesCandidateSwitch(t *testing.T) {
	fakeCh := &biz.Channel{Channel: &ent.Channel{ID: 1, Name: "fake", Settings: &objects.ChannelSettings{}}}
	fakeCh.Policies.FakeStream = objects.FakeStreamModeFake
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalRequestStream:   lo.ToPtr(true),
		FakeStreamHandlerEnabled: true,
		CurrentCandidate:        &ChannelModelsCandidate{Channel: fakeCh},
	}}
	recording := &performanceRecording{outbound: outbound}

	// Attempt 1: a fake_stream candidate forces the upstream attempt non-stream.
	recording.OnOutboundRawRequest(context.Background(), &httpclient.Request{})
	require.False(t, outbound.state.Perf.Stream, "fake mode must record the upstream attempt as non-stream")

	// Attempt 2: failover lands on an off candidate whose upstream genuinely
	// streams — the client's original intent is the baseline, not attempt 1's flag.
	offCh := &biz.Channel{Channel: &ent.Channel{ID: 2, Name: "off", Settings: &objects.ChannelSettings{}}}
	offCh.Policies.FakeStream = objects.FakeStreamModeOff
	outbound.state.CurrentCandidate = &ChannelModelsCandidate{Channel: offCh}
	recording.OnOutboundRawRequest(context.Background(), &httpclient.Request{})
	require.True(t, outbound.state.Perf.Stream, "a real streaming attempt after a fake attempt must be recorded as streaming")
}
