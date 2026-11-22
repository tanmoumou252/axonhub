package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

type stubResponseStream struct {
	events []*llm.Response
	index  int
}

func (s *stubResponseStream) Next() bool {
	if s.index >= len(s.events) {
		return false
	}
	s.index++
	return true
}

func (s *stubResponseStream) Current() *llm.Response {
	if s.index == 0 || s.index > len(s.events) {
		return nil
	}
	return s.events[s.index-1]
}

func (s *stubResponseStream) Err() error { return nil }

func (s *stubResponseStream) Close() error { return nil }

func TestModelCircuitBreakerTracker_StreamSuccessUsesCurrentChannel(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "gpt-4", false)
	}
	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4").State)

	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	stream, err := tracker.OnOutboundLlmStream(ctx, &stubResponseStream{
		events: []*llm.Response{{
			Usage: &llm.Usage{CompletionTokens: 8},
		}},
	})
	require.NoError(t, err)

	require.True(t, stream.Next())
	require.NotNil(t, stream.Current())
	require.NoError(t, stream.Close())

	stats := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, biz.StateClosed, stats.State)
	require.Zero(t, stats.ConsecutiveFailures)
}

func TestModelCircuitBreakerTracker_SkipCandidateDoesNotRecordError(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	// Force open so the next request is skipped instead of probed.
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "gpt-4", false)
	}
	before := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, biz.StateOpen, before.State)
	failuresBefore := before.ConsecutiveFailures
	lastFailureBefore := before.LastFailureAt

	_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
	require.ErrorIs(t, err, errSkipCandidateByCircuitBreaker)
	tracker.OnOutboundRawError(ctx, err)

	after := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, failuresBefore, after.ConsecutiveFailures)
	require.Equal(t, lastFailureBefore, after.LastFailureAt)
}

func TestModelCircuitBreakerTracker_NonCircuitBreakerSkipsStreamTracking(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "gpt-4", false)
	}
	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4").State)

	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	raw := &stubResponseStream{
		events: []*llm.Response{{
			Usage: &llm.Usage{CompletionTokens: 8},
		}},
	}
	stream, err := tracker.OnOutboundLlmStream(ctx, raw)
	require.NoError(t, err)
	require.Equal(t, streams.Stream[*llm.Response](raw), stream)

	require.True(t, stream.Next())
	require.NotNil(t, stream.Current())

	stats := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, biz.StateOpen, stats.State)
	require.Equal(t, policy.OpenThreshold, stats.ConsecutiveFailures)
}

// TestModelCircuitBreakerTrackerRecordsActualModelKeys asserts the breaker counts
// on the (channel, actual model) pair the attempt really sends upstream. The
// client-visible alias must not be the state key: siblings of one channel would
// otherwise share one breaker the resolver already treats apart.
func TestModelCircuitBreakerTrackerRecordsActualModelKeys(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{ID: 5, Name: "alias", Settings: &objects.ChannelSettings{}}},
		Models: []biz.ChannelModelEntry{{
			RequestModel: "public-alias",
			ActualModel:  "upstream-real",
		}},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalModel: "public-alias",
		RoutingPolicy: EffectiveRoutingPolicy{
			LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
		},
		CurrentCandidate: candidate,
	}}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
		require.NoError(t, err)
		tracker.OnOutboundRawError(ctx, errors.New("upstream model overloaded"))
	}

	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 5, "upstream-real").State,
		"model-scoped failures on the attempted upstream model must open that pair")
	require.Equal(t, biz.StateClosed, cb.GetModelCircuitBreakerStats(ctx, 5, "public-alias").State,
		"the client-visible alias must not be the breaker key")
}

// TestModelCircuitBreakerTrackerChannelScopeCountsAttemptedModelOnly asserts a
// channel-scoped verdict is charged to the model actually attempted this turn and
// never fanned out to sibling models the request did not attempt. The selector
// reads the candidate's first actual model, so charging siblings would record
// channel availability under model-health keys — the granularity inversion the
// feature exists to remove.
func TestModelCircuitBreakerTrackerChannelScopeCountsAttemptedModelOnly(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{ID: 6, Name: "deadkey", Settings: &objects.ChannelSettings{}}},
		Models: []biz.ChannelModelEntry{
			{RequestModel: "m-a", ActualModel: "up-a"},
			{RequestModel: "m-b", ActualModel: "up-b"},
		},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalModel: "m-a",
		RoutingPolicy: EffectiveRoutingPolicy{
			LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
		},
		CurrentCandidate: candidate,
	}}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
		require.NoError(t, err)
		tracker.OnOutboundRawError(ctx, &httpclient.Error{StatusCode: http.StatusUnauthorized})
	}

	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 6, "up-a").State,
		"a channel-scoped failure must be charged to the model actually attempted")
	require.Zero(t, cb.GetModelCircuitBreakerStats(ctx, 6, "up-b").ConsecutiveFailures,
		"a sibling the request never attempted must not inherit the penalty")
	require.Zero(t, cb.GetModelCircuitBreakerStats(ctx, 6, "m-a").ConsecutiveFailures,
		"the alias must stay uncounted now that keys are resolved to actual models")
}

// TestModelCircuitBreakerTrackerChannelScopeUsesScoredKey covers the case where a
// model-scoped failure advanced the attempt index before the channel-level one
// arrived (PrepareForRetry does exactly that, and a responses continuation can
// start at index>0). The attempt key is then a sibling the selector never reads,
// so a channel verdict charged there would be invisible to routing.
func TestModelCircuitBreakerTrackerChannelScopeUsesScoredKey(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{ID: 14, Name: "drift", Settings: &objects.ChannelSettings{}}},
		Models: []biz.ChannelModelEntry{
			{RequestModel: "m-a", ActualModel: "up-a"},
			{RequestModel: "m-b", ActualModel: "up-b"},
		},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalModel: "m-b",
		RoutingPolicy: EffectiveRoutingPolicy{
			LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
		},
		CurrentCandidate:  candidate,
		CurrentModelIndex: 1,
	}}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
	require.NoError(t, err)
	tracker.OnOutboundRawError(ctx, &httpclient.Error{StatusCode: http.StatusUnauthorized})

	require.Equal(t, 1, cb.GetModelCircuitBreakerStats(ctx, 14, "up-a").ConsecutiveFailures,
		"a channel-scoped fault on a later sibling must charge the key routing reads")
	require.Zero(t, cb.GetModelCircuitBreakerStats(ctx, 14, "up-b").ConsecutiveFailures,
		"the attempted-but-unscored sibling key must not absorb a channel verdict")
}

// TestModelCircuitBreakerTrackerIdleTimeoutIsNotClientCancel asserts a stream-idle
// abort that reaches the tracker as a joined cancellation stays an upstream
// fault. Today the tracker's plain context.Canceled test swallows it, so an
// endpoint that idles forever never trips.
func TestModelCircuitBreakerTrackerIdleTimeoutIsNotClientCancel(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{ID: 7, Name: "idler", Settings: &objects.ChannelSettings{}}},
		Models:  []biz.ChannelModelEntry{{RequestModel: "m", ActualModel: "up"}},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalModel: "m",
		RoutingPolicy: EffectiveRoutingPolicy{
			LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
		},
		CurrentCandidate: candidate,
	}}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	idleDisguise := fmtWrap(httpclient.ErrStreamIdleTimeout, context.Canceled)
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
		require.NoError(t, err)
		tracker.OnOutboundRawError(ctx, idleDisguise)
	}

	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 7, "up").State,
		"an idle timeout must trip the breaker like any other model-scoped failure")
}

// TestModelCircuitBreakerTrackerNilErrorIsNotAFailure states the obvious that the
// recorder must not treat an absent failure as evidence against the endpoint.
func TestModelCircuitBreakerTrackerNilErrorIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{ID: 8, Name: "nilcase", Settings: &objects.ChannelSettings{}}},
		Models:  []biz.ChannelModelEntry{{RequestModel: "m", ActualModel: "up"}},
	}
	outbound := &PersistentOutboundTransformer{state: &PersistenceState{
		OriginalModel: "m",
		RoutingPolicy: EffectiveRoutingPolicy{
			LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
		},
		CurrentCandidate: candidate,
	}}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
		require.NoError(t, err)
		tracker.OnOutboundRawError(ctx, nil)
	}

	stats := cb.GetModelCircuitBreakerStats(ctx, 8, "up")
	require.Equal(t, biz.StateClosed, stats.State)
	require.Zero(t, stats.ConsecutiveFailures, "an absent failure is not a model failure")
}
