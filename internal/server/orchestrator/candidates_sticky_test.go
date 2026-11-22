package orchestrator

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

type fakePreviousChannelProvider struct {
	traceChannelIDs  map[int]int
	threadChannelIDs map[int]int
}

func (p *fakePreviousChannelProvider) GetPreviousChannelID(_ context.Context, traceID int) (int, error) {
	return p.traceChannelIDs[traceID], nil
}

func (p *fakePreviousChannelProvider) GetPreviousChannelIDByThread(_ context.Context, threadID int) (int, error) {
	return p.threadChannelIDs[threadID], nil
}

func stickyTestCandidate(channelID, priority int) *ChannelModelsCandidate {
	return &ChannelModelsCandidate{
		Channel: &biz.Channel{
			Channel: &ent.Channel{
				ID:   channelID,
				Name: "channel",
			},
		},
		Priority: priority,
	}
}

func TestLoadBalancedSelector_TracksOnlyFinalTopCandidateAcrossPriorityGroups(t *testing.T) {
	tracker := &mockSelectionTracker{}
	lb := NewLoadBalancer(nil, tracker)
	selector := &LoadBalancedSelector{}
	candidates := []*ChannelModelsCandidate{
		stickyTestCandidate(1, 0),
		stickyTestCandidate(2, 1),
		stickyTestCandidate(3, 1),
	}

	result := selector.sortCandidates(context.Background(), lb, candidates, &llm.Request{Model: "gpt-4"}, 3, 3, true)
	require.Len(t, result, 3)
	require.Equal(t, 1, tracker.selections[1])
	require.Zero(t, tracker.selections[2])
	require.Zero(t, tracker.selections[3])
}

func TestLoadBalancedSelector_TracksSingleCandidate(t *testing.T) {
	tracker := &mockSelectionTracker{}
	lb := NewLoadBalancer(nil, tracker)
	selector := &LoadBalancedSelector{}
	candidate := stickyTestCandidate(1, 0)

	result := selector.sortCandidates(context.Background(), lb, []*ChannelModelsCandidate{candidate}, &llm.Request{Model: "gpt-4"}, 1, 1, true)
	require.Len(t, result, 1)
	require.Equal(t, 1, tracker.selections[1])
}

func TestLoadBalancedSelector_RetryPolicyLimitsCandidateCount(t *testing.T) {
	strategy := &channelBasedStrategy{
		name:   "score",
		scores: map[int]float64{1: 10, 2: 30, 3: 20, 4: 40},
	}

	tests := []struct {
		name   string
		policy biz.RetryPolicy
		want   []int
	}{
		{
			name:   "retry disabled keeps only the best candidate",
			policy: biz.RetryPolicy{Enabled: false, MaxChannelRetries: 3},
			want:   []int{2},
		},
		{
			name:   "retries fill from the next priority group",
			policy: biz.RetryPolicy{Enabled: true, MaxChannelRetries: 2},
			want:   []int{2, 1, 4},
		},
		{
			name:   "retries beyond the candidate count keep every candidate",
			policy: biz.RetryPolicy{Enabled: true, MaxChannelRetries: 10},
			want:   []int{2, 1, 4, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := []*ChannelModelsCandidate{
				stickyTestCandidate(1, 0),
				stickyTestCandidate(2, 0),
				stickyTestCandidate(3, 1),
				stickyTestCandidate(4, 1),
			}
			policy := tt.policy
			selector := WithLoadBalancedSelector(
				&staticChannelSelector{candidates: candidates},
				NewLoadBalancer(nil, nil, strategy),
				&policy,
			)

			result, err := selector.Select(context.Background(), &llm.Request{Model: "gpt-4"})
			require.NoError(t, err)
			require.Equal(t, tt.want, lo.Map(result, func(candidate *ChannelModelsCandidate, _ int) int {
				return candidate.Channel.ID
			}))
		})
	}
}

func TestLoadBalancedSelector_TraceStickySelection(t *testing.T) {
	trace := &ent.Trace{ID: 10, ThreadID: 20}
	thread := &ent.Thread{ID: 20}
	ctx := contexts.WithThread(contexts.WithTrace(context.Background(), trace), thread)

	t.Run("trace overrides association priority and removes duplicate fallback", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			stickyTestCandidate(1, 0),
			stickyTestCandidate(2, 2),
			stickyTestCandidate(3, 1),
			stickyTestCandidate(2, 4),
		}
		policy := &biz.RetryPolicy{
			Enabled:                 true,
			MaxChannelRetries:       2,
			MaxSingleChannelRetries: 2,
			TraceStickyMode:         biz.TraceStickyPreferPreviousChannel,
		}
		tracker := &mockSelectionTracker{}
		selector := WithTraceStickyLoadBalancedSelector(
			&staticChannelSelector{candidates: candidates},
			NewLoadBalancer(nil, tracker),
			policy,
			&fakePreviousChannelProvider{traceChannelIDs: map[int]int{10: 2}},
		)

		result, err := selector.Select(ctx, &llm.Request{Model: "gpt-4"})
		require.NoError(t, err)
		require.Greater(t, len(result), 0)
		require.Equal(t, 2, result[0].Channel.ID)
		require.True(t, result[0].TraceSticky)
	})
}

func TestLoadBalancedSelector_RequiredCount_CoversSiblingModelsAcrossPriorities(t *testing.T) {
	// 10 条规则指向同一个 Channel ID, 优先级 0~9, 各携不同实际模型。
	candidates := make([]*ChannelModelsCandidate, 0, 10)
	for priority := 0; priority < 10; priority++ {
		candidates = append(candidates, &ChannelModelsCandidate{
			Channel: &biz.Channel{
				Channel: &ent.Channel{ID: 1},
			},
			Priority: priority,
		})
	}

	strategy := &channelBasedStrategy{
		name:   "score",
		scores: map[int]float64{1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60, 7: 70, 8: 80, 9: 90, 10: 100},
	}
	policy := &biz.RetryPolicy{
		Enabled:                true,
		MaxChannelRetries:      3,
		MaxSingleChannelRetries: 8,
	}

	selector := WithLoadBalancedSelector(
		&staticChannelSelector{candidates: candidates},
		NewLoadBalancer(nil, nil, strategy),
		policy,
	)

	result, err := selector.Select(context.Background(), &llm.Request{Model: "gpt-4"})
	require.NoError(t, err)

	// 若 requiredCount 仍按 1+MaxChannelRetries 计算, 只会保留前 4 个候选;
	// 修正后 requiredCount=1+3+8=12, 应全部 10 个同渠道候选可达。
	require.Len(t, result, 10, "same-channel different-priority rules must all materialize as candidates")
	require.Equal(t, 1, lo.Map(result, func(c *ChannelModelsCandidate, _ int) int {
		return c.Channel.ID
	})[0], "all candidates share channel ID 1")
}

func TestLoadBalancedSelector_EffectiveRoutingPolicyPriority(t *testing.T) {
	trace := &ent.Trace{ID: 10}
	ctx := contexts.WithTrace(context.Background(), trace)
	modelPolicy := &ModelRoutingPolicy{
		LoadBalancerStrategy: biz.LoadBalancerStrategyFailover,
		TraceStickyMode:      string(biz.TraceStickyDisabled),
	}
	candidates := []*ChannelModelsCandidate{
		stickyTestCandidate(1, 0),
		stickyTestCandidate(2, 0),
	}
	for _, candidate := range candidates {
		candidate.ModelRoutingPolicy = modelPolicy
	}

	systemPolicy := &biz.RetryPolicy{
		Enabled:              true,
		MaxChannelRetries:    1,
		LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive,
		TraceStickyMode:      biz.TraceStickyPreferPreviousChannel,
	}
	adaptiveTracker := &mockSelectionTracker{}
	failoverTracker := &mockSelectionTracker{}
	roundRobinTracker := &mockSelectionTracker{}
	profileLoadBalancer := biz.LoadBalancerStrategyRoundRobin
	profileTraceStickyMode := string(biz.TraceStickyPreferPreviousChannel)
	apiKey := &ent.APIKey{Profiles: &objects.APIKeyProfiles{
		ActiveProfile: "default",
		Profiles: []objects.APIKeyProfile{{
			Name:                "default",
			LoadBalanceStrategy: &profileLoadBalancer,
			TraceStickyMode:     &profileTraceStickyMode,
		}},
	}}
	effectivePolicy := EffectiveRoutingPolicy{}
	selector := WithRoutingPolicyLoadBalancedSelector(
		&staticChannelSelector{candidates: candidates},
		map[string]*LoadBalancer{
			biz.LoadBalancerStrategyAdaptive:   NewLoadBalancer(nil, adaptiveTracker),
			biz.LoadBalancerStrategyFailover:   NewLoadBalancer(nil, failoverTracker),
			biz.LoadBalancerStrategyRoundRobin: NewLoadBalancer(nil, roundRobinTracker),
		},
		systemPolicy,
		&fakePreviousChannelProvider{traceChannelIDs: map[int]int{trace.ID: 2}},
		apiKey,
		&effectivePolicy,
		nil,
	)

	result, err := selector.Select(ctx, &llm.Request{Model: "gpt-4"})
	require.NoError(t, err)
	require.Equal(t, 2, result[0].Channel.ID)
	require.True(t, result[0].TraceSticky)
	require.Equal(t, biz.LoadBalancerStrategyRoundRobin, effectivePolicy.LoadBalancerStrategy)
	require.Equal(t, biz.TraceStickyPreferPreviousChannel, effectivePolicy.TraceStickyMode)
	require.Equal(t, 1, roundRobinTracker.selections[2])
	require.Empty(t, adaptiveTracker.selections)
	require.Empty(t, failoverTracker.selections)
}
