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
)

func fakeStreamCandidate(mode objects.FakeStreamMode) *ChannelModelsCandidate {
	return &ChannelModelsCandidate{
		Channel: &biz.Channel{
			Channel: &ent.Channel{
				Policies: objects.ChannelPolicies{FakeStream: mode},
			},
		},
	}
}

func TestFakeStreamDecision(t *testing.T) {
	t.Run("callback receives first candidate placeholder mode", func(t *testing.T) {
		var got objects.FakeStreamMode
		called := false
		ctx := WithFakeStreamDecision(context.Background(), func(mode objects.FakeStreamMode) {
			got = mode
			called = true
		})

		cb, ok := fakeStreamDecisionFrom(ctx)
		require.True(t, ok)
		cb(fakeStreamOf(fakeStreamCandidate(objects.FakeStreamModePlaceholder)))
		require.True(t, called)
		require.Equal(t, objects.FakeStreamModePlaceholder, got)
	})

	t.Run("empty and off modes normalize to off", func(t *testing.T) {
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamOf(fakeStreamCandidate(objects.FakeStreamModeOff)))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamOf(fakeStreamCandidate("")))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamOf(nil))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamOf(&ChannelModelsCandidate{}))
	})

	t.Run("no callback does not panic", func(t *testing.T) {
		_, ok := fakeStreamDecisionFrom(context.Background())
		require.False(t, ok)
	})
}

func TestFakeStreamIsIndependentOfStreamPolicy(t *testing.T) {
	selector := WithStreamPolicySelector(&mockSelector{candidates: []*ChannelModelsCandidate{
		{
			Models: []biz.ChannelModelEntry{{RequestModel: "synthetic"}},
			Channel: &biz.Channel{
				Channel: &ent.Channel{
					Policies: objects.ChannelPolicies{
						Stream:     objects.CapabilityPolicyUnlimited,
						FakeStream: objects.FakeStreamModeFake,
					},
				},
			},
		},
	}})

	got, err := selector.Select(context.Background(), &llm.Request{Stream: lo.ToPtr(true)})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "synthetic", got[0].Models[0].RequestModel)
}

func TestFakeStreamDecisionMode(t *testing.T) {
	t.Run("first_candidate keeps the delivered first-candidate semantics", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			fakeStreamCandidate(objects.FakeStreamModeOff),
			fakeStreamCandidate(objects.FakeStreamModeFake),
		}
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(candidates, objects.FakeStreamDecisionScopeFirstCandidate))
	})

	t.Run("any_candidate commits when any candidate enables fake streaming", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			fakeStreamCandidate(objects.FakeStreamModeOff),
			fakeStreamCandidate(objects.FakeStreamModePlaceholder),
			fakeStreamCandidate(objects.FakeStreamModeFake),
		}
		require.Equal(t, objects.FakeStreamModePlaceholder, fakeStreamDecisionMode(candidates, objects.FakeStreamDecisionScopeAnyCandidate))
	})

	t.Run("any_candidate falls back to off when every candidate is off", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			fakeStreamCandidate(objects.FakeStreamModeOff),
			fakeStreamCandidate(""),
		}
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(candidates, objects.FakeStreamDecisionScopeAnyCandidate))
	})

	t.Run("empty scope normalizes to first_candidate", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			fakeStreamCandidate(objects.FakeStreamModeOff),
			fakeStreamCandidate(objects.FakeStreamModeFake),
		}
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(candidates, ""))
	})

	t.Run("unknown scope normalizes to first_candidate", func(t *testing.T) {
		candidates := []*ChannelModelsCandidate{
			fakeStreamCandidate(objects.FakeStreamModeOff),
			fakeStreamCandidate(objects.FakeStreamModeFake),
		}
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(candidates, objects.FakeStreamDecisionScope("every_candidate")))
	})

	t.Run("nil and empty candidate lists normalize to off", func(t *testing.T) {
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(nil, objects.FakeStreamDecisionScopeAnyCandidate))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode([]*ChannelModelsCandidate{}, objects.FakeStreamDecisionScopeAnyCandidate))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode([]*ChannelModelsCandidate{nil}, objects.FakeStreamDecisionScopeAnyCandidate))
		require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode(nil, objects.FakeStreamDecisionScopeFirstCandidate))
	})

	t.Run("scope travels through the context with first_candidate default", func(t *testing.T) {
		ctx := WithFakeStreamDecisionScope(context.Background(), objects.FakeStreamDecisionScopeAnyCandidate)
		require.Equal(t, objects.FakeStreamDecisionScopeAnyCandidate, fakeStreamScopeFrom(ctx))
		require.Equal(t, objects.FakeStreamDecisionScopeFirstCandidate, fakeStreamScopeFrom(context.Background()))
		require.Equal(t, objects.FakeStreamDecisionScopeFirstCandidate, fakeStreamScopeFrom(WithFakeStreamDecisionScope(context.Background(), objects.FakeStreamDecisionScope("bogus"))))
	})
}
