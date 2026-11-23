// 职责: 在候选选定后，把首个候选的 FakeStream 模式经 context 回调通知 handler，
//
//	使 handler 能在 Process 阻塞前决定是否先向下游提交占位。
//
// 关联文件:
//
//	internal/server/orchestrator/select_candidates.go
//	internal/server/api/chat.go
//
// 注意事项:
//
//	决策范围（scope）决定按 candidates[0] 还是任一非 off 候选计算模式；
//	回调不改候选选择逻辑；候选选择仍在 Process 内单点完成。
package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/objects"
)

// FakeStreamDecision is invoked once per streaming request right after channel
// candidates are selected, carrying the scoped decision mode. A nil decision
// means the caller does not want placeholder handling (off path).
type FakeStreamDecision func(mode objects.FakeStreamMode)

type fakeStreamDecisionKey struct{}

// WithFakeStreamDecision returns a ctx carrying the decision callback.
func WithFakeStreamDecision(ctx context.Context, cb FakeStreamDecision) context.Context {
	return context.WithValue(ctx, fakeStreamDecisionKey{}, cb)
}

func fakeStreamDecisionFrom(ctx context.Context) (FakeStreamDecision, bool) {
	cb, ok := ctx.Value(fakeStreamDecisionKey{}).(FakeStreamDecision)
	return cb, ok && cb != nil
}

// fakeStreamOf reads the candidate's channel FakeStream policy, defaulting to off.
func fakeStreamOf(candidate *ChannelModelsCandidate) objects.FakeStreamMode {
	if candidate == nil || candidate.Channel == nil {
		return objects.FakeStreamModeOff
	}
	if candidate.Channel.Policies.FakeStream != "" {
		return candidate.Channel.Policies.FakeStream
	}
	return objects.FakeStreamModeOff
}

type fakeStreamScopeKey struct{}

// WithFakeStreamDecisionScope returns a ctx carrying the decision scope. The
// zero value (unset) means first_candidate, preserving the delivered behavior.
func WithFakeStreamDecisionScope(ctx context.Context, scope objects.FakeStreamDecisionScope) context.Context {
	return context.WithValue(ctx, fakeStreamScopeKey{}, scope)
}

func fakeStreamScopeFrom(ctx context.Context) objects.FakeStreamDecisionScope {
	if scope, ok := ctx.Value(fakeStreamScopeKey{}).(objects.FakeStreamDecisionScope); ok {
		switch scope {
		case objects.FakeStreamDecisionScopeFirstCandidate, objects.FakeStreamDecisionScopeAnyCandidate:
			return scope
		}
	}
	return objects.FakeStreamDecisionScopeFirstCandidate
}

// fakeStreamDecisionMode computes the mode reported to the handler. Under
// first_candidate only candidates[0] decides; under any_candidate the first
// non-off candidate in priority order commits the placeholder.
func fakeStreamDecisionMode(candidates []*ChannelModelsCandidate, scope objects.FakeStreamDecisionScope) objects.FakeStreamMode {
	if scope == objects.FakeStreamDecisionScopeAnyCandidate {
		for _, candidate := range candidates {
			if mode := fakeStreamOf(candidate); mode != objects.FakeStreamModeOff {
				return mode
			}
		}
		return objects.FakeStreamModeOff
	}
	if len(candidates) == 0 {
		return objects.FakeStreamModeOff
	}
	return fakeStreamOf(candidates[0])
}
