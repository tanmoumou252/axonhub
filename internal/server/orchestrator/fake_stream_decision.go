// 职责: 在候选选定后，把首个候选的 FakeStream 模式经 context 回调通知 handler，
//       使 handler 能在 Process 阻塞前决定是否先向下游提交占位。
// 关联文件:
//   internal/server/orchestrator/select_candidates.go
//   internal/server/api/chat.go
// 注意事项:
//   回调只读 candidates[0]，不改候选选择逻辑；候选选择仍在 Process 内单点完成。
package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/objects"
)

// FakeStreamDecision is invoked once per streaming request right after channel
// candidates are selected, carrying the first candidate's FakeStream mode. A nil
// decision means the caller does not want placeholder handling (off path).
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
