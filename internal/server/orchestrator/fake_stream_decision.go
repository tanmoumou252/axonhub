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
//	模式经 effectiveFakeStreamMode 计算：per-model 覆盖优先于渠道级；
//	绑 openai/responses-ws（物理只能流式）端点的模型，fake_stream 降 placeholder
//	（保留占位/心跳，跳过 Mode B 强制非流式与合成）。
package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm"
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

// modelFakeStream resolves the per-model FakeStream override for the candidate's
// model at modelIndex, falling back to the channel-level policy when no override
// applies. Out-of-range modelIndex falls back to index 0 (the first attempt).
func modelFakeStream(candidate *ChannelModelsCandidate, modelIndex int) objects.FakeStreamMode {
	if candidate == nil || candidate.Channel == nil {
		return objects.FakeStreamModeOff
	}
	if modelIndex < 0 || modelIndex >= len(candidate.Models) {
		modelIndex = 0
	}
	if modelIndex < len(candidate.Models) && candidate.Channel.Settings != nil {
		entry := candidate.Models[modelIndex]
		if override, found := candidate.Channel.ModelFakeStreamOverride(entry.RequestModel); found {
			return override
		}
		if entry.ActualModel != "" && entry.ActualModel != entry.RequestModel {
			if override, found := candidate.Channel.ModelFakeStreamOverride(entry.ActualModel); found {
				return override
			}
		}
	}
	return fakeStreamOf(candidate)
}

// outboundFormatForModelIndex returns the selected endpoint format for the model
// at modelIndex, falling back to the candidate-level format.
func outboundFormatForModelIndex(candidate *ChannelModelsCandidate, modelIndex int) string {
	if candidate == nil {
		return ""
	}
	if modelIndex >= 0 && modelIndex < len(candidate.modelAPIFormats) && candidate.modelAPIFormats[modelIndex] != "" {
		return candidate.modelAPIFormats[modelIndex]
	}
	return candidate.APIFormat
}

// effectiveFakeStreamMode is the single authority for the fake-stream mode of a
// candidate's attempt at modelIndex: per-model override (or channel policy), then
// the endpoint gate. The gate ONLY rewrites fake_stream: a model bound to a
// physically stream-only endpoint (openai/responses-ws) cannot honor Mode B's
// forced non-streaming upstream, so fake_stream degrades to placeholder (Mode A:
// keep the upstream streaming and splice after the placeholder — exactly what a
// WebSocket transport does natively). placeholder and off pass through unchanged
// (placeholder already works on ws; off must stay off). Responses HTTP and
// compact are NOT gated (compact is inherently non-streaming; HTTP responses
// synthesizes). Note: at the decision layer fake_stream and placeholder both
// commit a placeholder (chat.go:178), so this rewrite is decision-equivalent —
// its effect is purely at execution (skip Mode B force + skip synthesis).
func effectiveFakeStreamMode(candidate *ChannelModelsCandidate, modelIndex int) objects.FakeStreamMode {
	mode := modelFakeStream(candidate, modelIndex)
	if mode != objects.FakeStreamModeFake {
		return mode
	}
	if outboundFormatForModelIndex(candidate, modelIndex) == llm.APIFormatOpenAIResponseWebSocket.String() {
		return objects.FakeStreamModePlaceholder
	}
	return mode
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
			if mode := effectiveFakeStreamMode(candidate, 0); mode != objects.FakeStreamModeOff {
				return mode
			}
		}
		return objects.FakeStreamModeOff
	}
	if len(candidates) == 0 {
		return objects.FakeStreamModeOff
	}
	return effectiveFakeStreamMode(candidates[0], 0)
}
