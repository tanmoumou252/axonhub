// 职责: 定义 pipeline 侧 FakeStreamMode 与 full→delta 适配器，供 Mode B 合成协议合规 SSE。
// 关联文件:
//   llm/pipeline/pipeline.go
//   llm/transformer/interfaces.go
// 注意事项:
//   llm 是独立 module，不能 import internal/objects；模式由 orchestrator 侧翻译注入。
package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

// FakeStreamMode mirrors internal/objects.FakeStreamMode across the module boundary.
type FakeStreamMode uint8

const (
	FakeStreamModeOff FakeStreamMode = iota
	FakeStreamModePlaceholder
	FakeStreamModeFake
)

// FakeStreamPolicy is an optional interface Outbound implements to report the
// current candidate's fake-stream mode (translated from the channel policy).
type FakeStreamPolicy interface {
	FakeStreamMode() FakeStreamMode
}

func fakeStreamModeOf(outbound interface{}) FakeStreamMode {
	if p, ok := outbound.(FakeStreamPolicy); ok {
		return p.FakeStreamMode()
	}
	return FakeStreamModeOff
}

// supportsFakeStreamSynthesis mirrors the orchestrator's auto-aggregate gate
// across the module boundary: only chat/completion-shaped requests can be
// synthesized from a complete response. TTS, Decisions, realtime and other
// request types must fall back to the direct path.
func supportsFakeStreamSynthesis(request *llm.Request) bool {
	if request == nil {
		return false
	}
	switch request.RequestType {
	case "", llm.RequestTypeChat:
		switch request.APIFormat {
		case "",
			llm.APIFormatOpenAIChatCompletion,
			llm.APIFormatOpenAIResponse,
			llm.APIFormatAnthropicMessage,
			llm.APIFormatGeminiContents,
			llm.APIFormatOllamaChat:
			return true
		default:
			return false
		}
	case llm.RequestTypeCompletion:
		return request.APIFormat == "" || request.APIFormat == llm.APIFormatOpenAICompletion
	default:
		return false
	}
}

// normalizeToolCallIndices renumbers a choice's tool calls by position ONLY when
// duplicate Index values are present. A complete (non-streaming) response wire
// form usually omits the delta-only `index` field, so a decoder defaults every
// call to 0; the Anthropic inbound stream transformer groups deltas by Index and
// would merge distinct calls into one (dropping IDs/names and concatenating
// arguments) before validateToolArguments fails the whole stream. Legitimate
// sparse or unique indices pass through untouched. The fix operates on a copied
// slice so the input response is never mutated.
func normalizeToolCallIndices(calls []llm.ToolCall) []llm.ToolCall {
	if len(calls) < 2 {
		return calls
	}
	seen := make(map[int]struct{}, len(calls))
	duplicate := false
	for _, call := range calls {
		if _, ok := seen[call.Index]; ok {
			duplicate = true
			break
		}
		seen[call.Index] = struct{}{}
	}
	if !duplicate {
		return calls
	}
	out := make([]llm.ToolCall, len(calls))
	copy(out, calls)
	for i := range out {
		out[i].Index = i
	}
	return out
}

// toDeltaStreamShape rewrites each choice's full Message into a Delta-bearing
// choice so Anthropic's inbound stream transformer (which reads only choice.Delta)
// does not silently drop the body. Idempotent for already-Delta choices.
func toDeltaStreamShape(resp *llm.Response) *llm.Response {
	if resp == nil {
		return resp
	}
	out := *resp
	out.Choices = make([]llm.Choice, 0, len(resp.Choices))
	for _, c := range resp.Choices {
		nc := c
		if c.Message != nil && c.Delta == nil {
			nc.Delta = c.Message
			nc.Message = nil
		}
		if nc.Delta != nil && len(nc.Delta.ToolCalls) > 0 {
			if calls := normalizeToolCallIndices(nc.Delta.ToolCalls); &calls[0] != &nc.Delta.ToolCalls[0] {
				nd := *nc.Delta
				nd.ToolCalls = calls
				nc.Delta = &nd
			}
		}
		out.Choices = append(out.Choices, nc)
	}
	return &out
}

// synthesizeStreamFromNonStream runs the upstream as non-streaming, then
// synthesizes a protocol-conformant inbound SSE stream from the complete
// response. Used only when the selected candidate's FakeStream mode is fake_stream.
func (p *pipeline) synthesizeStreamFromNonStream(
	ctx context.Context,
	executor Executor,
	request *httpclient.Request,
) (streams.Stream[*httpclient.StreamEvent], error) {
	_, nonStreamTimeout := p.effectiveResponseTimeouts(ctx)
	timeoutCtx, cancel := withNonStreamTimeout(ctx, nonStreamTimeout)
	defer cancel()

	httpResp, err := executor.Do(timeoutCtx, request)
	if err != nil {
		p.applyRawErrorResponseMiddlewares(ctx, err)
		// Preserve the non-stream timeout sentinel like the sibling branches
		// (pipeline.go auto-aggregate / default non-stream) so callers can
		// errors.Is(err, ErrNonStreamResponseTimeout). Without this the deadline
		// is wrapped into a generic UpstreamError and the timeout classification
		// (isResponseTimeoutError → model-level failure) silently degrades.
		if isNonStreamTimeout(timeoutCtx, nonStreamTimeout) {
			return nil, ErrNonStreamResponseTimeout
		}
		if httpErr, ok := errors.AsType[*httpclient.Error](err); ok {
			return nil, WrapUpstreamError(p.Outbound.TransformError(ctx, httpErr))
		}

		return nil, WrapUpstreamError(fmt.Errorf("failed to do request: %w", err))
	}

	httpResp, err = p.applyRawResponseMiddlewares(ctx, httpResp)
	if err != nil {
		p.applyRawErrorResponseMiddlewares(ctx, err)

		return nil, fmt.Errorf("failed to apply raw response middlewares: %w", err)
	}

	llmResp, err := p.Outbound.TransformResponse(ctx, httpResp)
	if err != nil {
		p.applyRawErrorResponseMiddlewares(ctx, err)

		return nil, WrapUpstreamError(fmt.Errorf("failed to transform response: %w", err))
	}

	llmResp, err = p.applyLlmResponseMiddlewares(ctx, llmResp)
	if err != nil {
		p.applyRawErrorResponseMiddlewares(ctx, err)

		return nil, fmt.Errorf("failed to apply llm response middlewares: %w", err)
	}

	if p.emptyResponseDetection && !hasResponseContent(llmResp) {
		p.applyRawErrorResponseMiddlewares(ctx, ErrEmptyResponse)

		return nil, ErrEmptyResponse
	}

	llmResp = withTransformerMetadata(request.TransformerMetadata, llmResp)

	// E-1: normalize the complete Message-shaped response into Delta form before
	// feeding the inbound stream transformer (Anthropic inbound reads only Delta).
	llmResp = toDeltaStreamShape(llmResp)

	respHeaders := httpclient.MergeForwardResponseHeaders(nil, httpResp.Headers)
	inboundStream, err := p.Inbound.TransformStream(ctx, streams.SliceStream([]*llm.Response{llmResp}))
	if err != nil {
		return nil, fmt.Errorf("failed to transform stream: %w", err)
	}

	return httpclient.WithResponseHeaders(inboundStream, respHeaders), nil
}
