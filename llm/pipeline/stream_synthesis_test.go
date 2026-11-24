// 职责: Mode B full→delta 适配器与 pipeline 侧模式解析的契约测试（正文/工具/推理字段不得丢失）。
// 关联文件:
//   llm/pipeline/stream_synthesis.go
//   llm/pipeline/pipeline.go
package pipeline

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm"
)

func TestToDeltaStreamShape(t *testing.T) {
	t.Run("moves full Message into Delta preserving every field", func(t *testing.T) {
		finish := "tool_calls"
		reasoning := "thought"
		message := &llm.Message{
			Content:   llm.MessageContent{Content: lo.ToPtr("body")},
			Reasoning: &reasoning,
			ToolCalls: []llm.ToolCall{{Index: 1, ID: "call_1", Type: "function"}},
			Refusal:   "no",
		}
		in := &llm.Response{Choices: []llm.Choice{{Index: 0, Message: message, FinishReason: &finish}}}

		out := toDeltaStreamShape(in)

		require.Len(t, out.Choices, 1)
		require.Nil(t, out.Choices[0].Message)
		require.NotNil(t, out.Choices[0].Delta)
		require.Equal(t, lo.ToPtr("body"), out.Choices[0].Delta.Content.Content)
		require.Equal(t, &reasoning, out.Choices[0].Delta.Reasoning)
		require.Equal(t, "call_1", out.Choices[0].Delta.ToolCalls[0].ID)
		require.Equal(t, 1, out.Choices[0].Delta.ToolCalls[0].Index)
		require.Equal(t, "no", out.Choices[0].Delta.Refusal)
		require.Equal(t, &finish, out.Choices[0].FinishReason)
		require.NotNil(t, in.Choices[0].Message, "input must not be mutated")
	})

	t.Run("idempotent for already-Delta choices", func(t *testing.T) {
		delta := &llm.Message{Content: llm.MessageContent{Content: lo.ToPtr("already streaming")}}
		in := &llm.Response{Choices: []llm.Choice{{Delta: delta}}}
		out := toDeltaStreamShape(in)
		require.Same(t, delta, out.Choices[0].Delta)
	})

	t.Run("finish-only complete response still yields a terminal delta", func(t *testing.T) {
		finish := "stop"
		out := toDeltaStreamShape(&llm.Response{Choices: []llm.Choice{{FinishReason: &finish}}})
		require.Equal(t, &finish, out.Choices[0].FinishReason)
	})
}

func TestToDeltaStreamShapeNormalizesDuplicateToolCallIndices(t *testing.T) {
	resp := &llm.Response{Choices: []llm.Choice{{
		Message: &llm.Message{ToolCalls: []llm.ToolCall{
			{Index: 0, ID: "call_a", Type: "function", Function: llm.FunctionCall{Name: "a", Arguments: `{"x":1}`}},
			{Index: 0, ID: "call_b", Type: "function", Function: llm.FunctionCall{Name: "b", Arguments: `{"y":2}`}},
		}},
	}}}
	out := toDeltaStreamShape(resp)
	require.Equal(t, 0, out.Choices[0].Delta.ToolCalls[0].Index)
	require.Equal(t, 1, out.Choices[0].Delta.ToolCalls[1].Index, "duplicate indices from a complete response must be renormalized or the Anthropic inbound stream merges the calls")
	require.Equal(t, "call_a", out.Choices[0].Delta.ToolCalls[0].ID)
	require.Equal(t, "call_b", out.Choices[0].Delta.ToolCalls[1].ID)
	require.Equal(t, `{"y":2}`, out.Choices[0].Delta.ToolCalls[1].Function.Arguments)
	require.NotNil(t, resp.Choices[0].Message, "input must not be mutated")
}

func TestSupportsFakeStreamSynthesis(t *testing.T) {
	require.True(t, supportsFakeStreamSynthesis(&llm.Request{RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion}))
	require.True(t, supportsFakeStreamSynthesis(&llm.Request{RequestType: llm.RequestTypeCompletion, APIFormat: llm.APIFormatOpenAICompletion}))
	require.False(t, supportsFakeStreamSynthesis(&llm.Request{RequestType: llm.RequestTypeSpeech, APIFormat: llm.APIFormatOpenAISpeech}))
	require.False(t, supportsFakeStreamSynthesis(&llm.Request{RequestType: llm.RequestTypeDecisions, APIFormat: llm.APIFormatOpenAIDecisions}))
	require.False(t, supportsFakeStreamSynthesis(nil))
}

type fakeStreamPolicyOutbound struct{ mode FakeStreamMode }

func (f *fakeStreamPolicyOutbound) FakeStreamMode() FakeStreamMode { return f.mode }

func TestFakeStreamModeOf(t *testing.T) {
	require.Equal(t, FakeStreamModeFake, fakeStreamModeOf(&fakeStreamPolicyOutbound{mode: FakeStreamModeFake}))
	require.Equal(t, FakeStreamModeOff, fakeStreamModeOf(struct{}{}))
	require.Equal(t, FakeStreamModeOff, fakeStreamModeOf(nil))
}
