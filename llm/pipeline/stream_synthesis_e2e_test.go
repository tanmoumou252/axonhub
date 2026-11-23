// 职责: 回归锁定 Mode B 合成流：真实 anthropic inbound 从完整非流式响应合成合规消息事件序列。
// 关联文件:
//
//	llm/pipeline/stream_synthesis.go
//	llm/pipeline/integration_test.go
//	llm/transformer/anthropic/inbound_stream.go
package pipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
	"github.com/looplj/axonhub/llm/transformer/openai"
)

// fakeStreamOutboundWrapper decorates a real outbound transformer with the
// FakeStreamPolicy so Process takes the Mode B synthesis branch.
type fakeStreamOutboundWrapper struct {
	transformer.Outbound
	mode pipeline.FakeStreamMode
}

func (w fakeStreamOutboundWrapper) FakeStreamMode() pipeline.FakeStreamMode { return w.mode }

func TestSynthesizeStream_AnthropicInboundFromNonStream(t *testing.T) {
	upstreamBody := []byte(`{"id":"chatcmpl-fake","object":"chat.completion","created":1760000000,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"hello fake"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)
	executor := &mockExecutor{doFunc: func(ctx context.Context, request *httpclient.Request) (*httpclient.Response, error) {
		return &httpclient.Response{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
			Body:       upstreamBody,
		}, nil
	}}
	outbound, err := openai.NewOutboundTransformer("https://upstream.test", "test-api-key")
	require.NoError(t, err)
	p := pipeline.NewFactory(executor).Pipeline(anthropic.NewInboundTransformer(), fakeStreamOutboundWrapper{Outbound: outbound, mode: pipeline.FakeStreamModeFake})

	request := &httpclient.Request{
		Method: http.MethodPost,
		URL:    "/v1/messages",
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: []byte(`{"model":"gpt-test","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello"}]}`),
	}

	result, err := p.Process(context.Background(), request)
	require.NoError(t, err)
	require.True(t, result.Stream)
	require.NotNil(t, result.EventStream)

	var types []string
	var frames []string
	var textDelta strings.Builder
	for result.EventStream.Next() {
		event := result.EventStream.Current()
		types = append(types, event.Type)
		frames = append(frames, string(event.Data))
		if event.Type == "content_block_delta" {
			textDelta.Write(event.Data)
		}
	}
	require.NoError(t, result.EventStream.Err())

	// 契约 E2：完整响应合成流的事件序列（anthropic messages union），
	// 每类事件恰一次、正文去重、终帧后无帧、usage 非零。
	require.Equal(t, 1, countSynthEvents(types, "message_start"))
	require.Equal(t, 1, countSynthEvents(types, "content_block_start"))
	require.GreaterOrEqual(t, countSynthEvents(types, "content_block_delta"), 1)
	require.Equal(t, 1, countSynthEvents(types, "content_block_stop"))
	require.Equal(t, 1, countSynthEvents(types, "message_delta"))
	require.Equal(t, 1, countSynthEvents(types, "message_stop"))
	require.Equal(t, "message_stop", types[len(types)-1], "no frame may follow message_stop")
	require.Equal(t, 1, strings.Count(textDelta.String(), "hello fake"), "synthesized text must not duplicate")
	require.True(t, hasNonZeroUsage(frames), "synthesized stream must carry non-zero usage")
}

func countSynthEvents(types []string, want string) int {
	count := 0
	for _, typ := range types {
		if typ == want {
			count++
		}
	}
	return count
}

// hasNonZeroUsage parses collected Anthropic event payloads for a usage object
// carrying at least one non-zero token count (protocol field names).
func hasNonZeroUsage(frames []string) bool {
	for _, payload := range frames {
		var frame struct {
			Usage struct {
				InputTokens  int64 `json:"input_tokens"`
				OutputTokens int64 `json:"output_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &frame); err != nil {
			continue
		}
		if frame.Usage.InputTokens > 0 || frame.Usage.OutputTokens > 0 {
			return true
		}
	}
	return false
}
