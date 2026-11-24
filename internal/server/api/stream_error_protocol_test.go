// 职责: 验证流内终结错误帧按入站协议合规——Anthropic 必须有顶层 type 判别字段,
//       OpenAI Chat 保持现状形态不被破坏。
// 关联文件:
//   internal/server/api/chat.go
//   internal/server/api/sse_encoder.go
// 注意事项:
//   本缺陷由本网关实测复现: OpenAI 形态错误帧经 Anthropic 流下发时, 严格 SDK 按
//   顶层 type 判别联合失败(invalid_union)并断流。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/llm/httpclient"
)

func TestFormatStreamErrorFor_AnthropicHasTopLevelType(t *testing.T) {
	err := errors.New("upstream provider request failed")

	result := FormatStreamErrorFor(StreamProtocolAnthropic, context.Background(), err)
	data, marshalErr := json.Marshal(result)
	require.NoError(t, marshalErr)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(data, &parsed))

	// This is the assertion the live crash violated: the Anthropic SDK dispatches
	// its event union on the top-level `type` discriminator.
	assert.Equal(t, "error", parsed["type"], "Anthropic in-band error must carry top-level type=error")

	errObj, ok := parsed["error"].(map[string]any)
	require.True(t, ok, "Anthropic error frame must carry an error object")
	assert.Equal(t, "upstream provider request failed", errObj["message"])
	assert.Equal(t, "server_error", errObj["type"])
}

func TestFormatStreamErrorFor_OpenAIUnchanged(t *testing.T) {
	err := errors.New("boom")
	got := FormatStreamErrorFor(StreamProtocolOpenAI, context.Background(), err)
	want := FormatStreamError(context.Background(), err)

	dataGot, _ := json.Marshal(got)
	dataWant, _ := json.Marshal(want)
	require.JSONEq(t, string(dataWant), string(dataGot), "OpenAI Chat error shape must stay byte-compatible")

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(dataGot, &parsed))
	assert.NotContains(t, parsed, "type", "OpenAI shape keeps type nested under error, not at root")
}

func TestWriteSSEStream_AnthropicErrorFrameIsConformant(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	upstreamErr := &llmUpstreamError{message: "provider stalled"}
	stream := &errorAfterStream{err: upstreamErr}

	h := &ChatCompletionHandlers{streamProtocol: StreamProtocolAnthropic}
	h.writeSSEStream(c, stream)

	body := w.Body.String()
	require.Contains(t, body, "event:error\n", "error frame must use the error event name")
	// Parse the data line and assert the discriminator is present.
	var frame map[string]any
	found := false
	for _, line := range splitSSEDataLines(body) {
		if json.Unmarshal([]byte(line), &frame) == nil {
			if v, ok := frame["type"]; ok && v == "error" {
				found = true
				break
			}
		}
	}
	require.True(t, found, "Anthropic error frame data must contain a top-level type discriminator; body=%s", body)
}

type llmUpstreamError struct{ message string }

func (e *llmUpstreamError) Error() string { return e.message }

func splitSSEDataLines(body string) []string {
	out := make([]string, 0)
	for _, line := range splitLines(body) {
		const prefix = "data: "
		if len(line) >= len(prefix) && line[:len(prefix)] == prefix {
			out = append(out, line[len(prefix):])
		}
	}

	return out
}

func splitLines(s string) []string {
	out := make([]string, 0)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}

	return out
}

var _ = httpclient.StreamEvent{}
