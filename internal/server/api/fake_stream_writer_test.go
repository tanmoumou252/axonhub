// 职责: Mode A 假流式写入器的时序与契约测试：先提交 200/SSE/占位帧，再心跳，最后无缝拼接真实流。
// 关联文件:
//
//	internal/server/api/fake_stream_writer.go
//	internal/server/api/chat.go
package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm/httpclient"
)

// syncResponseRecorder serializes recorder access between the writer goroutine
// and the test's polling reads; httptest.ResponseRecorder is not thread-safe.
type syncResponseRecorder struct {
	mu  sync.Mutex
	rec *httptest.ResponseRecorder
}

func newSyncResponseRecorder() *syncResponseRecorder {
	return &syncResponseRecorder{rec: httptest.NewRecorder()}
}

func (s *syncResponseRecorder) Header() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Header()
}

func (s *syncResponseRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Write(p)
}

func (s *syncResponseRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.WriteHeader(code)
}

func (s *syncResponseRecorder) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.Flush()
}

func (s *syncResponseRecorder) Unwrap() http.ResponseWriter { return s.rec }

func (s *syncResponseRecorder) body() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Body.String()
}

// code and contentType read under the same lock the writer goroutine holds when
// it calls WriteHeader/Write, so the test's polling reads never race the recorder.
// Reading the header value inside the lock (rather than returning the map) avoids
// touching the header map outside the critical section.
func (s *syncResponseRecorder) code() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Code
}

func (s *syncResponseRecorder) contentType() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Header().Get("Content-Type")
}

type fakeStreamGate struct {
	release chan struct{}
	idx     int
	events  []*httpclient.StreamEvent
}

func (g *fakeStreamGate) Next() bool {
	if g.idx < 0 {
		<-g.release
		g.idx = 0
	}
	return g.idx < len(g.events)
}

func (g *fakeStreamGate) Current() *httpclient.StreamEvent {
	cur := g.events[g.idx]
	g.idx++
	return cur
}
func (g *fakeStreamGate) Err() error   { return nil }
func (g *fakeStreamGate) Close() error { return nil }

func TestFakeSSEStream_OpenAI_PlaceholderBeforeResultThenSplice(t *testing.T) {
	w := newSyncResponseRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	h := &ChatCompletionHandlers{
		ChatCompletionOrchestrator: &orchestrator.ChatCompletionOrchestrator{SystemService: nil},
		inputUsageCache:            newInputUsageCache(),
		sseKeepAlive:               SSEKeepAliveConfig{Enabled: true, Interval: 5 * time.Millisecond},
		sseHeartbeatFormat:         sseHeartbeatOpenAI,
		streamProtocol:             StreamProtocolOpenAI,
	}
	gate := &fakeStreamGate{release: make(chan struct{}), idx: -1, events: []*httpclient.StreamEvent{
		{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"real"},"finish_reason":null}]}`)},
		{Data: []byte("[DONE]")},
	}}
	resultCh := make(chan fakeStreamResult, 1)
	processCtx, cancelProcess := context.WithCancel(c.Request.Context())
	defer cancelProcess()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{Body: []byte(`{"stream":true}`)}, nil, nil, time.Now())
	}()

	require.Eventually(t, func() bool {
		return w.code() == http.StatusOK && strings.Contains(w.contentType(), "text/event-stream") && strings.Contains(w.body(), `"delta":{}`)
	}, 100*time.Millisecond, time.Millisecond, "placeholder must be committed before Process returns")
	require.Eventually(t, func() bool {
		return strings.Contains(w.body(), ": keep-alive\n\n")
	}, 200*time.Millisecond, time.Millisecond, "heartbeat must be emitted while waiting")
	select {
	case <-done:
		t.Fatal("writer returned before the real result arrived")
	default:
	}

	close(gate.release)
	resultCh <- fakeStreamResult{result: orchestrator.ChatCompletionResult{ChatCompletionStream: gate}}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not splice the real result")
	}
	body := w.body()
	require.Contains(t, body, `"content":"real"`)
	require.Contains(t, body, "data: [DONE]\n\n")
	require.Equal(t, 1, strings.Count(body, `"content":"real"`), "real content must not be duplicated")
}

func TestFakeStreamRouteEligible(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		path     string
		body     string
		eligible bool
	}{
		{"chat completions is the primary fake-stream route", true, "/v1/chat/completions", `{"stream":true}`, true},
		{"anthropic messages", true, "/v1/messages", `{"stream":true}`, true},
		{"gemini stream generate content", true, "/v1/models/gemini:streamGenerateContent", `{}`, true},
		{"legacy completions stay synchronous", true, "/v1/completions", `{"stream":true}`, false},
		{"responses stays synchronous", true, "/v1/responses", `{"stream":true}`, false},
		{"audio speech SSE stays synchronous", true, "/v1/audio/speech", `{"stream_format":"sse"}`, false},
		{"audio speech binary stays synchronous", true, "/v1/audio/speech", `{"stream_format":"audio"}`, false},
		{"non stream chat", true, "/v1/chat/completions", `{"stream":false}`, false},
		{"global switch off", false, "/v1/chat/completions", `{"stream":true}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &httpclient.Request{Path: tc.path, Body: []byte(tc.body)}
			require.Equal(t, tc.eligible, fakeStreamRouteEligible(tc.enabled, req))
		})
	}
}

func TestFakeSSEStream_GeminiPlaceholderAndHeartbeat(t *testing.T) {
	w := newSyncResponseRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/models/gemini:streamGenerateContent", nil)
	h := &ChatCompletionHandlers{
		sseKeepAlive:       SSEKeepAliveConfig{Interval: 5 * time.Millisecond},
		sseHeartbeatFormat: sseHeartbeatOpenAI,
		streamProtocol:     StreamProtocolGemini,
	}
	resultCh := make(chan fakeStreamResult, 1)
	processCtx, cancelProcess := context.WithCancel(c.Request.Context())
	defer cancelProcess()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{}, nil, nil, time.Now())
	}()
	require.Eventually(t, func() bool {
		return strings.Contains(w.body(), ": keep-alive\n\n")
	}, time.Second, time.Millisecond)
	resultCh <- fakeStreamResult{err: errors.New("stop")}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not return after result")
	}

	body := w.body()
	require.Contains(t, body, ": placeholder\n\n", "Gemini placeholder must be a comment frame")
	require.NotContains(t, body, "choices", "Gemini stream must not contain OpenAI chat delta shape")
	require.Contains(t, body, ": keep-alive\n\n", "Gemini heartbeat must be a comment frame")
}

func TestFakeSSEStream_ErrorAfterPlaceholderIsInBand(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	h := &ChatCompletionHandlers{
		sseKeepAlive:       SSEKeepAliveConfig{Interval: time.Hour},
		sseHeartbeatFormat: sseHeartbeatOpenAI,
		streamProtocol:     StreamProtocolOpenAI,
	}
	resultCh := make(chan fakeStreamResult, 1)
	resultCh <- fakeStreamResult{err: errors.New("upstream exploded")}
	processCtx, cancelProcess := context.WithCancel(c.Request.Context())
	defer cancelProcess()
	h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{Body: []byte(`{"stream":true}`)}, nil, nil, time.Now())

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `event:error`)
	require.Contains(t, w.Body.String(), "upstream exploded")
}

func TestFakeSSEStream_AnthropicErrorFrameIsConformant(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	h := &ChatCompletionHandlers{
		sseKeepAlive:       SSEKeepAliveConfig{Interval: time.Hour},
		sseHeartbeatFormat: sseHeartbeatAnthropic,
		streamProtocol:     StreamProtocolAnthropic,
	}
	resultCh := make(chan fakeStreamResult, 1)
	resultCh <- fakeStreamResult{err: &llmUpstreamError{message: "provider stalled"}}
	processCtx, cancelProcess := context.WithCancel(c.Request.Context())
	defer cancelProcess()
	h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{Body: []byte(`{"stream":true}`)}, nil, nil, time.Now())

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "event:error\n")
	require.True(t, hasSSEFrameWithTopLevelErrorType(w.Body.String()), "Anthropic fake-stream error frame must carry a top-level type discriminator; body=%s", w.Body.String())
}

func TestFakeSSEStream_ClientDisconnectCancelsProcess(t *testing.T) {
	w := newSyncResponseRecorder()
	c, _ := gin.CreateTestContext(w)
	ctx, cancel := context.WithCancel(context.Background())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	h := &ChatCompletionHandlers{
		sseKeepAlive:       SSEKeepAliveConfig{Interval: time.Hour},
		sseHeartbeatFormat: sseHeartbeatOpenAI,
		streamProtocol:     StreamProtocolOpenAI,
	}
	resultCh := make(chan fakeStreamResult, 1)
	processCtx, cancelProcess := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{Body: []byte(`{"stream":true}`)}, nil, nil, time.Now())
	}()
	require.Eventually(t, func() bool { return strings.Contains(w.body(), `"delta":{}`) }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not exit on client disconnect")
	}
	require.ErrorIs(t, processCtx.Err(), context.Canceled)
}

// recordingStream records whether Close and WaitForFinalization were invoked,
// used to assert the raced-result drain path does not skip terminal persistence.
type recordingStream struct {
	closed bool
	waited bool
}

func (s *recordingStream) Next() bool                        { return false }
func (s *recordingStream) Current() *httpclient.StreamEvent  { return nil }
func (s *recordingStream) Err() error                        { return nil }
func (s *recordingStream) Close() error                      { s.closed = true; return nil }
func (s *recordingStream) WaitForFinalization()              { s.waited = true }

// TestFakeSSEStream_DeadlineEmitsInBandError red: a request deadline after the
// placeholder was committed must surface as an in-band terminal error, not a
// silent truncation. Modern state: writer ctx.Done branch returns without
// writing an error frame.
func TestFakeSSEStream_DeadlineEmitsInBandError(t *testing.T) {
	w := newSyncResponseRecorder()
	c, _ := gin.CreateTestContext(w)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	h := &ChatCompletionHandlers{
		sseKeepAlive:       SSEKeepAliveConfig{Enabled: true, Interval: 5 * time.Millisecond},
		sseHeartbeatFormat: sseHeartbeatOpenAI,
		streamProtocol:     StreamProtocolOpenAI,
	}
	gate := &fakeStreamGate{release: make(chan struct{}), idx: -1, events: []*httpclient.StreamEvent{
		{Data: []byte(`{"choices":[{"index":0,"delta":{"content":"late"},"finish_reason":null}]}`)},
	}}
	resultCh := make(chan fakeStreamResult, 1)
	processCtx, cancelProcess := context.WithCancel(ctx)
	defer cancelProcess()
	go func() {
		h.writeFakeSSEStream(c, resultCh, processCtx, cancelProcess, &httpclient.Request{Body: []byte(`{"stream":true}`)}, nil, nil, time.Now())
		close(gate.release)
	}()
	require.Eventually(t, func() bool { return strings.Contains(w.body(), "event:error") }, 2*time.Second, time.Millisecond, "a request deadline after the placeholder was committed must surface as an in-band terminal error")
	require.NotContains(t, w.body(), `"late"`, "the gated upstream result must not splice after the deadline error frame")
}

// TestFakeSSEStream_DrainRacedResultFinalizes red: a result racing with ctx.Done
// must still be Closed so terminal persistence runs. The writer's ctx.Done branch
// delegates to drainRacedFakeStreamResult; the skeleton drains without Close, so
// the raced stream is never finalized.
func TestFakeSSEStream_DrainRacedResultFinalizes(t *testing.T) {
	resultCh := make(chan fakeStreamResult, 1)
	stream := &recordingStream{}
	resultCh <- fakeStreamResult{result: orchestrator.ChatCompletionResult{ChatCompletionStream: stream}}
	drainRacedFakeStreamResult(resultCh)
	require.True(t, stream.closed, "a result racing with ctx.Done must still be Closed so terminal persistence runs")
	require.True(t, stream.waited, "WaitForFinalization contract must not be skipped for a raced result")
	require.Len(t, resultCh, 0, "the raced result must be consumed from the channel")
}
