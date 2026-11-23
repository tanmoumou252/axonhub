// 职责: 假流式全栈端到端回归：Anthropic Mode B 合成事件序列、any_candidate 提前占位、
//
//	first_candidate 对照边界、上游流中断后的协议合规带内错误帧。
//
// 关联文件:
//
//	internal/server/api/fake_stream_writer.go
//	internal/server/api/chat.go
//	internal/server/orchestrator/fake_stream_decision.go
//
// 注意事项:
//
//	handler 必须置 StreamWriter=nil（NewChatCompletionHandlers 默认 OpenAI writer 会绕过
//	FormatStreamErrorFor 的协议感知形状）；假上游返回完整 JSON 时 Content-Type 必须是 application/json。
//	流中断用例覆盖两类传输中断：idle timeout（ErrStreamIdleTimeout 分类分支）与
//	Windows 原生 TCP RST（Winsock WSAECONNRESET(10054)，由分类器 winsock 分支识别，
//	与 POSIX 编号的 syscall.ECONNRESET(NTSTATUS) 不同值）。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/datastorage"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
)

const fakeStreamE2EModel = "gpt-test"

const fakeStreamAnthropicRequestBody = `{"model":"gpt-test","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello"}]}`

const fakeStreamUpstreamJSONBody = `{"id":"chatcmpl-fake","object":"chat.completion","created":1760000000,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"hello fake"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`

// fakeStreamE2EUpstream pairs one httptest upstream with its channel policies.
type fakeStreamE2EUpstream struct {
	handler  http.HandlerFunc
	policies objects.ChannelPolicies
}

type fakeStreamE2EFixture struct {
	ctx         context.Context
	client      *ent.Client
	channelRows []*ent.Channel
	upstreams   []*httptest.Server
}

func newFakeStreamE2EFixture(t *testing.T, upstreams ...fakeStreamE2EUpstream) *fakeStreamE2EFixture {
	t.Helper()
	dbName := strings.ReplaceAll(t.Name(), "/", "_")
	client := enttest.NewEntClient(t, "sqlite3", "file:fake-stream-e2e-"+dbName+"?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	project, err := client.Project.Create().SetName("fake-stream-e2e").Save(ctx)
	require.NoError(t, err)
	_, err = client.DataStorage.Create().SetName("fake-stream-e2e-storage").SetDescription("fake stream test storage").SetPrimary(true).SetType(datastorage.TypeDatabase).SetSettings(&objects.DataStorageSettings{}).SetStatus(datastorage.StatusActive).Save(ctx)
	require.NoError(t, err)

	f := &fakeStreamE2EFixture{ctx: contexts.WithProjectID(ctx, project.ID), client: client}
	for i, spec := range upstreams {
		server := httptest.NewServer(spec.handler)
		f.upstreams = append(f.upstreams, server)
		channelRow, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName(fmt.Sprintf("fake-stream-e2e-channel-%d", i)).SetBaseURL(server.URL).SetCredentials(objects.ChannelCredentials{APIKey: "synthetic"}).SetSupportedModels([]string{fakeStreamE2EModel}).SetDefaultTestModel(fakeStreamE2EModel).SetStatus(channel.StatusEnabled).SetPolicies(spec.policies).SetEndpoints([]objects.ChannelEndpoint{{APIFormat: llm.APIFormatOpenAIChatCompletion.String(), BaseURL: server.URL, Transport: objects.ChannelEndpointTransportHTTP}}).Save(ctx)
		require.NoError(t, err)
		f.channelRows = append(f.channelRows, channelRow)
	}
	t.Cleanup(func() {
		for _, server := range f.upstreams {
			server.CloseClientConnections()
			server.Close()
		}
	})
	return f
}

func (f *fakeStreamE2EFixture) anthropicHandler(t *testing.T) *ChatCompletionHandlers {
	t.Helper()
	channelService, requestService, systemService, usageService := setupSpeechAPITestServices(t, f.client)
	promptService := biz.NewPromptService(biz.PromptServiceParams{Ent: f.client})
	storage, err := f.client.DataStorage.Query().Only(f.ctx)
	require.NoError(t, err)
	require.NoError(t, systemService.SetDefaultDataStorageID(f.ctx, storage.ID))
	promptProtection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{CacheConfig: xcache.Config{Mode: xcache.ModeMemory}, Ent: f.client})
	t.Cleanup(promptProtection.Stop)
	require.NoError(t, systemService.SetRetryPolicy(f.ctx, &biz.RetryPolicy{Enabled: true, MaxChannelRetries: 3, MaxSingleChannelRetries: 1, RetryDelayMs: 0, LoadBalancerStrategy: "adaptive"}))
	price := decimal.NewFromFloat(0.01)
	built := make([]*biz.Channel, 0, len(f.channelRows))
	for _, row := range f.channelRows {
		_, err = f.client.ChannelModelPrice.Create().SetChannelID(row.ID).SetModelID(fakeStreamE2EModel).SetPrice(objects.ModelPrice{Items: []objects.ModelPriceItem{{ItemCode: objects.PriceItemCodeUsage, Pricing: objects.Pricing{Mode: objects.PricingModeUsagePerUnit, UsagePerUnit: &price}}, {ItemCode: objects.PriceItemCodeCompletion, Pricing: objects.Pricing{Mode: objects.PricingModeUsagePerUnit, UsagePerUnit: &price}}}}).SetReferenceID(fmt.Sprintf("fake-stream-e2e-price-%d", row.ID)).Save(f.ctx)
		require.NoError(t, err)
		channelBuilt, err := channelService.GetChannel(f.ctx, row.ID)
		require.NoError(t, err)
		channelService.PreloadModelPricesForTest(f.ctx, channelBuilt)
		built = append(built, channelBuilt)
	}
	channelService.SetEnabledChannelsForTest(built)
	candidates := make([]*orchestrator.ChannelModelsCandidate, 0, len(built))
	for _, channelBuilt := range built {
		candidates = append(candidates, &orchestrator.ChannelModelsCandidate{Channel: channelBuilt, Models: []biz.ChannelModelEntry{{RequestModel: fakeStreamE2EModel, ActualModel: fakeStreamE2EModel, Source: "direct"}}})
	}
	orch := orchestrator.NewChatCompletionOrchestrator(channelService, nil, requestService, httpclient.NewHttpClient(), anthropic.NewInboundTransformer(), systemService, usageService, promptService, nil, promptProtection, biz.NewLiveStreamRegistry(), orchestrator.NewChannelLimiterManager(), nil, orchestrator.PluginIntegration{}).WithChannelSelector(&streamDeliveryE2ESelector{candidates: candidates})
	h := NewChatCompletionHandlers(orch)
	// StreamWriter 必须为 nil：默认 OpenAI writer 会绕过 FormatStreamErrorFor 的协议感知形状。
	h.StreamWriter = nil
	h.fakeStreamEnabled = true
	h.sseHeartbeatFormat = sseHeartbeatAnthropic
	h.streamProtocol = StreamProtocolAnthropic
	h.sseKeepAlive = SSEKeepAliveConfig{Enabled: true, Interval: time.Hour}
	return h
}

func (f *fakeStreamE2EFixture) postMessages(t *testing.T, h *ChatCompletionHandlers, body string) (int, string) {
	t.Helper()
	return f.postMessagesWithContext(t, h, body, func(ctx context.Context) context.Context { return ctx })
}

// postMessagesWithContext allows tests to inject upstream-specific context
// settings (e.g. a short idle timeout) into the real gin request path.
func (f *fakeStreamE2EFixture) postMessagesWithContext(t *testing.T, h *ChatCompletionHandlers, body string, decorate func(context.Context) context.Context) (int, string) {
	t.Helper()
	engine := gin.New()
	engine.Use(f.testContext)
	engine.POST("/v1/messages", func(c *gin.Context) {
		c.Request = c.Request.WithContext(decorate(c.Request.Context()))
		h.ChatCompletion(c)
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, server.URL+"/v1/messages", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

func (f *fakeStreamE2EFixture) testContext(c *gin.Context) {
	ctx := authz.WithTestBypass(c.Request.Context())
	ctx = ent.NewContext(ctx, f.client)
	projectID, ok := contexts.GetProjectID(f.ctx)
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	ctx = contexts.WithProjectID(ctx, projectID)
	c.Request = c.Request.WithContext(ctx)
	c.Next()
}

func fakeStreamCompleteJSONUpstream(body string, seen chan<- []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		seen <- payload
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

func fakeStreamErrorUpstream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":{"message":"upstream unavailable","type":"server_error","code":"server_error"}}`)
	}
}

func fakeStreamStallUpstream(payload string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, payload)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
}

func assertAnthropicSynthesisWire(t *testing.T, wire string) {
	t.Helper()
	events := sseEventNames(wire)
	require.Equal(t, 1, countWireEvents(events, "message_start"))
	require.Equal(t, 1, countWireEvents(events, "content_block_start"))
	require.GreaterOrEqual(t, countWireEvents(events, "content_block_delta"), 1)
	require.Equal(t, 1, countWireEvents(events, "content_block_stop"))
	require.Equal(t, 1, countWireEvents(events, "message_delta"))
	require.Equal(t, 1, countWireEvents(events, "message_stop"))
	require.Equal(t, "message_stop", events[len(events)-1], "no frame may follow message_stop")
	require.Contains(t, wire, `"text_delta"`)
	require.Equal(t, 1, strings.Count(wire, "hello fake"), "synthesized text must not duplicate")
	// usage 透传存在性锁定；token 具体映射值的红/绿仲裁按未验证假设 A3 出口执行。
	require.Contains(t, wire, `"usage"`)
	require.NotContains(t, wire, "event:error")
}

// sseEventNames extracts SSE event names; the looplj encoder writes `event:`
// without a space while the handwritten heartbeat frame uses `event: ping`.
func sseEventNames(wire string) []string {
	var names []string
	for _, line := range strings.Split(wire, "\n") {
		if rest, ok := strings.CutPrefix(line, "event:"); ok {
			names = append(names, strings.TrimSpace(rest))
		}
	}
	return names
}

func countWireEvents(names []string, want string) int {
	count := 0
	for _, name := range names {
		if name == want {
			count++
		}
	}
	return count
}

func hasSSEFrameWithTopLevelErrorType(wire string) bool {
	for _, line := range strings.Split(wire, "\n") {
		const prefix = "data: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		var frame map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &frame) == nil {
			if frame["type"] == "error" {
				return true
			}
		}
	}
	return false
}

func waitForUpstreamPayload(t *testing.T, seen <-chan []byte) {
	t.Helper()
	select {
	case payload := <-seen:
		require.Contains(t, string(payload), `"stream":false`)
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received the non-stream request")
	}
}

func TestFakeStreamE2E_AnthropicSynthesisFromNonStream(t *testing.T) {
	seen := make(chan []byte, 1)
	f := newFakeStreamE2EFixture(t, fakeStreamE2EUpstream{handler: fakeStreamCompleteJSONUpstream(fakeStreamUpstreamJSONBody, seen), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeFake}})
	h := f.anthropicHandler(t)

	status, wire := f.postMessages(t, h, fakeStreamAnthropicRequestBody)

	require.Equal(t, http.StatusOK, status)
	assertAnthropicSynthesisWire(t, wire)
	waitForUpstreamPayload(t, seen)
}

func TestFakeStreamE2E_AnyCandidateScopeCommitsPlaceholderBeforeFailover(t *testing.T) {
	seen := make(chan []byte, 1)
	f := newFakeStreamE2EFixture(t,
		fakeStreamE2EUpstream{handler: fakeStreamErrorUpstream(), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeOff}},
		fakeStreamE2EUpstream{handler: fakeStreamCompleteJSONUpstream(fakeStreamUpstreamJSONBody, seen), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeFake}},
	)
	h := f.anthropicHandler(t)
	h.fakeStreamScope = objects.FakeStreamDecisionScopeAnyCandidate

	status, wire := f.postMessages(t, h, fakeStreamAnthropicRequestBody)

	require.Equal(t, http.StatusOK, status)
	// any_candidate：首候选 off 也提前提交占位（协议合法 ping 帧先于真实内容）。
	require.Contains(t, wire, "event: ping")
	assertAnthropicSynthesisWire(t, wire)
	waitForUpstreamPayload(t, seen)
}

func TestFakeStreamE2E_FirstCandidateScopeKeepsSynchronousPath(t *testing.T) {
	seen := make(chan []byte, 1)
	f := newFakeStreamE2EFixture(t,
		fakeStreamE2EUpstream{handler: fakeStreamErrorUpstream(), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeOff}},
		fakeStreamE2EUpstream{handler: fakeStreamCompleteJSONUpstream(fakeStreamUpstreamJSONBody, seen), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeFake}},
	)
	h := f.anthropicHandler(t)
	// scope 零值 = first_candidate：首候选 off，不提交占位。

	status, wire := f.postMessages(t, h, fakeStreamAnthropicRequestBody)

	require.Equal(t, http.StatusOK, status)
	require.NotContains(t, wire, "event: ping", "first_candidate scope must not commit a placeholder when the first candidate is off")
	assertAnthropicSynthesisWire(t, wire)
	waitForUpstreamPayload(t, seen)
}

func TestFakeStreamE2E_AnthropicUpstreamInterruptedEmitsTypedErrorFrame(t *testing.T) {
	partial := "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n"
	f := newFakeStreamE2EFixture(t, fakeStreamE2EUpstream{handler: fakeStreamStallUpstream(partial), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModePlaceholder}})
	h := f.anthropicHandler(t)

	// Upstream idle timeout after partial content is a genuine transport-level
	// stream interruption exercising the ErrStreamIdleTimeout classification
	// branch; the sibling TCP-reset case covers the winsock reset branch.
	status, wire := f.postMessagesWithContext(t, h, fakeStreamAnthropicRequestBody, func(ctx context.Context) context.Context {
		return httpclient.WithStreamIdleTimeout(ctx, 200*time.Millisecond)
	})

	require.Equal(t, http.StatusOK, status)
	// 占位先提交（placeholder 模式），真实内容透传后中断，错误帧带内下发（契约 E1）。
	require.Contains(t, wire, "event: ping")
	require.Contains(t, wire, `"text_delta"`)
	require.Contains(t, wire, "event:error")
	require.Contains(t, wire, "upstream_stream_interrupted")
	require.True(t, hasSSEFrameWithTopLevelErrorType(wire), "Anthropic error frame data must carry a top-level type discriminator; wire=%s", wire)
}

// fakeStreamRSTUpstream writes an SSE prefix then aborts the TCP connection with
// SetLinger(0), producing a real connection reset (Winsock WSAECONNRESET on
// Windows, ECONNRESET on Unix) instead of a clean EOF.
func fakeStreamRSTUpstream(payload string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, payload)
		w.(http.Flusher).Flush()
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_ = buf.Flush()
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}
}

func TestFakeStreamE2E_AnthropicTCPResetEmitsClassifiedErrorFrame(t *testing.T) {
	partial := "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n"
	f := newFakeStreamE2EFixture(t, fakeStreamE2EUpstream{handler: fakeStreamRSTUpstream(partial), policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModePlaceholder}})
	h := f.anthropicHandler(t)

	// 契约 W2：真实 TCP RST（Windows Winsock WSAECONNRESET）必须被分类为
	// upstream_stream_interrupted，并以协议合规的带内错误帧下发（用户 5 次
	// invalid_union 复现路径的真值回归）。
	status, wire := f.postMessages(t, h, fakeStreamAnthropicRequestBody)

	require.Equal(t, http.StatusOK, status)
	require.Contains(t, wire, "event: ping")
	require.Contains(t, wire, `"text_delta"`)
	require.Contains(t, wire, "event:error")
	require.Contains(t, wire, "upstream_stream_interrupted")
	require.True(t, hasSSEFrameWithTopLevelErrorType(wire), "Anthropic error frame data must carry a top-level type discriminator; wire=%s", wire)
}
