package orchestrator

import (
	"context"
	"net/http"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/transformer/openai"
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

// fakeStreamModeCandidate builds a candidate whose single model "m" resolves to
// the given channel policy, optional per-model override, and outbound format.
// An empty overrideMode means "no per-model override configured".
func fakeStreamModeCandidate(channelMode, overrideMode objects.FakeStreamMode, outboundFormat string) *ChannelModelsCandidate {
	ch := &biz.Channel{Channel: &ent.Channel{Policies: objects.ChannelPolicies{FakeStream: channelMode}}}
	if overrideMode != "" {
		ch.Channel.Settings = &objects.ChannelSettings{
			ModelProtocols: []objects.ModelProtocol{
				{Model: "m", APIFormats: []string{outboundFormat}, FakeStream: overrideMode},
			},
		}
	}
	return &ChannelModelsCandidate{
		Channel:         ch,
		Models:          []biz.ChannelModelEntry{{RequestModel: "m", ActualModel: "m"}},
		APIFormat:       outboundFormat,
		modelAPIFormats: []string{outboundFormat},
	}
}

const wsFormat = "openai/responses-ws"

func TestEffectiveFakeStreamMode(t *testing.T) {
	chat := llm.APIFormatOpenAIChatCompletion.String()
	responsesHTTP := llm.APIFormatOpenAIResponse.String()
	compact := llm.APIFormatOpenAIResponseCompact.String()

	tests := []struct {
		name       string
		candidate  *ChannelModelsCandidate
		modelIndex int
		want       objects.FakeStreamMode
	}{
		// 存量渠道级（无 override）——有效模式必须 == HEAD fakeStreamOf。
		{name: "channel fake chat", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, "", chat), want: objects.FakeStreamModeFake},
		{name: "channel placeholder chat", candidate: fakeStreamModeCandidate(objects.FakeStreamModePlaceholder, "", chat), want: objects.FakeStreamModePlaceholder},
		{name: "channel off chat", candidate: fakeStreamModeCandidate(objects.FakeStreamModeOff, "", chat), want: objects.FakeStreamModeOff},
		// per-model override 双向。
		{name: "override off wins over channel fake", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, objects.FakeStreamModeOff, chat), want: objects.FakeStreamModeOff},
		{name: "override fake wins over channel off", candidate: fakeStreamModeCandidate(objects.FakeStreamModeOff, objects.FakeStreamModeFake, chat), want: objects.FakeStreamModeFake},
		{name: "override placeholder wins over channel off", candidate: fakeStreamModeCandidate(objects.FakeStreamModeOff, objects.FakeStreamModePlaceholder, chat), want: objects.FakeStreamModePlaceholder},
		// E-3：枚举外垃圾 override 值继承渠道级（骨架下即绿，属回归锁，非主红）。
		{name: "unknown override value inherits channel", candidate: fakeStreamModeCandidate(objects.FakeStreamModePlaceholder, objects.FakeStreamMode("yes"), chat), want: objects.FakeStreamModePlaceholder},
		// 端点闸：仅 responses-ws 把 fake 降 placeholder（契约 P2 单元素集合）。
		{name: "ws bound channel fake -> placeholder", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, "", wsFormat), want: objects.FakeStreamModePlaceholder},
		{name: "ws bound channel placeholder stays placeholder", candidate: fakeStreamModeCandidate(objects.FakeStreamModePlaceholder, "", wsFormat), want: objects.FakeStreamModePlaceholder},
		{name: "ws bound channel off stays off", candidate: fakeStreamModeCandidate(objects.FakeStreamModeOff, "", wsFormat), want: objects.FakeStreamModeOff},
		{name: "ws bound override fake -> placeholder", candidate: fakeStreamModeCandidate(objects.FakeStreamModeOff, objects.FakeStreamModeFake, wsFormat), want: objects.FakeStreamModePlaceholder},
		// 防过修：responses(HTTP) 与 compact 不降级。
		{name: "responses HTTP channel fake stays fake", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, "", responsesHTTP), want: objects.FakeStreamModeFake},
		{name: "compact channel fake stays fake", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, "", compact), want: objects.FakeStreamModeFake},
		// 退化输入。
		{name: "nil candidate -> off", candidate: nil, want: objects.FakeStreamModeOff},
		{name: "modelIndex out of range falls back to 0", candidate: fakeStreamModeCandidate(objects.FakeStreamModeFake, "", wsFormat), modelIndex: 5, want: objects.FakeStreamModePlaceholder},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, effectiveFakeStreamMode(tt.candidate, tt.modelIndex))
		})
	}
}

func TestFakeStreamDecisionModeEndpointFilter(t *testing.T) {
	// 端点闸 fake→placeholder 在决策层是等价无操作（chat.go:178 对二者同样提交占位），
	// 故 ws 绑定的 fake 候选仍贡献非 off（placeholder）——这是正确的（placeholder 在 ws 上物理可用）。
	wsFake := fakeStreamModeCandidate(objects.FakeStreamModeFake, "", wsFormat)
	require.Equal(t, objects.FakeStreamModePlaceholder, fakeStreamDecisionMode([]*ChannelModelsCandidate{wsFake}, objects.FakeStreamDecisionScopeAnyCandidate))
	// 真正改变决策的是 per-model override=off：候选贡献 off → 走同步路径不提交占位。
	chatOverrideOff := fakeStreamModeCandidate(objects.FakeStreamModeFake, objects.FakeStreamModeOff, llm.APIFormatOpenAIChatCompletion.String())
	require.Equal(t, objects.FakeStreamModeOff, fakeStreamDecisionMode([]*ChannelModelsCandidate{chatOverrideOff}, objects.FakeStreamDecisionScopeAnyCandidate))
}

// E-4：per-model override 必须命中映射后的 ActualModel（别名场景），与
// forcedAPIFormatsForCandidate 的三名展开先例一致（select_endpoints.go:136-177）。
// 独立构造（fakeStreamModeCandidate 硬编码 RequestModel==ActualModel，无法表达别名）。
func TestEffectiveFakeStreamModeMappedAlias(t *testing.T) {
	chat := llm.APIFormatOpenAIChatCompletion.String()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{Channel: &ent.Channel{
			Policies: objects.ChannelPolicies{FakeStream: objects.FakeStreamModeOff},
			Settings: &objects.ChannelSettings{ModelProtocols: []objects.ModelProtocol{
				{Model: "real-model", APIFormats: []string{chat}, FakeStream: objects.FakeStreamModeFake},
			}},
		}},
		Models:          []biz.ChannelModelEntry{{RequestModel: "alias", ActualModel: "real-model"}},
		APIFormat:       chat,
		modelAPIFormats: []string{chat},
	}
	require.Equal(t, objects.FakeStreamModeFake, effectiveFakeStreamMode(candidate, 0))
}

func TestFakeStreamModeMasterSwitchGating(t *testing.T) {
	candidate := fakeStreamModeCandidate(objects.FakeStreamModeFake, "", llm.APIFormatOpenAIChatCompletion.String())
	enabled := &PersistentOutboundTransformer{state: &PersistenceState{
		CurrentCandidate:        candidate,
		FakeStreamHandlerEnabled: true,
	}}
	require.Equal(t, pipeline.FakeStreamModeFake, enabled.FakeStreamMode())
	gated := &PersistentOutboundTransformer{state: &PersistenceState{CurrentCandidate: candidate}}
	require.Equal(t, pipeline.FakeStreamModeOff, gated.FakeStreamMode(),
		"without the handler-enabled marker the master switch is off and no mode may leak through")
}

// outboundStreamCapturer records the stream flag of every raw outbound attempt,
// which is the externally visible consequence of the master-switch marker.
type outboundStreamCapturer struct {
	pipeline.DummyMiddleware

	attempts []bool
}

func (c *outboundStreamCapturer) Name() string { return "test-outbound-stream-capturer" }

func (c *outboundStreamCapturer) OnOutboundRawRequest(ctx context.Context, request *httpclient.Request) (*httpclient.Request, error) {
	c.attempts = append(c.attempts, gjson.GetBytes(request.Body, "stream").Bool())
	return request, nil
}

// TestFakeStreamHandlerFlagCapturedThroughProcess proves the per-request
// master-switch marker is captured from the context by the real entry point, the
// way the handler installs it, rather than being handed to the state by a test.
func TestFakeStreamHandlerFlagCapturedThroughProcess(t *testing.T) {
	run := func(t *testing.T, withDecision, wantStream bool) {
		t.Helper()

		ctx := authz.WithTestBypass(context.Background())
		client := enttest.NewEntClient(t, "sqlite3", "file:fake-stream-flag?mode=memory&_fk=0")
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		ctx = ent.NewContext(ctx, client)

		project := createTestProject(t, ctx, client)
		ch := createTestChannel(t, ctx, client)
		ch.Policies.FakeStream = objects.FakeStreamModeFake

		outbound, err := openai.NewOutboundTransformer(ch.BaseURL, ch.Credentials.APIKey)
		require.NoError(t, err)

		executor := &mockExecutor{
			response: &httpclient.Response{
				StatusCode: 200,
				Body:       buildMockOpenAIResponse("chatcmpl-flag", "gpt-4", "ok", 1, 2),
				Headers:    http.Header{"Content-Type": {"application/json"}},
			},
			streamEvents: []*httpclient.StreamEvent{
				{Data: []byte(`{"id":"chatcmpl-flag","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"}}]}`)},
				{Data: []byte(`{"id":"chatcmpl-flag","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)},
			},
		}

		bizChannel := &biz.Channel{Channel: ch, Outbound: outbound}
		processor := newTestOrchestrator(t,
			&staticChannelSelector{candidates: channelsToTestCandidates([]*biz.Channel{bizChannel}, "gpt-4")},
			client, executor)
		capturer := &outboundStreamCapturer{}
		processor.Middlewares = append(processor.Middlewares, capturer)

		if withDecision {
			ctx = WithFakeStreamDecision(ctx, func(objects.FakeStreamMode) {})
		}

		ctx = contexts.WithProjectID(ctx, project.ID)
		_, _ = processor.Process(ctx, buildTestRequest("gpt-4", "Hello!", true))

		// The direction is asserted here, not left for execution time: without
		// the callback the marker stays false and the upstream attempt must keep
		// streaming; with it the marker is captured and Mode B must rewrite the
		// attempt to non-streaming. Len==1 pins that this is the first attempt, so
		// a failover retry cannot silently supply the observed value instead.
		require.Len(t, capturer.attempts, 1, "exactly one outbound attempt is expected in this fixture")
		require.Equal(t, wantStream, capturer.attempts[0],
			"the per-request master-switch marker captured from the context is what gates the upstream stream rewrite")
		require.EqualValues(t, 1, executor.requestCalls.Load(), "the attempt must reach the executor")
	}

	run(t, true, false)
	run(t, false, true)
}
