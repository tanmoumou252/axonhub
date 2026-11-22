// 职责: 按 (channelID, actualModel) 身份记录跨请求熔断信号，按 ErrorScope 归属分流计数并管理探针租约。
// 关联文件:
//   internal/server/biz/model_circuit_breaker.go
//   internal/server/orchestrator/error_scope.go
//   internal/server/orchestrator/candidates.go
// 注意事项:
//   熔断键 = (channelID, actualModel)，与打分侧同源；禁改回请求别名。
//   渠道级失败计在候选首个 actual model（scoreKey），模型级失败计在本次尝试的 actual model。
package orchestrator

import (
	"context"
	"errors"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

func withModelCircuitBreaker(outbound *PersistentOutboundTransformer, modelCircuitBreaker *biz.ModelCircuitBreaker) pipeline.Middleware {
	return &modelCircuitBreakerTracker{
		outbound:            outbound,
		modelCircuitBreaker: modelCircuitBreaker,
	}
}

type modelCircuitBreakerTracker struct {
	pipeline.DummyMiddleware

	outbound            *PersistentOutboundTransformer
	modelCircuitBreaker *biz.ModelCircuitBreaker

	// attempt is the (channel, actual model) identity of the outbound attempt in
	// flight. It is resolved once at attempt start and reused by the probe lease,
	// the success/error record and the lease release, so a model-index advance
	// between attempt start and the terminal callback cannot move the signal onto
	// a different key than the one that was probed.
	attempt breakerAttemptKeys

	probeActive bool
}

// breakerAttemptKeys is the circuit-breaker identity of one outbound attempt:
// modelID is the upstream model actually attempted now (gates the probe and takes
// model-scoped failures), scoreKey is the model the selector reads for this
// candidate on the next request (Models[0]) and takes channel-scoped failures.
// The two coincide at the first attempt and may differ once PrepareForRetry or a
// responses continuation advances the index; keeping them explicit is what stops
// a channel verdict from being written onto a key routing never consults.
type breakerAttemptKeys struct {
	channelID int
	modelID   string
	scoreKey  string
}

// resolveBreakerKeys captures the breaker identity for the candidate being
// attempted. The unit is (ChannelID, ActualModelID) — the same pair
// aggregateChannelModelCandidates deduplicates on. The client-visible alias is
// only a fallback for candidates that carry no resolved model, which keeps the
// pre-resolution paths (and their tests) intact.
func (m *modelCircuitBreakerTracker) resolveBreakerKeys() breakerAttemptKeys {
	keys := breakerAttemptKeys{}

	if m.outbound == nil || m.outbound.state == nil {
		return keys
	}

	if channel := m.outbound.GetCurrentChannel(); channel != nil {
		keys.channelID = channel.ID
	}

	candidate := m.outbound.state.CurrentCandidate
	if candidate != nil {
		// The key the selector will consult for this candidate on the next request.
		keys.scoreKey = candidate.FirstActualModel()

		index := m.outbound.state.CurrentModelIndex
		if index >= 0 && index < len(candidate.Models) {
			keys.modelID = candidate.Models[index].ActualModel
		}
	}

	if keys.modelID == "" {
		keys.modelID = m.outbound.GetRequestedModel()
	}

	if keys.scoreKey == "" {
		keys.scoreKey = keys.modelID
	}

	return keys
}

// keys returns the captured identity, resolving it on demand for a callback that
// arrives without a preceding request hook.
func (m *modelCircuitBreakerTracker) keys() breakerAttemptKeys {
	if m.attempt.channelID != 0 && m.attempt.modelID != "" {
		return m.attempt
	}

	return m.resolveBreakerKeys()
}

func (m *modelCircuitBreakerTracker) Name() string {
	return "model-circuit-breaker-tracker"
}

func (m *modelCircuitBreakerTracker) OnOutboundRawRequest(ctx context.Context, request *httpclient.Request) (*httpclient.Request, error) {
	if m.outbound == nil || m.outbound.state == nil ||
		m.outbound.state.RoutingPolicy.LoadBalancerStrategy != biz.LoadBalancerStrategyCircuitBreaker ||
		m.modelCircuitBreaker == nil {
		return request, nil
	}

	m.attempt = m.resolveBreakerKeys()
	if m.attempt.channelID == 0 || m.attempt.modelID == "" {
		return request, nil
	}

	stats := m.modelCircuitBreaker.GetModelCircuitBreakerStats(ctx, m.attempt.channelID, m.attempt.modelID)
	if stats == nil || stats.State != biz.StateOpen {
		return request, nil
	}

	if !m.modelCircuitBreaker.TryBeginProbe(ctx, m.attempt.channelID, m.attempt.modelID) {
		log.Debug(ctx, "skipping candidate by circuit breaker: probe conditions not met or another probe in progress",
			log.Int("channel_id", m.attempt.channelID),
			log.String("model_id", m.attempt.modelID),
		)

		return nil, errSkipCandidateByCircuitBreaker
	}

	m.probeActive = true

	return request, nil
}

func (m *modelCircuitBreakerTracker) OnOutboundLlmResponse(ctx context.Context, response *llm.Response) (*llm.Response, error) {
	if !m.shouldTrack() {
		return response, nil
	}

	m.releaseProbeLease()

	keys := m.keys()
	if keys.channelID == 0 || keys.modelID == "" {
		return response, nil
	}
	m.modelCircuitBreaker.RecordSuccess(ctx, keys.channelID, keys.modelID)

	return response, nil
}

func (m *modelCircuitBreakerTracker) OnOutboundRawError(ctx context.Context, err error) {
	if isLocalAdmissionError(err) {
		m.releaseProbeLease()
		return
	}
	if !m.shouldTrack() {
		return
	}

	// Capture whether this attempt was an active probe BEFORE releasing the lease,
	// so RecordError can decide whether to apply exponential backoff.
	wasProbe := m.probeActive
	m.releaseProbeLease()

	// A nil failure carries no evidence about this endpoint: it must never be
	// charged to the breaker. The probe lease is already released above.
	if err == nil {
		return
	}

	// Local skips never reached upstream — must not count as model errors.
	if errors.Is(err, errSkipCandidateByCircuitBreaker) {
		return
	}

	keys := m.keys()
	if keys.channelID == 0 || keys.modelID == "" {
		return
	}

	// A real client gone-away is not evidence against the endpoint; using the
	// classifier instead of a bare context.Canceled test also keeps the
	// stream-idle watchdog's joined cancellation counted as the upstream fault it
	// is.
	switch scope := m.outbound.ErrorScopeFor(err); scope {
	case errorScopeClient:
		return
	case errorScopeChannel:
		// A channel-scoped fault says nothing about whichever model happened to be
		// attempted this turn: routing only ever consults the candidate's first
		// resolved model, so charging the (possibly advanced) attempt key would be
		// invisible to the next request. Charge the scored key once — still exactly
		// one key, never a broadcast over the candidate's siblings.
		m.modelCircuitBreaker.RecordError(ctx, keys.channelID, keys.scoreKey, wasProbe && keys.scoreKey == keys.modelID)

		return
	}

	m.modelCircuitBreaker.RecordError(ctx, keys.channelID, keys.modelID, wasProbe)
}

func (m *modelCircuitBreakerTracker) OnOutboundLlmStream(ctx context.Context, stream streams.Stream[*llm.Response]) (streams.Stream[*llm.Response], error) {
	if !m.shouldTrack() {
		return stream, nil
	}

	keys := m.keys()
	channelID := keys.channelID
	modelID := keys.modelID

	return &probeReleasingStream{
		ctx:       ctx,
		stream:    stream,
		state:     m.outbound.state,
		channelID: channelID,
		modelID:   modelID,
		release: func() {
			if m.outbound != nil {
				m.releaseProbeLease()
			}
		},
		released:            false,
		recorded:            false,
		modelCircuitBreaker: m.modelCircuitBreaker,
	}, nil
}

func (m *modelCircuitBreakerTracker) shouldTrack() bool {
	return m.outbound != nil &&
		m.outbound.state != nil &&
		m.outbound.state.RoutingPolicy.LoadBalancerStrategy == biz.LoadBalancerStrategyCircuitBreaker &&
		m.modelCircuitBreaker != nil
}

func (m *modelCircuitBreakerTracker) releaseProbeLease() {
	if m.outbound == nil || m.outbound.state == nil || m.modelCircuitBreaker == nil {
		return
	}

	if !m.probeActive {
		return
	}

	m.modelCircuitBreaker.EndProbe(m.attempt.channelID, m.attempt.modelID)
	m.probeActive = false
}

//nolint:containedctx // Checked.
type probeReleasingStream struct {
	ctx      context.Context
	stream   streams.Stream[*llm.Response]
	state    *PersistenceState
	release  func()
	released bool
	recorded bool

	modelCircuitBreaker *biz.ModelCircuitBreaker
	channelID           int
	modelID             string
}

func (s *probeReleasingStream) Next() bool {
	return s.stream.Next()
}

func (s *probeReleasingStream) Current() *llm.Response {
	event := s.stream.Current()
	if event == nil {
		return nil
	}

	if s.modelCircuitBreaker == nil {
		return event
	}

	if !s.recorded {
		if tokenCount := event.Usage.GetCompletionTokens(); tokenCount != nil && *tokenCount > 0 {
			if s.channelID != 0 && s.modelID != "" {
				s.modelCircuitBreaker.RecordSuccess(s.ctx, s.channelID, s.modelID)
			}
			s.recorded = true
		}
	}

	return event
}

func (s *probeReleasingStream) Err() error {
	return s.stream.Err()
}

func (s *probeReleasingStream) Close() error {
	if !s.released && s.release != nil {
		s.released = true
		s.release()
	}

	return s.stream.Close()
}
