// 职责: 将上游失败按重试粒度分类为 errorScope，决定 CanRetry 是否早退换渠道。
// 关联文件:
//   internal/server/orchestrator/outbound.go
//   internal/server/orchestrator/retry.go
// 注意事项:
//   渠道级致命集合从窄不从宽：仅 401/402/403/520/未 opt-in 的 429 与渠道离服哨兵。
//   传输层确定性不可达（DNS 拒析/TCP refused）暂不纳入——临时抖动与确定性不可达的
//   区分未定，误升级会把模型级 5xx 连坐整条渠道；登记后续专项。
package orchestrator

import (
	"context"
	"errors"
	"net/http"

	"github.com/looplj/axonhub/llm/httpclient"
)

// errorScope is the retry granularity an upstream failure warrants.
type errorScope uint8

const (
	// errorScopeModel is specific to the current (channel, model) attempt:
	// timeouts, empty responses, retryable model-level HTTP statuses. Same-channel
	// retry may advance to a sibling model.
	errorScopeModel errorScope = iota
	// errorScopeChannel is a failure of the channel as a whole (invalid key,
	// frozen account, dead edge, non-opted-in rate limit, channel left service).
	// Same-channel retries cannot win; CanRetry returns false to switch channels.
	errorScopeChannel
	// errorScopeClient means the downstream client went away. A real client
	// cancellation is already stopped by the pipeline ctx.Err() guard; this value
	// is used for failure-reason classification and logging.
	errorScopeClient
)

// ErrorScopeFor maps an upstream failure to the retry granularity for the current
// candidate. It is consumed by CanRetry; errorScopeChannel short-circuits to a
// channel switch.
func (p *PersistentOutboundTransformer) ErrorScopeFor(err error) errorScope {
	if err == nil {
		return errorScopeModel
	}

	// A downstream cancellation stops every retry. The idle-timeout watchdog
	// cancels its context with a sentinel cause that can present as a
	// cancellation, so the idle sentinel must be excluded first — same
	// disambiguation as execution persistence.
	if errors.Is(err, context.Canceled) && !errors.Is(err, httpclient.ErrStreamIdleTimeout) {
		return errorScopeClient
	}

	if errors.Is(err, errChannelUnavailableForRetry) {
		return errorScopeChannel
	}

	// Channel-scoped HTTP failures: the account or edge behind this channel
	// cannot serve any model, so iterating sibling models burns attempts. An
	// operator may opt any of these codes back into same-channel retry by
	// listing it in ChannelSettings.RetryableStatusCodes (the general form of
	// the 429 opt-in); such a code is treated as model-level for that channel.
	// The narrow default set is 401/402/403/520/429; transport-layer deterministic
	// unreachability (DNS/refused) is deliberately excluded (registered follow-up).
	code := ExtractStatusCodeFromError(err)
	switch code {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusPaymentRequired, 520, http.StatusTooManyRequests:
		if p.state.CurrentCandidate != nil &&
			!channelOptsRetryForCode(p.state.CurrentCandidate.Channel, code) {
			return errorScopeChannel
		}
	}

	return errorScopeModel
}
