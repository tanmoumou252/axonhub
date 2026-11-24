package orchestrator

import (
	"context"
	"time"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
)

// ErrorAwareStrategy deprioritizes channels based on their recent error history.
// It calculates a health score by applying time-decayed penalties for failures.
//
// This strategy only applies PENALTIES for errors and does not provide boosts for
// successful requests. This design prevents the "Matthew effect" where high-performing
// channels dominate the distribution at the expense of others.
//
// Historical success rate is intentionally excluded to allow channels to recover
// quickly after a period of instability, as long as they are currently working.
//
// Penalties applied:
//   - Consecutive failures: -30 per failure, decaying linearly over the cooldown period.
//   - Recent failure: A base penalty of -40 that decays linearly over the cooldown period.
type ErrorAwareStrategy struct {
	// maxScore is the maximum score for a perfectly healthy channel (default: 200)
	maxScore float64
	// basePenalty is the base penalty for any recent failure (default: 40)
	basePenalty float64
	// penaltyPerConsecutiveFailure is the score penalty per consecutive failure (default: 30)
	penaltyPerConsecutiveFailure float64
	// errorCooldownMinutes is how long to remember errors (default: 5 minutes)
	errorCooldownMinutes int
}

// NewErrorAwareStrategy creates a new error-aware strategy.
func NewErrorAwareStrategy() *ErrorAwareStrategy {
	return &ErrorAwareStrategy{
		maxScore:                     200.0,
		basePenalty:                  40.0,
		penaltyPerConsecutiveFailure: 30.0,
		errorCooldownMinutes:         5,
	}
}

// Score returns a health score based on recent errors.
// Production path without debug logging.
//
// Penalties are scope-routed: the channel term is driven only by channel-scoped
// failures (ScopedConsecutiveFailures/ScopedLastFailureAt) and the model term
// only by the candidate model's entry in ModelStats (model ID comes from the
// scoring context, mirroring ModelAwareCircuitBreakerStrategy). The legacy
// ConsecutiveFailures keeps counting every failure for other consumers and is
// deliberately ignored here, so one failing model cannot drag down its
// siblings on the same channel.
func (s *ErrorAwareStrategy) Score(ctx context.Context, _ *biz.Channel, metrics *biz.AggregatedMetrics) float64 {
	if metrics == nil {
		// If we can't get metrics, give neutral score
		return s.maxScore / 2
	}

	score, _ := s.computeScore(ctx, metrics)

	return score
}

// computeScore applies the scope-routed penalties and returns the clamped score
// plus debug details shared by Score and ScoreWithDebug.
func (s *ErrorAwareStrategy) computeScore(ctx context.Context, metrics *biz.AggregatedMetrics) (float64, map[string]any) {
	score := s.maxScore
	details := map[string]any{}

	// Channel term: only channel-scoped failures penalize every candidate.
	score -= s.penaltyFor(metrics.ScopedConsecutiveFailures, metrics.ScopedLastFailureAt)

	// Model term: only the candidate model's own failures penalize it.
	modelID := attemptActualModelFromContext(ctx)
	if modelID != "" && metrics.ModelStats != nil {
		if entry := metrics.ModelStats[modelID]; entry != nil {
			score -= s.penaltyFor(entry.ConsecutiveFailures, entry.LastFailureAt)
			details["model_id"] = modelID
			details["model_consecutive_failures"] = entry.ConsecutiveFailures
		}
	}

	// Ensure score doesn't go below 0
	if score < 0 {
		score = 0
	}

	return score, details
}

// penaltyFor applies the consecutive-failure and recent-failure penalties with
// linear decay over the cooldown period.
func (s *ErrorAwareStrategy) penaltyFor(consecutiveFailures int64, lastFailureAt *time.Time) float64 {
	cooldownRatio := 0.0
	if lastFailureAt != nil {
		timeSinceFailure := time.Since(*lastFailureAt)
		if timeSinceFailure < time.Duration(s.errorCooldownMinutes)*time.Minute {
			cooldownRatio = 1.0 - (timeSinceFailure.Minutes() / float64(s.errorCooldownMinutes))
		}
	} else if consecutiveFailures > 0 {
		// Fallback: failures without a timestamp are assumed to have just happened.
		cooldownRatio = 1.0
	}

	if cooldownRatio <= 0 {
		return 0
	}

	penalty := float64(consecutiveFailures) * s.penaltyPerConsecutiveFailure * cooldownRatio
	penalty += s.basePenalty * cooldownRatio

	return penalty
}

// ScoreWithDebug returns a health score with detailed debug information.
// Debug path with comprehensive logging.
func (s *ErrorAwareStrategy) ScoreWithDebug(ctx context.Context, channel *biz.Channel, metrics *biz.AggregatedMetrics) (float64, StrategyScore) {
	log.Info(ctx, "ErrorAwareStrategy: starting score calculation",
		log.Int("channel_id", channel.ID),
		log.String("channel_name", channel.Name),
	)

	if metrics == nil {
		// If we can't get metrics, give neutral score
		neutralScore := s.maxScore / 2

		return neutralScore, StrategyScore{
			StrategyName: s.Name(),
			Score:        neutralScore,
			Details: map[string]any{
				"reason": metricsUnavailableReason,
			},
		}
	}

	score, details := s.computeScore(ctx, metrics)
	details["scoped_consecutive_failures"] = metrics.ScopedConsecutiveFailures
	details["request_count"] = metrics.RequestCount

	log.Info(ctx, "ErrorAwareStrategy: calculated final score",
		log.Int("channel_id", channel.ID),
		log.String("channel_name", channel.Name),
		log.Float64("final_score", score),
		log.Any("calculation_details", details),
	)

	return score, StrategyScore{
		StrategyName: s.Name(),
		Score:        score,
		Details:      details,
	}
}

// Name returns the strategy name.
func (s *ErrorAwareStrategy) Name() string {
	return "ErrorAware"
}
