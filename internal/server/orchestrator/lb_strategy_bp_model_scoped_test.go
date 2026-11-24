package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
)

// 与 candidateScoringContext（load_balancer.go:117-127）注入同一 context key。
func modelScoringCtx(modelID string) context.Context {
	return context.WithValue(context.Background(), attemptActualModelContextKey{}, modelID)
}

func newScopedTestChannel() *biz.Channel {
	return &biz.Channel{Channel: &ent.Channel{ID: 1, Name: "test"}}
}

// 核心修复：兄弟模型零连坐。
func TestErrorAware_ModelScoped_NoSiblingPenalty(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{ModelStats: map[string]*biz.ModelMetrics{
		"model-a": {ConsecutiveFailures: 3, LastFailureAt: &now},
	}}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-b"), newScopedTestChannel(), metrics)
	assert.Equal(t, 200.0, score)
}

// 本模型受罚：200 - 3*30 - 40 = 70。
func TestErrorAware_ModelScoped_PenalizesOwnModel(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{ModelStats: map[string]*biz.ModelMetrics{
		"model-a": {ConsecutiveFailures: 3, LastFailureAt: &now},
	}}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-a"), newScopedTestChannel(), metrics)
	assert.Equal(t, 70.0, score)
}

// 渠道级致命错误仍惩罚所有候选：200 - 2*30 - 40 = 100。
func TestErrorAware_ChannelScoped_PenalizesAllModels(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{ScopedConsecutiveFailures: 2, ScopedLastFailureAt: &now}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-b"), newScopedTestChannel(), metrics)
	assert.Equal(t, 100.0, score)
}

// 两项叠加：200 - (2*30+40) - (1*30+40) = 30。
func TestErrorAware_CombinedTerms(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{
		ScopedConsecutiveFailures: 2,
		ScopedLastFailureAt:       &now,
		ModelStats: map[string]*biz.ModelMetrics{
			"model-a": {ConsecutiveFailures: 1, LastFailureAt: &now},
		},
	}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-a"), newScopedTestChannel(), metrics)
	assert.Equal(t, 30.0, score)
}

// legacy 字段被刻意忽略（约束 1 镜像面）。
func TestErrorAware_LegacyFieldsIgnored(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{ConsecutiveFailures: 5, LastFailureAt: &now}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-b"), newScopedTestChannel(), metrics)
	assert.Equal(t, 200.0, score)
}

// 时间衰减：2.5 分钟前 → ratio≈0.5 → 200 - 0.5*(60+40) = 150。
func TestErrorAware_Decay(t *testing.T) {
	at := time.Now().Add(-2*time.Minute - 30*time.Second)
	metrics := &biz.AggregatedMetrics{ScopedConsecutiveFailures: 2, ScopedLastFailureAt: &at}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-b"), newScopedTestChannel(), metrics)
	assert.InDelta(t, 150.0, score, 1.0)
}

// cooldown 外：零惩罚。
func TestErrorAware_OutsideCooldown(t *testing.T) {
	at := time.Now().Add(-10 * time.Minute)
	metrics := &biz.AggregatedMetrics{ScopedConsecutiveFailures: 5, ScopedLastFailureAt: &at}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-b"), newScopedTestChannel(), metrics)
	assert.Equal(t, 200.0, score)
}

// nil metrics：中性分（不变）。
func TestErrorAware_NilMetrics_Neutral(t *testing.T) {
	score := NewErrorAwareStrategy().Score(context.Background(), newScopedTestChannel(), nil)
	assert.Equal(t, 100.0, score)
}

// ctx 无模型：仅渠道项。
func TestErrorAware_NoModelInCtx_ChannelTermOnly(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{
		ScopedConsecutiveFailures: 2,
		ScopedLastFailureAt:       &now,
		ModelStats: map[string]*biz.ModelMetrics{
			"model-a": {ConsecutiveFailures: 3, LastFailureAt: &now},
		},
	}
	score := NewErrorAwareStrategy().Score(context.Background(), newScopedTestChannel(), metrics)
	assert.Equal(t, 100.0, score)
}

// 条目有失败计数但无时间戳：ratio=1（镜像 legacy fallback lb_strategy_bp.go:64-67）。
func TestErrorAware_ModelEntryWithoutTimestamp(t *testing.T) {
	metrics := &biz.AggregatedMetrics{ModelStats: map[string]*biz.ModelMetrics{
		"model-a": {ConsecutiveFailures: 2},
	}}
	score := NewErrorAwareStrategy().Score(modelScoringCtx("model-a"), newScopedTestChannel(), metrics)
	assert.Equal(t, 100.0, score)
}

// Score/ScoreWithDebug 一致性 + details 契约。
func TestErrorAware_ScoreWithDebug_ConsistencyAndDetails(t *testing.T) {
	now := time.Now()
	metrics := &biz.AggregatedMetrics{
		ScopedConsecutiveFailures: 2,
		ScopedLastFailureAt:       &now,
		RequestCount:              7,
		ModelStats: map[string]*biz.ModelMetrics{
			"model-a": {ConsecutiveFailures: 1, LastFailureAt: &now},
		},
	}
	ctx := modelScoringCtx("model-a")
	strategy := NewErrorAwareStrategy()
	score := strategy.Score(ctx, newScopedTestChannel(), metrics)
	debugScore, ss := strategy.ScoreWithDebug(ctx, newScopedTestChannel(), metrics)
	assert.InDelta(t, score, debugScore, 0.01)
	assert.Equal(t, "ErrorAware", ss.StrategyName)
	assert.Equal(t, int64(2), ss.Details["scoped_consecutive_failures"])
	assert.Equal(t, "model-a", ss.Details["model_id"])
	assert.Equal(t, int64(1), ss.Details["model_consecutive_failures"])
	assert.Equal(t, int64(7), ss.Details["request_count"])
}

// metrics == nil 的 debug 分支保留 reason details。
func TestErrorAware_ScoreWithDebug_NilMetrics(t *testing.T) {
	_, ss := NewErrorAwareStrategy().ScoreWithDebug(context.Background(), newScopedTestChannel(), nil)
	assert.Equal(t, 100.0, ss.Score)
	assert.Equal(t, metricsUnavailableReason, ss.Details["reason"])
}
