package biz

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/internal/objects"
)

func newModelStatsTestService(t *testing.T) *ChannelService {
	return newTestChannelService(enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0"))
}

// 契约 1：模型级失败只进模型条目，不进 scope 渠道计数器；legacy 照旧递增。
func TestRecordFailure_ModelScope_CountsModelEntry(t *testing.T) {
	svc := newModelStatsTestService(t)
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-a",
		EndTime:          time.Now(),
		Success:          false,
		RequestCompleted: true,
	})

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), metrics.ConsecutiveFailures)
	require.Zero(t, metrics.ScopedConsecutiveFailures)
	require.Nil(t, metrics.ScopedLastFailureAt)
	require.Len(t, metrics.ModelStats, 1)
	require.Equal(t, int64(1), metrics.ModelStats["model-a"].ConsecutiveFailures)
	require.NotNil(t, metrics.ModelStats["model-a"].LastFailureAt)
}

// 契约 1：渠道级失败只进 scope 渠道计数器，不进模型条目。
func TestRecordFailure_ChannelScope_CountsScopedChannelCounter(t *testing.T) {
	svc := newModelStatsTestService(t)
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-a",
		FailureScope:     FailureScopeChannel,
		EndTime:          time.Now(),
		Success:          false,
		RequestCompleted: true,
	})

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), metrics.ConsecutiveFailures)
	require.Equal(t, int64(1), metrics.ScopedConsecutiveFailures)
	require.NotNil(t, metrics.ScopedLastFailureAt)
	require.Empty(t, metrics.ModelStats)
}

// 契约 1 退化输入：模型级但无模型归属 → 保守归渠道计数器。
func TestRecordFailure_ModelScopeEmptyModel_DegradesToChannelCounter(t *testing.T) {
	svc := newModelStatsTestService(t)
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		EndTime:          time.Now(),
		Success:          false,
		RequestCompleted: true,
	})

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), metrics.ScopedConsecutiveFailures)
	require.Empty(t, metrics.ModelStats)
}

// 契约 1 边界：cap 拒绝第 513 个新模型，已存在的模型不受影响。
func TestRecordFailure_ModelStatsCap(t *testing.T) {
	svc := newModelStatsTestService(t)
	for i := 0; i < maxModelStatsEntries; i++ {
		svc.RecordPerformance(context.Background(), &PerformanceRecord{
			ChannelID:        1,
			ModelID:          "model-" + strconv.Itoa(i),
			EndTime:          time.Now(),
			Success:          false,
			RequestCompleted: true,
		})
	}
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-over-cap",
		EndTime:          time.Now(),
		Success:          false,
		RequestCompleted: true,
	})

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, metrics.ModelStats, maxModelStatsEntries)
	require.NotContains(t, metrics.ModelStats, "model-over-cap")
	// 边界内第 512 个新模型被接受。
	require.Contains(t, metrics.ModelStats, "model-"+strconv.Itoa(maxModelStatsEntries-1))
	// cap 满后已存在的模型仍可递增（先查 map 再判 cap）。
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-0",
		EndTime:          time.Now(),
		Success:          false,
		RequestCompleted: true,
	})
	metrics, err = svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), metrics.ModelStats["model-0"].ConsecutiveFailures)
}

// 契约 2：任意成功清零 scope 渠道项；被成功模型的条目被删除。
func TestRecordSuccess_ClearsScopedAndModelEntry(t *testing.T) {
	svc := newModelStatsTestService(t)
	now := time.Now()
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		FailureScope:     FailureScopeChannel,
		EndTime:          now,
		Success:          false,
		RequestCompleted: true,
	})
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-a",
		EndTime:          now.Add(time.Second),
		Success:          false,
		RequestCompleted: true,
	})
	svc.RecordPerformance(context.Background(), &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-a",
		EndTime:          now.Add(2 * time.Second),
		Success:          true,
		RequestCompleted: true,
	})

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Zero(t, metrics.ConsecutiveFailures)
	require.Zero(t, metrics.ScopedConsecutiveFailures)
	require.Empty(t, metrics.ModelStats)
	// 契约 2 不该触发域：ScopedLastFailureAt 不因成功清空，靠衰减。
	require.NotNil(t, metrics.ScopedLastFailureAt)
}

// 契约 1 不该触发域：Canceled 不进任何计数器。
func TestRecordPerformance_Canceled_SkipsAllCounters(t *testing.T) {
	svc := newModelStatsTestService(t)
	perf := &PerformanceRecord{
		ChannelID:        1,
		ModelID:          "model-a",
		EndTime:          time.Now(),
		RequestCompleted: true,
	}
	perf.MarkCanceled()
	svc.RecordPerformance(context.Background(), perf)

	metrics, err := svc.GetChannelMetrics(context.Background(), 1)
	require.NoError(t, err)
	require.Zero(t, metrics.ConsecutiveFailures)
	require.Zero(t, metrics.ScopedConsecutiveFailures)
	require.Empty(t, metrics.ModelStats)
}

// cache-compat 闸：Clone 深拷贝 ModelStats，快照与缓存互不共享可变状态。
func TestAggregatedMetrics_Clone_DeepCopiesModelStats(t *testing.T) {
	now := time.Now()
	m := &AggregatedMetrics{ModelStats: map[string]*ModelMetrics{
		"model-a": {ConsecutiveFailures: 2, LastFailureAt: &now},
	}}

	c := m.Clone()
	c.ModelStats["model-a"].ConsecutiveFailures = 99
	*c.ModelStats["model-a"].LastFailureAt = now.Add(time.Hour)
	c.ModelStats["model-b"] = &ModelMetrics{}
	delete(c.ModelStats, "model-a")

	require.Equal(t, int64(2), m.ModelStats["model-a"].ConsecutiveFailures)
	require.Equal(t, now.Unix(), m.ModelStats["model-a"].LastFailureAt.Unix())
	require.Len(t, m.ModelStats, 1)
	require.Contains(t, m.ModelStats, "model-a")
}

func TestAggregatedMetrics_Clone_NilModelStatsStaysNil(t *testing.T) {
	m := &AggregatedMetrics{}
	require.Nil(t, m.Clone().ModelStats)
}

// 契约 4：恢复启发式的分类真值表。
func TestChannelScopedRecoveryCode(t *testing.T) {
	require.True(t, channelScopedRecoveryCode(401))
	require.True(t, channelScopedRecoveryCode(402))
	require.True(t, channelScopedRecoveryCode(403))
	require.True(t, channelScopedRecoveryCode(520))
	require.False(t, channelScopedRecoveryCode(429))
	require.False(t, channelScopedRecoveryCode(500))
	require.False(t, channelScopedRecoveryCode(0))
}

// 契约 4：启动恢复按 response_status_code 分流；legacy CF 口径不变。
func TestLoadAllFromExecutions_ScopeRouting(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	defer client.Close()

	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	now := time.Now().UTC().Truncate(time.Second)
	svc := &ChannelService{}

	create := func(requestID int, model string, status requestexecution.Status, statusCode int, at time.Time) {
		t.Helper()
		create := client.RequestExecution.Create().
			SetProjectID(1).
			SetRequestID(requestID).
			SetChannelID(1).
			SetModelID(model).
			SetRequestBody(objects.JSONRawMessage(`{}`)).
			SetStatus(status).
			SetCreatedAt(at).
			SetUpdatedAt(at)
		if statusCode > 0 {
			create = create.SetResponseStatusCode(statusCode)
		}
		create.SaveX(ctx)
	}

	// 渠道 1：gpt-4 成功 → gpt-4 401 失败（渠道级）→ model-b 500 失败（模型级）。
	create(1, "gpt-4", requestexecution.StatusCompleted, 0, now.Add(-30*time.Minute))
	create(2, "gpt-4", requestexecution.StatusFailed, 401, now.Add(-20*time.Minute))
	create(3, "model-b", requestexecution.StatusFailed, 500, now.Add(-10*time.Minute))

	metrics, err := svc.loadAllChannelMetricsFromExecutions(ctx, client, now.Add(-6*time.Hour))
	require.NoError(t, err)
	require.Contains(t, metrics, 1)
	m := metrics[1]
	require.Equal(t, int64(2), m.ConsecutiveFailures)
	require.Equal(t, int64(1), m.ScopedConsecutiveFailures)
	require.NotNil(t, m.ScopedLastFailureAt)
	require.Equal(t, now.Add(-20*time.Minute).Unix(), m.ScopedLastFailureAt.Unix())
	require.Len(t, m.ModelStats, 1)
	require.Equal(t, int64(1), m.ModelStats["model-b"].ConsecutiveFailures)
	require.Equal(t, now.Add(-10*time.Minute).Unix(), m.ModelStats["model-b"].LastFailureAt.Unix())
	require.NotContains(t, m.ModelStats, "gpt-4")
}

// 契约 4 退化输入：response_status_code 未持久化 → 按模型级；429 → 模型级。
func TestLoadAllFromExecutions_MissingStatusAnd429_ModelScoped(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	defer client.Close()

	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	now := time.Now().UTC().Truncate(time.Second)
	svc := &ChannelService{}

	create := func(requestID int, model string, status requestexecution.Status, statusCode int, at time.Time) {
		t.Helper()
		create := client.RequestExecution.Create().
			SetProjectID(1).
			SetRequestID(requestID).
			SetChannelID(2).
			SetModelID(model).
			SetRequestBody(objects.JSONRawMessage(`{}`)).
			SetStatus(status).
			SetCreatedAt(at).
			SetUpdatedAt(at)
		if statusCode > 0 {
			create = create.SetResponseStatusCode(statusCode)
		}
		create.SaveX(ctx)
	}

	create(1, "gpt-4", requestexecution.StatusCompleted, 0, now.Add(-30*time.Minute))
	create(2, "gpt-4", requestexecution.StatusFailed, 0, now.Add(-20*time.Minute))
	create(3, "gpt-4", requestexecution.StatusFailed, 429, now.Add(-10*time.Minute))

	metrics, err := svc.loadAllChannelMetricsFromExecutions(ctx, client, now.Add(-6*time.Hour))
	require.NoError(t, err)
	require.Contains(t, metrics, 2)
	m := metrics[2]
	require.Zero(t, m.ScopedConsecutiveFailures)
	require.Nil(t, m.ScopedLastFailureAt)
	require.Len(t, m.ModelStats, 1)
	require.Equal(t, int64(2), m.ModelStats["gpt-4"].ConsecutiveFailures)
}

// 契约 4/5：启动恢复同样受 maxModelStatsEntries 约束；cap 满后第 513 个
// 新模型被拒，已在 map 内的模型仍可继续递增。
func TestLoadAllFromExecutions_ModelStatsCap(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	defer client.Close()

	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	now := time.Now().UTC().Truncate(time.Second)
	svc := &ChannelService{}

	for i := 0; i <= maxModelStatsEntries; i++ {
		client.RequestExecution.Create().
			SetProjectID(1).
			SetRequestID(i + 1).
			SetChannelID(3).
			SetModelID("model-" + strconv.Itoa(i)).
			SetRequestBody(objects.JSONRawMessage(`{}`)).
			SetStatus(requestexecution.StatusFailed).
			SetResponseStatusCode(500).
			SetCreatedAt(now.Add(-time.Duration(maxModelStatsEntries-i+1) * time.Minute)).
			SetUpdatedAt(now.Add(-time.Duration(maxModelStatsEntries-i+1) * time.Minute)).
			SaveX(ctx)
	}

	metrics, err := svc.loadAllChannelMetricsFromExecutions(ctx, client, now.Add(-24*time.Hour))
	require.NoError(t, err)
	require.Contains(t, metrics, 3)
	m := metrics[3]
	require.Len(t, m.ModelStats, maxModelStatsEntries)
	require.Contains(t, m.ModelStats, "model-"+strconv.Itoa(maxModelStatsEntries-1))
	require.NotContains(t, m.ModelStats, "model-"+strconv.Itoa(maxModelStatsEntries))
}
