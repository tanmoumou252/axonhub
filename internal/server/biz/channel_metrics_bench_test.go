package biz

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkRecordFailure_ModelStats 测 scope 分流 + 模型条目维护的热路径开销。
func BenchmarkRecordFailure_ModelStats(b *testing.B) {
	cm := newChannelMetrics(1)
	slot := &timeSlotMetrics{timestamp: time.Now().Unix()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		perf := &PerformanceRecord{
			ChannelID:        1,
			ModelID:          fmt.Sprintf("model-%d", i%100),
			EndTime:          time.Now(),
			RequestCompleted: true,
		}
		cm.recordFailure(slot, perf)
	}
}

// BenchmarkRecordSuccess_ModelEntryDeletion 测成功删除条目路径。
func BenchmarkRecordSuccess_ModelEntryDeletion(b *testing.B) {
	cm := newChannelMetrics(1)
	slot := &timeSlotMetrics{timestamp: time.Now().Unix()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		perf := &PerformanceRecord{
			ChannelID:        1,
			ModelID:          fmt.Sprintf("model-%d", i%100),
			EndTime:          time.Now(),
			RequestCompleted: true,
		}
		cm.recordFailure(slot, perf)
		cm.recordSuccess(slot, perf)
	}
}

// BenchmarkAggregatedMetrics_Clone_ModelStats 测快照深拷贝 100 条目开销。
func BenchmarkAggregatedMetrics_Clone_ModelStats(b *testing.B) {
	m := &AggregatedMetrics{ModelStats: make(map[string]*ModelMetrics, 100)}
	for i := 0; i < 100; i++ {
		now := time.Now()
		m.ModelStats[fmt.Sprintf("model-%d", i)] = &ModelMetrics{ConsecutiveFailures: 2, LastFailureAt: &now}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Clone()
	}
}
