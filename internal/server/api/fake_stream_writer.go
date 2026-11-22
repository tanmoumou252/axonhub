// 职责: 候选容灾与上游首字等待期间，先向下游提交 200 SSE + 占位/心跳，待真实流到达后无缝透传。
// 关联文件:
//   internal/server/api/chat.go
//   internal/server/orchestrator/orchestrator.go
// 注意事项:
//   仅主 goroutine 触碰 c.Writer（契约 C2b）；占位帧不计入真实 TTFT。
package api

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/internal/server/safeguards"
	"github.com/looplj/axonhub/llm/httpclient"
)

// fakeStreamResult carries the outcome of the single background Process() call.
type fakeStreamResult struct {
	result orchestrator.ChatCompletionResult
	err    error
}

// writeFakeSSEStream consumes an ALREADY-RUNNING background Process result (fed
// by the handler's single goroutine G1) — it does NOT spawn Process itself, to
// avoid double-spawn. It commits 200 + text/event-stream + a protocol-legal
// placeholder, emits heartbeats until the real result arrives, then splices.
// Only this handler goroutine touches c.Writer (contract C2b).
func (handlers *ChatCompletionHandlers) writeFakeSSEStream(
	c *gin.Context,
	resultCh <-chan fakeStreamResult,
	processCtx context.Context,
	cancelProcess context.CancelFunc,
	genericReq *httpclient.Request,
	evaluator *safeguards.Evaluator,
	abortUpstream context.CancelFunc,
	started time.Time,
) {
	_ = processCtx // Held by the caller's single Process goroutine; cancellation flows through cancelProcess.
	ctx := c.Request.Context()

	setSSEHeaders(c)
	if err := flushSSE(ctx, c.Writer); err != nil {
		cancelProcess()
		return
	}
	if err := writePlaceholderFirstFrame(ctx, c.Writer, handlers.streamProtocol); err != nil {
		log.Warn(ctx, "Failed to write SSE placeholder frame")
		cancelProcess()
		return
	}

	interval := handlers.sseKeepAlive.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	encodeErr := encodeStreamError(func(x context.Context, e error) any {
		return FormatStreamErrorFor(handlers.streamProtocol, x, e)
	})

	for {
		select {
		case <-ctx.Done():
			cancelProcess() // E7/E9: abort background Process on client disconnect.
			return
		case res := <-resultCh:
			if res.err != nil {
				_ = writeSSEErrorEvent(ctx, c.Writer, encodeErr, res.err)
				return
			}
			// Splice the real stream on the SAME handler goroutine (C2b). G1 has
			// already returned (resultCh fired), so no concurrent writer exists.
			handlers.writeChatCompletionResult(c, genericReq, res.result, evaluator, abortUpstream, started)
			return
		case <-ticker.C:
			if err := writeSSEHeartbeatEvent(ctx, c.Writer, handlers.sseHeartbeatFormat); err != nil {
				log.Warn(ctx, "Fake-stream heartbeat failed")
				cancelProcess()
				return
			}
		}
	}
}

func writePlaceholderFirstFrame(ctx context.Context, writer http.ResponseWriter, protocol StreamProtocol) error {
	switch protocol {
	case StreamProtocolAnthropic:
		return writeSSEHeartbeatEvent(ctx, writer, sseHeartbeatAnthropic)
	case StreamProtocolGemini:
		return writeAndFlushSSE(ctx, writer, func(w io.Writer) error {
			_, err := io.WriteString(w, ": placeholder\n\n")
			return err
		})
	default:
		return writeAndFlushSSE(ctx, writer, func(w io.Writer) error {
			_, err := io.WriteString(w, `data: {"choices":[{"index":0,"delta":{},"finish_reason":null}]}`+"\n\n")
			return err
		})
	}
}
