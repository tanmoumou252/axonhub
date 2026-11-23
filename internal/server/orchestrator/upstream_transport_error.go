package orchestrator

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

const (
	// ErrTypeUpstreamError is the error type reported when the upstream connection breaks.
	ErrTypeUpstreamError = "upstream_error"
	// ErrCodeUpstreamStreamInterrupted is the stable code for an upstream connection that
	// ended before the response completed: EOF, reset, timeout or a missing terminal event.
	ErrCodeUpstreamStreamInterrupted = "upstream_stream_interrupted"
)

// IsUpstreamTransportError reports whether err is a transport-level failure of the
// upstream connection rather than an HTTP error body or a local cancellation: the
// connection ended early (io.EOF / io.ErrUnexpectedEOF), the stream never delivered a
// terminal event, or the network reported a reset or timeout.
func IsUpstreamTransportError(err error) bool {
	if err == nil {
		return false
	}

	// Already carries a status code or structured detail: nothing to classify.
	if _, ok := errors.AsType[*httpclient.Error](err); ok {
		return false
	}

	if _, ok := errors.AsType[*llm.ResponseError](err); ok {
		return false
	}

	if errors.Is(err, httpclient.ErrStreamIdleTimeout) {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, llm.ErrStreamIncomplete) ||
		errors.Is(err, ErrStreamIncomplete) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		isWinsockResetError(err) {
		return true
	}

	if isHTTP2ConnectionError(err) {
		return true
	}

	var netErr net.Error

	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

// Winsock connection-reset errnos surfaced by real TCP resets and aborted
// sockets on Windows. Go's windows syscall package defines them natively as
// WSAECONNRESET (10054) and WSAECONNABORTED (10053) in
// src/syscall/types_windows.go, while the POSIX-numbered syscall.ECONNRESET is
// renumbered into the invented APPLICATION_ERROR range on windows builds — so
// errors.Is(err, syscall.ECONNRESET) never matches a real Windows TCP reset.
// POSIX errno values stay far below 10000, so these comparisons are dead code
// on non-Windows platforms and cannot collide with any Unix errno.
const (
	winsockWSAECONNRESET   syscall.Errno = 10054
	winsockWSAECONNABORTED syscall.Errno = 10053
)

// isWinsockResetError reports whether the error chain terminates in a Winsock
// connection-reset errno (a real TCP RST or aborted socket on Windows).
func isWinsockResetError(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	return errno == winsockWSAECONNRESET || errno == winsockWSAECONNABORTED
}

// isHTTP2ConnectionError reports whether err is an HTTP/2 stream reset or GOAWAY from
// the peer. net/http keeps these error types unexported (they live in its bundled h2
// copy), so the only way to recognize them is by message.
func isHTTP2ConnectionError(err error) bool {
	msg := err.Error()

	if strings.Contains(msg, "stream error: stream ID ") &&
		(strings.Contains(msg, "INTERNAL_ERROR") || strings.Contains(msg, "REFUSED_STREAM")) {
		return true
	}

	return strings.Contains(msg, "http2: server sent GOAWAY")
}

// ClassifyUpstreamTransportError converts a transport-level upstream failure into a
// *llm.ResponseError with 502 semantics, a stable code and the original cause, so
// clients and the request log see a classifiable error instead of a bare
// "unexpected EOF". Any other error is returned unchanged.
func ClassifyUpstreamTransportError(err error) error {
	if !IsUpstreamTransportError(err) {
		return err
	}

	return &llm.ResponseError{
		StatusCode: http.StatusBadGateway,
		Detail: llm.ErrorDetail{
			Message: "Upstream provider closed the connection before the response completed: " + err.Error(),
			Type:    ErrTypeUpstreamError,
			Code:    ErrCodeUpstreamStreamInterrupted,
		},
		Cause: err,
	}
}

// persistRequestExecutionFailure marks an execution failed (or canceled) with a classified
// error message and status code, keeping any latency metrics captured before the failure.
func persistRequestExecutionFailure(
	ctx context.Context,
	requestService *biz.RequestService,
	execution *ent.RequestExecution,
	rawErr error,
	metrics *biz.LatencyMetrics,
	upstreamModelID string,
	responseHeaders http.Header,
	chunks ...*httpclient.StreamEvent,
) error {
	status := requestexecution.StatusFailed
	if !errors.Is(rawErr, httpclient.ErrStreamIdleTimeout) &&
		(errors.Is(rawErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled)) {
		status = requestexecution.StatusCanceled
	}

	failure := ClassifyUpstreamTransportError(rawErr)

	return requestService.UpdateRequestExecutionStatusWithMetrics(
		ctx,
		execution,
		status,
		ExtractErrorMessage(failure),
		ExtractErrorInfo(failure),
		metrics,
		upstreamModelID,
		responseHeaders, chunks...,
	)
}
