package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent/requestexecution"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
)

func TestFailureReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "idle timeout", err: httpclient.ErrStreamIdleTimeout, want: FailureReasonUpstreamIdleTimeout},
		{name: "idle timeout with child cancellation", err: errors.Join(httpclient.ErrStreamIdleTimeout, context.Canceled), want: FailureReasonUpstreamIdleTimeout},
		{name: "client cancel", err: context.Canceled, want: FailureReasonClientCancel},
		{name: "first token timeout", err: pipeline.ErrStreamFirstEventTimeout, want: FailureReasonFirstTokenTimeout},
		{name: "non stream timeout", err: pipeline.ErrNonStreamResponseTimeout, want: FailureReasonNonStreamTimeout},
		{name: "eof", err: io.EOF, want: FailureReasonUpstreamEOF},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: FailureReasonUpstreamEOF},
		{name: "wrapped unexpected eof", err: fmt.Errorf("HTTP stream request failed: %w", io.ErrUnexpectedEOF), want: FailureReasonUpstreamEOF},
		{name: "llm stream incomplete", err: llm.ErrStreamIncomplete, want: FailureReasonUpstreamStreamInterrupted},
		{name: "orchestrator stream incomplete", err: ErrStreamIncomplete, want: FailureReasonUpstreamStreamInterrupted},
		{name: "connection reset", err: syscall.ECONNRESET, want: FailureReasonUpstreamStreamInterrupted},
		{name: "winsock reset", err: syscall.Errno(10054), want: FailureReasonUpstreamStreamInterrupted},
		{name: "http error", err: &httpclient.Error{StatusCode: http.StatusTooManyRequests, Body: []byte(`{}`)}, want: FailureReasonUpstreamHTTPError},
		{name: "classified error resolves through cause chain", err: ClassifyUpstreamTransportError(io.ErrUnexpectedEOF), want: FailureReasonUpstreamEOF},
		{name: "classified winsock reset keeps interrupt reason", err: ClassifyUpstreamTransportError(&net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "wsarecv", Err: syscall.Errno(10054)}}), want: FailureReasonUpstreamStreamInterrupted},
		{name: "empty response", err: pipeline.ErrEmptyResponse, want: FailureReasonEmptyResponse},
		{name: "plain error", err: errors.New("boom"), want: FailureReasonUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, FailureReason(tt.err))
		})
	}
}

func TestIsUpstreamTransportError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: true},
		{name: "wrapped unexpected EOF", err: fmt.Errorf("HTTP stream request failed: %w", io.ErrUnexpectedEOF), want: true},
		{name: "EOF", err: io.EOF, want: true},
		{name: "llm stream incomplete", err: llm.ErrStreamIncomplete, want: true},
		{name: "orchestrator stream incomplete", err: ErrStreamIncomplete, want: true},
		{name: "stream idle timeout", err: httpclient.ErrStreamIdleTimeout, want: true},
		{name: "idle timeout with child cancellation", err: errors.Join(httpclient.ErrStreamIdleTimeout, context.Canceled), want: true},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
		{name: "http error", err: &httpclient.Error{StatusCode: http.StatusBadGateway, Body: []byte(`{}`)}, want: false},
		{name: "response error", err: &llm.ResponseError{StatusCode: http.StatusTooManyRequests}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsUpstreamTransportError(tt.err))
		})
	}
}

func TestClassifyUpstreamTransportError(t *testing.T) {
	cause := fmt.Errorf("read body: %w", io.ErrUnexpectedEOF)

	err := ClassifyUpstreamTransportError(cause)

	respErr := &llm.ResponseError{}
	require.True(t, errors.As(err, &respErr))
	require.Equal(t, http.StatusBadGateway, respErr.StatusCode)
	require.Equal(t, ErrTypeUpstreamError, respErr.Detail.Type)
	require.Equal(t, ErrCodeUpstreamStreamInterrupted, respErr.Detail.Code)
	require.Contains(t, respErr.Detail.Message, "unexpected EOF")
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF), "the cause must stay matchable through the classified error")

	// Idempotent for already classified errors, transparent for everything else.
	require.Same(t, err, ClassifyUpstreamTransportError(err))

	plain := errors.New("boom")
	require.Equal(t, plain, ClassifyUpstreamTransportError(plain))
	require.Equal(t, context.Canceled, ClassifyUpstreamTransportError(context.Canceled))
}

func TestExtractErrorMessageAndInfo_ClassifiedTransportError(t *testing.T) {
	err := ClassifyUpstreamTransportError(io.ErrUnexpectedEOF)

	require.Equal(t,
		"Upstream provider closed the connection before the response completed: unexpected EOF",
		ExtractErrorMessage(err))

	info := ExtractErrorInfo(err)
	require.NotNil(t, info)
	require.NotNil(t, info.StatusCode)
	require.Equal(t, http.StatusBadGateway, *info.StatusCode)

	// Plain errors keep the previous behavior.
	require.Equal(t, "boom", ExtractErrorMessage(errors.New("boom")))
	require.Nil(t, ExtractErrorInfo(errors.New("boom")))
}

func TestClassifyUpstreamStreamIdleTimeout(t *testing.T) {
	err := ClassifyUpstreamTransportError(httpclient.ErrStreamIdleTimeout)
	var responseError *llm.ResponseError
	require.ErrorAs(t, err, &responseError)
	require.Equal(t, http.StatusBadGateway, responseError.StatusCode)
	require.ErrorIs(t, err, httpclient.ErrStreamIdleTimeout)
	require.Same(t, err, ClassifyUpstreamTransportError(err))
}

func TestIsUpstreamTransportError_WinsockReset(t *testing.T) {
	// Real Windows TCP-reset shapes: the windows syscall package exposes
	// WSAECONNRESET (10054) / WSAECONNABORTED (10053) with their native Winsock
	// values, which differ from the POSIX-numbered syscall.ECONNRESET that
	// windows builds renumber into the invented APPLICATION_ERROR range.
	wsReset := syscall.Errno(10054)
	wsAbort := syscall.Errno(10053)
	wsResetOpError := &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "wsarecv", Err: wsReset}}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "bare winsock reset errno", err: wsReset, want: true},
		{name: "winsock reset via syscall error", err: &os.SyscallError{Syscall: "wsarecv", Err: wsReset}, want: true},
		{name: "winsock reset via net op error", err: wsResetOpError, want: true},
		{name: "winsock reset via url error", err: &url.Error{Op: "Post", URL: "https://example.invalid", Err: wsResetOpError}, want: true},
		{name: "winsock abort errno", err: wsAbort, want: true},
		{name: "other errno stays unclassified", err: syscall.Errno(10060), want: false},
		{name: "cancellation wins over winsock reset", err: errors.Join(context.Canceled, wsReset), want: false},
		{name: "classified winsock reset stays structured", err: ClassifyUpstreamTransportError(wsResetOpError), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsUpstreamTransportError(tt.err))
		})
	}
}

func TestClassifyUpstreamTransportError_WinsockReset(t *testing.T) {
	cause := &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "wsarecv", Err: syscall.Errno(10054)}}

	err := ClassifyUpstreamTransportError(cause)

	respErr := &llm.ResponseError{}
	require.True(t, errors.As(err, &respErr))
	require.Equal(t, http.StatusBadGateway, respErr.StatusCode)
	require.Equal(t, ErrTypeUpstreamError, respErr.Detail.Type)
	require.Equal(t, ErrCodeUpstreamStreamInterrupted, respErr.Detail.Code)
	// 错误文案跨平台不同（Windows=WSA 文案，Linux=errno 数字），只锁分类器前缀。
	require.Contains(t, respErr.Detail.Message, "closed the connection before the response completed")
	require.True(t, errors.Is(err, cause), "the winsock cause must stay matchable through the classified error")
}

// TestPersistRequestExecutionFailureCanceledWritesReason locks the execution row
// and the failure event onto the same coarse reason. A cancellation carries no
// HTTP-shaped error, so without an explicit fallback the row stayed blank while
// the event already published client_cancel.
func TestPersistRequestExecutionFailureCanceledWritesReason(t *testing.T) {
	ctx, client, state := newUpstreamModelPersistenceTest(t)

	canceled := createUpstreamModelTestExecution(t, ctx, client, state.Request, "model", llm.APIFormatOpenAIChatCompletion, true)
	require.NoError(t, persistRequestExecutionFailure(ctx, state.RequestService, canceled, context.Canceled, nil, "", nil))

	saved, err := client.RequestExecution.Get(ctx, canceled.ID)
	require.NoError(t, err)
	require.Equal(t, requestexecution.StatusCanceled, saved.Status)
	require.Equal(t, FailureReasonClientCancel, saved.FailureReason,
		"a client cancellation must persist the same coarse reason the failure event publishes")
	require.Empty(t, saved.ErrorCode, "a cancellation carries no classified upstream code")

	httpFailed := createUpstreamModelTestExecution(t, ctx, client, state.Request, "model", llm.APIFormatOpenAIChatCompletion, true)
	require.NoError(t, persistRequestExecutionFailure(ctx, state.RequestService, httpFailed, &httpclient.Error{StatusCode: http.StatusTooManyRequests}, nil, "", nil))
	savedFailed, err := client.RequestExecution.Get(ctx, httpFailed.ID)
	require.NoError(t, err)
	require.Equal(t, requestexecution.StatusFailed, savedFailed.Status)
	require.Equal(t, FailureReasonUpstreamHTTPError, savedFailed.FailureReason,
		"the classified failure path must keep the shape it already had")

	idle := createUpstreamModelTestExecution(t, ctx, client, state.Request, "model", llm.APIFormatOpenAIChatCompletion, true)
	require.NoError(t, persistRequestExecutionFailure(ctx, state.RequestService, idle, fmtWrap(httpclient.ErrStreamIdleTimeout, context.Canceled), nil, "", nil))
	savedIdle, err := client.RequestExecution.Get(ctx, idle.ID)
	require.NoError(t, err)
	require.NotEqual(t, requestexecution.StatusCanceled, savedIdle.Status,
		"an idle timeout must not be recorded as a client cancellation")
	require.Equal(t, FailureReasonUpstreamIdleTimeout, savedIdle.FailureReason)

	// Ctx-derived cancellation: the raw error is a real upstream fault with a
	// structured shape, so the reason must stay the classified one — the
	// client_cancel fallback applies only when nothing structured was produced.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	viaCtx := createUpstreamModelTestExecution(t, ctx, client, state.Request, "model", llm.APIFormatOpenAIChatCompletion, true)
	require.NoError(t, persistRequestExecutionFailure(canceledCtx, state.RequestService, viaCtx, io.ErrUnexpectedEOF, nil, "", nil))
	savedViaCtx, err := client.RequestExecution.Get(ctx, viaCtx.ID)
	require.NoError(t, err)
	require.Equal(t, requestexecution.StatusCanceled, savedViaCtx.Status)
	require.Equal(t, FailureReasonUpstreamEOF, savedViaCtx.FailureReason,
		"a structured failure keeps its own reason even when the context was cancelled")
}
