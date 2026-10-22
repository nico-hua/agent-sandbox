package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

var errRequestBodyLimit = errors.New("request body limit exceeded")
var errResponseBodyLimit = errors.New("response body limit exceeded")

// ServeHTTP separates origin health from absolute-form proxy requests before acquiring shared capacity.
func (proxy *Proxy) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if !request.URL.IsAbs() && request.RequestURI == "/healthz" {
		if request.Method != "GET" {
			proxyError(response, 405)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, "{\"status\":\"ok\"}\n")
		return
	}
	if request.ProtoMajor != 1 || request.ProtoMinor != 1 || request.Header.Get("Upgrade") != "" || hasToken(request.Header.Values("Connection"), "upgrade") {
		proxyError(response, 400)
		return
	}
	if request.Method == "CONNECT" {
		proxy.connect(response, request)
		return
	}
	switch request.Method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
	default:
		proxyError(response, 405)
		return
	}
	if !strings.EqualFold(request.URL.Scheme, "http") || request.URL.Host == "" || request.URL.Opaque != "" || request.URL.User != nil || request.URL.Fragment != "" || strings.Contains(request.RequestURI, "#") {
		proxyError(response, 400)
		return
	}
	target, err := proxy.authority(request.URL.Host, "80", true)
	if err != nil {
		proxyFailure(response, request, err)
		return
	}
	if request.ContentLength > proxy.limits.bodyBytes {
		proxyError(response, 413)
		return
	}
	task, status := proxy.begin(request, proxy.limits.httpTimeout)
	if status != 0 {
		proxyError(response, status)
		return
	}
	defer task.finish()
	proxy.forward(response, request, target, task)
}

// authority validates exact host syntax/policy and rebuilds canonical numeric-port authority without DNS.
func (proxy *Proxy) authority(authority, port string, optional bool) (string, error) {
	host := authority
	if strings.Contains(authority, ":") {
		var supplied string
		var err error
		host, supplied, err = net.SplitHostPort(authority)
		if err != nil || supplied != port || authority != net.JoinHostPort(host, supplied) {
			return "", network.ErrInvalidTarget
		}
	} else if !optional {
		return "", network.ErrInvalidTarget
	}
	host, err := proxy.dialer.CheckHost(host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}

// forward streams one request with a task-bound private Transport; it never follows redirects or uses an environment proxy.
func (proxy *Proxy) forward(response http.ResponseWriter, request *http.Request, target string, task *proxyTask) {
	_ = http.NewResponseController(response).EnableFullDuplex()
	body := &boundedBody{source: request.Body, remaining: proxy.limits.bodyBytes, limitError: errRequestBodyLimit}
	committed := false
	defer func() {
		task.stopRequest()
		// Disable inbound reuse and unblock a stalled upload before waiting for
		// Transport's asynchronous request-body closure. Preserve response writes.
		if half, ok := task.client.(interface{ CloseRead() error }); ok {
			_ = half.CloseRead()
		}
		_ = body.Close()
		if committed && body.exceeded.Load() {
			panic(http.ErrAbortHandler)
		}
	}()
	observed := &responseHeaders{limit: proxy.limits.headerBytes + 4096}
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := task.dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &observedHeaderConn{Conn: connection, headers: observed}, nil
	}, DisableKeepAlives: true,
		DisableCompression: true, MaxResponseHeaderBytes: int64(proxy.limits.headerBytes)}
	defer transport.CloseIdleConnections()
	out := request.Clone(task.ctx)
	out.URL.Scheme, out.URL.Host, out.Host, out.RequestURI = "http", target, target, ""
	out.Header = request.Header.Clone()
	stripHopHeaders(out.Header)
	out.Header.Del("Host")
	out.TransferEncoding, out.Trailer, out.GetBody, out.Close = nil, nil, nil, true
	out.Body = body
	if request.Body == nil || request.Body == http.NoBody {
		out.Body = nil
	}
	upstream, err := transport.RoundTrip(out)
	if err != nil {
		if body.exceeded.Load() {
			proxyError(response, 413)
		} else {
			if task.ctx.Err() != nil {
				err = task.ctx.Err()
			}
			proxyFailure(response, request, err)
		}
		return
	}
	defer upstream.Body.Close()
	nominated, err := observed.connectionFields()
	if err != nil {
		proxyError(response, 502)
		return
	}
	if body.exceeded.Load() {
		proxyError(response, 413)
		return
	}
	noBody := request.Method == "HEAD" || upstream.StatusCode == 204 || upstream.StatusCode == 304
	if upstream.StatusCode == 101 || !noBody && upstream.ContentLength > proxy.limits.bodyBytes {
		proxyError(response, 502)
		return
	}
	if !task.commit() {
		proxyFailure(response, request, task.ctx.Err())
		return
	}
	stripHopHeaders(upstream.Header)
	for _, value := range nominated {
		for _, name := range strings.Split(value, ",") {
			upstream.Header.Del(strings.TrimSpace(name))
		}
	}
	for name, values := range upstream.Header {
		for _, value := range values {
			response.Header().Add(name, value)
		}
	}
	// Inbound reuse is intentionally disabled as well, so early upstream replies
	// cannot leave an unread upload as the next request's framing.
	response.Header().Set("Connection", "close")
	committed = true
	response.WriteHeader(upstream.StatusCode)
	if noBody {
		return
	}
	reader := &boundedReader{source: upstream.Body, remaining: proxy.limits.bodyBytes, limitError: errResponseBodyLimit}
	if _, err := io.CopyBuffer(response, reader, make([]byte, 32<<10)); err != nil {
		// net/http aborts the connection without a terminating chunk or extra JSON.
		panic(http.ErrAbortHandler)
	}
}

// proxyFailure maps sanitized library/context failures without exposing URLs, headers or underlying diagnostics.
func proxyFailure(response http.ResponseWriter, request *http.Request, err error) {
	if request.Context().Err() != nil && !errors.Is(err, context.DeadlineExceeded) {
		panic(http.ErrAbortHandler)
	}
	var timed net.Error
	timeout := errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timed) && timed.Timeout()
	switch {
	case errors.Is(err, network.ErrInvalidTarget):
		proxyError(response, 400)
	case errors.Is(err, network.ErrPolicyDenied):
		proxyError(response, 403)
	case timeout:
		_ = http.NewResponseController(response).SetWriteDeadline(time.Now().Add(time.Second))
		proxyError(response, 504)
	case errors.Is(err, context.Canceled):
		proxyError(response, 503)
	default:
		proxyError(response, 502)
	}
}

// proxyError writes only fixed proxy-owned errors; upstream responses bypass this JSON conversion.
func proxyError(response http.ResponseWriter, status int) {
	code, message := "invalid_request", "invalid proxy request"
	switch status {
	case 403:
		code, message = "egress_policy_denied", "egress policy denied"
	case 405:
		code, message = "method_not_allowed", "method not supported"
	case 413:
		code, message = "request_too_large", "request body too large"
	case 429:
		code, message = "proxy_capacity_exceeded", "proxy capacity exceeded"
	case 502:
		code, message = "upstream_unavailable", "upstream unavailable"
	case 503:
		code, message = "proxy_stopping", "proxy is stopping"
	case 504:
		code, message = "upstream_timeout", "upstream timed out"
	}
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Connection", "close")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}
