package server

import (
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
)

var errTunnelLimit = errors.New("tunnel byte limit exceeded")

// connect validates authority/framing and keeps ownership of both copy workers until their cleanup finishes.
func (proxy *Proxy) connect(response http.ResponseWriter, request *http.Request) {
	if request.URL.Scheme != "" || request.URL.Host != request.RequestURI || request.URL.Path != "" || request.URL.RawQuery != "" || request.URL.User != nil || strings.ContainsAny(request.RequestURI, "/?#@") ||
		request.ContentLength != 0 || len(request.TransferEncoding) != 0 || len(request.Header.Values("Content-Length")) != 0 || request.Header.Get("Transfer-Encoding") != "" || request.Header.Get("Trailer") != "" {
		proxyError(response, 400)
		return
	}
	target, err := proxy.authority(request.RequestURI, "443", false)
	if err != nil {
		proxyFailure(response, request, err)
		return
	}
	task, status := proxy.begin(request, proxy.limits.tunnelTimeout)
	if status != 0 {
		proxyError(response, status)
		return
	}
	defer task.finish()
	// Request.Context also ends on a normal read half-close before Hijack.
	// The tunnel owns EOF handling from registration onward; deadlines, I/O
	// failures and service shutdown still interrupt its bounded context.
	upstream, err := task.dial(task.ctx, "tcp", target)
	if err != nil {
		proxyFailure(response, request, err)
		return
	}
	client, buffered, err := http.NewResponseController(response).Hijack()
	if err != nil {
		proxyError(response, 502)
		return
	}
	defer client.Close()
	deadline, _ := task.ctx.Deadline()
	if client.SetDeadline(deadline) != nil || upstream.SetDeadline(deadline) != nil || !task.commit() {
		return
	}
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	finished := make(chan error, 2)
	// Use Hijack's reader so tunnel bytes coalesced with CONNECT headers survive.
	go func() { finished <- tunnelCopy(upstream, buffered.Reader, proxy.limits.tunnelBytes) }()
	go func() { finished <- tunnelCopy(client, upstream, proxy.limits.tunnelBytes) }()
	for range 2 {
		if err := <-finished; err != nil {
			task.cancel()
		}
	}
}

// tunnelCopy forwards bounded raw TCP bytes, propagating normal EOF as a write half-close when available.
func tunnelCopy(destination net.Conn, source io.Reader, limit int64) error {
	reader := &boundedReader{source: source, remaining: limit, limitError: errTunnelLimit}
	if _, err := io.CopyBuffer(destination, reader, make([]byte, 32<<10)); err != nil {
		return err
	}
	return closeWrite(destination)
}
