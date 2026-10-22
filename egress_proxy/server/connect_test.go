package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

// openConnect sends a fixed test authority plus optional coalesced bytes and preserves the buffered tunnel reader.
func openConnect(t *testing.T, address, extra string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	parsed, _ := url.Parse(address)
	connection, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = io.WriteString(connection, "CONNECT example.com:443 HTTP/1.1\r\nHost: forged.invalid\r\n\r\n"+extra)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil {
		t.Fatal(err)
	}
	return connection, reader, response
}

// TestConnectTLSUsesClientTrust verifies end-to-end TLS validation, without MITM or InsecureSkipVerify.
func TestConnectTLSUsesClientTrust(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "secure") }))
	defer upstream.Close()
	var calls atomic.Int32
	proxy := startTestProxy(t, NewProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls)))
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	client := proxyClient(t, proxy)
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	response, err := client.Get("https://example.com/secure")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "secure" || calls.Load() != 1 {
		t.Fatalf("TLS result: %q %v (%d)", data, err, calls.Load())
	}
}

// TestConnectPreservesBufferedBytesAndHalfClose ensures a client EOF does not truncate the upstream reply.
func TestConnectPreservesBufferedBytesAndHalfClose(t *testing.T) {
	upstream, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	finished := make(chan error, 1)
	go func() {
		connection, err := upstream.Accept()
		if err != nil {
			finished <- err
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		data, err := io.ReadAll(connection)
		if err == nil && string(data) != "\x00\xfffirst" {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = connection.Write([]byte("\xff\x00reply"))
		}
		_ = closeWrite(connection)
		finished <- err
	}()
	var calls atomic.Int32
	address := startTestProxy(t, NewProxy(mappedDialer(t, upstream.Addr().String(), &calls)))
	connection, reader, response := openConnect(t, address, "\x00\xfffirst")
	if response.StatusCode != 200 || response.Header.Get("Content-Length") != "" || response.Header.Get("Transfer-Encoding") != "" {
		t.Fatalf("CONNECT framing: %v", response)
	}
	if err := connection.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	upstreamErr := <-finished
	if err != nil || string(data) != "\xff\x00reply" {
		t.Fatalf("reverse after half-close: %q %v; upstream: %v", data, err, upstreamErr)
	}
	if upstreamErr != nil {
		t.Fatal(upstreamErr)
	}
}

// TestConnectRejectsBeforeDial exercises authority, framing and method validation without network effects.
func TestConnectRejectsBeforeDial(t *testing.T) {
	proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolverFunc(func(_ context.Context, _, _ string) ([]netip.Addr, error) {
		t.Error("invalid target resolved")
		return nil, nil
	}), nil))
	for _, target := range []string{"example.com", "example.com:80", "127.0.0.1:443", "[::1]:443", "http://example.com:443", "user@example.com:443", "example.com:443/path", "example.com:443?x=1"} {
		request := httptest.NewRequest("CONNECT", "http://placeholder", nil)
		request.RequestURI = target
		request.URL = &url.URL{Host: target}
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != 400 {
			t.Fatalf("target %s: %d %s", target, response.Code, response.Body.String())
		}
	}
	for _, header := range []string{"Content-Length", "Transfer-Encoding"} {
		request := httptest.NewRequest("CONNECT", "http://placeholder", nil)
		request.RequestURI, request.URL = "example.com:443", &url.URL{Host: "example.com:443"}
		request.Header.Set(header, "0")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != 400 {
			t.Fatalf("framed CONNECT: %d", response.Code)
		}
	}
}

// TestConnectByteLimitAndDeadline closes established tunnels rather than injecting HTTP errors into opaque data.
func TestConnectByteLimitAndDeadline(t *testing.T) {
	for _, mode := range []string{"limit", "reverse-limit", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			upstream, _ := net.Listen("tcp4", "127.0.0.1:0")
			defer upstream.Close()
			finished := make(chan []byte, 1)
			go func() {
				connection, err := upstream.Accept()
				if err != nil {
					finished <- nil
					return
				}
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(time.Second))
				if mode == "reverse-limit" {
					_, _ = io.WriteString(connection, "abcdef")
				}
				data, _ := io.ReadAll(connection)
				finished <- data
			}()
			var calls atomic.Int32
			limits := defaultProxyLimits()
			limits.tunnelBytes = 3
			limits.tunnelTimeout = 100 * time.Millisecond
			address := startTestProxy(t, newProxy(mappedDialer(t, upstream.Addr().String(), &calls), limits))
			connection, reader, response := openConnect(t, address, "")
			if response.StatusCode != 200 {
				t.Fatalf("CONNECT: %d", response.StatusCode)
			}
			if mode == "limit" {
				_, _ = io.WriteString(connection, "abcdef")
			}
			data, _ := io.ReadAll(reader)
			if mode != "reverse-limit" && len(data) != 0 || mode == "reverse-limit" && string(data) != "abc" || strings.Contains(string(data), "error") {
				t.Fatalf("injected tunnel response: %q", data)
			}
			upstreamData := <-finished
			if mode == "limit" && string(upstreamData) != "abc" {
				t.Fatalf("upstream byte bound: %q", upstreamData)
			}
		})
	}
}

// TestConnectHalfCloseBeforeEstablished preserves a complete request followed by FIN before the upstream dial completes.
func TestConnectHalfCloseBeforeEstablished(t *testing.T) {
	upstream, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	finished := make(chan error, 1)
	go func() {
		connection, err := upstream.Accept()
		if err != nil {
			finished <- err
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		_, err = io.Copy(io.Discard, connection)
		if err == nil {
			_, err = io.WriteString(connection, "reply")
		}
		_ = closeWrite(connection)
		finished <- err
	}()
	original := make(chan context.Context, 1)
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dial := func(ctx context.Context, network, target string) (net.Conn, error) {
		if network != "tcp4" || target != "8.8.8.8:443" {
			t.Errorf("unguarded dial: %s %s", network, target)
		}
		requestContext := <-original
		select {
		case <-requestContext.Done(): // Ensure the HTTP server has observed the client's read EOF.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp4", upstream.Addr().String())
	}
	proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolve, dial))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		original <- r.Context()
		proxy.ServeHTTP(w, r)
	}))
	server.Config.ConnContext = func(ctx context.Context, connection net.Conn) context.Context {
		return context.WithValue(ctx, clientConnectionKey{}, connection)
	}
	server.Start()
	defer server.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxy.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	parsed, _ := url.Parse(server.URL)
	connection, err := net.DialTimeout("tcp4", parsed.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	// A complete CONNECT header followed by FIN is a valid empty client-to-upstream stream.
	_, _ = io.WriteString(connection, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
	if err := connection.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("pre-establishment half-close: %v %v", response, err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "reply" {
		t.Fatalf("half-close reply: %q %v", data, err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
