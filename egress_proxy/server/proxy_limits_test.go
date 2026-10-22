package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nico-hua/agent-sandbox/egress_proxy/network"
)

// TestProxyValidationAndDefaultDeny rejects unsupported HTTP before any DNS/dial and keeps local health available.
func TestProxyValidationAndDefaultDeny(t *testing.T) {
	resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		t.Error("invalid target resolved")
		return nil, nil
	})
	proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolve, nil))
	for _, test := range []struct {
		method, target string
		status         int
	}{
		{"GET", "/relative", 400}, {"GET", "https://example.com/", 400}, {"GET", "ftp://example.com/", 400},
		{"GET", "http://user:secret@example.com/", 400}, {"GET", "http://example.com/#part", 400},
		{"GET", "http://127.0.0.1/", 400}, {"GET", "http://[::1]/", 400}, {"GET", "http://example.com:443/", 400},
		{"GET", "http://example.com:080/", 400}, {"GET", "http://not-allowed.invalid/healthz", 403}, {"TRACE", "http://example.com/", 405},
	} {
		request := httptest.NewRequest(test.method, test.target, nil)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != test.status || !strings.Contains(response.Body.String(), `"error"`) {
			t.Fatalf("%s %s: %d %s", test.method, test.target, response.Code, response.Body.String())
		}
	}
	for _, header := range []string{"Upgrade", "Connection"} {
		request := httptest.NewRequest("GET", "http://example.com/", nil)
		request.Header.Set(header, "upgrade")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != 400 {
			t.Fatalf("upgrade: %d", response.Code)
		}
	}
	empty := NewProxy(nil)
	for _, test := range []struct {
		target string
		status int
	}{{"/healthz", 200}, {"http://example.com/healthz", 403}} {
		response := httptest.NewRecorder()
		empty.ServeHTTP(response, httptest.NewRequest("GET", test.target, nil))
		if response.Code != test.status {
			t.Fatalf("default deny: %d", response.Code)
		}
	}
}

// TestProxyRedirectAndEnvironment keeps redirects at the client boundary and ignores ambient proxy configuration.
func TestProxyRedirectAndEnvironment(t *testing.T) {
	var environmentCalls, calls atomic.Int32
	trap := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { environmentCalls.Add(1) }))
	defer trap.Close()
	t.Setenv("HTTP_PROXY", trap.URL)
	t.Setenv("HTTPS_PROXY", trap.URL)
	t.Setenv("ALL_PROXY", trap.URL)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "" {
			t.Error("proxy enabled automatic compression")
		}
		w.Header().Set("Location", "http://not-allowed.invalid/private")
		w.WriteHeader(302)
		_, _ = io.WriteString(w, "redirect unchanged")
	}))
	defer upstream.Close()
	address := startTestProxy(t, NewProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls)))
	client := proxyClient(t, address)
	client.Transport.(*http.Transport).DisableCompression = true
	response, err := client.Get("http://example.com/redirect")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 302 || response.Header.Get("Location") != "http://not-allowed.invalid/private" || string(data) != "redirect unchanged" || calls.Load() != 1 || environmentCalls.Load() != 0 {
		t.Fatalf("redirect/env: %d %q %d %d", response.StatusCode, data, calls.Load(), environmentCalls.Load())
	}
}

// TestHTTPBodyLimits bounds both known and streamed lengths, aborting a committed oversized response.
func TestHTTPBodyLimits(t *testing.T) {
	for _, mode := range []string{"request-known", "request-chunked", "response-known", "response-chunked", "response-short"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				if len(data) > 4096 {
					t.Errorf("oversized upload forwarded: %d", len(data))
				}
				switch mode {
				case "response-known":
					w.Header().Set("Content-Length", "5000")
					_, _ = io.WriteString(w, strings.Repeat("x", 5000))
				case "response-chunked":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, strings.Repeat("x", 5000))
				case "response-short":
					w.Header().Set("Content-Length", "4000")
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, "partial")
				default:
					_, _ = io.WriteString(w, "ok")
				}
			}))
			defer upstream.Close()
			limits := defaultProxyLimits()
			limits.bodyBytes = 4096
			address := startTestProxy(t, newProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls), limits))
			client := proxyClient(t, address)
			var body io.Reader
			if strings.HasPrefix(mode, "request") {
				body = strings.NewReader(strings.Repeat("x", 5000))
				if mode == "request-chunked" {
					body = struct{ io.Reader }{body}
				}
			}
			request, _ := http.NewRequest("POST", "http://example.com/body", body)
			response, err := client.Do(request)
			if mode == "response-chunked" || mode == "response-short" {
				if err == nil {
					data, readErr := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if readErr == nil || len(data) > 4096 || strings.Contains(string(data), `"error"`) {
						t.Fatalf("partial response disguised as success: %d %v", len(data), readErr)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			want := 413
			if mode == "response-known" {
				want = 502
			}
			if response.StatusCode != want || !strings.Contains(string(data), `"error"`) {
				t.Fatalf("limit response: %d %s", response.StatusCode, data)
			}
			if mode == "request-known" && calls.Load() != 0 {
				t.Fatal("known oversized body connected upstream")
			}
		})
	}
}

// TestHTTPDeadlineAndBodylessResponses returns a pre-commit timeout and preserves HEAD/204/304 body semantics.
func TestHTTPDeadlineAndBodylessResponses(t *testing.T) {
	for _, status := range []int{204, 304, 0} {
		var calls atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status == 0 {
				<-r.Context().Done()
				return
			}
			w.Header().Set("Content-Length", "99999999")
			w.WriteHeader(status)
		}))
		limits := defaultProxyLimits()
		limits.httpTimeout = 100 * time.Millisecond
		address := startTestProxy(t, newProxy(mappedDialer(t, upstream.Listener.Addr().String(), &calls), limits))
		response, err := proxyClient(t, address).Get("http://example.com/wait")
		if err != nil {
			upstream.Close()
			t.Fatal(err)
		}
		data, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		want := status
		if status == 0 {
			want = 504
		}
		if response.StatusCode != want || status != 0 && len(data) != 0 {
			t.Errorf("deadline/bodyless: %d %q", response.StatusCode, data)
		}
		upstream.Close()
	}
}

// TestProxyFailureMapping keeps policy, DNS, connection and timeout errors distinct before CONNECT success.
func TestProxyFailureMapping(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{{network.ErrPolicyDenied, 403}, {network.ErrDNSFailed, 502}, {network.ErrConnectFailed, 502}, {context.DeadlineExceeded, 504}} {
		resolve := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			if errors.Is(test.err, network.ErrPolicyDenied) {
				return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
			}
			return nil, test.err
		})
		proxy := NewProxy(network.NewDialer(allowedPolicy(t), resolve, func(context.Context, string, string) (net.Conn, error) { t.Error("failed DNS dialed"); return nil, nil }))
		request := httptest.NewRequest("CONNECT", "http://placeholder", nil)
		request.RequestURI = "example.com:443"
		request.URL.Host = request.RequestURI
		request.URL.Scheme = ""
		request.URL.Path = ""
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("mapping: %d %s", response.Code, response.Body.String())
		}
	}
}
