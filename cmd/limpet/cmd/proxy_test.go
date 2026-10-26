package cmd

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/arclabs561/limpet"
	"github.com/arclabs561/limpet/blob"
)

func TestProxyHTTP(t *testing.T) {
	pt := setupProxy(t)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("hello from origin"))
	}))
	t.Cleanup(origin.Close)

	// Connect to proxy and send an absolute-URI HTTP request (proxy style).
	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// Write raw HTTP with absolute URI so the proxy sees the host.
	reqLine := fmt.Sprintf("GET %s/test HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		origin.URL, origin.Listener.Addr().String())
	if _, err := fmt.Fprint(conn, reqLine); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "hello from origin" {
		t.Errorf("body = %q, want %q", body, "hello from origin")
	}
	if src := resp.Header.Get("X-Limpet-Source"); src != "http.plain" {
		t.Errorf("X-Limpet-Source = %q, want %q", src, "http.plain")
	}
}

func TestProxyHTTPNon200(t *testing.T) {
	pt := setupProxy(t)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	t.Cleanup(origin.Close)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	reqLine := fmt.Sprintf("GET %s/missing HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		origin.URL, origin.Listener.Addr().String())
	fmt.Fprint(conn, reqLine)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if string(body) != "not found" {
		t.Errorf("body = %q, want %q", body, "not found")
	}
}

func TestProxyCONNECTBlocksLoopback(t *testing.T) {
	pt := setupProxyOpts(t, false)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// CONNECT to loopback should be blocked by SSRF check.
	fmt.Fprintf(conn, "CONNECT 127.0.0.1:443 HTTP/1.1\r\nHost: 127.0.0.1:443\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestProxyCONNECTBlocksPrivate(t *testing.T) {
	pt := setupProxyOpts(t, false)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// CONNECT to RFC 1918 address should be blocked.
	fmt.Fprintf(conn, "CONNECT 10.0.0.1:443 HTTP/1.1\r\nHost: 10.0.0.1:443\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestValidateConnectHost(t *testing.T) {
	tests := []struct {
		host    string
		wantErr bool
	}{
		{"127.0.0.1:443", true},
		{"localhost:443", true},
		{"10.0.0.1:443", true},
		{"192.168.1.1:443", true},
		{"172.16.0.1:443", true},
		{"169.254.1.1:443", true},
		{"[::1]:443", true},
		{"0.0.0.0:443", true},
		{"[::]:443", true},
		{"100.64.0.1:443", true},
		// Public addresses should pass.
		{"1.1.1.1:443", false},
		{"8.8.8.8:443", false},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			err := validateConnectHost(tt.host)
			if tt.wantErr && err == nil {
				t.Errorf("validateConnectHost(%q) = nil, want error", tt.host)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateConnectHost(%q) = %v, want nil", tt.host, err)
			}
		})
	}
}

func TestBlockedIPCoversSharedAndUnspecifiedRanges(t *testing.T) {
	tests := []struct {
		ip   string
		want bool
	}{
		{"100.64.0.1", true},
		{"100.127.255.254", true},
		{"100.128.0.1", false},
		{"0.0.0.0", true},
		{"::", true},
		{"8.8.8.8", false},
	}
	for _, tt := range tests {
		if got := blockedIP(net.ParseIP(tt.ip)); got != tt.want {
			t.Errorf("blockedIP(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}

// The plain-HTTP path must refuse private targets when it dials, not only
// when it validates the hostname.
func TestProxyClientRefusesPrivateDial(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("internal"))
	}))
	t.Cleanup(origin.Close)

	bucket, err := blob.NewBucket(t.Context(), t.TempDir(), &blob.BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	cl, err := limpet.NewClient(t.Context(), bucket, proxyClientOptions(false)...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, origin.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := cl.Do(t.Context(), req, limpet.DoConfig{}); err == nil {
		t.Fatal("fetch of a loopback origin succeeded, want a blocked dial")
	}
}

// A redirect must reach the downstream client, so the next hop is a new
// request that goes through validation; following it in the proxy would let
// a public origin bounce the fetch to an internal address.
func TestProxyReturnsRedirectsInsteadOfFollowing(t *testing.T) {
	pt := setupProxy(t)
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("internal"))
	}))
	t.Cleanup(internal.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, internal.URL+"/secret", http.StatusFound)
	}))
	t.Cleanup(origin.Close)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close() //nolint:errcheck
	reqLine := fmt.Sprintf("GET %s/start HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		origin.URL, origin.Listener.Addr().String())
	if _, err := fmt.Fprint(conn, reqLine); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusFound)
	}
}

func TestCheckListenAddr(t *testing.T) {
	tests := []struct {
		addr        string
		allowRemote bool
		wantErr     bool
	}{
		{"localhost:8080", false, false},
		{"127.0.0.1:8080", false, false},
		{"[::1]:8080", false, false},
		{":8080", false, true},
		{"0.0.0.0:8080", false, true},
		{"192.168.1.5:8080", false, true},
		{"0.0.0.0:8080", true, false},
	}
	for _, tt := range tests {
		err := checkListenAddr(tt.addr, tt.allowRemote)
		if (err != nil) != tt.wantErr {
			t.Errorf("checkListenAddr(%q, %v) = %v, wantErr %v", tt.addr, tt.allowRemote, err, tt.wantErr)
		}
	}
}

type proxyState struct {
	addr string
}

func setupProxyOpts(t *testing.T, allowPrivate bool) proxyState {
	t.Helper()

	bucket, err := blob.NewBucket(t.Context(), t.TempDir(), &blob.BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	cl, err := limpet.NewClient(t.Context(), bucket, proxyClientOptions(allowPrivate)...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	target := &proxyTarget{ctx: t.Context(), cl: cl, allowPrivate: allowPrivate}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go target.HandleConn(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })

	return proxyState{addr: ln.Addr().String()}
}

func setupProxy(t *testing.T) proxyState {
	t.Helper()
	// Tests use localhost origins, so allow private by default.
	return setupProxyOpts(t, true)
}
