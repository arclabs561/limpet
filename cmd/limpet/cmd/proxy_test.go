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
	defer conn.Close()

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
	defer conn.Close()

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
	pt := setupProxy(t)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

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
	pt := setupProxy(t)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

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

type proxyState struct {
	addr string
}

func setupProxy(t *testing.T) proxyState {
	t.Helper()

	bucket, err := blob.NewBucket(t.Context(), t.TempDir(), &blob.BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	cl, err := limpet.NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	// Listen on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	target := &proxyTarget{ctx: t.Context(), cl: cl}

	// Accept connections in the background.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go target.HandleConn(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })

	return proxyState{addr: ln.Addr().String()}
}
