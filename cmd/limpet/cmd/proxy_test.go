package cmd

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/arclabs561/limpet"
	"github.com/arclabs561/limpet/blob"
)

func TestProxyHTTP(t *testing.T) {
	pt := setupProxy(t)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("hello from origin"))
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

func TestProxyCONNECT(t *testing.T) {
	pt := setupProxy(t)

	// TLS origin server.
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("hello over TLS"))
	}))
	t.Cleanup(origin.Close)

	// Connect to proxy.
	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	// Send CONNECT request to proxy.
	_, originPort, _ := net.SplitHostPort(origin.Listener.Addr().String())
	connectHost := net.JoinHostPort("127.0.0.1", originPort)
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", connectHost, connectHost)

	// Read the 200 Connection Established response.
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}

	// Upgrade to TLS over the tunnel.
	tlsConn := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true,
	})
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}

	// Send an HTTP request through the TLS tunnel.
	req, _ := http.NewRequest("GET", "https://"+connectHost+"/secure", nil)
	if err := req.Write(tlsConn); err != nil {
		t.Fatalf("write request over tunnel: %v", err)
	}

	resp, err = http.ReadResponse(bufio.NewReader(tlsConn), req)
	if err != nil {
		t.Fatalf("read response over tunnel: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "hello over TLS" {
		t.Errorf("body = %q, want %q", body, "hello over TLS")
	}
}

func TestProxyCONNECTBadUpstream(t *testing.T) {
	pt := setupProxy(t)

	conn, err := net.DialTimeout("tcp", pt.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	// CONNECT to a port nothing listens on.
	fmt.Fprintf(conn, "CONNECT 127.0.0.1:1 HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
}

type proxyState struct {
	addr string
}

func setupProxy(t *testing.T) proxyState {
	t.Helper()

	ctx := context.Background()

	bucketDir, err := os.MkdirTemp("", "limpet-proxy-bucket-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })

	cacheDir, err := os.MkdirTemp("", "limpet-proxy-cache-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })

	bucket, err := blob.NewBucket(ctx, bucketDir, &blob.BucketConfig{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	cl, err := limpet.NewClient(ctx, bucket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	// Listen on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	target := &proxyTarget{ctx: ctx, cl: cl}

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
