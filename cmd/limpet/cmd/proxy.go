package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/arclabs561/limpet"
)

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Proxy exposes a caching HTTP proxy server",
	RunE:  proxyRunE,
}

func init() {
	proxyCmd.Flags().StringP(
		"addr",
		"a",
		"localhost:8080",
		"address to listen on",
	)
}

func proxyRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cl, err := newClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	addr := mustFlagString(cmd, "addr")
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	defer ln.Close()
	log.Debug().Str("addr", addr).Msg("starting proxy server")

	target := &proxyTarget{ctx: ctx, cl: cl}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}
		go target.HandleConn(conn)
	}
}

type proxyTarget struct {
	ctx context.Context
	cl  *limpet.Client
}

func (s *proxyTarget) HandleConn(downstream net.Conn) {
	defer downstream.Close()

	br := bufio.NewReader(downstream)
	req, err := http.ReadRequest(br)
	if err != nil {
		log.Error().Err(err).Msg("failed to read request")
		return
	}
	req = req.WithContext(s.ctx)
	if req.URL == nil || req.URL.Host == "" {
		log.Error().Stringer("url", req.URL).Msg("invalid request URL")
		return
	}

	if req.Method == http.MethodConnect {
		s.handleConnect(downstream, req)
		return
	}

	// http.ReadRequest populates RequestURI, which Go's http.Client rejects.
	// Clear it so the request can be forwarded.
	req.RequestURI = ""

	log.Debug().Str("host", req.URL.Host).Str("path", req.URL.Path).Msg("processing request")

	start := time.Now()
	page, err := s.cl.Do(s.ctx, req)
	if err != nil {
		// If the fetch succeeded but returned non-200, forward the response
		// instead of dropping the connection.
		var notOK *limpet.StatusError
		if errors.As(err, &notOK) {
			page = notOK.Page
		} else {
			writeHTTPError(downstream, http.StatusBadGateway, err)
			return
		}
	}
	doDur := time.Since(start)

	resp := page.HTTPResponse()
	resp.Header.Set("X-Limpet-Source", page.Meta.Source)
	resp.Header.Set("X-Limpet-Fetched-At", page.Meta.FetchedAt.Format(time.RFC3339))
	resp.Header.Set("X-Limpet-Fetch-Dur", page.Meta.FetchDur.String())
	resp.Header.Set("X-Limpet-Do-Dur", doDur.String())

	if err := resp.Write(downstream); err != nil {
		log.Error().Err(err).Msg("failed to write response")
	}
}

func writeHTTPError(conn net.Conn, status int, cause error) {
	log.Error().Err(cause).Int("status", status).Msg("proxy error")
	resp := &http.Response{
		StatusCode: status,
		ProtoMajor: 1, ProtoMinor: 1,
		Header: make(http.Header),
		Body:   http.NoBody,
	}
	resp.Header.Set("Connection", "close")
	_ = resp.Write(conn) // best-effort error response; connection is closing
}

// handleConnect implements HTTP CONNECT tunneling for HTTPS proxying.
// The proxy establishes a TCP connection to the target host and relays
// bytes bidirectionally. Tunneled traffic is opaque (TLS), so no caching
// is applied.
func (s *proxyTarget) handleConnect(downstream net.Conn, req *http.Request) {
	host := req.URL.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "443")
	}

	if err := validateConnectHost(host); err != nil {
		log.Warn().Err(err).Str("host", host).Msg("CONNECT blocked")
		writeHTTPError(downstream, http.StatusForbidden, err)
		return
	}

	log.Debug().Str("host", host).Msg("CONNECT tunnel")

	upstream, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		log.Error().Err(err).Str("host", host).Msg("failed to dial upstream")
		writeHTTPError(downstream, http.StatusBadGateway, err)
		return
	}
	defer upstream.Close()

	// Tell the client the tunnel is established.
	_, err = fmt.Fprintf(downstream, "HTTP/%d.%d 200 Connection Established\r\n\r\n",
		req.ProtoMajor, req.ProtoMinor)
	if err != nil {
		log.Error().Err(err).Msg("failed to write CONNECT response")
		return
	}

	// Relay bytes in both directions.
	var wg sync.WaitGroup
	wg.Add(2)
	relay := func(dst, src net.Conn) {
		defer wg.Done()
		io.Copy(dst, src)
		// Signal EOF to the other side. For TCP, half-close the write
		// direction so the peer sees EOF without tearing down the whole conn.
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}
	go relay(upstream, downstream)
	go relay(downstream, upstream)
	wg.Wait()
}

// validateConnectHost blocks CONNECT tunnels to loopback, private (RFC 1918),
// and link-local addresses to prevent SSRF via the proxy.
func validateConnectHost(hostport string) error {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return fmt.Errorf("invalid host: %w", err)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("DNS lookup failed: %w", err)
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("CONNECT to %s (%s) blocked: private/loopback address", host, ip)
		}
	}
	return nil
}
