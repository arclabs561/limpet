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
	"syscall"
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
	proxyCmd.Flags().Bool(
		"allow-private",
		false,
		"allow requests to private/loopback addresses, including a private "+
			"upstream HTTP(S)_PROXY (disables SSRF protection)",
	)
	proxyCmd.Flags().Bool(
		"allow-remote",
		false,
		"listen on a non-loopback address (the proxy has no authentication)",
	)
}

func proxyRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	addr := mustFlagString(cmd, "addr")
	if err := checkListenAddr(addr, mustFlagBool(cmd, "allow-remote")); err != nil {
		return err
	}
	allowPrivate := mustFlagBool(cmd, "allow-private")
	cl, bucket, err := newClient(cmd, proxyClientOptions(allowPrivate)...)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer cl.Close()
	defer bucket.Close()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	defer ln.Close()
	log.Debug().Str("addr", addr).Msg("starting proxy server")

	target := &proxyTarget{ctx: ctx, cl: cl, allowPrivate: allowPrivate}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}
		go target.HandleConn(conn)
	}
}

type proxyTarget struct {
	ctx          context.Context
	cl           *limpet.Client
	allowPrivate bool
}

func (s *proxyTarget) HandleConn(downstream net.Conn) {
	defer downstream.Close() //nolint:errcheck // best-effort close

	// Deadline prevents slow-loris: drop connections that don't send a
	// complete request within 30 seconds.
	_ = downstream.SetReadDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(downstream)
	req, err := http.ReadRequest(br)
	if err != nil {
		log.Error().Err(err).Msg("failed to read request")
		return
	}
	// Clear deadline for the relay/response phase.
	_ = downstream.SetReadDeadline(time.Time{})
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

	// Block plain HTTP requests to private/loopback addresses (SSRF prevention).
	if !s.allowPrivate {
		host := req.URL.Host
		if _, _, err := net.SplitHostPort(host); err != nil {
			host = net.JoinHostPort(host, "80")
		}
		if err := validateConnectHost(host); err != nil {
			log.Warn().Err(err).Str("host", req.URL.Host).Msg("plain HTTP request blocked")
			writeHTTPError(downstream, http.StatusForbidden, err)
			return
		}
	}

	log.Debug().Str("host", req.URL.Host).Str("path", req.URL.Path).Msg("processing request")

	start := time.Now()
	page, err := s.cl.Do(s.ctx, req, limpet.DoConfig{})
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
	defer resp.Body.Close()
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

	if !s.allowPrivate {
		if err := validateConnectHost(host); err != nil {
			log.Warn().Err(err).Str("host", host).Msg("CONNECT blocked")
			writeHTTPError(downstream, http.StatusForbidden, err)
			return
		}
	}

	log.Debug().Str("host", host).Msg("CONNECT tunnel")

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if !s.allowPrivate {
		dialer.Control = rejectBlockedDial
	}
	upstream, err := dialer.DialContext(s.ctx, "tcp", host)
	if err != nil {
		log.Error().Err(err).Str("host", host).Msg("failed to dial upstream")
		writeHTTPError(downstream, http.StatusBadGateway, err)
		return
	}
	defer upstream.Close() //nolint:errcheck // best-effort close

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
		if _, err := io.Copy(dst, src); err != nil {
			log.Debug().Err(err).Msg("relay copy ended")
		}
		// Signal EOF to the other side. For TCP, half-close the write
		// direction so the peer sees EOF without tearing down the whole conn.
		if tc, ok := dst.(*net.TCPConn); ok {
			if err := tc.CloseWrite(); err != nil {
				log.Debug().Err(err).Msg("half-close write")
			}
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
		if blockedIP(ip) {
			return fmt.Errorf("CONNECT to %s (%s) blocked: private/loopback address", host, ip)
		}
	}
	return nil
}

// cgnat is the RFC 6598 shared address space used inside carrier and cloud
// networks; net.IP.IsPrivate does not cover it.
var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

func blockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || cgnat.Contains(ip)
}

// rejectBlockedDial is a net.Dialer Control hook. It checks the address the
// socket actually connects to, so a hostname that resolved to a public
// address during validation cannot be re-resolved to a private one.
func rejectBlockedDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid dial address %q: %w", address, err)
	}
	if ip := net.ParseIP(host); ip == nil || blockedIP(ip) {
		return fmt.Errorf("dial to %s blocked: private/loopback address", address)
	}
	return nil
}

// proxyClientOptions returns the HTTP client the proxy fetches with. Redirects
// go back to the downstream client instead of being followed, so each hop is
// validated as a new request. Without allowPrivate, dials to private addresses
// are refused at connect time.
func proxyClientOptions(allowPrivate bool) []limpet.Option {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		dialer.Control = rejectBlockedDial
	}
	hc := &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         dialer.DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return []limpet.Option{limpet.WithHTTPClient(hc)}
}

// checkListenAddr refuses a non-loopback listen address unless allowRemote is
// set: the proxy has no authentication.
func checkListenAddr(addr string, allowRemote bool) error {
	if allowRemote {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf(
		"refusing to listen on %q: the proxy has no authentication; "+
			"use a loopback address or pass --allow-remote", addr,
	)
}
