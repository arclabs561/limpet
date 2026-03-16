package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"inet.af/tcpproxy"

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
	cl, err := newClient(cmd, args)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	addr := mustFlagString(cmd, "addr")
	var p tcpproxy.Proxy
	p.AddRoute(addr, &proxyTarget{ctx, cl})
	log.Debug().Str("addr", addr).Msg("starting proxy server")
	return p.Run()
}

type proxyTarget struct {
	ctx context.Context
	cl *limpet.Client
}

func (s *proxyTarget) HandleConn(downstream net.Conn) {
	defer downstream.Close()

	tcp, ok := downstream.(*net.TCPConn)
	if !ok {
		log.Error().Msg("downstream connection is not a TCP connection")
		return
	}
	br := bufio.NewReader(tcp)
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
		s.handleConnect(tcp, req)
		return
	}

	// http.ReadRequest populates RequestURI, which Go's http.Client rejects.
	// Clear it so the request can be forwarded.
	req.RequestURI = ""

	log.Debug().Str("host", req.URL.Host).Str("path", req.URL.Path).Msg("processing request")

	start := time.Now()
	page, err := s.cl.Do(s.ctx, req)
	if err != nil {
		log.Error().Err(err).Msg("failed to get page")
		return
	}
	doDur := time.Since(start)

	resp := page.HTTPResponse()
	if page.Meta.Version != limpet.LatestPageVersion {
		log.Error().Uint16("version", page.Meta.Version).Msg("unknown page version, dropping connection")
		return
	}

	resp.Header.Add("X-Limpet-Version", fmt.Sprintf("%d", page.Meta.Version))
	resp.Header.Add("X-Limpet-Source", page.Meta.Source)
	resp.Header.Add("X-Limpet-Fetched-At", page.Meta.FetchedAt.Format(time.RFC3339))
	resp.Header.Add("X-Limpet-Fetch-Dur", page.Meta.FetchDur.String())
	resp.Header.Add("X-Limpet-Do-Dur", doDur.String())

	err = resp.Write(downstream)
	if err != nil {
		log.Error().Err(err).Msg("failed to write response")
		return
	}
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
	log.Debug().Str("host", host).Msg("CONNECT tunnel")

	upstream, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		log.Error().Err(err).Str("host", host).Msg("failed to dial upstream")
		resp := &http.Response{
			StatusCode: http.StatusBadGateway,
			ProtoMajor: 1, ProtoMinor: 1,
		}
		resp.Write(downstream)
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
