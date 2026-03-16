package cmd

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
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
	sc, err := newScraper(cmd, args)
	if err != nil {
		return fmt.Errorf("failed to create scraper: %w", err)
	}
	addr := mustFlagString(cmd, "addr")
	var p tcpproxy.Proxy
	p.AddRoute(addr, &scraperTarget{ctx, sc})
	log.Debug().Str("addr", addr).Msg("starting proxy server")
	return p.Run()
}

type scraperTarget struct {
	ctx context.Context
	sc  *limpet.Scraper
}

func (s *scraperTarget) HandleConn(downstream net.Conn) {
	defer downstream.Close()

	tcp, ok := downstream.(*net.TCPConn)
	if !ok {
		log.Error().Msg("downstream connection is not a TCP connection")
		return
	}
	req, err := http.ReadRequest(bufio.NewReader(tcp))
	if err != nil {
		log.Error().Err(err).Msg("failed to read request")
		return
	}
	req = req.WithContext(s.ctx)
	if req.URL == nil || req.URL.Host == "" {
		log.Error().Stringer("url", req.URL).Msg("invalid request URL")
		return
	}
	log.Debug().Str("host", req.URL.Host).Str("path", req.URL.Path).Msg("processing request")

	start := time.Now()
	page, err := s.sc.Do(s.ctx, req)
	if err != nil {
		log.Error().Err(err).Msg("failed to get page")
		return
	}
	doDur := time.Since(start)

	resp := page.HTTPResponse()
	switch page.Meta.Version {
	case 0:
		resp.ProtoMajor = 1
		resp.ProtoMinor = 1
	case 1:
	default:
		panic(fmt.Errorf("unknown page version: %d", page.Meta.Version))
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
