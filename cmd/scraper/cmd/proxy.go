package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"inet.af/tcpproxy"

	"github.com/henrywallace/scraper"
)

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Proxy exposes a scraper tcp proxy server",
	RunE:  proxyRunE,
}

func init() {
	proxyCmd.Flags().StringP(
		"addr",
		"a",
		"localhost:8080",
		"whether to use browser automation",
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
	sc  *scraper.Scraper
}

// HandleConn is called when an incoming connection is matched. After the call
// to HandleConn, the tcpproxy package never touches the conn
// again. Implementations are responsible for closing the connection when
// needed.
//
// The concrete type of conn will be of type *Conn if any bytes have been
// consumed for the purposes of route matching.
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

	resp := &http.Response{
		StatusCode:       page.Response.StatusCode,
		ProtoMajor:       page.Response.ProtoMajor,
		ProtoMinor:       page.Response.ProtoMinor,
		TransferEncoding: page.Response.TransferEncoding,
		Header:           page.Response.Header,
		Trailer:          page.Response.Trailer,
		Body:             io.NopCloser(bytes.NewReader(page.Response.Body)),
		ContentLength:    page.Response.ContentLength,
	}
	switch page.Meta.Version {
	case 0:
		resp.ProtoMajor = 1
		resp.ProtoMinor = 1
	case 1:
	default:
		panic(fmt.Errorf("unknown page version: %d", page.Meta.Version))
	}

	resp.Header.Add("X-Scraper-Version", fmt.Sprintf("%d", page.Meta.Version))
	resp.Header.Add("X-Scraper-Source", page.Meta.Source)
	resp.Header.Add("X-Scraper-Retrieve-Dur", page.Meta.RetrieveDur.String())
	resp.Header.Add("X-Scraper-Fetched-At", page.Meta.FetchedAt.Format(time.RFC3339))
	resp.Header.Add("X-Scraper-Fetch-Dur", page.Meta.FetchDur.String())
	resp.Header.Add("X-Scraper-Do-Dur", doDur.String())

	err = resp.Write(downstream)
	if err != nil {
		log.Error().Err(err).Msg("failed to write response")
		return
	}
}

func proxy(wg *sync.WaitGroup, a, b net.Conn) {
	defer wg.Done()
	atcp, btcp := a.(*net.TCPConn), b.(*net.TCPConn)
	if _, err := io.Copy(atcp, btcp); err != nil {
		log.Error().
			Str("src_remote", atcp.RemoteAddr().String()).
			Str("src_local", atcp.LocalAddr().String()).
			Str("dst_local", btcp.LocalAddr().String()).
			Str("dst_remote", btcp.RemoteAddr().String()).
			Err(err).
			Msg("proxy copy error")
	}
	btcp.CloseWrite()
	atcp.CloseRead()
}

func extractSNI(r io.Reader) (string, int, error) {
	handshake, tlsver, err := handshakeRecord(r)
	if err != nil {
		return "", 0, fmt.Errorf("reading TLS record: %s", err)
	}

	sni, err := parseHello(handshake)
	if err != nil {
		return "", 0, fmt.Errorf("reading ClientHello: %s", err)
	}
	if len(sni) == 0 {
		// ClientHello did not present an SNI extension. Valid packet,
		// no hostname.
		return "", tlsver, nil
	}

	hostname, err := parseSNI(sni)
	if err != nil {
		return "", 0, fmt.Errorf("parsing SNI extension: %s", err)
	}
	return hostname, tlsver, nil
}

// Extract the indicated hostname, if any, from the given SNI
// extension bytes.
func parseSNI(b []byte) (string, error) {
	b, _, err := vector(b, 2)
	if err != nil {
		return "", err
	}

	var ret []byte
	for len(b) >= 3 {
		typ := b[0]
		ret, b, err = vector(b[1:], 2)
		if err != nil {
			return "", fmt.Errorf("truncated SNI extension")
		}

		if typ == sniHostnameID {
			return string(ret), nil
		}
	}

	if len(b) != 0 {
		return "", fmt.Errorf("trailing garbage at end of SNI extension")
	}

	// No DNS-based SNI present.
	return "", nil
}

const sniExtensionID = 0
const sniHostnameID = 0

// Parse a TLS handshake record as a ClientHello message and extract
// the SNI extension bytes, if any.
func parseHello(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("zero length handshake record")
	}
	if b[0] != 1 {
		return nil, fmt.Errorf("non-ClientHello handshake record type %d", b[0])
	}

	// We're expecting a stricter TLS parser to run after we've
	// proxied, so we ignore any trailing bytes that might be present
	// (e.g. another handshake message).
	b, _, err := vector(b[1:], 3)
	if err != nil {
		return nil, fmt.Errorf("reading ClientHello: %s", err)
	}

	// ClientHello must be at least 34 bytes to reach the first vector
	// length byte. The actual minimal size is larger than that, but
	// vector() will correctly handle truncated packets.
	if len(b) < 34 {
		return nil, errors.New("ClientHello packet too short")
	}

	if b[0] != 3 {
		return nil, fmt.Errorf("ClientHello has unsupported version %d.%d", b[0], b[1])
	}
	switch b[1] {
	case 1, 2, 3:
		// TLS 1.0, TLS 1.1, TLS 1.2
	default:
		return nil, fmt.Errorf("TLS record has unsupported version %d.%d", b[0], b[1])
	}

	// Skip over version and random struct
	b = b[34:]

	// We don't technically care about SessionID, but we care that the
	// framing is well-formed all the way up to the SNI field, so that
	// we are sure that we're pulling the same SNI bytes as the
	// eventual TLS implementation.
	vec, b, err := vector(b, 1)
	if err != nil {
		return nil, fmt.Errorf("reading ClientHello SessionID: %s", err)
	}
	if len(vec) > 32 {
		return nil, fmt.Errorf("ClientHello SessionID too long (%db)", len(vec))
	}

	// Likewise, we're just checking the bare minimum of framing.
	vec, b, err = vector(b, 2)
	if err != nil {
		return nil, fmt.Errorf("reading ClientHello CipherSuites: %s", err)
	}
	if len(vec) < 2 || len(vec)%2 != 0 {
		return nil, fmt.Errorf("ClientHello CipherSuites invalid length %d", len(vec))
	}

	vec, b, err = vector(b, 1)
	if err != nil {
		return nil, fmt.Errorf("reading ClientHello CompressionMethods: %s", err)
	}
	if len(vec) < 1 {
		return nil, fmt.Errorf("ClientHello CompressionMethods invalid length %d", len(vec))
	}

	// Finally, we reach the extensions.
	if len(b) == 0 {
		// No extensions. This is not an error, it just means we have
		// no SNI payload.
		return nil, nil
	}
	b, vec, err = vector(b, 2)
	if err != nil {
		return nil, fmt.Errorf("reading ClientHello extensions: %s", err)
	}
	if len(vec) != 0 {
		return nil, fmt.Errorf("%d bytes of trailing garbage in ClientHello", len(vec))
	}

	for len(b) >= 4 {
		typ := binary.BigEndian.Uint16(b[:2])
		vec, b, err = vector(b[2:], 2)
		if err != nil {
			return nil, fmt.Errorf("reading ClientHello extension %d: %s", typ, err)
		}
		if typ == sniExtensionID {
			// Found the SNI extension, return its payload. We don't
			// care about anything in the packet beyond this point.
			return vec, nil
		}
	}

	if len(b) != 0 {
		return nil, fmt.Errorf("%d bytes of trailing garbage in ClientHello", len(b))
	}

	// Successfully parsed all extensions, but there was no SNI.
	return nil, nil
}

const maxTLSRecordLength = 16384

// Read one TLS record, which must be for the handshake protocol, from r.
func handshakeRecord(r io.Reader) ([]byte, int, error) {
	var hdr struct {
		Type         uint8
		Major, Minor uint8
		Length       uint16
	}
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return nil, 0, fmt.Errorf("reading TLS record header: %s", err)
	}

	if hdr.Type != 22 {
		return nil, 0, fmt.Errorf("TLS record is not a handshake")
	}

	if hdr.Major != 3 {
		return nil, 0, fmt.Errorf("TLS record has unsupported version %d.%d", hdr.Major, hdr.Minor)
	}
	switch hdr.Minor {
	case 1, 2, 3:
		// TLS 1.0, TLS 1.1, TLS 1.2
	default:
		return nil, 0, fmt.Errorf("TLS record has unsupported version %d.%d", hdr.Major, hdr.Minor)
	}

	if hdr.Length > maxTLSRecordLength {
		return nil, 0, fmt.Errorf("TLS record length is greater than %d", maxTLSRecordLength)
	}

	ret := make([]byte, hdr.Length)
	if _, err := io.ReadFull(r, ret); err != nil {
		return nil, 0, err
	}

	return ret, int(hdr.Minor), nil
}

func vector(b []byte, lenBytes int) ([]byte, []byte, error) {
	if len(b) < lenBytes {
		return nil, nil, errors.New("not enough space in packet for vector")
	}
	var l int
	for _, b := range b[:lenBytes] {
		l = (l << 8) + int(b)
	}
	if len(b) < l+lenBytes {
		return nil, nil, errors.New("not enough space in packet for vector")
	}
	return b[lenBytes : l+lenBytes], b[l+lenBytes:], nil
}
