package limpet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog/log"
	"go.uber.org/ratelimit"

	"github.com/arclabs561/limpet/blob"
)

var reNumericPrefix = regexp.MustCompile(`^\d+`)

// parseRateLimit parses a rate limit string like "100", "10/1m", or "none".
func parseRateLimit(raw string) (ratelimit.Limiter, error) {
	switch strings.ToLower(raw) {
	case "none", "unlimited", "disabled", "off", "nolimit":
		return ratelimit.NewUnlimited(), nil
	}
	parts := strings.SplitN(raw, "/", 2)
	rate, err := strconv.ParseInt(parts[0], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rate %q: %w", raw, err)
	}
	var opts []ratelimit.Option
	if len(parts) == 2 {
		per := parts[1]
		if !reNumericPrefix.MatchString(per) {
			per = fmt.Sprintf("1%s", per)
		}
		dur, err := time.ParseDuration(per)
		if err != nil {
			return nil, fmt.Errorf("failed to parse rate duration %q: %w", raw, err)
		}
		opts = append(opts, ratelimit.Per(dur))
	}
	return ratelimit.New(int(rate), opts...), nil
}

// Client fetches web pages with automatic caching.
type Client struct {
	httpClient       *http.Client
	bucket           *blob.Bucket
	mu               *sync.Mutex
	pw               *playwright.Playwright
	browser          playwright.Browser
	alwaysDoBrowser  bool
	chromiumSandbox  bool
	startBrowser     func() error
	requestBodyLimit int64 // no limit when <= 0
	respBodyLimit    int64 // no limit when <= 0
	rateLimit        ratelimit.Limiter
	startedAt        time.Time
	requests         atomic.Uint64
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithBrowser configures the client to always use headless browser.
func WithBrowser() Option {
	return func(c *Client) { c.alwaysDoBrowser = true }
}

// WithChromiumSandbox controls whether the headless Chromium browser runs
// with OS-level sandboxing. Defaults to true. Set to false in environments
// where sandboxing is unsupported (e.g. CI containers without suid sandbox).
func WithChromiumSandbox(enabled bool) Option {
	return func(c *Client) { c.chromiumSandbox = enabled }
}

// WithRateLimit sets a programmatic rate limit, overriding the env var.
func WithRateLimit(rps int, opts ...ratelimit.Option) Option {
	return func(c *Client) {
		c.rateLimit = ratelimit.New(rps, opts...)
	}
}

// NewClient creates a new Client with the given blob bucket and options.
func NewClient(
	ctx context.Context,
	bucket *blob.Bucket,
	opts ...Option,
) (*Client, error) {
	c := &Client{
		bucket:           bucket,
		mu:               new(sync.Mutex),
		chromiumSandbox:  true,
		requestBodyLimit: 10e6,  // 10 MB
		respBodyLimit:    100e6, // 100 MB
		rateLimit:        ratelimit.New(100),
		startedAt:        time.Now(),
	}

	// Check env var for rate limit override.
	if raw, ok := os.LookupEnv("LIMPET_RATE_LIMIT"); ok {
		lim, err := parseRateLimit(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse LIMPET_RATE_LIMIT=%q: %w", raw, err)
		}
		c.rateLimit = lim
	}

	for _, opt := range opts {
		opt(c)
	}

	c.httpClient = &http.Client{Transport: &http.Transport{}}
	c.startBrowser = sync.OnceValue(c.newBrowser)

	if c.alwaysDoBrowser {
		if err := c.startBrowser(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Close shuts down the browser (if started) and releases resources.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeBrowser()
}

// Get is a convenience method for fetching a URL with GET.
func (c *Client) Get(ctx context.Context, url string, cfgs ...DoConfig) (*Page, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	return c.Do(ctx, req, cfgs...)
}

// StatusError is returned when the HTTP status is not 200 OK. The
// Page contains the response and status.
type StatusError struct {
	Page *Page
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("bad fetch status: %d", e.Page.Response.StatusCode)
}

func errPageStatusNotOK(page *Page) error {
	if page.Response.StatusCode != 200 {
		return &StatusError{Page: page}
	}
	return nil
}

// ThrottledError is returned when the fetch is throttled.
type ThrottledError struct{}

func (e *ThrottledError) Error() string {
	return "fetch throttled"
}

// Do fetches the given request, returning a cached result if available.
// Pass a DoConfig to control caching, browser mode, and rate limiting.
func (c *Client) Do(
	ctx context.Context,
	req *http.Request,
	cfgs ...DoConfig,
) (page *Page, err error) {
	var cfg DoConfig
	for _, c := range cfgs {
		if c.Replace {
			cfg.Replace = true
		}
		if c.Browser {
			cfg.Browser = true
		}
		if c.Archive {
			cfg.Archive = true
		}
		if c.SilentThrottle != nil {
			cfg.SilentThrottle = c.SilentThrottle
		}
		if c.Limiter != nil {
			cfg.Limiter = c.Limiter
		}
	}
	opts := doOptions{
		Replace:          cfg.Replace,
		Archive:          cfg.Archive,
		ReSilentThrottle: cfg.SilentThrottle,
		Limiter:          cfg.Limiter,
	}
	fn := c.fetchHTTP
	if cfg.Browser {
		fn = c.fetchBrowser
	}
	return c.do(ctx, req, opts, fn)
}

func (c *Client) newBrowser() (err error) {
	start := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	log.Debug().Msg("starting playwright instance")
	pw, err := playwright.Run(&playwright.RunOptions{
		Verbose: true,
	})
	if err != nil {
		return fmt.Errorf("failed to run playwright instance: %w", err)
	}
	c.pw = pw
	log.Debug().Msg("launching headless chromium browser")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless:        playwright.Bool(true),
		ChromiumSandbox: playwright.Bool(c.chromiumSandbox),
	})
	if err != nil {
		return fmt.Errorf("failed to launch browser: %w", err)
	}
	if !browser.IsConnected() {
		return fmt.Errorf("browser is not connected")
	}
	c.browser = browser

	log.Debug().
		Stringer("dur", time.Since(start).Round(time.Microsecond)).
		Msg("browser ready")
	return nil
}

func (c *Client) closeBrowser() {
	if c.browser != nil {
		if err := c.browser.Close(); err != nil {
			log.Err(err).Msg("failed to close browser")
		}
		c.browser = nil
	}
	if c.pw != nil {
		if err := c.pw.Stop(); err != nil {
			log.Err(err).Msg("failed to close playwright instance")
		}
		c.pw = nil
	}
}

type doOptions struct {
	Replace          bool
	Archive          bool
	ReSilentThrottle *regexp.Regexp
	Limiter          Limiter
}

type fetchFn func(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error)

func (c *Client) fetchHTTP(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error) {
	start := time.Now()

	var resp *http.Response
	var body []byte
	var err error
	attemptsMax := 5
	waitMin := 1 * time.Second
	waitMax := 1 * time.Minute
	waitJitter := 1 * time.Second
	wait := func(attempt int) error {
		d := time.Duration(math.Pow(2, float64(attempt))) * waitMin
		d += time.Duration(rand.Intn(int(waitJitter)))
		if d > waitMax {
			d = waitMax
		}
		t := time.After(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t:
			return nil
		}
	}
	for i := 0; i < attemptsMax; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Apply rate limiting before each attempt.
		val, ok := req.Context().Value(ctxKeyLimiter{}).(ctxValLimiter)
		if ok {
			val.Limiter.Take()
		} else {
			c.rateLimit.Take()
		}
		c.requests.Add(1)

		resp, err = c.httpClient.Do(req)
		if err != nil {
			if lastAttempt := i >= attemptsMax-1; lastAttempt {
				return nil, fmt.Errorf("failed to perform http request: %w", err)
			}
			log.Warn().Err(err).Int("attempt", i).Msg("http request failed, retrying")
			if err := wait(i); err != nil {
				return nil, err
			}
			// Restore request body for retry.
			req.Body = io.NopCloser(bytes.NewReader(reqBody))
			continue
		}
		rdr := resp.Body
		if c.respBodyLimit > 0 {
			rdr = http.MaxBytesReader(nil, resp.Body, c.respBodyLimit)
		}
		body, err = io.ReadAll(rdr)
		resp.Body.Close()
		lastAttempt := i >= attemptsMax-1
		if err != nil {
			if lastAttempt {
				return nil, fmt.Errorf("failed to read http resp body: %w", err)
			}
			log.Warn().Err(err).Int("attempt", i).Msg("failed to read http resp body, retrying")
			if err := wait(i); err != nil {
				return nil, err
			}
			continue
		}
		if opts.ReSilentThrottle != nil && opts.ReSilentThrottle.Match(body) {
			n := c.requests.Load()
			rate := float64(n) / (time.Since(c.startedAt).Minutes())
			log.Warn().
				Str("rate", fmt.Sprintf("%0.3f/m", rate)).
				Msg("silently throttled")
			if lastAttempt {
				return nil, &ThrottledError{}
			}
			log.Warn().Int("attempt", i).Msg("response is silently throttled, retrying")
			if err := wait(i); err != nil {
				return nil, err
			}
			continue
		}
		break
	}

	redirect := ""
	if resp.Request.URL.String() != req.URL.String() {
		redirect = resp.Request.URL.String()
	}
	dur := time.Since(start)
	return &Page{
		Meta: PageMeta{
			Version:   latestPageVersion,
			Source:    "http.plain",
			FetchedAt: time.Now(),
			FetchDur:  dur,
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: redirect,
			Method:        req.Method,
			Header:        resp.Request.Header,
			Body:          reqBody,
		},
		Response: PageResponse{
			StatusCode:       resp.StatusCode,
			ProtoMajor:       resp.ProtoMajor,
			ProtoMinor:       resp.ProtoMinor,
			TransferEncoding: resp.TransferEncoding,
			Trailer:          resp.Trailer,
			Body:             body,
			ContentLength:    resp.ContentLength,
			Header:           resp.Header,
		},
	}, nil
}

func (c *Client) fetchBrowser(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	opts doOptions,
) (*Page, error) {
	// Hold the mutex while checking/starting the browser to avoid a data race
	// with closeBrowser (which also writes c.browser under mu).
	c.mu.Lock()
	needStart := c.browser == nil
	c.mu.Unlock()
	if needStart {
		if err := c.startBrowser(); err != nil {
			return nil, fmt.Errorf("failed to start browser: %w", err)
		}
	}
	c.mu.Lock()
	if c.browser == nil || !c.browser.IsConnected() {
		c.mu.Unlock()
		return nil, fmt.Errorf("browser is not connected")
	}
	c.mu.Unlock()
	if req.Method != "GET" {
		return nil, fmt.Errorf("browser only supports requests with GET method")
	}

	bctx, err := c.browser.NewContext(playwright.BrowserNewContextOptions{
		ServiceWorkers: playwright.ServiceWorkerPolicyBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create context: %w", err)
	}
	defer bctx.Close()

	fulfill := func(route playwright.Route, fn func() (*Page, error)) {
		page, err := fn()
		if err != nil {
			log.Err(err).Msg("failed to fulfill route")
			err2 := route.Fulfill(playwright.RouteFulfillOptions{
				Status: playwright.Int(http.StatusInternalServerError),
			})
			if err2 != nil {
				log.Err(err2).Msgf("failed to fulfill route (%d)", http.StatusInternalServerError)
			}
			return
		}
		headers := make(map[string]string)
		for key := range page.Response.Header {
			headers[key] = page.Response.Header.Get(key)
		}
		err = route.Fulfill(playwright.RouteFulfillOptions{
			Body:    page.Response.Body,
			Headers: headers,
			Status:  playwright.Int(page.Response.StatusCode),
		})
		if err != nil {
			log.Err(err).Msgf("failed to fulfill route (%d)", page.Response.StatusCode)
		}
	}
	err = bctx.Route("**/*", func(route playwright.Route) {
		fulfill(route, func() (*Page, error) {
			req := route.Request()
			r, err := http.NewRequest(req.Method(), req.URL(), nil)
			if err != nil {
				return nil, fmt.Errorf("failed to make new request: %w", err)
			}
			r.Header = make(http.Header)
			for k, v := range req.Headers() {
				r.Header.Set(k, v)
			}
			return c.fetchHTTP(ctx, r, reqBody, opts)
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to intercept browser routes: %w", err)
	}

	browserPage, err := bctx.NewPage()
	if err != nil {
		return nil, fmt.Errorf("could not create page: %w", err)
	}

	browserPage.On("request", func(req playwright.Request) {
		log.Debug().
			Str("url", req.URL()).
			Str("method", req.Method()).
			Msg("browser making page request")
	})
	resp, err := browserPage.Goto(req.URL.String(), playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
	})
	if err != nil {
		return nil, fmt.Errorf("page failed to goto url: %w", err)
	}
	html, err := browserPage.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get content from page: %w", err)
	}
	header := make(http.Header)
	allHeaders, err := resp.AllHeaders()
	if err != nil {
		return nil, fmt.Errorf("failed to get page response headers: %w", err)
	}
	for key, value := range allHeaders {
		header.Set(key, value)
	}
	return &Page{
		Meta: PageMeta{
			Version:   latestPageVersion,
			Source:    "http.browser",
			FetchedAt: time.Now(),
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: resp.URL(),
			Method:        req.Method,
			Header:        req.Header,
			Body:          reqBody,
		},
		Response: PageResponse{
			StatusCode: resp.Status(),
			Header:     header,
			Body:       []byte(html),
		},
	}, nil
}

// readCachedPage reads a cached page from the bucket. Returns the page and
// its source label, or a *blob.NotFoundError if not cached.
func readCachedPage(ctx context.Context, bucket *blob.Bucket, key string) (*Page, error) {
	b, err := bucket.GetBlob(ctx, key)
	if err != nil {
		return nil, err
	}
	page := new(Page)
	if err := json.Unmarshal(b.Data, page); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached page: %w", err)
	}
	page.Meta.Source = b.Source
	return page, nil
}

// writeCachedPage writes a page to the bucket.
func writeCachedPage(ctx context.Context, bucket *blob.Bucket, key string, page *Page) error {
	data, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("failed to marshal page: %w", err)
	}
	return bucket.SetBlob(ctx, key, data)
}

func (c *Client) do(
	ctx context.Context,
	req *http.Request,
	opts doOptions,
	fetchFn fetchFn,
) (page *Page, err error) {
	start := time.Now()

	bkey, reqBody, err := c.blobKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob key: %w", err)
	}

	if !opts.Replace {
		page, err := readCachedPage(ctx, c.bucket, bkey)
		var notFound *blob.NotFoundError
		if !errors.As(err, &notFound) {
			if err != nil {
				return nil, fmt.Errorf("failed to read from blob: %w", err)
			}
			if err := errPageStatusNotOK(page); err != nil {
				return nil, err
			}
			return page, nil
		}
	}

	if opts.Limiter != nil {
		rctx := req.Context()
		rctx = context.WithValue(rctx, ctxKeyLimiter{}, ctxValLimiter{opts.Limiter})
		req = req.WithContext(rctx)
	}
	page, err = fetchFn(ctx, req, reqBody, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page: %w", err)
	}

	// Only cache successful (200) responses to prevent transient errors
	// from becoming permanent cache entries.
	if page.Response.StatusCode == 200 {
		if err := writeCachedPage(ctx, c.bucket, bkey, page); err != nil {
			return nil, fmt.Errorf("failed to write page: %w", err)
		}
		// Write a timestamped archive snapshot for version history.
		if opts.Archive {
			akey := archiveKey(bkey, page.Meta.FetchedAt)
			if err := writeCachedPage(ctx, c.bucket, akey, page); err != nil {
				log.Warn().Err(err).Str("key", akey).Msg("failed to write archive snapshot")
			}
		}
	}

	log.Info().
		Stringer("url", req.URL).
		Str("method", page.Request.Method).
		Int("status", page.Response.StatusCode).
		Int("resp_bytes", len(page.Response.Body)).
		Stringer("dur", time.Since(start).Round(time.Millisecond)).
		Str("content_type", page.Response.Header.Get("Content-Type")).
		Str("req_body", string(reqBody)).
		Msg("fetched page")

	if err := errPageStatusNotOK(page); err != nil {
		return nil, err
	}
	return page, nil
}

func (c *Client) blobKey(req *http.Request) (string, []byte, error) {
	return blobKey(req, c.requestBodyLimit)
}

// blobKey computes a deterministic cache key from an HTTP request.
// The key is SHA-256 of (URL + method + headers + body), placed under the
// request hostname. bodyLimit limits how much of the request body is read
// (0 means no limit). The request body is restored after reading.
func blobKey(req *http.Request, bodyLimit int64) (string, []byte, error) {
	var buf bytes.Buffer
	buf.WriteString(req.URL.String())
	buf.WriteString(".")
	buf.WriteString(req.Method)
	buf.WriteString(".")
	// Sort header keys for deterministic cache keys. http.Header is a map
	// with non-deterministic iteration order; WriteSubset would produce
	// different hashes for identical headers.
	keys := make([]string, 0, len(req.Header))
	for k := range req.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range req.Header[k] {
			buf.WriteString(k)
			buf.WriteString(": ")
			buf.WriteString(v)
			buf.WriteString("\r\n")
		}
	}
	buf.WriteString(".")
	body, err := peekRequestBody(req, bodyLimit)
	if err != nil {
		return "", nil, err
	}
	buf.Write(body)
	buf.WriteString(".")
	h := sha256.Sum256(buf.Bytes())
	henc := base64.RawURLEncoding.EncodeToString(h[:])
	bkey := filepath.Join(req.URL.Hostname(), henc) + ".json"
	return bkey, body, nil
}

// archiveKey returns a timestamped key for storing a version snapshot.
// Given "hostname/hash.json" and a time, returns "hostname/hash@20060102T150405.000Z.json".
// Millisecond precision avoids collisions for rapid sequential fetches.
func archiveKey(bkey string, t time.Time) string {
	base := strings.TrimSuffix(bkey, ".json")
	return base + "@" + t.UTC().Format("20060102T150405.000Z") + ".json"
}

// archivePrefix returns the prefix for listing all archive snapshots of a key.
func archivePrefix(bkey string) string {
	return strings.TrimSuffix(bkey, ".json") + "@"
}

// PageVersion describes a single archived snapshot of a cached page.
type PageVersion struct {
	Key       string    // Cache key for this snapshot.
	FetchedAt time.Time // When this snapshot was fetched.
	BodyHash  string    // SHA-256 hex digest of the response body.
}

// Versions lists all archived snapshots for the given request, ordered by
// fetch time (oldest first). Returns nil if no archive entries exist.
func (c *Client) Versions(ctx context.Context, req *http.Request) ([]PageVersion, error) {
	bkey, _, err := c.blobKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to compute blob key: %w", err)
	}
	prefix := archivePrefix(bkey)
	entries, err := c.bucket.ListCache(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list archive entries: %w", err)
	}
	versions := make([]PageVersion, 0, len(entries))
	for _, entry := range entries {
		// Parse timestamp from key: "hostname/hash@20060102T150405Z.json"
		at := strings.TrimPrefix(entry.Key, strings.TrimSuffix(prefix, "@")+"@")
		at = strings.TrimSuffix(at, ".json")
		t, err := time.Parse("20060102T150405.000Z", at)
		if err != nil {
			continue // skip malformed entries
		}
		versions = append(versions, PageVersion{
			Key:       entry.Key,
			FetchedAt: t,
		})
	}
	return versions, nil
}

// Version reads a specific archived page snapshot by its key.
func (c *Client) Version(ctx context.Context, key string) (*Page, error) {
	return readCachedPage(ctx, c.bucket, key)
}

// Diff compares two pages and returns whether the response body changed.
func Diff(a, b *Page) PageDiff {
	aHash := sha256.Sum256(a.Response.Body)
	bHash := sha256.Sum256(b.Response.Body)
	return PageDiff{
		Changed:    aHash != bHash,
		OldSize:    len(a.Response.Body),
		NewSize:    len(b.Response.Body),
		OldFetched: a.Meta.FetchedAt,
		NewFetched: b.Meta.FetchedAt,
	}
}

// PageDiff describes the difference between two page snapshots.
type PageDiff struct {
	Changed    bool
	OldSize    int
	NewSize    int
	OldFetched time.Time
	NewFetched time.Time
}

// peekRequestBody reads the request body (up to limit bytes) and replaces it
// with a fresh reader so it can be sent again.
func peekRequestBody(req *http.Request, limit int64) ([]byte, error) {
	var body []byte
	if req.Body != nil {
		rdr := req.Body
		if limit > 0 {
			rdr = http.MaxBytesReader(nil, req.Body, limit)
		}
		var err error
		body, err = io.ReadAll(rdr)
		if err != nil {
			return nil, fmt.Errorf("failed to read http req body: %w", err)
		}
	}
	req.Body = io.NopCloser(bytes.NewBuffer(body))
	return body, nil
}

// DoConfig controls per-request behavior for Client.Do and Client.Get.
type DoConfig struct {
	// Replace skips cache read, forcing a fresh fetch (still caches the result).
	Replace bool
	// Browser uses headless Chromium instead of plain HTTP.
	Browser bool
	// Archive stores a timestamped snapshot alongside the latest cache entry.
	// Use Client.Versions to list snapshots and detect changes over time.
	Archive bool
	// SilentThrottle detects and retries when a site silently serves
	// captcha/block pages matching this regexp.
	SilentThrottle *regexp.Regexp
	// Limiter applies a per-request rate limiter instead of the client default.
	Limiter Limiter
}

type ctxKeyLimiter struct{}
type ctxValLimiter struct {
	Limiter Limiter
}

// Limiter is a rate limiter interface compatible with go.uber.org/ratelimit.
type Limiter interface {
	Take() time.Time
}

// latestPageVersion is the current cache page schema version.
const latestPageVersion = 1

// Page is a cached HTTP request/response pair with metadata.
type Page struct {
	Meta     PageMeta     `json:"meta"`
	Request  PageRequest  `json:"request"`
	Response PageResponse `json:"response"`
}

// Stale reports whether this cached page is stale according to HTTP cache
// semantics (Cache-Control max-age, Expires header). Returns false if the
// response has no cache directives (treat as fresh). This is informational --
// the caller decides whether to re-fetch.
func (p *Page) Stale() bool {
	if p.Meta.FetchedAt.IsZero() {
		return true
	}
	age := time.Since(p.Meta.FetchedAt)

	// Check Cache-Control: max-age=N
	cc := p.Response.Header.Get("Cache-Control")
	if cc != "" {
		for _, directive := range strings.Split(cc, ",") {
			directive = strings.TrimSpace(directive)
			if strings.EqualFold(directive, "no-cache") || strings.EqualFold(directive, "no-store") {
				return true
			}
			if strings.HasPrefix(strings.ToLower(directive), "max-age=") {
				secs, err := strconv.Atoi(strings.TrimPrefix(strings.ToLower(directive), "max-age="))
				if err == nil {
					return age > time.Duration(secs)*time.Second
				}
			}
		}
	}

	// Check Expires header.
	if expires := p.Response.Header.Get("Expires"); expires != "" {
		t, err := http.ParseTime(expires)
		if err == nil {
			return time.Now().After(t)
		}
	}

	return false
}

// StaleAfter reports whether this cached page was fetched more than maxAge ago.
// Use this for scraping targets that send no HTTP cache headers.
func (p *Page) StaleAfter(maxAge time.Duration) bool {
	if p.Meta.FetchedAt.IsZero() {
		return true
	}
	return time.Since(p.Meta.FetchedAt) > maxAge
}

// HTTPResponse reconstructs a standard *http.Response from the cached page.
func (p *Page) HTTPResponse() *http.Response {
	return &http.Response{
		StatusCode:       p.Response.StatusCode,
		ProtoMajor:       p.Response.ProtoMajor,
		ProtoMinor:       p.Response.ProtoMinor,
		Header:           p.Response.Header,
		Body:             io.NopCloser(bytes.NewReader(p.Response.Body)),
		ContentLength:    p.Response.ContentLength,
		TransferEncoding: p.Response.TransferEncoding,
		Trailer:          p.Response.Trailer,
	}
}

// PageMeta contains cache metadata for a fetched page.
type PageMeta struct {
	Version   uint16        `json:"version"`
	Source    string        `json:"-"`
	FetchedAt time.Time     `json:"fetched_at"`
	FetchDur  time.Duration `json:"fetch_dur"`
}

// PageRequest stores the original HTTP request details.
type PageRequest struct {
	URL           string      `json:"url"`
	RedirectedURL string      `json:"redirected_url,omitempty"`
	Method        string      `json:"method"`
	Header        http.Header `json:"header,omitempty"`
	Body          []byte      `json:"body,omitempty"`
}

// PageResponse stores the HTTP response details including the body.
type PageResponse struct {
	StatusCode       int         `json:"status_code"`
	ProtoMajor       int         `json:"proto_major"`
	ProtoMinor       int         `json:"proto_minor"`
	TransferEncoding []string    `json:"transfer_encoding,omitempty"`
	ContentLength    int64       `json:"content_length"`
	Header           http.Header `json:"header"`
	Body             []byte      `json:"body"`
	Trailer          http.Header `json:"trailer,omitempty"`
}
