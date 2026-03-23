package limpet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	azuretls "github.com/Noooste/azuretls-client"
	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog/log"
	"go.uber.org/ratelimit"

	"github.com/arclabs561/limpet/blob"
)

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
		// Prefix bare duration units (e.g. "s", "m") with "1" so time.ParseDuration works.
		if len(per) > 0 && (per[0] < '0' || per[0] > '9') {
			per = "1" + per
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
	cache           cacheLayer
	httpClient      *http.Client
	mu              *sync.Mutex
	pw              *playwright.Playwright
	browser         playwright.Browser
	alwaysDoBrowser bool
	alwaysDoStealth bool
	chromiumSandbox bool
	startBrowser    func() error
	respBodyLimit   int64 // no limit when <= 0
	userAgent       string
	retry           RetryConfig
	rateLimit       ratelimit.Limiter
	startedAt       time.Time
	requests        atomic.Uint64
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithBrowser configures the client to always use headless browser.
func WithBrowser() Option {
	return func(c *Client) { c.alwaysDoBrowser = true }
}

// WithStealth configures the client to always use the stealth (azuretls)
// transport, which presents a real browser TLS fingerprint to bypass
// Cloudflare and similar bot-detection systems.
func WithStealth() Option {
	return func(c *Client) { c.alwaysDoStealth = true }
}

// WithChromiumSandbox controls whether the headless Chromium browser runs
// with OS-level sandboxing. Defaults to true. Set to false in environments
// where sandboxing is unsupported (e.g. CI containers without suid sandbox).
func WithChromiumSandbox(enabled bool) Option {
	return func(c *Client) { c.chromiumSandbox = enabled }
}

// WithRequestBodyLimit sets the maximum request body size used for cache key
// computation. 0 means no limit. Default: 10 MB.
func WithRequestBodyLimit(n int64) Option {
	return func(c *Client) { c.cache.requestBodyLimit = n }
}

// WithResponseBodyLimit sets the maximum response body size to read and cache.
// 0 means no limit. Default: 100 MB.
func WithResponseBodyLimit(n int64) Option {
	return func(c *Client) { c.respBodyLimit = n }
}

// WithIgnoreHeaders excludes the named headers from cache key computation.
// Useful for scraping where User-Agent or Accept-Encoding vary between
// requests but should map to the same cache entry.
func WithIgnoreHeaders(names ...string) Option {
	return func(c *Client) {
		if c.cache.ignoreHeaders == nil {
			c.cache.ignoreHeaders = make(map[string]bool)
		}
		for _, n := range names {
			c.cache.ignoreHeaders[http.CanonicalHeaderKey(n)] = true
		}
	}
}

// WithIgnoreParams excludes the named query parameters from cache key
// computation. Useful for stripping auth tokens, timestamps, or tracking
// params (utm_source, etc.) that vary between requests to the same resource.
func WithIgnoreParams(names ...string) Option {
	return func(c *Client) {
		if c.cache.ignoreParams == nil {
			c.cache.ignoreParams = make(map[string]bool)
		}
		for _, n := range names {
			c.cache.ignoreParams[n] = true
		}
	}
}

// WithCacheStatuses sets which HTTP status codes are eligible for caching.
// By default only 200 is cached. Use this to also cache redirects, 404s, etc.
func WithCacheStatuses(codes ...int) Option {
	return func(c *Client) {
		c.cache.cacheStatuses = make(map[int]bool, len(codes))
		for _, code := range codes {
			c.cache.cacheStatuses[code] = true
		}
	}
}

// WithUserAgent sets a default User-Agent header on all HTTP requests.
// The header is added before each request if not already set by the caller.
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.userAgent = ua }
}

// WithRateLimit sets a programmatic rate limit, overriding the env var.
func WithRateLimit(rps int, opts ...ratelimit.Option) Option {
	return func(c *Client) {
		c.rateLimit = ratelimit.New(rps, opts...)
	}
}

// RetryConfig controls retry behavior for failed HTTP requests.
type RetryConfig struct {
	// Attempts is the maximum number of tries (including the first). Default: 5.
	Attempts int
	// MinWait is the base wait duration for exponential backoff. Default: 1s.
	MinWait time.Duration
	// MaxWait caps the backoff duration. Default: 1m.
	MaxWait time.Duration
	// Jitter adds random jitter up to this duration per attempt. Default: 1s.
	Jitter time.Duration
}

// WithHTTPClient sets the underlying HTTP client used for requests.
// Use this to configure custom transports, proxies, or TLS settings.
// If not set, a default client with proxy support (HTTP_PROXY/HTTPS_PROXY)
// is created automatically.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithRefreshPatterns sets URL-based cache TTL rules. When writing to cache,
// the first matching pattern determines the TTL. Patterns are checked in order.
// If no pattern matches, the bucket's default TTL applies.
//
// This is analogous to Squid's refresh_pattern directive.
func WithRefreshPatterns(patterns ...RefreshPattern) Option {
	return func(c *Client) { c.cache.refreshPatterns = patterns }
}

// WithStaleIfError configures the client to return a stale cached response
// when a fresh fetch fails (network error, server error, etc.). Only applies
// when a cached response exists (e.g. during CachePolicyReplace / force-refetch).
func WithStaleIfError(enabled bool) Option {
	return func(c *Client) { c.cache.staleIfError = enabled }
}

// WithRetry configures retry behavior. Zero-value fields keep defaults.
func WithRetry(cfg RetryConfig) Option {
	return func(c *Client) {
		if cfg.Attempts > 0 {
			c.retry.Attempts = cfg.Attempts
		}
		if cfg.MinWait > 0 {
			c.retry.MinWait = cfg.MinWait
		}
		if cfg.MaxWait > 0 {
			c.retry.MaxWait = cfg.MaxWait
		}
		if cfg.Jitter > 0 {
			c.retry.Jitter = cfg.Jitter
		}
	}
}

// NewClient creates a new Client with the given blob bucket and options.
func NewClient(
	ctx context.Context,
	bucket *blob.Bucket,
	opts ...Option,
) (*Client, error) {
	c := &Client{
		cache: cacheLayer{
			bucket:           bucket,
			requestBodyLimit: 10e6, // 10 MB
		},
		mu:              new(sync.Mutex),
		chromiumSandbox: true,
		respBodyLimit:   100e6, // 100 MB
		retry: RetryConfig{
			Attempts: 5,
			MinWait:  1 * time.Second,
			MaxWait:  1 * time.Minute,
			Jitter:   1 * time.Second,
		},
		rateLimit: ratelimit.New(100),
		startedAt: time.Now(),
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

	if c.httpClient == nil {
		c.httpClient = &http.Client{Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		}}
	}
	c.startBrowser = sync.OnceValue(c.newBrowser)

	if c.alwaysDoBrowser {
		if err := c.startBrowser(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Close shuts down the browser (if started) and releases resources.
// Close does NOT close the underlying blob.Bucket -- the caller owns its
// lifecycle and must close it separately.
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
	var cfg DoConfig
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	return c.Do(ctx, req, cfg)
}

// StatusError is returned when the HTTP status is not 200 OK. The
// Page contains the response and status.
type StatusError struct {
	Page *Page
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("bad fetch status: %d", e.Page.Response.StatusCode)
}

// errPageStatusNotOK returns a StatusError if the page's status code is not
// in the set of accepted statuses. By default only 200 is accepted. When
// WithCacheStatuses is configured, all cacheable statuses are accepted.
func (c *Client) errPageStatusNotOK(page *Page) error {
	if c.cache.isCacheable(page.Response.StatusCode) {
		return nil
	}
	return &StatusError{Page: page}
}

// ThrottledError is returned when the fetch is throttled.
type ThrottledError struct{}

func (e *ThrottledError) Error() string {
	return "fetch throttled"
}

// Do fetches the given request, returning a cached result if available.
func (c *Client) Do(
	ctx context.Context,
	req *http.Request,
	cfg DoConfig,
) (page *Page, err error) {
	if cfg.Browser && cfg.Stealth {
		return nil, fmt.Errorf("limpet: Browser and Stealth are mutually exclusive")
	}
	fn := c.fetchHTTP
	if cfg.Stealth || c.alwaysDoStealth {
		fn = c.fetchStealth
	}
	if cfg.Browser || c.alwaysDoBrowser {
		fn = c.fetchBrowser
	}
	return c.do(ctx, req, cfg, fn)
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

type fetchFn func(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	cfg DoConfig,
) (*Page, error)

func (c *Client) fetchHTTP(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	cfg DoConfig,
) (*Page, error) {
	start := time.Now()

	var resp *http.Response
	var body []byte
	err := c.retryDo(ctx, req, cfg, func() error {
		var err error
		// Restore request body for each attempt.
		req.Body = io.NopCloser(bytes.NewReader(reqBody))

		resp, err = c.httpClient.Do(req) //nolint:bodyclose // closed on line below (ReadAll + Close)
		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			return fmt.Errorf("failed to perform http request: %w", err)
		}
		rdr := resp.Body
		if c.respBodyLimit > 0 {
			rdr = http.MaxBytesReader(nil, resp.Body, c.respBodyLimit)
		}
		body, err = io.ReadAll(rdr)
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("failed to read http resp body: %w", err)
		}
		return nil
	}, func() []byte { return body })
	if err != nil {
		return nil, err
	}

	redirect := ""
	if resp.Request.URL.String() != req.URL.String() {
		redirect = resp.Request.URL.String()
	}
	dur := time.Since(start)
	return &Page{
		Meta: PageMeta{
			Version:   latestPageVersion,
			Source:    SourceHTTPPlain,
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

func (c *Client) fetchStealth(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	cfg DoConfig,
) (*Page, error) {
	start := time.Now()

	session := azuretls.NewSessionWithContext(ctx)
	defer session.Close()

	session.Browser = azuretls.Chrome

	// Honor proxy from environment (same as plain HTTP transport).
	if proxy := os.Getenv("HTTPS_PROXY"); proxy != "" {
		if err := session.SetProxy(proxy); err != nil {
			return nil, fmt.Errorf("stealth: failed to set proxy: %w", err)
		}
	} else if proxy := os.Getenv("HTTP_PROXY"); proxy != "" {
		if err := session.SetProxy(proxy); err != nil {
			return nil, fmt.Errorf("stealth: failed to set proxy: %w", err)
		}
	}

	// Copy headers from the stdlib request into azuretls ordered headers.
	var oh azuretls.OrderedHeaders
	for key, vals := range req.Header {
		for _, v := range vals {
			oh = append(oh, []string{key, v})
		}
	}
	if c.userAgent != "" && req.Header.Get("User-Agent") == "" {
		oh = append(oh, []string{"User-Agent", c.userAgent})
	}

	azReq := &azuretls.Request{
		Method:         req.Method,
		Url:            req.URL.String(),
		OrderedHeaders: oh,
	}
	if len(reqBody) > 0 {
		azReq.Body = reqBody
	}

	var azResp *azuretls.Response
	err := c.retryDo(ctx, req, cfg, func() error {
		var err error
		azResp, err = session.Do(azReq)
		if err != nil {
			return fmt.Errorf("stealth: request failed: %w", err)
		}
		return nil
	}, func() []byte {
		if azResp != nil {
			return azResp.Body
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	redirect := ""
	if azResp.Url != req.URL.String() {
		redirect = azResp.Url
	}

	// Convert fhttp.Header (azuretls fork) to net/http.Header.
	respHeader := make(http.Header, len(azResp.Header))
	for k, v := range azResp.Header {
		respHeader[k] = v
	}

	dur := time.Since(start)
	return &Page{
		Meta: PageMeta{
			Version:   latestPageVersion,
			Source:    SourceHTTPStealth,
			FetchedAt: time.Now(),
			FetchDur:  dur,
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: redirect,
			Method:        req.Method,
			Header:        req.Header,
			Body:          reqBody,
		},
		Response: PageResponse{
			StatusCode:    azResp.StatusCode,
			Body:          azResp.Body,
			ContentLength: int64(len(azResp.Body)),
			Header:        respHeader,
		},
	}, nil
}

// retryDo runs attempt up to c.retry.Attempts times with exponential backoff.
// It handles rate limiting and silent throttle detection. The bodyFn returns
// the response body for throttle pattern matching (called only on success).
func (c *Client) retryDo(
	ctx context.Context,
	req *http.Request,
	cfg DoConfig,
	attempt func() error,
	bodyFn func() []byte,
) error {
	retry := c.retry
	for i := 0; i < retry.Attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Rate limit.
		if val, ok := req.Context().Value(ctxKeyLimiter{}).(ctxValLimiter); ok {
			val.Limiter.Take()
		} else {
			c.rateLimit.Take()
		}
		c.requests.Add(1)

		err := attempt()
		lastAttempt := i >= retry.Attempts-1
		if err != nil {
			if lastAttempt {
				return err
			}
			log.Warn().Err(err).Int("attempt", i).Msg("request failed, retrying")
			if err := c.retryWait(ctx, i); err != nil {
				return err
			}
			continue
		}
		// Silent throttle detection.
		if cfg.SilentThrottle != nil {
			if body := bodyFn(); cfg.SilentThrottle.Match(body) {
				n := c.requests.Load()
				rate := float64(n) / (time.Since(c.startedAt).Minutes())
				log.Warn().
					Str("rate", fmt.Sprintf("%0.3f/m", rate)).
					Msg("silently throttled")
				if lastAttempt {
					return &ThrottledError{}
				}
				log.Warn().Int("attempt", i).Msg("response is silently throttled, retrying")
				if err := c.retryWait(ctx, i); err != nil {
					return err
				}
				continue
			}
		}
		return nil
	}
	return fmt.Errorf("retry loop completed with 0 attempts")
}

// retryWait performs exponential backoff with jitter for the given attempt number.
func (c *Client) retryWait(ctx context.Context, attempt int) error {
	retry := c.retry
	d := time.Duration(math.Pow(2, float64(attempt))) * retry.MinWait
	if retry.Jitter > 0 {
		d += time.Duration(rand.Intn(int(retry.Jitter)))
	}
	if d > retry.MaxWait {
		d = retry.MaxWait
	}
	t := time.After(d)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t:
		return nil
	}
}

func (c *Client) fetchBrowser(
	ctx context.Context,
	req *http.Request,
	reqBody []byte,
	cfg DoConfig,
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
	defer bctx.Close() //nolint:errcheck // best-effort close

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
			return c.fetchHTTP(ctx, r, reqBody, cfg)
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
			Source:    SourceHTTPBrowser,
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

func (c *Client) do(
	ctx context.Context,
	req *http.Request,
	cfg DoConfig,
	fetchFn fetchFn,
) (page *Page, err error) {
	start := time.Now()

	// Apply default User-Agent if configured and not already set.
	if c.userAgent != "" && req.Header.Get("User-Agent") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("User-Agent", c.userAgent)
	}

	bkey, reqBody, err := c.cache.cacheKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob key: %w", err)
	}

	// Determine effective cache policy: context CachePolicy takes precedence,
	// DoConfig.Replace upgrades Default to Replace for backward compatibility.
	policy := cachePolicyFromContext(ctx)
	if cfg.Replace && policy == CachePolicyDefault {
		policy = CachePolicyReplace
	}

	// Cache read: return on hit (Default), or keep for conditional headers (Replace).
	var cachedPage *Page
	if policy != CachePolicySkip {
		if cp, err := c.cache.readPage(ctx, bkey); err == nil {
			if policy == CachePolicyDefault {
				if err := c.errPageStatusNotOK(cp); err != nil {
					return nil, err
				}
				return cp, nil
			}
			cachedPage = cp
			req = setConditionalHeaders(req, cp)
		} else if policy == CachePolicyDefault {
			var notFound *blob.NotFoundError
			if !errors.As(err, &notFound) {
				return nil, fmt.Errorf("failed to read from blob: %w", err)
			}
		}
	}

	if cfg.Limiter != nil {
		rctx := req.Context()
		rctx = context.WithValue(rctx, ctxKeyLimiter{}, ctxValLimiter{cfg.Limiter})
		req = req.WithContext(rctx)
	}
	page, err = fetchFn(ctx, req, reqBody, cfg)
	if err != nil {
		if c.cache.staleIfError && cachedPage != nil {
			cachedPage.Meta.Source = SourceStale
			return cachedPage, nil
		}
		return nil, fmt.Errorf("failed to fetch page: %w", err)
	}

	// 304 Not Modified: return the cached page if available.
	if page.Response.StatusCode == 304 && cachedPage != nil {
		cachedPage.Meta.Source = SourceRevalidated
		return cachedPage, nil
	}

	// Cache write.
	if c.cache.isCacheable(page.Response.StatusCode) && policy != CachePolicySkip {
		if err := c.cache.writePage(ctx, bkey, page, req.URL.String()); err != nil {
			return nil, fmt.Errorf("failed to write page: %w", err)
		}
		if cfg.Archive {
			akey := archiveKey(bkey, page.Meta.FetchedAt)
			if err := c.cache.writePage(ctx, akey, page, req.URL.String()); err != nil {
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
		Msg("fetched page")

	if err := c.errPageStatusNotOK(page); err != nil {
		return nil, err
	}
	return page, nil
}

// PageVersion describes a single archived snapshot of a cached page.
type PageVersion struct {
	Key       string    // Cache key for this snapshot.
	FetchedAt time.Time // When this snapshot was fetched.
}

// Versions lists all archived snapshots for the given request, ordered by
// fetch time (oldest first). Returns nil if no archive entries exist.
func (c *Client) Versions(ctx context.Context, req *http.Request) ([]PageVersion, error) {
	bkey, _, err := c.cache.cacheKey(req)
	if err != nil {
		return nil, fmt.Errorf("failed to compute blob key: %w", err)
	}
	prefix := archivePrefix(bkey)
	entries, err := c.cache.bucket.ListCache(prefix)
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
	return c.cache.readPage(ctx, key)
}

// DoConfig controls per-request behavior for Client.Do and Client.Get.
type DoConfig struct {
	// Deprecated: Use WithCachePolicy(ctx, CachePolicyReplace) instead.
	// Replace skips cache read, forcing a fresh fetch (still caches the result).
	Replace bool
	// Browser uses headless Chromium instead of plain HTTP.
	Browser bool
	// Stealth uses azuretls with a real browser TLS fingerprint to bypass
	// Cloudflare and similar bot-detection systems. Mutually exclusive with Browser.
	Stealth bool
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

// PageMeta.Source values describing how a page was obtained.
const (
	SourceHTTPPlain   = "http.plain"
	SourceHTTPStealth = "http.stealth"
	SourceHTTPBrowser = "http.browser"
	SourceStale       = "stale"
	SourceRevalidated = "revalidated"
)

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
// The returned response has its own header map (safe for concurrent use).
func (p *Page) HTTPResponse() *http.Response {
	var trailer http.Header
	if p.Response.Trailer != nil {
		trailer = p.Response.Trailer.Clone()
	}
	return &http.Response{
		StatusCode:       p.Response.StatusCode,
		ProtoMajor:       p.Response.ProtoMajor,
		ProtoMinor:       p.Response.ProtoMinor,
		Header:           p.Response.Header.Clone(),
		Body:             io.NopCloser(bytes.NewReader(p.Response.Body)),
		ContentLength:    p.Response.ContentLength,
		TransferEncoding: p.Response.TransferEncoding,
		Trailer:          trailer,
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
