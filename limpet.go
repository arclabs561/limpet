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

	"github.com/hashicorp/go-retryablehttp"
	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog"
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

// Client fetches web pages with automatic caching and request deduplication.
type Client struct {
	httpClient       *retryablehttp.Client
	bucket           *blob.Bucket
	mu               *sync.Mutex
	pw               *playwright.Playwright
	browser          playwright.Browser
	alwaysDoBrowser  bool
	startBrowser     func() error
	requestBodyLimit int64 // no limit when <= 0
	respBodyLimit    int64 // no limit when <= 0
	rateLimit        ratelimit.Limiter
	startedAt        time.Time
	requests         atomic.Uint64
}

// Option configures a Client at construction time.
type Option func(*Client)

// OptAlwaysBrowser configures the client to always use headless browser.
func OptAlwaysBrowser() Option {
	return func(c *Client) { c.alwaysDoBrowser = true }
}

// OptRateLimit sets a programmatic rate limit, overriding the env var.
func OptRateLimit(rps int, opts ...ratelimit.Option) Option {
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

	httpClient := retryablehttp.NewClient()
	httpClient.HTTPClient = &http.Client{Transport: &http.Transport{}}
	httpClient.Logger = leveledLogger{log.Ctx(ctx)}
	httpClient.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, _ int) {
		val, ok := req.Context().Value(ctxKeyLimiter{}).(ctxValLimiter)
		if ok {
			val.Limiter.Take()
		} else {
			c.rateLimit.Take()
		}
		c.requests.Add(1)
	}
	c.httpClient = httpClient
	c.startBrowser = sync.OnceValue(c.newBrowser)

	if c.alwaysDoBrowser {
		if err := c.startBrowser(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeBrowser()
}

// Get is a convenience method for fetching a URL with GET.
func (c *Client) Get(ctx context.Context, url string, opts ...DoOption) (*Page, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	return c.Do(ctx, req, opts...)
}

// FetchStatusNotOKError is returned when the fetch status is not 200 OK. The
// Page contains the response and status.
type FetchStatusNotOKError struct {
	Page *Page
}

func (e *FetchStatusNotOKError) Error() string {
	return fmt.Sprintf("bad fetch status: %d", e.Page.Response.StatusCode)
}

func errPageStatusNotOK(page *Page) error {
	if page.Response.StatusCode != 200 {
		return &FetchStatusNotOKError{Page: page}
	}
	return nil
}

// FetchThrottledError is returned when the fetch is throttled.
type FetchThrottledError struct{}

func (e *FetchThrottledError) Error() string {
	return "fetch throttled"
}

func (c *Client) Do(
	ctx context.Context,
	req *http.Request,
	options ...DoOption,
) (page *Page, err error) {
	opts := doOptions{}
	browser := false
	for _, opt := range options {
		switch opt := opt.(type) {
		case *OptDoReplace:
			opts.Replace = true
		case *OptDoSilentThrottle:
			opts.ReSilentThrottle = opt.PageBytesRegexp
		case *OptDoLimiter:
			opts.Limiter = opt.Limiter
		case *OptDoBrowser:
			browser = true
		}
	}
	fn := c.fetchHTTP
	if browser {
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
		ChromiumSandbox: playwright.Bool(true),
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
		rreq, err := retryablehttp.FromRequest(req)
		if err != nil {
			return nil, err
		}
		resp, err = c.httpClient.Do(rreq)
		if err != nil {
			return nil, fmt.Errorf("failed to perform http get: %w", err)
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
				return nil, &FetchThrottledError{}
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
			Version:   LatestPageVersion,
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
			Version:   LatestPageVersion,
			Source:    "http.browser",
			FetchedAt: time.Now(),
		},
		Request: PageRequest{
			URL:           req.URL.String(),
			RedirectedURL: resp.URL(),
			Method:        req.Method,
			Header:        header,
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

type DoOption interface {
	doOption()
}

type OptDoReplace struct{}

type OptDoSilentThrottle struct {
	PageBytesRegexp *regexp.Regexp
}

type ctxKeyLimiter struct{}
type ctxValLimiter struct {
	Limiter Limiter
}

type OptDoLimiter struct {
	Limiter Limiter
}

type Limiter interface {
	Take() time.Time
}

type OptDoBrowser struct{}

func (o *OptDoReplace) doOption()        {}
func (o *OptDoSilentThrottle) doOption() {}
func (o *OptDoLimiter) doOption()        {}
func (o *OptDoBrowser) doOption()        {}

var _ retryablehttp.LeveledLogger = (*leveledLogger)(nil)

type leveledLogger struct{ log *zerolog.Logger }

func (l leveledLogger) fields(keysAndValues []any) *zerolog.Logger {
	log := l.log.With().CallerWithSkipFrameCount(3)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		key := fmt.Sprintf("%v", keysAndValues[i])
		log = log.Interface(key, keysAndValues[i+1])
	}
	lg := log.Logger()
	return &lg
}

func (l leveledLogger) Error(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Error().Msg(msg)
}

func (l leveledLogger) Warn(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Warn().Msg(msg)
}

func (l leveledLogger) Info(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Info().Msg(msg)
}

func (l leveledLogger) Debug(msg string, keysAndValues ...any) {
	l.fields(keysAndValues).Debug().Msg(msg)
}

const LatestPageVersion = 1

type Page struct {
	Meta     PageMeta     `json:"meta"`
	Request  PageRequest  `json:"request"`
	Response PageResponse `json:"response"`
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

type PageMeta struct {
	Version   uint16        `json:"version"`
	Source    string        `json:"-"`
	FetchedAt time.Time     `json:"fetched_at"`
	FetchDur  time.Duration `json:"fetch_dur"`
}

type PageRequest struct {
	URL           string      `json:"url"`
	RedirectedURL string      `json:"redirected_url,omitempty"`
	Method        string      `json:"method"`
	Header        http.Header `json:"header,omitempty"`
	Body          []byte      `json:"body,omitempty"`
}

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
