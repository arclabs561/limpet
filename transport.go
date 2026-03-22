package limpet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/ratelimit"
	"golang.org/x/sync/singleflight"

	"github.com/arclabs561/limpet/blob"
)

// Transport is an http.RoundTripper that caches responses in a blob.Bucket.
// Responses are fully buffered (no streaming). Only HTTP 200 responses are cached.
//
// Use WithCachePolicy on the request context to control per-request caching.
type Transport struct {
	// Base is the underlying RoundTripper. Nil means http.DefaultTransport.
	Base http.RoundTripper

	bucket           *blob.Bucket
	rateLimit        ratelimit.Limiter
	requestBodyLimit int64
	respBodyLimit    int64
	ignoreHeaders    map[string]bool
	ignoreParams     map[string]bool
	cacheStatuses    map[int]bool
	userAgent        string
	refreshPatterns  []RefreshPattern
	staleIfError     bool
	flight           singleflight.Group
	stats            transportStats
}

// transportStats tracks cache performance counters. All fields are safe for
// concurrent access. Read via Transport.Stats().
type transportStats struct {
	hits        atomic.Int64
	misses      atomic.Int64
	revalidated atomic.Int64
	coalesced   atomic.Int64
	staleServed atomic.Int64
}

// TransportOption configures a Transport.
type TransportOption func(*Transport)

// TransportWithRateLimit sets a rate limit on outgoing requests.
func TransportWithRateLimit(rps int, opts ...ratelimit.Option) TransportOption {
	return func(t *Transport) {
		t.rateLimit = ratelimit.New(rps, opts...)
	}
}

// TransportWithRequestBodyLimit sets the maximum request body size used for
// cache key computation. 0 means no limit.
func TransportWithRequestBodyLimit(n int64) TransportOption {
	return func(t *Transport) { t.requestBodyLimit = n }
}

// TransportWithResponseBodyLimit sets the maximum response body size to cache.
// 0 means no limit.
func TransportWithResponseBodyLimit(n int64) TransportOption {
	return func(t *Transport) { t.respBodyLimit = n }
}

// TransportWithIgnoreHeaders excludes the named headers from cache key
// computation. Useful when User-Agent or Accept-Encoding vary between
// requests but should map to the same cache entry.
func TransportWithIgnoreHeaders(names ...string) TransportOption {
	return func(t *Transport) {
		if t.ignoreHeaders == nil {
			t.ignoreHeaders = make(map[string]bool)
		}
		for _, n := range names {
			t.ignoreHeaders[http.CanonicalHeaderKey(n)] = true
		}
	}
}

// TransportWithIgnoreParams excludes the named query parameters from cache key
// computation. Useful for stripping auth tokens or tracking params.
func TransportWithIgnoreParams(names ...string) TransportOption {
	return func(t *Transport) {
		if t.ignoreParams == nil {
			t.ignoreParams = make(map[string]bool)
		}
		for _, n := range names {
			t.ignoreParams[n] = true
		}
	}
}

// TransportWithUserAgent sets a default User-Agent header on all requests.
func TransportWithUserAgent(ua string) TransportOption {
	return func(t *Transport) { t.userAgent = ua }
}

// TransportWithCacheStatuses sets which HTTP status codes are eligible for
// caching. By default only 200 is cached.
func TransportWithCacheStatuses(codes ...int) TransportOption {
	return func(t *Transport) {
		t.cacheStatuses = make(map[int]bool, len(codes))
		for _, code := range codes {
			t.cacheStatuses[code] = true
		}
	}
}

// NewTransport creates a caching Transport backed by the given bucket.
func NewTransport(bucket *blob.Bucket, opts ...TransportOption) *Transport {
	t := &Transport{
		bucket:           bucket,
		requestBodyLimit: 10e6,
		respBodyLimit:    100e6,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Stats returns a snapshot of the transport's cache performance counters.
func (t *Transport) Stats() TransportStatsSnapshot {
	return TransportStatsSnapshot{
		Hits:        t.stats.hits.Load(),
		Misses:      t.stats.misses.Load(),
		Revalidated: t.stats.revalidated.Load(),
		Coalesced:   t.stats.coalesced.Load(),
		StaleServed: t.stats.staleServed.Load(),
	}
}

// TransportStatsSnapshot is a point-in-time snapshot of cache stats.
type TransportStatsSnapshot struct {
	Hits        int64
	Misses      int64
	Revalidated int64
	Coalesced   int64
	StaleServed int64
}

// TransportWithRefreshPatterns sets URL-based cache TTL rules for the transport.
// The first matching pattern determines the TTL for cache writes.
func TransportWithRefreshPatterns(patterns ...RefreshPattern) TransportOption {
	return func(t *Transport) { t.refreshPatterns = patterns }
}

// TransportWithStaleIfError configures the transport to return a stale cached
// response when the upstream fetch fails, rather than propagating the error.
func TransportWithStaleIfError(enabled bool) TransportOption {
	return func(t *Transport) { t.staleIfError = enabled }
}

func (t *Transport) isCacheable(statusCode int) bool {
	if t.cacheStatuses == nil {
		return statusCode == 200
	}
	return t.cacheStatuses[statusCode]
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

// WithCacheTTL returns a context that overrides the default cache TTL for
// writes made with this context. Works with both Client and Transport.
// Use 0 for no expiry, or a positive duration for a custom TTL.
func WithCacheTTL(ctx context.Context, ttl time.Duration) context.Context {
	return blob.WithCacheTTL(ctx, ttl)
}

// CachePolicy controls per-request caching behavior.
type CachePolicy int

const (
	// CachePolicyDefault reads from cache on hit, writes on miss (status 200).
	CachePolicyDefault CachePolicy = iota
	// CachePolicyReplace skips cache read but still writes on status 200.
	CachePolicyReplace
	// CachePolicySkip bypasses cache entirely (no read, no write).
	CachePolicySkip
)

type ctxKeyCachePolicy struct{}

// WithCachePolicy returns a context that carries the given cache policy.
func WithCachePolicy(ctx context.Context, p CachePolicy) context.Context {
	return context.WithValue(ctx, ctxKeyCachePolicy{}, p)
}

func cachePolicyFromContext(ctx context.Context) CachePolicy {
	if p, ok := ctx.Value(ctxKeyCachePolicy{}).(CachePolicy); ok {
		return p
	}
	return CachePolicyDefault
}

// RoundTrip executes a single HTTP transaction with caching.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.userAgent != "" && req.Header.Get("User-Agent") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("User-Agent", t.userAgent)
	}

	policy := cachePolicyFromContext(req.Context())

	key, _, err := blobKey(req, t.requestBodyLimit, t.ignoreHeaders, t.ignoreParams)
	if err != nil {
		return nil, fmt.Errorf("limpet: failed to compute cache key: %w", err)
	}

	// Cache read: return on hit (Default policy), or keep for conditional
	// request headers (Replace policy).
	var cachedPage *Page
	if policy != CachePolicySkip {
		if page, err := readCachedPage(req.Context(), t.bucket, key); err == nil {
			if policy == CachePolicyDefault {
				t.stats.hits.Add(1)
				resp := page.HTTPResponse()
				resp.Header.Set("X-Limpet-Source", page.Meta.Source)
				return resp, nil
			}
			// Replace: keep cached page for conditional request headers.
			cachedPage = page
			if etag := page.Response.Header.Get("ETag"); etag != "" {
				req = req.Clone(req.Context())
				req.Header.Set("If-None-Match", etag)
			} else if lm := page.Response.Header.Get("Last-Modified"); lm != "" {
				req = req.Clone(req.Context())
				req.Header.Set("If-Modified-Since", lm)
			}
		} else if policy == CachePolicyDefault {
			t.stats.misses.Add(1)
		}
	}

	// Replace-policy requests bypass singleflight: they carry per-caller
	// conditional headers and a per-caller cachedPage for 304 handling.
	// Coalescing them with cache-miss requests would leak one caller's
	// state into another's flight.
	if policy == CachePolicyReplace {
		page, err := t.fetchAndCache(req, key, policy, cachedPage)
		if err != nil {
			if t.staleIfError && cachedPage != nil {
				t.stats.staleServed.Add(1)
				resp := cachedPage.HTTPResponse()
				resp.Header.Set("X-Limpet-Source", "stale")
				return resp, nil
			}
			return nil, err
		}
		resp := page.HTTPResponse()
		resp.Header.Set("X-Limpet-Source", page.Meta.Source)
		return resp, nil
	}

	// Default/Skip: coalesce concurrent requests via singleflight.
	v, err, shared := t.flight.Do(key, func() (any, error) {
		page, err := t.fetchAndCache(req, key, policy, nil)
		if err != nil {
			t.flight.Forget(key)
			return nil, err
		}
		return page, nil
	})
	if err != nil {
		// stale-if-error: return cached page on upstream failure.
		if t.staleIfError && cachedPage != nil {
			t.stats.staleServed.Add(1)
			resp := cachedPage.HTTPResponse()
			resp.Header.Set("X-Limpet-Source", "stale")
			return resp, nil
		}
		return nil, err
	}
	result := v.(*Page)
	if shared {
		t.stats.coalesced.Add(1)
	}
	resp := result.HTTPResponse()
	resp.Header.Set("X-Limpet-Source", result.Meta.Source)
	return resp, nil
}

// fetchAndCache performs an HTTP round-trip, handles 304 revalidation,
// buffers the body, and writes to cache. Used by both the singleflight
// path and the direct Replace-policy path.
func (t *Transport) fetchAndCache(
	req *http.Request,
	key string,
	policy CachePolicy,
	cachedPage *Page,
) (*Page, error) {
	if t.rateLimit != nil {
		t.rateLimit.Take()
	}

	resp, err := t.base().RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// 304 Not Modified: return the cached response.
	if resp.StatusCode == 304 && cachedPage != nil {
		resp.Body.Close()
		cachedPage.Meta.Source = "revalidated"
		t.stats.revalidated.Add(1)
		return cachedPage, nil
	}

	// Buffer the response body for caching.
	var body []byte
	if resp.Body != nil {
		rdr := resp.Body
		if t.respBodyLimit > 0 {
			rdr = http.MaxBytesReader(nil, resp.Body, t.respBodyLimit)
		}
		body, err = io.ReadAll(rdr)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("limpet: failed to read response body: %w", err)
		}
	}

	page := pageFromRoundTrip(req, resp, body)
	page.Meta.Source = "fetch"

	// Cache write with refresh pattern TTL.
	if t.isCacheable(resp.StatusCode) && policy != CachePolicySkip {
		writeCtx := req.Context()
		if len(t.refreshPatterns) > 0 {
			if _, hasCtxTTL := writeCtx.Value(blob.CacheTTLKey{}).(time.Duration); !hasCtxTTL {
				if ttl, ok := matchRefreshTTL(t.refreshPatterns, req.URL.String()); ok {
					writeCtx = blob.WithCacheTTL(writeCtx, ttl)
				}
			}
		}
		if err := writeCachedPage(writeCtx, t.bucket, key, page); err != nil {
			return nil, fmt.Errorf("failed to write cache: %w", err)
		}
	}

	return page, nil
}

// pageFromRoundTrip constructs a Page from a raw HTTP round-trip result.
func pageFromRoundTrip(req *http.Request, resp *http.Response, body []byte) *Page {
	return &Page{
		Meta: PageMeta{
			Version:   latestPageVersion,
			FetchedAt: time.Now(),
		},
		Request: PageRequest{
			URL:    req.URL.String(),
			Method: req.Method,
			Header: req.Header,
		},
		Response: PageResponse{
			StatusCode:       resp.StatusCode,
			ProtoMajor:       resp.ProtoMajor,
			ProtoMinor:       resp.ProtoMinor,
			TransferEncoding: resp.TransferEncoding,
			ContentLength:    resp.ContentLength,
			Header:           resp.Header,
			Body:             body,
			Trailer:          resp.Trailer,
		},
	}
}
