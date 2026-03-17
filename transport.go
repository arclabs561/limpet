package limpet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/ratelimit"

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

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
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
	policy := cachePolicyFromContext(req.Context())

	key, _, err := blobKey(req, t.requestBodyLimit)
	if err != nil {
		return nil, fmt.Errorf("limpet: failed to compute cache key: %w", err)
	}

	// Cache read.
	if policy == CachePolicyDefault {
		resp, err := t.cacheRead(req.Context(), key)
		if err == nil {
			return resp, nil
		}
		// Not found -- fall through to fetch.
	}

	// Rate limit.
	if t.rateLimit != nil {
		t.rateLimit.Take()
	}

	// Delegate to base transport.
	resp, err := t.base().RoundTrip(req)
	if err != nil {
		return nil, err
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
	resp.Body = io.NopCloser(bytes.NewReader(body))

	// Cache write (200 only).
	if resp.StatusCode == 200 && policy != CachePolicySkip {
		page := pageFromRoundTrip(req, resp, body)
		if err := writeCachedPage(req.Context(), t.bucket, key, page); err != nil {
			return nil, fmt.Errorf("failed to write cache: %w", err)
		}
	}

	resp.Header.Set("X-Limpet-Source", "fetch")
	return resp, nil
}

func (t *Transport) cacheRead(ctx context.Context, key string) (*http.Response, error) {
	page, err := readCachedPage(ctx, t.bucket, key)
	if err != nil {
		return nil, err
	}
	resp := page.HTTPResponse()
	resp.Header.Set("X-Limpet-Source", page.Meta.Source)
	return resp, nil
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
