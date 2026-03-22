package limpet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/arclabs561/limpet/blob"
)

// cacheLayer provides shared cache operations for Client and Transport.
// Both types embed this struct to avoid duplicating cache configuration
// and read/write logic.
type cacheLayer struct {
	bucket           *blob.Bucket
	requestBodyLimit int64
	ignoreHeaders    map[string]bool
	ignoreParams     map[string]bool
	cacheStatuses    map[int]bool
	refreshPatterns  []RefreshPattern
	staleIfError     bool
}

// cacheKey computes a deterministic cache key from an HTTP request.
func (cl *cacheLayer) cacheKey(req *http.Request) (string, []byte, error) {
	return blobKey(req, cl.requestBodyLimit, cl.ignoreHeaders, cl.ignoreParams)
}

// isCacheable reports whether the given status code should be cached.
// By default only 200 is cached; WithCacheStatuses overrides this.
func (cl *cacheLayer) isCacheable(statusCode int) bool {
	if cl.cacheStatuses == nil {
		return statusCode == 200
	}
	return cl.cacheStatuses[statusCode]
}

// readPage reads a cached page from the bucket. Returns the page and
// its source label, or a *blob.NotFoundError if not cached.
func (cl *cacheLayer) readPage(ctx context.Context, key string) (*Page, error) {
	b, err := cl.bucket.GetBlob(ctx, key)
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

// writePage writes a page to the bucket, applying refresh pattern TTL
// if no per-request TTL is set on the context.
func (cl *cacheLayer) writePage(ctx context.Context, key string, page *Page, reqURL string) error {
	if len(cl.refreshPatterns) > 0 {
		if _, hasCtxTTL := ctx.Value(blob.CacheTTLKey{}).(time.Duration); !hasCtxTTL {
			if ttl, ok := matchRefreshTTL(cl.refreshPatterns, reqURL); ok {
				ctx = blob.WithCacheTTL(ctx, ttl)
			}
		}
	}
	data, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("failed to marshal page: %w", err)
	}
	return cl.bucket.SetBlob(ctx, key, data)
}

// setConditionalHeaders adds If-None-Match or If-Modified-Since headers
// to the request based on the cached response, enabling 304 revalidation.
// Returns the (possibly cloned) request.
func setConditionalHeaders(req *http.Request, cached *Page) *http.Request {
	if etag := cached.Response.Header.Get("ETag"); etag != "" {
		req = req.Clone(req.Context())
		req.Header.Set("If-None-Match", etag)
		return req
	}
	if lm := cached.Response.Header.Get("Last-Modified"); lm != "" {
		req = req.Clone(req.Context())
		req.Header.Set("If-Modified-Since", lm)
		return req
	}
	return req
}
