package limpet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
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

// blobKey computes a deterministic cache key from an HTTP request.
// The key is SHA-256 of (URL + method + headers + body), placed under the
// request hostname. bodyLimit limits how much of the request body is read
// (0 means no limit). The request body is restored after reading.
// ignoreHeaders and ignoreParams exclude named headers/query params from the key.
func blobKey(req *http.Request, bodyLimit int64, ignoreHeaders, ignoreParams map[string]bool) (string, []byte, error) {
	var buf bytes.Buffer
	u := *req.URL
	// Normalize: lowercase host, sort query params, strip default ports.
	u.Host = strings.ToLower(u.Host)
	u.Host = stripDefaultPort(u.Host, u.Scheme)
	q := u.Query()
	for p := range ignoreParams {
		q.Del(p)
	}
	u.RawQuery = q.Encode() // Encode sorts keys alphabetically.
	buf.WriteString(u.String())
	buf.WriteString(".")
	buf.WriteString(req.Method)
	buf.WriteString(".")
	// Sort header keys for deterministic cache keys. http.Header is a map
	// with non-deterministic iteration order; WriteSubset would produce
	// different hashes for identical headers.
	keys := make([]string, 0, len(req.Header))
	for k := range req.Header {
		if ignoreHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
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
	bkey := path.Join(strings.ToLower(u.Hostname()), henc) + ".json"
	return bkey, body, nil
}

// stripDefaultPort removes :80 for http and :443 for https so that
// URLs with and without the default port produce the same cache key.
func stripDefaultPort(host, scheme string) string {
	switch {
	case scheme == "http" && strings.HasSuffix(host, ":80"):
		return strings.TrimSuffix(host, ":80")
	case scheme == "https" && strings.HasSuffix(host, ":443"):
		return strings.TrimSuffix(host, ":443")
	}
	return host
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
