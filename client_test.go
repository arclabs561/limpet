package limpet

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arclabs561/limpet/blob"
)

func setupClientBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	bucket, err := blob.NewBucket(t.Context(), t.TempDir(), &blob.BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

func TestClientDefaultTransportRespectsProxy(t *testing.T) {
	// Verify that the default Client transport has Proxy set.
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	tr, ok := cl.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", cl.httpClient.Transport)
	}
	if tr.Proxy == nil {
		t.Error("default transport Proxy is nil, should be http.ProxyFromEnvironment")
	}
	if tr.MaxIdleConnsPerHost != 100 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 100", tr.MaxIdleConnsPerHost)
	}
}

func TestClientWithHTTPClient(t *testing.T) {
	bucket := setupClientBucket(t)

	var gotUA string
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	// Custom client with a round-tripper that adds a header.
	custom := &http.Client{Transport: &headerTransport{
		base:   http.DefaultTransport,
		header: "X-Custom",
		value:  "test-value",
	}}

	cl, err := NewClient(t.Context(), bucket, WithHTTPClient(custom))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	req, _ := http.NewRequest("GET", svr.URL, nil)
	req.Header.Set("User-Agent", "custom-ua")
	page, err := cl.Do(t.Context(), req, DoConfig{Replace: true})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotUA != "custom-ua" {
		t.Errorf("User-Agent = %q, want custom-ua", gotUA)
	}
	// Verify the custom transport's header was seen by the server.
	if got := page.Request.Header.Get("X-Custom"); got != "test-value" {
		t.Errorf("X-Custom = %q, want test-value", got)
	}
}

// headerTransport adds a fixed header to every request.
type headerTransport struct {
	base   http.RoundTripper
	header string
	value  string
}

func (h *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set(h.header, h.value)
	return h.base.RoundTrip(req)
}

func TestClientStaleIfError(t *testing.T) {
	bucket := setupClientBucket(t)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("cached-content"))
	}))
	t.Cleanup(svr.Close)

	cl, err := NewClient(t.Context(), bucket,
		WithStaleIfError(true),
		WithRetry(RetryConfig{Attempts: 1}), // no retries for faster test
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	// Populate cache.
	req, _ := http.NewRequest("GET", svr.URL+"/stale", nil)
	page, err := cl.Do(t.Context(), req, DoConfig{})
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	if string(page.Response.Body) != "cached-content" {
		t.Fatalf("populate body = %q", page.Response.Body)
	}

	// Close server to force error on Replace.
	svr.Close()

	// Replace: should return stale cached page instead of error.
	req2, _ := http.NewRequest("GET", svr.URL+"/stale", nil)
	page, err = cl.Do(t.Context(), req2, DoConfig{Replace: true})
	if err != nil {
		t.Fatalf("stale-if-error: unexpected error: %v", err)
	}
	if string(page.Response.Body) != "cached-content" {
		t.Errorf("stale body = %q, want cached-content", page.Response.Body)
	}
	if page.Meta.Source != "stale" {
		t.Errorf("source = %q, want stale", page.Meta.Source)
	}
}

func TestClientStaleIfErrorDisabled(t *testing.T) {
	bucket := setupClientBucket(t)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	cl, err := NewClient(t.Context(), bucket,
		WithRetry(RetryConfig{Attempts: 1}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	// Populate cache.
	req, _ := http.NewRequest("GET", svr.URL+"/no-stale", nil)
	_, err = cl.Do(t.Context(), req, DoConfig{})
	if err != nil {
		t.Fatalf("populate: %v", err)
	}

	svr.Close()

	// Replace: should propagate error (staleIfError disabled by default).
	req2, _ := http.NewRequest("GET", svr.URL+"/no-stale", nil)
	_, err = cl.Do(t.Context(), req2, DoConfig{Replace: true})
	if err == nil {
		t.Error("expected error when stale-if-error disabled")
	}
}

func TestClientRefreshPatterns(t *testing.T) {
	bucket := setupClientBucket(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		fmt.Fprintf(w, "resp-%d", n)
	}))
	t.Cleanup(svr.Close)

	cl, err := NewClient(t.Context(), bucket,
		WithRefreshPatterns(
			RefreshPattern{Pattern: regexp.MustCompile(`/ephemeral`), MaxAge: 1 * time.Millisecond},
			RefreshPattern{Pattern: regexp.MustCompile(`/stable`), MaxAge: 24 * time.Hour},
		),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	// Fetch /stable: should be cached with long TTL.
	req, _ := http.NewRequest("GET", svr.URL+"/stable", nil)
	page, err := cl.Do(t.Context(), req, DoConfig{})
	if err != nil {
		t.Fatalf("stable fetch: %v", err)
	}
	if string(page.Response.Body) != "resp-1" {
		t.Errorf("body = %q", page.Response.Body)
	}

	// Fetch /stable again: should hit cache.
	page, err = cl.Do(t.Context(), req, DoConfig{})
	if err != nil {
		t.Fatalf("stable cached: %v", err)
	}
	if string(page.Response.Body) != "resp-1" {
		t.Errorf("cached body = %q, want resp-1", page.Response.Body)
	}
	if hits.Load() != 1 {
		t.Errorf("hits = %d after stable requests, want 1", hits.Load())
	}

	// Fetch /ephemeral: cached with 1ms TTL.
	req2, _ := http.NewRequest("GET", svr.URL+"/ephemeral", nil)
	page, err = cl.Do(t.Context(), req2, DoConfig{})
	if err != nil {
		t.Fatalf("ephemeral fetch: %v", err)
	}
	if string(page.Response.Body) != "resp-2" {
		t.Errorf("ephemeral body = %q", page.Response.Body)
	}

	// Verify both paths were fetched.
	if hits.Load() != 2 {
		t.Errorf("total hits = %d, want 2", hits.Load())
	}
}

func TestClientCachePolicySkip(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "hello")
	}))
	t.Cleanup(svr.Close)

	// First request: should fetch and cache.
	page, err := cl.Get(t.Context(), svr.URL)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if string(page.Response.Body) != "hello" {
		t.Fatalf("body = %q", page.Response.Body)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}

	// Second request with CachePolicySkip: should bypass cache entirely.
	ctx := WithCachePolicy(t.Context(), CachePolicySkip)
	_, err = cl.Get(ctx, svr.URL)
	if err != nil {
		t.Fatalf("skip get: %v", err)
	}
	if hits.Load() != 2 {
		t.Errorf("hits = %d, want 2 (CachePolicySkip should bypass cache)", hits.Load())
	}

	// Third request without skip: should still return cached (first fetch).
	_, err = cl.Get(t.Context(), svr.URL)
	if err != nil {
		t.Fatalf("cached get: %v", err)
	}
	if hits.Load() != 2 {
		t.Errorf("hits = %d, want 2 (should serve from cache)", hits.Load())
	}
}

func TestClientCachePolicyReplace(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "hello")
	}))
	t.Cleanup(svr.Close)

	// Populate cache.
	_, err = cl.Get(t.Context(), svr.URL)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}

	// CachePolicyReplace via context should force re-fetch (equivalent to DoConfig.Replace).
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	_, err = cl.Get(ctx, svr.URL)
	if err != nil {
		t.Fatalf("replace get: %v", err)
	}
	if hits.Load() != 2 {
		t.Errorf("hits = %d, want 2 (CachePolicyReplace should re-fetch)", hits.Load())
	}
}

func TestClientSilentThrottle(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket,
		WithRetry(RetryConfig{Attempts: 2, MinWait: 1 * time.Millisecond, MaxWait: 1 * time.Millisecond, Jitter: 1 * time.Millisecond}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	// Server always returns a "captcha" page.
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("please solve this captcha to continue"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/throttled", nil)
	cfg := DoConfig{
		SilentThrottle: regexp.MustCompile(`captcha`),
		Replace:        true,
	}
	_, err = cl.Do(t.Context(), req, cfg)
	if err == nil {
		t.Fatal("expected ThrottledError")
	}
	var throttleErr *ThrottledError
	if !errors.As(err, &throttleErr) {
		t.Fatalf("expected ThrottledError, got %T: %v", err, throttleErr)
	}
	if throttleErr.URL == "" {
		t.Error("ThrottledError.URL is empty, expected the request URL")
	}
}

func TestClientSilentThrottleNotTriggered(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("real content here"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/ok", nil)
	cfg := DoConfig{
		SilentThrottle: regexp.MustCompile(`captcha`),
		Replace:        true,
	}
	page, err := cl.Do(t.Context(), req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(page.Response.Body) != "real content here" {
		t.Errorf("body = %q", page.Response.Body)
	}
}

func TestClientPerRequestLimiter(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket,
		WithRateLimit(1), // very slow default
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	// Use a per-request limiter that is much faster.
	fastLimiter := &unlimitedLimiter{}

	req, _ := http.NewRequest("GET", svr.URL+"/fast", nil)
	cfg := DoConfig{
		Limiter: fastLimiter,
		Replace: true,
	}

	start := time.Now()
	_, err = cl.Do(t.Context(), req, cfg)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	// With the fast limiter, the request should complete quickly
	// (not be held by the 1 req/s default limiter).
	if dur := time.Since(start); dur > 500*time.Millisecond {
		t.Errorf("request took %v, expected < 500ms with per-request limiter", dur)
	}
}

// unlimitedLimiter satisfies the Limiter interface with no rate limiting.
type unlimitedLimiter struct{}

func (u *unlimitedLimiter) Take() time.Time { return time.Now() }

func TestClientBrowserStealthMutualExclusion(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	req, _ := http.NewRequest("GET", "http://localhost/test", nil)
	_, err = cl.Do(t.Context(), req, DoConfig{Browser: true, Stealth: true})
	if err == nil {
		t.Error("expected error for Browser + Stealth")
	}
}

func TestClientConditionalRevalidation(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("original"))
	}))
	t.Cleanup(svr.Close)

	// Populate cache.
	req, _ := http.NewRequest("GET", svr.URL+"/etag", nil)
	page, err := cl.Do(t.Context(), req, DoConfig{})
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	if string(page.Response.Body) != "original" {
		t.Fatalf("body = %q", page.Response.Body)
	}

	// Replace: server returns 304, should get cached body back.
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	req2, _ := http.NewRequest("GET", svr.URL+"/etag", nil)
	page, err = cl.Do(ctx, req2, DoConfig{})
	if err != nil {
		t.Fatalf("conditional: %v", err)
	}
	if string(page.Response.Body) != "original" {
		t.Errorf("revalidated body = %q, want original", page.Response.Body)
	}
	if page.Meta.Source != SourceRevalidated {
		t.Errorf("source = %q, want %q", page.Meta.Source, SourceRevalidated)
	}
	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2", hits.Load())
	}
}
