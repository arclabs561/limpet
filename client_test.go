package limpet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arclabs561/limpet/blob"
	"go.uber.org/ratelimit"
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

func TestClientWithIgnoreHeaders(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket, WithIgnoreHeaders("X-Trace-Id"))
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

	// Two requests with different ignored headers should share cache.
	req1, _ := http.NewRequest("GET", svr.URL+"/hdr", nil)
	req1.Header.Set("X-Trace-Id", "aaa")
	if _, err := cl.Do(t.Context(), req1, DoConfig{}); err != nil {
		t.Fatalf("first: %v", err)
	}

	req2, _ := http.NewRequest("GET", svr.URL+"/hdr", nil)
	req2.Header.Set("X-Trace-Id", "bbb")
	if _, err := cl.Do(t.Context(), req2, DoConfig{}); err != nil {
		t.Fatalf("second: %v", err)
	}

	if hits.Load() != 1 {
		t.Errorf("hits = %d, want 1 (ignored header should not affect cache key)", hits.Load())
	}
}

func TestClientWithIgnoreParams(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket, WithIgnoreParams("_t"))
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

	if _, err := cl.Get(t.Context(), svr.URL+"/page?_t=111"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := cl.Get(t.Context(), svr.URL+"/page?_t=222"); err != nil {
		t.Fatalf("second: %v", err)
	}

	if hits.Load() != 1 {
		t.Errorf("hits = %d, want 1 (ignored param should not affect cache key)", hits.Load())
	}
}

func TestClientWithCacheStatuses(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket, WithCacheStatuses(200, 404))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(404)
		_, _ = w.Write([]byte("not found"))
	}))
	t.Cleanup(svr.Close)

	// 404 is cacheable, so first fetch should cache it and return without error.
	page, err := cl.Get(t.Context(), svr.URL+"/missing")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if page.Response.StatusCode != 404 {
		t.Errorf("status = %d, want 404", page.Response.StatusCode)
	}

	// Second fetch should hit cache.
	page, err = cl.Get(t.Context(), svr.URL+"/missing")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if hits.Load() != 1 {
		t.Errorf("hits = %d, want 1 (404 should be cached)", hits.Load())
	}
	if string(page.Response.Body) != "not found" {
		t.Errorf("body = %q", page.Response.Body)
	}
}

func TestClientWithUserAgent(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket, WithUserAgent("limpet-test/1.0"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	var gotUA string
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/ua", nil)
	if _, err := cl.Do(t.Context(), req, DoConfig{Replace: true}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotUA != "limpet-test/1.0" {
		t.Errorf("User-Agent = %q, want limpet-test/1.0", gotUA)
	}

	// Caller-set UA should not be overridden.
	req2, _ := http.NewRequest("GET", svr.URL+"/ua2", nil)
	req2.Header.Set("User-Agent", "custom/2.0")
	if _, err := cl.Do(t.Context(), req2, DoConfig{Replace: true}); err != nil {
		t.Fatalf("Do custom: %v", err)
	}
	if gotUA != "custom/2.0" {
		t.Errorf("caller UA overridden: got %q, want custom/2.0", gotUA)
	}
}

func TestClientStatusError(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("server error"))
	}))
	t.Cleanup(svr.Close)

	_, err = cl.Get(t.Context(), svr.URL+"/fail")
	if err == nil {
		t.Fatal("expected StatusError for 500")
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected StatusError, got %T: %v", err, err)
	}
	if se.Page.Response.StatusCode != 500 {
		t.Errorf("StatusError status = %d, want 500", se.Page.Response.StatusCode)
	}
	if se.Error() != "bad fetch status: 500" {
		t.Errorf("Error() = %q", se.Error())
	}
	if string(se.Page.Response.Body) != "server error" {
		t.Errorf("body = %q", se.Page.Response.Body)
	}
}

func TestClientGetMany(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("page:" + r.URL.Path))
	}))
	t.Cleanup(svr.Close)

	urls := []string{
		svr.URL + "/a",
		svr.URL + "/b",
		svr.URL + "/c",
	}

	var count atomic.Int32
	err = cl.GetMany(t.Context(), urls, 2, DoConfig{}, func(url string, page *Page, fetchErr error) error {
		if fetchErr != nil {
			return fetchErr
		}
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if count.Load() != 3 {
		t.Errorf("callback count = %d, want 3", count.Load())
	}
}

func TestClientGetManyEarlyStop(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	urls := make([]string, 20)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/%d", svr.URL, i)
	}

	var count atomic.Int32
	stopErr := fmt.Errorf("stop after 2")
	err = cl.GetMany(t.Context(), urls, 1, DoConfig{}, func(url string, page *Page, fetchErr error) error {
		if count.Add(1) >= 2 {
			return stopErr
		}
		return nil
	})
	if !errors.Is(err, stopErr) {
		t.Errorf("GetMany error = %v, want stopErr", err)
	}
}

func TestClientRateLimitRespectsContext(t *testing.T) {
	bucket := setupClientBucket(t)
	// Very slow rate limiter: 1 request per 10 seconds.
	cl, err := NewClient(t.Context(), bucket,
		WithRateLimit(1, ratelimit.Per(10*time.Second)),
		WithRetry(RetryConfig{Attempts: 1}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	// First request consumes the token.
	req, _ := http.NewRequest("GET", svr.URL+"/first", nil)
	if _, err := cl.Do(t.Context(), req, DoConfig{Replace: true}); err != nil {
		t.Fatalf("first: %v", err)
	}

	// Second request with a short-lived context should be cancelled
	// while waiting for the rate limiter, not block for 10 seconds.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	req2, _ := http.NewRequest("GET", svr.URL+"/second", nil)
	start := time.Now()
	_, err = cl.Do(ctx, req2, DoConfig{Replace: true})
	dur := time.Since(start)

	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
	if dur > 2*time.Second {
		t.Errorf("took %v, expected < 2s (should abort on context cancel, not wait for rate limit)", dur)
	}
}

func TestClientPerHostRateLimit(t *testing.T) {
	bucket := setupClientBucket(t)
	cl, err := NewClient(t.Context(), bucket,
		WithPerHostRateLimit(100), // fast enough to not slow the test
		WithRateLimit(100),
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

	// Fetch two different paths on the same host.
	for _, path := range []string{"/a", "/b", "/c"} {
		ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
		page, err := cl.Get(ctx, svr.URL+path)
		if err != nil {
			t.Fatalf("Get %s: %v", path, err)
		}
		if string(page.Response.Body) != "ok" {
			t.Errorf("body = %q", page.Response.Body)
		}
	}
	if hits.Load() != 3 {
		t.Errorf("hits = %d, want 3", hits.Load())
	}

	// Verify the host limiter was created.
	_, loaded := cl.hostLimiters.Load("127.0.0.1")
	if !loaded {
		t.Error("expected host limiter to be created for 127.0.0.1")
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
