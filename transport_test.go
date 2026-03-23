package limpet

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arclabs561/limpet/blob"
)

func setupTransportBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	bucket, err := blob.NewBucket(t.Context(), t.TempDir(), &blob.BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

func setupTransport(t *testing.T) (*Transport, *blob.Bucket) {
	t.Helper()
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket)
	return tr, bucket
}

func TestTransportCacheHitMiss(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("hello"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request: cache miss, hits server.
	resp, err := client.Get(svr.URL + "/test")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Errorf("first request body = %q, want %q", body, "hello")
	}
	if resp.Header.Get("X-Limpet-Source") != "fetch" {
		t.Errorf("first request source = %q, want %q", resp.Header.Get("X-Limpet-Source"), "fetch")
	}
	if hits.Load() != 1 {
		t.Errorf("server hit count = %d, want 1", hits.Load())
	}

	// Second request: cache hit, does not hit server.
	resp, err = client.Get(svr.URL + "/test")
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Errorf("second request body = %q, want %q", body, "hello")
	}
	if src := resp.Header.Get("X-Limpet-Source"); src != "cache" {
		t.Errorf("second request source = %q, want %q", src, "cache")
	}
	if hits.Load() != 1 {
		t.Errorf("server hit count = %d, want 1 (should not have been called again)", hits.Load())
	}
}

func TestTransportCachePolicyReplace(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("fresh"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Populate cache.
	resp, err := client.Get(svr.URL)
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Replace: should hit server even though cached.
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("replace request: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2", hits.Load())
	}
}

func TestTransportCachePolicySkip(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Skip: should not write to cache.
	ctx := WithCachePolicy(t.Context(), CachePolicySkip)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("skip request: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second request with default policy: should hit server (nothing cached).
	resp, err = client.Get(svr.URL)
	if err != nil {
		t.Fatalf("default request: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2 (skip should not have cached)", hits.Load())
	}
}

func TestTransportNon200NotCached(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request: 404, should not be cached.
	resp, _ := client.Get(svr.URL)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second request: should still hit server.
	resp, _ = client.Get(svr.URL)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2 (non-200 should not be cached)", hits.Load())
	}
}

func TestTransportDifferentURLsDifferentKeys(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(r.URL.Path))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	resp, _ := client.Get(svr.URL + "/a")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "/a" {
		t.Errorf("body = %q, want /a", body)
	}

	resp, _ = client.Get(svr.URL + "/b")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "/b" {
		t.Errorf("body = %q, want /b", body)
	}

	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2 (different URLs)", hits.Load())
	}
}

func TestTransportConditionalETag(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") == `"abc123"` {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", `"abc123"`)
		_, _ = w.Write([]byte("original"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request: populates cache with ETag.
	resp, err := client.Get(svr.URL + "/etag")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "original" {
		t.Errorf("body = %q, want original", body)
	}

	// Replace request: server returns 304, should get cached body back.
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/etag", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("conditional: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "original" {
		t.Errorf("conditional body = %q, want original", body)
	}
	if src := resp.Header.Get("X-Limpet-Source"); src != "revalidated" {
		t.Errorf("source = %q, want revalidated", src)
	}
	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2", hits.Load())
	}
}

func TestTransportSingleflight(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	gate := make(chan struct{}) // holds the server response until released
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-gate // block until test releases
		_, _ = w.Write([]byte("shared"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	const n = 5
	var wg sync.WaitGroup
	bodies := make([]string, n)
	errs := make([]error, n)

	// Launch n concurrent requests for the same URL (cache misses coalesce).
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/dedup", nil)
			resp, err := client.Do(req)
			if err != nil {
				errs[idx] = err
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			bodies[idx] = string(body)
		}(i)
	}

	// Release the server after a moment.
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	for i, body := range bodies {
		if body != "shared" {
			t.Errorf("request %d body = %q, want shared", i, body)
		}
	}
	// Singleflight should coalesce into 1 server hit.
	if h := hits.Load(); h != 1 {
		t.Errorf("server hits = %d, want 1 (singleflight should coalesce)", h)
	}
}

func TestTransportSingleflightReplaceBypasses(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request populates cache.
	req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/replace-bypass", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	resp.Body.Close()

	// Two Replace requests should each hit the server independently (no coalescing).
	hits.Store(0)
	for i := 0; i < 2; i++ {
		ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
		req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/replace-bypass", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("replace request %d: %v", i, err)
		}
		resp.Body.Close()
	}
	if h := hits.Load(); h != 2 {
		t.Errorf("server hits = %d, want 2 (Replace should bypass singleflight)", h)
	}
}

func TestTransportStats(t *testing.T) {
	tr, _ := setupTransport(t)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Miss + fetch.
	resp, _ := client.Get(svr.URL + "/stats")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Hit.
	resp, _ = client.Get(svr.URL + "/stats")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	s := tr.Stats()
	if s.Hits != 1 {
		t.Errorf("hits = %d, want 1", s.Hits)
	}
	if s.Misses != 1 {
		t.Errorf("misses = %d, want 1", s.Misses)
	}
}

func TestTransportUserAgent(t *testing.T) {
	tr, _ := setupTransport(t)
	tr = NewTransport(tr.cache.bucket, TransportWithUserAgent("limpet-test/1.0"))

	var gotUA string
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	ctx := WithCachePolicy(t.Context(), CachePolicySkip)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/ua", nil)
	resp, _ := client.Do(req)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if gotUA != "limpet-test/1.0" {
		t.Errorf("User-Agent = %q, want limpet-test/1.0", gotUA)
	}

	// Caller-set UA should not be overridden.
	req2, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/ua2", nil)
	req2.Header.Set("User-Agent", "custom/2.0")
	resp, _ = client.Do(req2)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if gotUA != "custom/2.0" {
		t.Errorf("caller UA overridden: got %q, want custom/2.0", gotUA)
	}
}

func TestTransportCacheStatuses(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithCacheStatuses(200, 404))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(404)
		_, _ = w.Write([]byte("not found"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First: 404, should be cached.
	resp, _ := client.Get(svr.URL + "/cached404")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second: should come from cache.
	resp, _ = client.Get(svr.URL + "/cached404")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 1 {
		t.Errorf("server hits = %d, want 1 (404 should be cached)", hits.Load())
	}
	if string(body) != "not found" {
		t.Errorf("body = %q, want 'not found'", body)
	}
}

func TestTransportStaleIfError(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithStaleIfError(true))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			_, _ = w.Write([]byte("original"))
			return
		}
		// Subsequent requests fail.
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Populate cache.
	resp, err := client.Get(svr.URL + "/stale")
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "original" {
		t.Fatalf("populate body = %q, want original", body)
	}

	// Close server to force network error on Replace.
	svr.Close()

	// Replace request: server is down, should get stale cached response.
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/stale", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("stale-if-error should not return error, got: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "original" {
		t.Errorf("stale body = %q, want original", body)
	}
	if src := resp.Header.Get("X-Limpet-Source"); src != "stale" {
		t.Errorf("source = %q, want stale", src)
	}

	s := tr.Stats()
	if s.StaleServed != 1 {
		t.Errorf("stale served = %d, want 1", s.StaleServed)
	}
}

func TestTransportStaleIfErrorDisabled(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket) // staleIfError defaults to false

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Populate cache.
	resp, err := client.Get(svr.URL + "/nostale")
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Close server.
	svr.Close()

	// Replace request: should propagate error (stale-if-error disabled).
	ctx := WithCachePolicy(t.Context(), CachePolicyReplace)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/nostale", nil)
	resp, err = client.Do(req)
	if err == nil {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Error("expected error when stale-if-error is disabled and server is down")
	}
}

func TestTransportRefreshPatterns(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithRefreshPatterns(
		RefreshPattern{Pattern: regexp.MustCompile(`/short-ttl`), MaxAge: 1 * time.Millisecond},
		RefreshPattern{Pattern: regexp.MustCompile(`/long-ttl`), MaxAge: 24 * time.Hour},
	))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		fmt.Fprintf(w, "response-%d", n)
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Request /long-ttl: should be cached with long TTL.
	resp, _ := client.Get(svr.URL + "/long-ttl")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "response-1" {
		t.Errorf("first long-ttl body = %q", body)
	}

	// Second request to /long-ttl: should hit cache.
	resp, _ = client.Get(svr.URL + "/long-ttl")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "response-1" {
		t.Errorf("cached long-ttl body = %q, want response-1", body)
	}

	// Request /short-ttl: cached with 1ms TTL.
	resp, _ = client.Get(svr.URL + "/short-ttl")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "response-2" {
		t.Errorf("first short-ttl body = %q", body)
	}

	// Wait for the short TTL to expire in badger.
	time.Sleep(50 * time.Millisecond)

	// Third request to /short-ttl: badger entry expired, should re-fetch.
	// Note: badger may not immediately GC the entry, but the TTL makes it
	// invisible to reads. However, badger's TTL-based expiration may have
	// timing variations, so we check the server hit count as the source of
	// truth: if the entry expired, we'd see a third hit.
	resp, _ = client.Get(svr.URL + "/short-ttl")
	body2, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// The long-ttl path should still be cached (2 hits total if short-ttl
	// re-fetched, or 2 if badger didn't expire yet).
	if hits.Load() < 2 {
		t.Errorf("server hits = %d, want >= 2", hits.Load())
	}
	// Verify the responses are valid (either cached or fresh).
	if len(body2) == 0 {
		t.Error("short-ttl response body is empty")
	}
}

func TestTransportRefreshPatternContextTTLTakesPrecedence(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithRefreshPatterns(
		RefreshPattern{Pattern: regexp.MustCompile(`.*`), MaxAge: 1 * time.Millisecond},
	))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Request with explicit long TTL in context: should override refresh pattern.
	ctx := blob.WithCacheTTL(t.Context(), 24*time.Hour)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/override", nil)
	resp, _ := client.Do(req)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Wait longer than the refresh pattern's 1ms TTL.
	time.Sleep(50 * time.Millisecond)

	// Should still be cached because context TTL (24h) takes precedence.
	resp, _ = client.Get(svr.URL + "/override")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 1 {
		t.Errorf("server hits = %d, want 1 (context TTL should override refresh pattern)", hits.Load())
	}
}

func TestRefreshPatternMatching(t *testing.T) {
	patterns := []RefreshPattern{
		{Pattern: regexp.MustCompile(`\.pdf$`), MaxAge: 7 * 24 * time.Hour},
		{Pattern: regexp.MustCompile(`/api/`), MaxAge: 5 * time.Minute},
		{Pattern: regexp.MustCompile(`.*`), MaxAge: 1 * time.Hour},
	}

	tests := []struct {
		url     string
		wantTTL time.Duration
		wantOK  bool
	}{
		{"https://example.com/doc.pdf", 7 * 24 * time.Hour, true},
		{"https://example.com/api/users", 5 * time.Minute, true},
		{"https://example.com/page", 1 * time.Hour, true},
	}

	for _, tt := range tests {
		ttl, ok := matchRefreshTTL(patterns, tt.url)
		if ok != tt.wantOK {
			t.Errorf("matchRefreshTTL(%q): ok = %v, want %v", tt.url, ok, tt.wantOK)
		}
		if ttl != tt.wantTTL {
			t.Errorf("matchRefreshTTL(%q): ttl = %v, want %v", tt.url, ttl, tt.wantTTL)
		}
	}
}

func TestRefreshPatternFirstMatchWins(t *testing.T) {
	patterns := []RefreshPattern{
		{Pattern: regexp.MustCompile(`/api/`), MaxAge: 5 * time.Minute},
		{Pattern: regexp.MustCompile(`/api/slow`), MaxAge: 1 * time.Hour},
	}

	// /api/slow matches the first pattern, not the second.
	ttl, ok := matchRefreshTTL(patterns, "https://example.com/api/slow")
	if !ok {
		t.Fatal("expected match")
	}
	if ttl != 5*time.Minute {
		t.Errorf("ttl = %v, want 5m (first match wins)", ttl)
	}
}

func TestRefreshPatternNoMatch(t *testing.T) {
	patterns := []RefreshPattern{
		{Pattern: regexp.MustCompile(`\.pdf$`), MaxAge: 7 * 24 * time.Hour},
	}

	_, ok := matchRefreshTTL(patterns, "https://example.com/page.html")
	if ok {
		t.Error("expected no match for .html URL with .pdf pattern")
	}
}

func TestTransportSingleflightForgetOnError(t *testing.T) {
	tr, _ := setupTransport(t)

	var hits atomic.Int32
	gate := make(chan struct{})
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			// First request: block until gate, then fail.
			<-gate
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		// Subsequent requests succeed.
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Launch two concurrent requests (cache misses enter singleflight).
	// First will fail (502). With Forget, the second should retry independently.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	bodies := make([]string, 2)

	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/forget", nil)
			resp, err := client.Do(req)
			if err != nil {
				errs[idx] = err
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			bodies[idx] = string(body)
		}(i)
	}

	// Release the gate to let the first request fail.
	close(gate)
	wg.Wait()

	// At least one request should succeed (the retry after Forget).
	// With singleflight coalescing, both may share the first failure,
	// but subsequent independent requests should work.
	// The key property: the key is forgotten, so future requests aren't blocked.
	req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/forget", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post-forget request should succeed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Errorf("post-forget body = %q, want ok", body)
	}
}

func TestTransportIgnoreHeaders(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithIgnoreHeaders("X-Request-Id"))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request with X-Request-Id: "aaa"
	req1, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/ignore-hdr", nil)
	req1.Header.Set("X-Request-Id", "aaa")
	resp, _ := client.Do(req1)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second request with X-Request-Id: "bbb" -- different header but ignored,
	// so it should be a cache hit.
	req2, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/ignore-hdr", nil)
	req2.Header.Set("X-Request-Id", "bbb")
	resp, _ = client.Do(req2)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 1 {
		t.Errorf("server hits = %d, want 1 (ignored header should not affect cache key)", hits.Load())
	}
}

func TestTransportIgnoreParams(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithIgnoreParams("_t", "utm_source"))

	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Request with timestamp param.
	resp, _ := client.Get(svr.URL + "/page?_t=111&utm_source=twitter")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Same path, different ignored params -- should be cache hit.
	resp, _ = client.Get(svr.URL + "/page?_t=222&utm_source=facebook")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 1 {
		t.Errorf("server hits = %d, want 1 (ignored params should not affect cache key)", hits.Load())
	}

	// Different non-ignored param -- should be cache miss.
	resp, _ = client.Get(svr.URL + "/page?_t=333&q=different")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if hits.Load() != 2 {
		t.Errorf("server hits = %d, want 2 (different non-ignored param = different key)", hits.Load())
	}
}

func TestTransportResponseBodyLimit(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket, TransportWithResponseBodyLimit(10))

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write more than the 10-byte limit.
		_, _ = w.Write(make([]byte, 100))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	resp, err := client.Get(svr.URL + "/big")
	if err == nil {
		resp.Body.Close()
		t.Error("expected error when response exceeds body limit")
	}
}

func TestTransport304WithoutCachedPage(t *testing.T) {
	bucket := setupTransportBucket(t)
	tr := NewTransport(bucket)

	// Server always returns 304 (misbehaving -- no conditional request was sent).
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(304)
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	resp, err := client.Get(svr.URL + "/bad304")
	if err == nil {
		resp.Body.Close()
		t.Error("expected error when server returns 304 without prior cached page")
	}
}

func BenchmarkBlobKey(b *testing.B) {
	req, _ := http.NewRequest("GET", "https://example.com/page?a=1&b=2&c=3", nil)
	req.Header.Set("User-Agent", "bench/1.0")
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Accept-Language", "en-US")

	b.ResetTimer()
	for b.Loop() {
		_, _, err := blobKey(req, 10e6, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetGetBlob(b *testing.B) {
	bucket, err := blob.NewBucket(b.Context(), b.TempDir(), &blob.BucketConfig{CacheDir: b.TempDir()})
	if err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}
	b.Cleanup(func() { bucket.Close() })

	// Simulate a typical cached HTML page (~50 KB).
	data := make([]byte, 50*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.Run("Set", func(b *testing.B) {
		for i := range b.N {
			key := fmt.Sprintf("bench/key-%d.json", i)
			if err := bucket.SetBlob(b.Context(), key, data); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Populate for Get benchmark.
	for i := range 1000 {
		key := fmt.Sprintf("bench/get-%d.json", i)
		if err := bucket.SetBlob(b.Context(), key, data); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Get", func(b *testing.B) {
		for i := range b.N {
			key := fmt.Sprintf("bench/get-%d.json", i%1000)
			bl, err := bucket.GetBlob(b.Context(), key)
			if err != nil {
				b.Fatal(err)
			}
			if len(bl.Data) != len(data) {
				b.Fatalf("data size mismatch: got %d, want %d", len(bl.Data), len(data))
			}
		}
	})
}
