package limpet

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arclabs561/limpet/blob"
)

func setupTransportBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	ctx := context.Background()
	bucketDir, err := os.MkdirTemp("", "limpet-transport-bucket-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })
	cacheDir, err := os.MkdirTemp("", "limpet-transport-cache-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })
	bucket, err := blob.NewBucket(ctx, bucketDir, &blob.BucketConfig{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

func setupTransport(t *testing.T) (*Transport, *blob.Bucket) {
	t.Helper()
	ctx := context.Background()

	bucketDir, err := os.MkdirTemp("", "limpet-transport-bucket-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })

	cacheDir, err := os.MkdirTemp("", "limpet-transport-cache-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })

	bucket, err := blob.NewBucket(ctx, bucketDir, &blob.BucketConfig{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

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
	ctx := WithCachePolicy(context.Background(), CachePolicyReplace)
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
	ctx := WithCachePolicy(context.Background(), CachePolicySkip)
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
	ctx := WithCachePolicy(context.Background(), CachePolicyReplace)
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

	// Launch n concurrent requests for the same URL.
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := WithCachePolicy(context.Background(), CachePolicyReplace)
			req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL+"/dedup", nil)
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
	tr = NewTransport(tr.bucket, TransportWithUserAgent("limpet-test/1.0"))

	var gotUA string
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	ctx := WithCachePolicy(context.Background(), CachePolicySkip)
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
