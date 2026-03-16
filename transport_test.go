package limpet

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/arclabs561/limpet/blob"
)

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
		w.Write([]byte("hello"))
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
		w.Write([]byte("fresh"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// Populate cache.
	resp, err := client.Get(svr.URL)
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Replace: should hit server even though cached.
	ctx := WithCachePolicy(context.Background(), CachePolicyReplace)
	req, _ := http.NewRequestWithContext(ctx, "GET", svr.URL, nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("replace request: %v", err)
	}
	io.ReadAll(resp.Body)
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
		w.Write([]byte("data"))
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
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second request with default policy: should hit server (nothing cached).
	resp, err = client.Get(svr.URL)
	if err != nil {
		t.Fatalf("default request: %v", err)
	}
	io.ReadAll(resp.Body)
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
		w.Write([]byte("not found"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}

	// First request: 404, should not be cached.
	resp, _ := client.Get(svr.URL)
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Second request: should still hit server.
	resp, _ = client.Get(svr.URL)
	io.ReadAll(resp.Body)
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
		w.Write([]byte(r.URL.Path))
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
