package limpet

import (
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
	page, err := cl.Do(t.Context(), req)
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
	_, err = cl.Do(t.Context(), req)
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
		_, _ = w.Write([]byte(fmt.Sprintf("resp-%d", n)))
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
	page, err := cl.Do(t.Context(), req)
	if err != nil {
		t.Fatalf("stable fetch: %v", err)
	}
	if string(page.Response.Body) != "resp-1" {
		t.Errorf("body = %q", page.Response.Body)
	}

	// Fetch /stable again: should hit cache.
	page, err = cl.Do(t.Context(), req)
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
	page, err = cl.Do(t.Context(), req2)
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
