package limpet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arclabs561/limpet/blob"
)

// Cache blobs land on disk and in S3; request credentials must not.
func TestCacheDoesNotPersistCredentials(t *testing.T) {
	tr, bucket := setupTransport(t)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/private", nil)
	req.Header.Set("Authorization", "Bearer s3cret-token")
	req.Header.Set("Cookie", "session=s3cret-cookie")
	req.Header.Set("Proxy-Authorization", "Basic s3cret-proxy")
	req.Header.Set("Accept", "text/plain")
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	key, _, err := tr.cache.cacheKey(req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := bucket.GetBlob(t.Context(), key)
	if err != nil {
		t.Fatalf("expected a cached blob: %v", err)
	}
	if strings.Contains(string(b.Data), "s3cret") {
		t.Fatalf("cached blob contains a credential: %s", b.Data)
	}
	var page Page
	if err := json.Unmarshal(b.Data, &page); err != nil {
		t.Fatal(err)
	}
	if got := page.Request.Header.Get("Accept"); got != "text/plain" {
		t.Errorf("non-secret request header dropped: Accept = %q", got)
	}
}

// RFC 9111 section 5.2.2.5: a cache must not store a no-store response.
func TestTransportNoStoreNotCached(t *testing.T) {
	tr, _ := setupTransport(t)
	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Cache-Control", "private, no-store")
		_, _ = w.Write([]byte("secret"))
	}))
	t.Cleanup(svr.Close)

	client := &http.Client{Transport: tr}
	for range 2 {
		resp, err := client.Get(svr.URL + "/nostore")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if h := hits.Load(); h != 2 {
		t.Errorf("server hits = %d, want 2 (no-store response was served from cache)", h)
	}
}

// The configured TTL (--cache-ttl) must expire entries in the remote tier
// too, not only in the local badger tier.
func TestRemoteTierHonorsCacheTTL(t *testing.T) {
	remote := t.TempDir()
	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("v"))
	}))
	t.Cleanup(svr.Close)

	writer, err := blob.NewBucket(t.Context(), remote, &blob.BucketConfig{
		CacheDir: t.TempDir(),
		CacheTTL: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: NewTransport(writer)}).Get(svr.URL + "/ttl")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	writer.Close() // flush the async remote write

	// A fresh process: no local tier, so any hit comes from the remote tier.
	reader, err := blob.NewBucket(t.Context(), remote, &blob.BucketConfig{NoCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	resp, err = (&http.Client{Transport: NewTransport(reader)}).Get(svr.URL + "/ttl")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if h := hits.Load(); h != 2 {
		t.Errorf("server hits = %d, want 2 (expired remote entry was served)", h)
	}
}

// RFC 9111 section 4.2: a stored response past max-age is stale and must be
// revalidated before reuse.
func TestTransportRevalidatesAfterMaxAge(t *testing.T) {
	tr, _ := setupTransport(t)
	var hits atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/maxage", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Age the stored entry past max-age.
	key, _, _ := tr.cache.cacheKey(req)
	page, err := tr.cache.readPage(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	page.Meta.FetchedAt = time.Now().Add(-2 * time.Hour)
	if err := tr.cache.writePage(t.Context(), key, page, req.URL.String()); err != nil {
		t.Fatal(err)
	}

	req, _ = http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/maxage", nil)
	resp, err = tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if h := hits.Load(); h != 2 {
		t.Fatalf("server hits = %d, want 2 (stale entry served without revalidation)", h)
	}
	if string(body) != "body" {
		t.Errorf("body = %q, want cached body after 304", body)
	}
	if src := resp.Header.Get("X-Limpet-Source"); src != SourceRevalidated {
		t.Errorf("source = %q, want %q", src, SourceRevalidated)
	}
}

// Coalesced requests share one fetch; one caller giving up must not fail the
// others.
func TestTransportSingleflightCallerCancelDoesNotFailOthers(t *testing.T) {
	tr, _ := setupTransport(t)
	started := make(chan struct{}, 1)
	gate := make(chan struct{})
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-gate
		_, _ = w.Write([]byte("shared"))
	}))
	t.Cleanup(svr.Close)

	ctxA, cancelA := context.WithCancel(t.Context())
	errA := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctxA, "GET", svr.URL+"/coalesce", nil)
		resp, err := tr.RoundTrip(req)
		if err == nil {
			resp.Body.Close()
		}
		errA <- err
	}()
	<-started // A's fetch is in flight

	type result struct {
		body string
		err  error
	}
	resB := make(chan result, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), "GET", svr.URL+"/coalesce", nil)
		resp, err := tr.RoundTrip(req)
		if err != nil {
			resB <- result{err: err}
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		resB <- result{body: string(b)}
	}()
	time.Sleep(200 * time.Millisecond) // let B join A's flight

	cancelA()
	if err := <-errA; !errors.Is(err, context.Canceled) {
		t.Errorf("caller A: err = %v, want context.Canceled", err)
	}
	close(gate)
	r := <-resB
	if r.err != nil {
		t.Fatalf("caller B failed because caller A canceled: %v", r.err)
	}
	if r.body != "shared" {
		t.Errorf("caller B body = %q, want shared", r.body)
	}
}
