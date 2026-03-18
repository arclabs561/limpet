package limpet

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestGetMany(t *testing.T) {
	cl := setupClient(t)
	ctx := t.Context()

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok:" + r.URL.Path))
	}))
	t.Cleanup(svr.Close)

	const n = 10
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/page/%d", svr.URL, i)
	}

	var mu sync.Mutex
	got := make(map[string]bool)

	err := cl.GetMany(ctx, urls, 3, DoConfig{}, func(url string, page *Page, fetchErr error) error {
		if fetchErr != nil {
			return fetchErr
		}
		mu.Lock()
		got[url] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != n {
		t.Errorf("fetched %d URLs, want %d", len(got), n)
	}
	for _, u := range urls {
		if !got[u] {
			t.Errorf("missing URL %s", u)
		}
	}
}

func TestGetManyRespectsConcurrency(t *testing.T) {
	cl := setupClient(t)
	ctx := t.Context()

	const maxConcurrency = 2

	var inflight atomic.Int32
	var maxSeen atomic.Int32

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		// Track the maximum concurrency observed.
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		// Yield to let other goroutines run, making overlap more likely.
		// No sleep -- just a non-trivial response body.
		_, _ = w.Write([]byte("data"))
		inflight.Add(-1)
	}))
	t.Cleanup(svr.Close)

	urls := make([]string, 20)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/%d", svr.URL, i)
	}

	err := cl.GetMany(ctx, urls, maxConcurrency, DoConfig{Replace: true}, func(_ string, _ *Page, fetchErr error) error {
		return fetchErr
	})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if m := maxSeen.Load(); m > int32(maxConcurrency) {
		t.Errorf("max concurrent requests = %d, want <= %d", m, maxConcurrency)
	}
}

func TestGetManyCallbackError(t *testing.T) {
	cl := setupClient(t)
	ctx := t.Context()

	var served atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(svr.Close)

	urls := make([]string, 50)
	for i := range urls {
		urls[i] = fmt.Sprintf("%s/%d", svr.URL, i)
	}

	sentinel := fmt.Errorf("stop here")
	var callbackCount atomic.Int32

	err := cl.GetMany(ctx, urls, 1, DoConfig{Replace: true}, func(_ string, _ *Page, fetchErr error) error {
		if fetchErr != nil {
			return fetchErr
		}
		n := callbackCount.Add(1)
		if n >= 3 {
			return sentinel
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error from GetMany, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("got error %v, want sentinel", err)
	}
	// With concurrency=1 and early cancel, we should not have fetched all 50.
	if s := served.Load(); s >= 50 {
		t.Errorf("served %d requests, expected early stop well before 50", s)
	}
}

func TestGetManyEmptyURLs(t *testing.T) {
	cl := setupClient(t)
	ctx := t.Context()

	err := cl.GetMany(ctx, nil, 3, DoConfig{}, func(_ string, _ *Page, _ error) error {
		t.Fatal("callback should not be called for empty URLs")
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error for empty URLs, got %v", err)
	}
}
