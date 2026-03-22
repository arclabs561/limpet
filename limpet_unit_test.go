package limpet

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/ratelimit"
)

func TestParseRateLimit(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"100", false},
		{"10/1m", false},
		{"5/30s", false},
		{"1/1h", false},
		{"none", false},
		{"unlimited", false},
		{"disabled", false},
		{"off", false},
		{"nolimit", false},
		{"10/m", false},  // bare duration unit, prepends "1"
		{"10/s", false},  // bare duration unit
		{"abc", true},    // non-numeric
		{"10/xyz", true}, // bad duration
		{"", true},       // empty
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lim, err := parseRateLimit(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseRateLimit(%q): expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseRateLimit(%q): unexpected error: %v", tt.input, err)
				return
			}
			if lim == nil {
				t.Errorf("parseRateLimit(%q): returned nil limiter", tt.input)
			}
		})
	}
}

func TestBlobKey(t *testing.T) {
	c := &Client{}

	t.Run("deterministic with headers", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com/page", nil)
		req1.Header.Set("Accept", "text/html")
		req1.Header.Set("User-Agent", "limpet/test")
		req1.Header.Set("X-Custom", "value")
		req2, _ := http.NewRequest("GET", "https://example.com/page", nil)
		req2.Header.Set("X-Custom", "value")
		req2.Header.Set("Accept", "text/html")
		req2.Header.Set("User-Agent", "limpet/test")

		key1, _, err := c.blobKey(req1)
		if err != nil {
			t.Fatalf("blobKey: %v", err)
		}
		key2, _, err := c.blobKey(req2)
		if err != nil {
			t.Fatalf("blobKey: %v", err)
		}
		if key1 != key2 {
			t.Errorf("same request produced different keys: %q vs %q", key1, key2)
		}
	})

	t.Run("different URLs produce different keys", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com/a", nil)
		req2, _ := http.NewRequest("GET", "https://example.com/b", nil)

		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 == key2 {
			t.Errorf("different URLs produced same key: %q", key1)
		}
	})

	t.Run("different methods produce different keys", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com", nil)
		req2, _ := http.NewRequest("POST", "https://example.com", nil)

		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 == key2 {
			t.Errorf("different methods produced same key: %q", key1)
		}
	})

	t.Run("hostname in key path", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://example.com/path", nil)

		key, _, _ := c.blobKey(req)
		if key == "" {
			t.Fatal("empty key")
		}
		// Key should start with hostname
		if !strings.HasPrefix(key, "example.com/") {
			t.Errorf("key %q does not start with hostname", key)
		}
	})

	t.Run("request body included", func(t *testing.T) {
		body1 := bytes.NewBufferString("body1")
		body2 := bytes.NewBufferString("body2")
		req1, _ := http.NewRequest("POST", "https://example.com", body1)
		req2, _ := http.NewRequest("POST", "https://example.com", body2)

		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 == key2 {
			t.Errorf("different bodies produced same key: %q", key1)
		}
	})

	t.Run("returns request body", func(t *testing.T) {
		body := bytes.NewBufferString("hello")
		req, _ := http.NewRequest("POST", "https://example.com", body)

		_, reqBody, err := c.blobKey(req)
		if err != nil {
			t.Fatalf("blobKey: %v", err)
		}
		if string(reqBody) != "hello" {
			t.Errorf("reqBody = %q, want %q", reqBody, "hello")
		}
	})
}

func TestBlobKeyIgnoreHeaders(t *testing.T) {
	c := &Client{
		ignoreHeaders: map[string]bool{
			"User-Agent":      true,
			"Accept-Encoding": true,
		},
	}

	req1, _ := http.NewRequest("GET", "https://example.com/page", nil)
	req1.Header.Set("User-Agent", "chrome")
	req1.Header.Set("Accept", "text/html")

	req2, _ := http.NewRequest("GET", "https://example.com/page", nil)
	req2.Header.Set("User-Agent", "firefox")
	req2.Header.Set("Accept", "text/html")

	key1, _, _ := c.blobKey(req1)
	key2, _, _ := c.blobKey(req2)
	if key1 != key2 {
		t.Errorf("ignored header changed cache key: %q vs %q", key1, key2)
	}

	// Non-ignored header should still differentiate.
	req3, _ := http.NewRequest("GET", "https://example.com/page", nil)
	req3.Header.Set("User-Agent", "chrome")
	req3.Header.Set("Accept", "application/json")

	key3, _, _ := c.blobKey(req3)
	if key1 == key3 {
		t.Errorf("non-ignored header difference produced same key")
	}
}

func TestBlobKeyIgnoreParams(t *testing.T) {
	c := &Client{
		ignoreParams: map[string]bool{
			"_t":         true,
			"utm_source": true,
		},
	}

	// Ignored params should not affect cache key.
	req1, _ := http.NewRequest("GET", "https://example.com/page?q=foo&_t=123", nil)
	req2, _ := http.NewRequest("GET", "https://example.com/page?q=foo&_t=999&utm_source=twitter", nil)

	key1, _, _ := c.blobKey(req1)
	key2, _, _ := c.blobKey(req2)
	if key1 != key2 {
		t.Errorf("ignored params changed cache key: %q vs %q", key1, key2)
	}

	// Non-ignored param should still differentiate.
	req3, _ := http.NewRequest("GET", "https://example.com/page?q=bar&_t=123", nil)
	key3, _, _ := c.blobKey(req3)
	if key1 == key3 {
		t.Errorf("non-ignored param difference produced same key")
	}

	// No params at all should still work.
	req4, _ := http.NewRequest("GET", "https://example.com/page?q=foo", nil)
	key4, _, _ := c.blobKey(req4)
	if key1 != key4 {
		t.Errorf("URL without ignored params should match: %q vs %q", key1, key4)
	}
}

func TestBlobKeyURLNormalization(t *testing.T) {
	c := &Client{}

	t.Run("case insensitive host", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://Example.COM/path", nil)
		req2, _ := http.NewRequest("GET", "https://example.com/path", nil)
		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 != key2 {
			t.Errorf("host case changed key: %q vs %q", key1, key2)
		}
	})

	t.Run("default port stripped", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com:443/path", nil)
		req2, _ := http.NewRequest("GET", "https://example.com/path", nil)
		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 != key2 {
			t.Errorf("default port changed key: %q vs %q", key1, key2)
		}
	})

	t.Run("query param order irrelevant", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com/path?b=2&a=1", nil)
		req2, _ := http.NewRequest("GET", "https://example.com/path?a=1&b=2", nil)
		key1, _, _ := c.blobKey(req1)
		key2, _, _ := c.blobKey(req2)
		if key1 != key2 {
			t.Errorf("param order changed key: %q vs %q", key1, key2)
		}
	})
}

func TestArchiveKey(t *testing.T) {
	ts := time.Date(2026, 3, 16, 21, 30, 45, 0, time.UTC)

	t.Run("format", func(t *testing.T) {
		key := archiveKey("example.com/abc123.json", ts)
		want := "example.com/abc123@20260316T213045.000Z.json"
		if key != want {
			t.Errorf("archiveKey = %q, want %q", key, want)
		}
	})

	t.Run("prefix", func(t *testing.T) {
		pfx := archivePrefix("example.com/abc123.json")
		want := "example.com/abc123@"
		if pfx != want {
			t.Errorf("archivePrefix = %q, want %q", pfx, want)
		}
	})
}

func TestDiff(t *testing.T) {
	t.Run("identical pages", func(t *testing.T) {
		a := &Page{Response: PageResponse{Body: []byte("hello")}}
		b := &Page{Response: PageResponse{Body: []byte("hello")}}
		d := Diff(a, b)
		if d.Changed {
			t.Error("expected Changed=false for identical bodies")
		}
	})

	t.Run("different pages", func(t *testing.T) {
		a := &Page{Response: PageResponse{Body: []byte("hello")}}
		b := &Page{Response: PageResponse{Body: []byte("world")}}
		d := Diff(a, b)
		if !d.Changed {
			t.Error("expected Changed=true for different bodies")
		}
		if d.OldSize != 5 || d.NewSize != 5 {
			t.Errorf("sizes = %d/%d, want 5/5", d.OldSize, d.NewSize)
		}
	})
}

func TestPageStale(t *testing.T) {
	t.Run("no headers means fresh", func(t *testing.T) {
		p := &Page{
			Meta:     PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: make(http.Header)},
		}
		if p.Stale() {
			t.Error("expected fresh when no cache headers")
		}
	})

	t.Run("max-age not expired", func(t *testing.T) {
		p := &Page{
			Meta:     PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: http.Header{"Cache-Control": {"max-age=3600"}}},
		}
		if p.Stale() {
			t.Error("expected fresh within max-age")
		}
	})

	t.Run("max-age expired", func(t *testing.T) {
		p := &Page{
			Meta:     PageMeta{FetchedAt: time.Now().Add(-2 * time.Hour)},
			Response: PageResponse{Header: http.Header{"Cache-Control": {"max-age=3600"}}},
		}
		if !p.Stale() {
			t.Error("expected stale past max-age")
		}
	})

	t.Run("no-cache is stale", func(t *testing.T) {
		p := &Page{
			Meta:     PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: http.Header{"Cache-Control": {"no-cache"}}},
		}
		if !p.Stale() {
			t.Error("expected stale with no-cache")
		}
	})

	t.Run("no-store is stale", func(t *testing.T) {
		p := &Page{
			Meta:     PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: http.Header{"Cache-Control": {"no-store"}}},
		}
		if !p.Stale() {
			t.Error("expected stale with no-store")
		}
	})

	t.Run("expires in future", func(t *testing.T) {
		p := &Page{
			Meta: PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: http.Header{
				"Expires": {time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)},
			}},
		}
		if p.Stale() {
			t.Error("expected fresh before Expires")
		}
	})

	t.Run("expires in past", func(t *testing.T) {
		p := &Page{
			Meta: PageMeta{FetchedAt: time.Now()},
			Response: PageResponse{Header: http.Header{
				"Expires": {time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)},
			}},
		}
		if !p.Stale() {
			t.Error("expected stale after Expires")
		}
	})
}

func TestPageStaleAfter(t *testing.T) {
	t.Run("recent fetch is fresh", func(t *testing.T) {
		p := &Page{Meta: PageMeta{FetchedAt: time.Now()}}
		if p.StaleAfter(time.Hour) {
			t.Error("expected fresh when fetched just now")
		}
	})

	t.Run("old fetch is stale", func(t *testing.T) {
		p := &Page{Meta: PageMeta{FetchedAt: time.Now().Add(-2 * time.Hour)}}
		if !p.StaleAfter(time.Hour) {
			t.Error("expected stale when fetched 2h ago with 1h maxAge")
		}
	})

	t.Run("zero FetchedAt is stale", func(t *testing.T) {
		p := &Page{}
		if !p.StaleAfter(time.Hour) {
			t.Error("expected stale when FetchedAt is zero")
		}
	})
}

func TestErrPageStatusNotOK(t *testing.T) {
	// Use a default Client (only 200 is cacheable/accepted).
	cl := &Client{}

	t.Run("200 returns nil", func(t *testing.T) {
		page := &Page{Response: PageResponse{StatusCode: 200}}
		if err := cl.errPageStatusNotOK(page); err != nil {
			t.Errorf("expected nil error for 200, got: %v", err)
		}
	})

	t.Run("404 returns error", func(t *testing.T) {
		page := &Page{Response: PageResponse{StatusCode: 404}}
		err := cl.errPageStatusNotOK(page)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
		var notOK *StatusError
		if !errors.As(err, &notOK) {
			t.Errorf("error is not StatusError: %T", err)
		}
		if notOK.Page != page {
			t.Error("error does not reference the original page")
		}
	})

	t.Run("404 accepted when in cacheStatuses", func(t *testing.T) {
		cl2 := &Client{cacheStatuses: map[int]bool{200: true, 404: true}}
		page := &Page{Response: PageResponse{StatusCode: 404}}
		if err := cl2.errPageStatusNotOK(page); err != nil {
			t.Errorf("expected nil for cacheable 404, got: %v", err)
		}
	})
}

func TestDoRejectsBrowserAndStealth(t *testing.T) {
	cl := &Client{
		mu:        new(sync.Mutex),
		rateLimit: ratelimit.NewUnlimited(),
		retry:     RetryConfig{Attempts: 1, MinWait: time.Millisecond, MaxWait: time.Millisecond},
	}
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	_, err := cl.Do(t.Context(), req, DoConfig{Browser: true, Stealth: true})
	if err == nil {
		t.Fatal("expected error for Browser+Stealth, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRetryWaitZeroJitter(t *testing.T) {
	// Verify that zero Jitter does not panic.
	cl := &Client{
		retry: RetryConfig{
			Attempts: 2,
			MinWait:  time.Millisecond,
			MaxWait:  10 * time.Millisecond,
			Jitter:   0,
		},
	}
	err := cl.retryWait(t.Context(), 0)
	if err != nil {
		t.Fatalf("retryWait with zero jitter: %v", err)
	}
}
