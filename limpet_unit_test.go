package limpet

import (
	"bytes"
	"errors"
	"net/http"
	"testing"
	"time"
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
		{"10/m", false},   // bare duration unit, prepends "1"
		{"10/s", false},   // bare duration unit
		{"abc", true},     // non-numeric
		{"10/xyz", true},  // bad duration
		{"", true},        // empty
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
		if !bytes.HasPrefix([]byte(key), []byte("example.com/")) {
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

func TestErrPageStatusNotOK(t *testing.T) {
	t.Run("200 returns nil", func(t *testing.T) {
		page := &Page{Response: PageResponse{StatusCode: 200}}
		if err := errPageStatusNotOK(page); err != nil {
			t.Errorf("expected nil error for 200, got: %v", err)
		}
	})

	t.Run("404 returns error", func(t *testing.T) {
		page := &Page{Response: PageResponse{StatusCode: 404}}
		err := errPageStatusNotOK(page)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
		var notOK *FetchStatusNotOKError
		if !errors.As(err, &notOK) {
			t.Errorf("error is not FetchStatusNotOKError: %T", err)
		}
		if notOK.Page != page {
			t.Error("error does not reference the original page")
		}
	})
}
