package limpet

import (
	"bytes"
	"errors"
	"net/http"
	"testing"
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

	t.Run("deterministic", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://example.com/page", nil)
		req2, _ := http.NewRequest("GET", "https://example.com/page", nil)

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
