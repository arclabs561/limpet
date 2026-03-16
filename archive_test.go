package limpet

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/arclabs561/limpet/blob"
)

func setupClient(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()

	bucketDir, err := os.MkdirTemp("", "limpet-archive-bucket-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })

	cacheDir, err := os.MkdirTemp("", "limpet-archive-cache-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })

	bucket, err := blob.NewBucket(ctx, bucketDir, &blob.BucketConfig{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })

	cl, err := NewClient(ctx, bucket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)

	return cl
}

func TestArchiveAndVersions(t *testing.T) {
	cl := setupClient(t)
	ctx := context.Background()

	var counter atomic.Int32
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := counter.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		if n == 1 {
			w.Write([]byte("version-1"))
		} else {
			w.Write([]byte("version-2"))
		}
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/page", nil)

	// First fetch with archive.
	page1, err := cl.Do(ctx, req, DoConfig{Archive: true, Replace: true})
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if string(page1.Response.Body) != "version-1" {
		t.Errorf("first body = %q, want %q", page1.Response.Body, "version-1")
	}

	// Second fetch with archive (force replace to get new content).
	page2, err := cl.Do(ctx, req, DoConfig{Archive: true, Replace: true})
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if string(page2.Response.Body) != "version-2" {
		t.Errorf("second body = %q, want %q", page2.Response.Body, "version-2")
	}

	// List versions.
	versions, err := cl.Versions(ctx, req)
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("versions count = %d, want 2", len(versions))
	}

	// Versions should be ordered oldest first.
	if !versions[0].FetchedAt.Before(versions[1].FetchedAt) {
		t.Errorf("versions not ordered: %v >= %v", versions[0].FetchedAt, versions[1].FetchedAt)
	}

	// Read back each version.
	v1, err := cl.Version(ctx, versions[0].Key)
	if err != nil {
		t.Fatalf("read version 0: %v", err)
	}
	if string(v1.Response.Body) != "version-1" {
		t.Errorf("version 0 body = %q, want %q", v1.Response.Body, "version-1")
	}

	v2, err := cl.Version(ctx, versions[1].Key)
	if err != nil {
		t.Fatalf("read version 1: %v", err)
	}
	if string(v2.Response.Body) != "version-2" {
		t.Errorf("version 1 body = %q, want %q", v2.Response.Body, "version-2")
	}

	// Diff should detect the change.
	d := Diff(v1, v2)
	if !d.Changed {
		t.Error("diff should detect change between version-1 and version-2")
	}
	if d.OldSize != 9 || d.NewSize != 9 {
		t.Errorf("diff sizes = %d/%d, want 9/9", d.OldSize, d.NewSize)
	}
}

func TestArchiveNotWrittenWithoutFlag(t *testing.T) {
	cl := setupClient(t)
	ctx := context.Background()

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/noarchive", nil)

	// Fetch without Archive flag.
	_, err := cl.Do(ctx, req, DoConfig{Replace: true})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	// Should have zero versions.
	versions, err := cl.Versions(ctx, req)
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("versions count = %d, want 0 (archive not requested)", len(versions))
	}
}

func TestDiffIdentical(t *testing.T) {
	cl := setupClient(t)
	ctx := context.Background()

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("static"))
	}))
	t.Cleanup(svr.Close)

	req, _ := http.NewRequest("GET", svr.URL+"/static", nil)

	// Two fetches of identical content.
	_, err := cl.Do(ctx, req, DoConfig{Archive: true, Replace: true})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err = cl.Do(ctx, req, DoConfig{Archive: true, Replace: true})
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	versions, err := cl.Versions(ctx, req)
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions) < 2 {
		t.Fatalf("versions count = %d, want >= 2", len(versions))
	}

	v1, _ := cl.Version(ctx, versions[0].Key)
	v2, _ := cl.Version(ctx, versions[1].Key)
	d := Diff(v1, v2)
	if d.Changed {
		t.Error("diff should not detect change for identical content")
	}
}
