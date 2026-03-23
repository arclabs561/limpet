package blob

import (
	"errors"
	"testing"
	"time"
)

func setupBucket(t *testing.T) *Bucket {
	t.Helper()
	bucket, err := NewBucket(t.Context(), t.TempDir(), &BucketConfig{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

func mustSet(t *testing.T, bu *Bucket, key string, data []byte) {
	t.Helper()
	if err := bu.SetBlob(t.Context(), key, data); err != nil {
		t.Fatalf("SetBlob(%q): %v", key, err)
	}
}

func TestSetGetBlob(t *testing.T) {
	bu := setupBucket(t)

	mustSet(t, bu, "test/key1", []byte("hello"))

	b, err := bu.GetBlob(t.Context(), "test/key1")
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if string(b.Data) != "hello" {
		t.Errorf("Data = %q, want hello", b.Data)
	}
	if b.Source != "cache" {
		t.Errorf("Source = %q, want cache", b.Source)
	}
}

func TestGetBlobNotFound(t *testing.T) {
	bu := setupBucket(t)

	_, err := bu.GetBlob(t.Context(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected NotFoundError, got %T: %v", err, err)
	}
}

func TestDeleteBlob(t *testing.T) {
	bu := setupBucket(t)

	mustSet(t, bu, "test/del", []byte("data"))

	if err := bu.DeleteBlob(t.Context(), "test/del"); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}

	_, err := bu.GetBlob(t.Context(), "test/del")
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteBlobNotFound(t *testing.T) {
	bu := setupBucket(t)

	// Should not error on missing key.
	if err := bu.DeleteBlob(t.Context(), "nonexistent"); err != nil {
		t.Fatalf("DeleteBlob on missing key: %v", err)
	}
}

func TestListCache(t *testing.T) {
	bu := setupBucket(t)

	mustSet(t, bu, "host/a", []byte("1"))
	mustSet(t, bu, "host/b", []byte("2"))
	mustSet(t, bu, "other/c", []byte("3"))

	entries, err := bu.ListCache("host/")
	if err != nil {
		t.Fatalf("ListCache: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}

	all, err := bu.ListCache("")
	if err != nil {
		t.Fatalf("ListCache all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("got %d entries, want 3", len(all))
	}
}

func TestPurgeCache(t *testing.T) {
	bu := setupBucket(t)

	mustSet(t, bu, "host/a", []byte("1"))
	mustSet(t, bu, "host/b", []byte("2"))
	mustSet(t, bu, "other/c", []byte("3"))

	n, err := bu.PurgeCache("host/")
	if err != nil {
		t.Fatalf("PurgeCache: %v", err)
	}
	if n != 2 {
		t.Errorf("purged %d, want 2", n)
	}

	remaining, err := bu.ListCache("")
	if err != nil {
		t.Fatalf("ListCache: %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1", len(remaining))
	}
}

func TestPurgeCacheAll(t *testing.T) {
	bu := setupBucket(t)

	mustSet(t, bu, "a", []byte("1"))
	mustSet(t, bu, "b", []byte("2"))

	n, err := bu.PurgeCache("")
	if err != nil {
		t.Fatalf("PurgeCache all: %v", err)
	}
	if n != 2 {
		t.Errorf("purged %d, want 2", n)
	}
}

func TestWithCacheTTL(t *testing.T) {
	bu := setupBucket(t)

	shortCtx := WithCacheTTL(t.Context(), 500*time.Millisecond)
	if err := bu.SetBlob(shortCtx, "ttl/short", []byte("ephemeral")); err != nil {
		t.Fatalf("SetBlob short: %v", err)
	}
	mustSet(t, bu, "ttl/default", []byte("normal"))

	b1, err := bu.GetBlob(t.Context(), "ttl/short")
	if err != nil {
		t.Fatalf("GetBlob short: %v", err)
	}
	if string(b1.Data) != "ephemeral" {
		t.Errorf("short data = %q", b1.Data)
	}

	b2, err := bu.GetBlob(t.Context(), "ttl/default")
	if err != nil {
		t.Fatalf("GetBlob default: %v", err)
	}
	if string(b2.Data) != "normal" {
		t.Errorf("default data = %q", b2.Data)
	}
}

func TestNoCacheBucket(t *testing.T) {
	bu, err := NewBucket(t.Context(), t.TempDir(), &BucketConfig{NoCache: true})
	if err != nil {
		t.Fatalf("NewBucket: %v", err)
	}
	defer bu.Close()

	if err := bu.SetBlob(t.Context(), "nc/key", []byte("data")); err != nil {
		t.Fatalf("SetBlob: %v", err)
	}
	b, err := bu.GetBlob(t.Context(), "nc/key")
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	if string(b.Data) != "data" {
		t.Errorf("Data = %q", b.Data)
	}
	if b.Source != "remote" {
		t.Errorf("Source = %q, want remote", b.Source)
	}

	entries, err := bu.ListCache("")
	if err != nil {
		t.Fatalf("ListCache: %v", err)
	}
	if entries != nil {
		t.Errorf("expected nil entries with no cache, got %d", len(entries))
	}
}

func TestBucketUseAfterClose(t *testing.T) {
	bu := setupBucket(t)
	mustSet(t, bu, "test/before-close", []byte("data"))

	bu.Close()

	// All operations should return ErrClosed after Close.
	if err := bu.SetBlob(t.Context(), "test/after-close", []byte("data")); !errors.Is(err, ErrClosed) {
		t.Errorf("SetBlob after Close: got %v, want ErrClosed", err)
	}
	if _, err := bu.GetBlob(t.Context(), "test/before-close"); !errors.Is(err, ErrClosed) {
		t.Errorf("GetBlob after Close: got %v, want ErrClosed", err)
	}
	if err := bu.DeleteBlob(t.Context(), "test/before-close"); !errors.Is(err, ErrClosed) {
		t.Errorf("DeleteBlob after Close: got %v, want ErrClosed", err)
	}

	// Double close should not panic.
	bu.Close()
}

func TestBucketCloseMultipleTimes(t *testing.T) {
	bu := setupBucket(t)
	bu.Close()
	bu.Close() // should not panic
}
