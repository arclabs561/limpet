package blob

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func setupBucket(t *testing.T) *Bucket {
	t.Helper()
	ctx := context.Background()

	bucketDir, err := os.MkdirTemp("", "limpet-blob-bucket-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })

	cacheDir, err := os.MkdirTemp("", "limpet-blob-cache-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(cacheDir) })

	bucket, err := NewBucket(ctx, bucketDir, &BucketConfig{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

func mustSet(t *testing.T, bu *Bucket, ctx context.Context, key string, data []byte) {
	t.Helper()
	if err := bu.SetBlob(ctx, key, data); err != nil {
		t.Fatalf("SetBlob(%q): %v", key, err)
	}
}

func TestSetGetBlob(t *testing.T) {
	bu := setupBucket(t)
	ctx := context.Background()

	mustSet(t, bu, ctx, "test/key1", []byte("hello"))

	b, err := bu.GetBlob(ctx, "test/key1")
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
	ctx := context.Background()

	_, err := bu.GetBlob(ctx, "nonexistent")
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
	ctx := context.Background()

	mustSet(t, bu, ctx, "test/del", []byte("data"))

	if err := bu.DeleteBlob(ctx, "test/del"); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}

	_, err := bu.GetBlob(ctx, "test/del")
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestDeleteBlobNotFound(t *testing.T) {
	bu := setupBucket(t)
	ctx := context.Background()

	// Should not error on missing key.
	if err := bu.DeleteBlob(ctx, "nonexistent"); err != nil {
		t.Fatalf("DeleteBlob on missing key: %v", err)
	}
}

func TestListCache(t *testing.T) {
	bu := setupBucket(t)
	ctx := context.Background()

	mustSet(t, bu, ctx, "host/a", []byte("1"))
	mustSet(t, bu, ctx, "host/b", []byte("2"))
	mustSet(t, bu, ctx, "other/c", []byte("3"))

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
	ctx := context.Background()

	mustSet(t, bu, ctx, "host/a", []byte("1"))
	mustSet(t, bu, ctx, "host/b", []byte("2"))
	mustSet(t, bu, ctx, "other/c", []byte("3"))

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
	ctx := context.Background()

	mustSet(t, bu, ctx, "a", []byte("1"))
	mustSet(t, bu, ctx, "b", []byte("2"))

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
	ctx := context.Background()

	shortCtx := WithCacheTTL(ctx, 1*time.Millisecond)
	mustSet(t, bu, shortCtx, "ttl/short", []byte("ephemeral"))
	mustSet(t, bu, ctx, "ttl/default", []byte("normal"))

	b1, err := bu.GetBlob(ctx, "ttl/short")
	if err != nil {
		t.Fatalf("GetBlob short: %v", err)
	}
	if string(b1.Data) != "ephemeral" {
		t.Errorf("short data = %q", b1.Data)
	}

	b2, err := bu.GetBlob(ctx, "ttl/default")
	if err != nil {
		t.Fatalf("GetBlob default: %v", err)
	}
	if string(b2.Data) != "normal" {
		t.Errorf("default data = %q", b2.Data)
	}
}

func TestNoCacheBucket(t *testing.T) {
	ctx := context.Background()
	bucketDir, err := os.MkdirTemp("", "limpet-blob-nocache-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(bucketDir) })

	bu, err := NewBucket(ctx, bucketDir, &BucketConfig{NoCache: true})
	if err != nil {
		t.Fatalf("NewBucket: %v", err)
	}
	defer bu.Close()

	if err := bu.SetBlob(ctx, "nc/key", []byte("data")); err != nil {
		t.Fatalf("SetBlob: %v", err)
	}
	b, err := bu.GetBlob(ctx, "nc/key")
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
