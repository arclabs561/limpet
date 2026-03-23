package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/badger/v3"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

const defaultCacheTTL = 24 * time.Hour

// Blob.Source values describing where a blob was read from.
const (
	SourceCache  = "cache"
	SourceRemote = "remote"
)

// BucketConfig configures a Bucket.
type BucketConfig struct {
	CacheDir string
	NoCache  bool
	CacheTTL time.Duration // 0 means use default (24h), negative means no expiry
	// Store overrides the default badger-based local cache with a custom
	// KVStore implementation. When set, CacheDir is ignored.
	Store KVStore
}

// zstd encoder/decoder pools to avoid ~1 MB allocation per call.
var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(nil)
			return w
		},
	}
	zstdDecoderPool = sync.Pool{
		New: func() any {
			r, _ := zstd.NewReader(nil)
			return r
		},
	}
)

// ErrClosed is returned when an operation is attempted on a closed Bucket.
var ErrClosed = errors.New("blob: bucket is closed")

// KVStore is an interface for the local cache backend. The default
// implementation uses badger. Alternative implementations (Pebble, bbolt)
// can be provided via BucketConfig.KVStore.
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte, ttl time.Duration) error
	Delete(key []byte) error
	List(prefix []byte) ([]KVEntry, error)
	Close() error
}

// KVEntry represents a key in the KV store with optional metadata.
type KVEntry struct {
	Key       []byte
	Size      int64
	ExpiresAt uint64 // Unix timestamp, 0 = no expiry
}

// Bucket provides two-tier blob storage (remote + local KV cache).
type Bucket struct {
	bucket    *blob.Bucket
	cache     KVStore
	cacheTTL  time.Duration
	stopGC    chan struct{} // closed to stop background GC goroutine
	writeWG   sync.WaitGroup // tracks in-flight async remote writes
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewBucket creates a Bucket backed by the given URL (file:// or s3://).
func NewBucket(
	ctx context.Context,
	bucketURL string,
	cfg *BucketConfig,
) (*Bucket, error) {
	if cfg == nil {
		cfg = &BucketConfig{}
	}
	cacheTTL := defaultCacheTTL
	if cfg.CacheTTL > 0 {
		cacheTTL = cfg.CacheTTL
	} else if cfg.CacheTTL < 0 {
		cacheTTL = 0 // no expiry
	}

	var cache KVStore
	if cfg.Store != nil {
		cache = cfg.Store
	} else if !cfg.NoCache {
		cacheDir := cfg.CacheDir
		if cacheDir == "" {
			base, err := os.UserCacheDir()
			if err != nil {
				return nil, fmt.Errorf("cannot determine cache directory: %w", err)
			}
			cacheDir = filepath.Join(base, "limpet", "blob")
		}
		cacheOpts := badger.DefaultOptions(cacheDir).
			WithLogger(newBadgerLogger(*log.Ctx(ctx)))
		store, err := NewBadgerStore(cacheOpts)
		if err != nil {
			// If another process holds the Badger lock, try read-only mode.
			cacheOpts = cacheOpts.WithReadOnly(true)
			store, err = NewBadgerStore(cacheOpts)
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).
					Str("dir", cacheDir).
					Msg("cannot open badger cache (locked by another process), proceeding without cache")
			} else {
				log.Ctx(ctx).Info().Str("dir", cacheDir).Msg("opened badger cache in read-only mode (locked by another process)")
				cache = store
			}
		} else {
			log.Ctx(ctx).Debug().Str("dir", cacheDir).Msg("opened badger cache")
			cache = store
		}
	}
	bucket, err := newBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	bu := &Bucket{
		bucket:   bucket,
		cache:    cache,
		cacheTTL: cacheTTL,
		stopGC:   make(chan struct{}),
	}
	if cache != nil {
		go bu.runGC()
	}
	return bu, nil
}

// runGC periodically triggers garbage collection on the cache backend.
// Only runs for backends that support it (e.g., BadgerStore).
func (bu *Bucket) runGC() {
	type gcRunner interface{ RunGC(float64) error }
	gc, ok := bu.cache.(gcRunner)
	if !ok {
		return // backend doesn't need GC
	}
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-bu.stopGC:
			return
		case <-ticker.C:
			for gc.RunGC(0.5) == nil {
			}
		}
	}
}

func newBucket(
	ctx context.Context,
	bucketURL string,
) (*blob.Bucket, error) {
	if !strings.Contains(bucketURL, "://") {
		bucketURL = "file://" + bucketURL
	}
	var bucket *blob.Bucket
	var err error
	switch {
	case strings.HasPrefix(bucketURL, "file://"):
		dir := strings.TrimPrefix(bucketURL, "file://")
		bucket, err = fileblob.OpenBucket(dir, &fileblob.Options{
			CreateDir: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open fileblob bucket %s: %w", dir, err)
		}
	case strings.HasPrefix(bucketURL, "s3://"):
		bucketName := strings.TrimRight(strings.TrimPrefix(bucketURL, "s3://"), "/")
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load default config: %w", err)
		}
		client := s3.NewFromConfig(cfg)
		bucket, err = s3blob.OpenBucketV2(ctx, client, bucketName, &s3blob.Options{})
		if err != nil {
			return nil, fmt.Errorf("failed to open s3 bucket %s: %w", bucketName, err)
		}
	default:
		return nil, fmt.Errorf(
			"unsupported bucket-url %s, supported schemes are file, s3",
			bucketURL,
		)
	}
	log.Ctx(ctx).Debug().Str("url", bucketURL).Msg("opened bucket")
	return bucket, nil
}

// Close shuts down the local cache and remote bucket. Safe to call multiple times.
func (bu *Bucket) Close() {
	bu.closed.Store(true)
	bu.writeWG.Wait() // flush in-flight async remote writes
	bu.closeOnce.Do(func() {
		close(bu.stopGC)
		if bu.cache != nil {
			if err := bu.cache.Close(); err != nil {
				log.Err(err).Msg("failed to close cache")
			}
		}
		if bu.bucket != nil {
			if err := bu.bucket.Close(); err != nil {
				log.Err(err).Msg("failed to close bucket")
			}
		}
	})
}

// CacheTTLKey is the context key for cache TTL overrides.
// Exported so callers can check whether a TTL is already set.
type CacheTTLKey = ctxKeyCacheTTL

type ctxKeyCacheTTL struct{}

// WithCacheTTL returns a context that overrides the bucket's default cache TTL
// for writes made with this context. Use 0 for no expiry, or a positive
// duration for a custom TTL.
func WithCacheTTL(ctx context.Context, ttl time.Duration) context.Context {
	return context.WithValue(ctx, ctxKeyCacheTTL{}, ttl)
}

// storageKey appends the .zst suffix to a logical key, producing the
// actual key used in both badger and the remote bucket. All blob operations
// must use this to ensure consistent key mapping.
func storageKey(key string) string { return key + ".zst" }

// compressZstd compresses data using a pooled zstd encoder.
func compressZstd(data []byte) []byte {
	zw := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(zw)
	return zw.EncodeAll(data, nil)
}

// SetBlob writes data to the remote bucket and local cache under the given key.
// Data is zstd-compressed in both tiers. If the context carries a TTL from
// WithCacheTTL, it overrides the bucket default.
func (bu *Bucket) SetBlob(ctx context.Context, key string, data []byte) error {
	if bu.closed.Load() {
		return ErrClosed
	}
	key = storageKey(key)
	compressed := compressZstd(data)

	// Write to local cache first (fast path).
	if bu.cache != nil {
		ttl := bu.cacheTTL
		if override, ok := ctx.Value(ctxKeyCacheTTL{}).(time.Duration); ok {
			ttl = override
		}
		if err := bu.cache.Set([]byte(key), compressed, ttl); err != nil {
			log.Err(err).Msg("failed to set cache")
		}
	}

	// Write to remote bucket. When a local cache exists, the remote write
	// runs asynchronously (local cache is the hot tier). Without a local
	// cache, the remote write is synchronous (it's the only store).
	if bu.bucket != nil {
		writeRemote := func() {
			w, err := bu.bucket.NewWriter(context.Background(), key, nil)
			if err != nil {
				log.Err(err).Str("key", key).Msg("remote write: failed to create writer")
				return
			}
			if _, err := w.Write(compressed); err != nil {
				_ = w.Close()
				log.Err(err).Str("key", key).Msg("remote write: write failed")
				return
			}
			if err := w.Close(); err != nil {
				log.Err(err).Str("key", key).Msg("remote write: close failed")
			}
		}
		if bu.cache != nil {
			bu.writeWG.Add(1)
			go func() {
				defer bu.writeWG.Done()
				writeRemote()
			}()
		} else {
			writeRemote()
		}
	}
	return nil
}

// NotFoundError is returned when a key is not found in the bucket.
type NotFoundError struct {
	Key string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
}

// Blob holds decompressed data and its source ("cache" or "remote").
type Blob struct {
	Data   []byte
	Source string
}

// maxDecompressed caps decompressed data to prevent zip-bomb memory exhaustion.
const maxDecompressed = 500e6 // 500 MB

// decompressZstd decompresses zstd data using a pooled decoder.
// Size is limited to maxDecompressed bytes via streaming decompression.
func decompressZstd(compressed []byte) ([]byte, error) {
	return decompressZstdStream(bytes.NewReader(compressed))
}

// decompressZstdStream decompresses zstd data from a reader using a pooled decoder.
// Returns an error if the decompressed data exceeds maxDecompressed bytes.
func decompressZstdStream(r io.Reader) ([]byte, error) {
	zr := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(zr)
	if err := zr.Reset(r); err != nil {
		return nil, fmt.Errorf("failed to reset zstd reader: %w", err)
	}
	lr := io.LimitReader(zr, int64(maxDecompressed)+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if len(data) > int(maxDecompressed) {
		return nil, fmt.Errorf("decompressed size exceeds limit %d", int(maxDecompressed))
	}
	return data, nil
}

// GetBlob reads data by key, checking the local cache first, then the remote bucket.
// Both tiers store zstd-compressed data; decompression is transparent.
func (bu *Bucket) GetBlob(ctx context.Context, key string) (b *Blob, err error) {
	if bu.closed.Load() {
		return nil, ErrClosed
	}
	if bu.cache == nil && bu.bucket == nil {
		return nil, errors.New("neither cache nor external bucket is configured")
	}

	start := time.Now()
	defer func() {
		if err != nil {
			return
		}
		log.Debug().
			Int("bytes", len(b.Data)).
			Stringer("dur", time.Since(start).Round(time.Microsecond)).
			Str("source", b.Source).
			Str("key", key).
			Msg("bucket read")
	}()
	key = storageKey(key)

	var cacheData []byte
	if bu.cache != nil {
		val, err := bu.cache.Get([]byte(key))
		if err != nil {
			var notFound *NotFoundError
			if !errors.As(err, &notFound) {
				if bu.bucket == nil {
					return nil, fmt.Errorf("failed to read cache: %w", err)
				}
				log.Err(err).Msg("failed to read cache")
			}
		} else {
			cacheData = val
		}
	}
	if cacheData != nil {
		data, err := decompressZstd(cacheData)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress cached data: %w", err)
		}
		return &Blob{Data: data, Source: SourceCache}, nil
	}

	if bu.bucket != nil {
		r, err := bu.bucket.NewReader(ctx, key, nil)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				return nil, &NotFoundError{key}
			}
			return nil, fmt.Errorf("failed to create bucket reader: %w", err)
		}
		data, err := decompressZstdStream(r)
		if closeErr := r.Close(); closeErr != nil && err == nil {
			return nil, fmt.Errorf("failed to close bucket reader: %w", closeErr)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decompress remote data: %w", err)
		}
		// Backfill local cache with compressed data.
		if bu.cache != nil {
			compressed := compressZstd(data)
			if err := bu.cache.Set([]byte(key), compressed, bu.cacheTTL); err != nil {
				log.Err(err).Msg("failed to backfill cache")
			}
		}
		return &Blob{
			Data:   data,
			Source: SourceRemote,
		}, nil
	}

	return nil, &NotFoundError{Key: key}
}

// CacheEntry represents a cached blob key with optional metadata.
type CacheEntry struct {
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	ExpiresAt uint64 `json:"expires_at"` // Unix timestamp, 0 means no expiry
}

// ListCache iterates all keys in the local badger cache.
// Returns nil if cache is disabled.
func (bu *Bucket) ListCache(prefix string) ([]CacheEntry, error) {
	if bu.closed.Load() {
		return nil, ErrClosed
	}
	if bu.cache == nil {
		return nil, nil
	}
	kvEntries, err := bu.cache.List([]byte(prefix))
	if err != nil {
		return nil, err
	}
	entries := make([]CacheEntry, 0, len(kvEntries))
	for _, kv := range kvEntries {
		cleanKey := strings.TrimSuffix(string(kv.Key), ".zst")
		entries = append(entries, CacheEntry{
			Key:       cleanKey,
			Size:      kv.Size,
			ExpiresAt: kv.ExpiresAt,
		})
	}
	return entries, nil
}

// DeleteBlob removes a key from both the remote bucket and local cache.
// Waits for any in-flight async remote writes to complete first.
func (bu *Bucket) DeleteBlob(ctx context.Context, key string) error {
	if bu.closed.Load() {
		return ErrClosed
	}
	bu.writeWG.Wait() // ensure async writes complete before delete
	key = storageKey(key)
	if bu.bucket != nil {
		if err := bu.bucket.Delete(ctx, key); err != nil {
			if gcerrors.Code(err) != gcerrors.NotFound {
				return fmt.Errorf("failed to delete from bucket: %w", err)
			}
		}
	}
	if bu.cache != nil {
		if err := bu.cache.Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete from cache: %w", err)
		}
	}
	return nil
}

// PurgeCache deletes all entries in the local cache matching the given prefix.
// An empty prefix deletes everything. Returns the number of entries deleted.
func (bu *Bucket) PurgeCache(prefix string) (int, error) {
	if bu.closed.Load() {
		return 0, ErrClosed
	}
	bu.writeWG.Wait() // ensure async writes complete before purge
	if bu.cache == nil {
		return 0, nil
	}
	kvEntries, err := bu.cache.List([]byte(prefix))
	if err != nil {
		return 0, fmt.Errorf("failed to list cache keys: %w", err)
	}
	for _, kv := range kvEntries {
		if err := bu.cache.Delete(kv.Key); err != nil {
			return 0, fmt.Errorf("failed to delete key: %w", err)
		}
	}
	return len(kvEntries), nil
}

var _ badger.Logger = (*badgerLogger)(nil)

type badgerLogger struct {
	zerolog.Logger
}

func newBadgerLogger(log zerolog.Logger) badgerLogger {
	log = log.With().
		Str("component", "cache").
		Caller().
		CallerWithSkipFrameCount(5).
		Logger()
	return badgerLogger{log}
}

func (l badgerLogger) Errorf(format string, args ...any) {
	l.log(zerolog.ErrorLevel, format, args)
}

func (l badgerLogger) Warningf(format string, args ...any) {
	l.log(zerolog.WarnLevel, format, args)
}

func (l badgerLogger) Infof(format string, args ...any) {
	l.log(zerolog.DebugLevel, format, args)
}

func (l badgerLogger) Debugf(format string, args ...any) {
	l.log(zerolog.TraceLevel, format, args)
}

func (l badgerLogger) log(lvl zerolog.Level, format string, args []any) {
	format = strings.TrimSpace(format)
	l.Logger.WithLevel(lvl).Msgf(format, args...)
}
