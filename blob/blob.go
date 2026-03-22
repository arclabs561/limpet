package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// BucketConfig configures a Bucket.
type BucketConfig struct {
	CacheDir string
	NoCache  bool
	CacheTTL time.Duration // 0 means use default (24h), negative means no expiry
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

// Bucket provides two-tier blob storage (remote + local badger cache).
type Bucket struct {
	bucket    *blob.Bucket
	cache     *badger.DB
	cacheTTL  time.Duration
	stopGC    chan struct{} // closed to stop background GC goroutine
	closeOnce sync.Once
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

	var cache *badger.DB
	if !cfg.NoCache {
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
		var err error
		cache, err = badger.Open(cacheOpts)
		if err != nil {
			// If another process holds the Badger lock, try read-only mode.
			// If that also fails, proceed without cache (degrade gracefully).
			cacheOpts = cacheOpts.WithReadOnly(true)
			cache, err = badger.Open(cacheOpts)
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).
					Str("dir", cacheDir).
					Msg("cannot open badger cache (locked by another process), proceeding without cache")
				cache = nil
			} else {
				log.Ctx(ctx).Info().Str("dir", cacheDir).Msg("opened badger cache in read-only mode (locked by another process)")
			}
		} else {
			log.Ctx(ctx).Debug().Str("dir", cacheDir).Msg("opened badger cache")
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

// runGC periodically triggers badger value log garbage collection.
// Without this, the value log grows unbounded as entries expire or are deleted.
func (bu *Bucket) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-bu.stopGC:
			return
		case <-ticker.C:
			// Run GC until it reports less than 50% space could be reclaimed.
			for {
				if bu.cache.RunValueLogGC(0.5) != nil {
					break
				}
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
	key += ".zst"
	compressed := compressZstd(data)

	if bu.bucket != nil {
		w, err := bu.bucket.NewWriter(ctx, key, nil)
		if err != nil {
			return fmt.Errorf("failed to create bucket writer: %w", err)
		}
		n, err := w.Write(compressed)
		if err != nil {
			_ = w.Close()
			return err
		}
		if n < len(compressed) {
			_ = w.Close()
			return fmt.Errorf("violation of io.Writer interface: %d < %d", n, len(compressed))
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close bucket writer: %w", err)
		}
	}
	if bu.cache != nil {
		ttl := bu.cacheTTL
		if override, ok := ctx.Value(ctxKeyCacheTTL{}).(time.Duration); ok {
			ttl = override
		}
		err := bu.cache.Update(func(txn *badger.Txn) error {
			entry := badger.NewEntry([]byte(key), compressed).WithDiscard()
			if ttl > 0 {
				entry.WithTTL(ttl)
			}
			return txn.SetEntry(entry)
		})
		if err != nil {
			log.Err(err).Msg("failed to set cache")
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

// decompressZstd decompresses zstd data using a pooled decoder.
func decompressZstd(compressed []byte) ([]byte, error) {
	zr := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(zr)
	// Cap decompressed size to prevent zip-bomb style memory exhaustion.
	const maxDecompressed = 500e6 // 500 MB
	data, err := zr.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	if len(data) > int(maxDecompressed) {
		return nil, fmt.Errorf("decompressed size %d exceeds limit %d", len(data), int(maxDecompressed))
	}
	return data, nil
}

// decompressZstdStream decompresses zstd data from a reader using a pooled decoder.
func decompressZstdStream(r io.Reader) ([]byte, error) {
	zr := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(zr)
	if err := zr.Reset(r); err != nil {
		return nil, fmt.Errorf("failed to reset zstd reader: %w", err)
	}
	const maxDecompressed = 500e6 // 500 MB
	data, err := io.ReadAll(io.LimitReader(zr, maxDecompressed))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetBlob reads data by key, checking the local cache first, then the remote bucket.
// Both tiers store zstd-compressed data; decompression is transparent.
func (bu *Bucket) GetBlob(ctx context.Context, key string) (b *Blob, err error) {
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
	key = key + ".zst"

	var cacheData []byte
	if bu.cache != nil {
		err := bu.cache.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err == nil {
				cacheData, err = item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return nil
			}
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &NotFoundError{Key: key}
			}
			return nil
		})
		var notFound *NotFoundError
		if err != nil && !errors.As(err, &notFound) {
			if bu.bucket == nil {
				return nil, fmt.Errorf("failed to read cache: %w", err)
			}
			log.Err(err).Msg("failed to read cache")
		}
	}
	if cacheData != nil {
		data, err := decompressZstd(cacheData)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress cached data: %w", err)
		}
		return &Blob{Data: data, Source: "cache"}, nil
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
			cacheErr := bu.cache.Update(func(txn *badger.Txn) error {
				entry := badger.NewEntry([]byte(key), compressed).WithDiscard()
				if bu.cacheTTL > 0 {
					entry.WithTTL(bu.cacheTTL)
				}
				return txn.SetEntry(entry)
			})
			if cacheErr != nil {
				log.Err(cacheErr).Msg("failed to set cache")
			}
		}
		return &Blob{
			Data:   data,
			Source: "remote",
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
	if bu.cache == nil {
		return nil, nil
	}
	var entries []CacheEntry
	err := bu.cache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		pfx := []byte(prefix)
		for it.Seek(pfx); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				break
			}
			// Strip .zst suffix so callers can pass keys to GetBlob without double-suffixing.
			cleanKey := strings.TrimSuffix(key, ".zst")
			entries = append(entries, CacheEntry{
				Key:       cleanKey,
				Size:      item.ValueSize(),
				ExpiresAt: item.ExpiresAt(),
			})
		}
		return nil
	})
	return entries, err
}

// DeleteBlob removes a key from both the remote bucket and local cache.
func (bu *Bucket) DeleteBlob(ctx context.Context, key string) error {
	key += ".zst"
	if bu.bucket != nil {
		if err := bu.bucket.Delete(ctx, key); err != nil {
			if gcerrors.Code(err) != gcerrors.NotFound {
				return fmt.Errorf("failed to delete from bucket: %w", err)
			}
		}
	}
	if bu.cache != nil {
		err := bu.cache.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte(key))
		})
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("failed to delete from cache: %w", err)
		}
	}
	return nil
}

// PurgeCache deletes all entries in the local cache matching the given prefix.
// An empty prefix deletes everything. Returns the number of entries deleted.
func (bu *Bucket) PurgeCache(prefix string) (int, error) {
	if bu.cache == nil {
		return 0, nil
	}
	var keys [][]byte
	err := bu.cache.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte(prefix)
		for it.Seek(pfx); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if prefix != "" && !strings.HasPrefix(string(key), prefix) {
				break
			}
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to list cache keys: %w", err)
	}
	// Use WriteBatch for efficient bulk deletion.
	wb := bu.cache.NewWriteBatch()
	for _, k := range keys {
		if err := wb.Delete(k); err != nil {
			wb.Cancel()
			return 0, fmt.Errorf("failed to batch delete key: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush batch delete: %w", err)
	}
	return len(keys), nil
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
