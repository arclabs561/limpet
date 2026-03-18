package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// Bucket provides two-tier blob storage (remote + local badger cache).
type Bucket struct {
	bucket   *blob.Bucket
	cache    *badger.DB
	cacheTTL time.Duration
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
			return nil, err
		}
		log.Ctx(ctx).Debug().Str("dir", cacheDir).Msg("opened badger cache")
	}
	bucket, err := newBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		bucket:   bucket,
		cache:    cache,
		cacheTTL: cacheTTL,
	}, nil
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

// Close shuts down the local cache and remote bucket.
func (bu *Bucket) Close() {
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
}

type ctxKeyCacheTTL struct{}

// WithCacheTTL returns a context that overrides the bucket's default cache TTL
// for writes made with this context. Use 0 for no expiry, or a positive
// duration for a custom TTL.
func WithCacheTTL(ctx context.Context, ttl time.Duration) context.Context {
	return context.WithValue(ctx, ctxKeyCacheTTL{}, ttl)
}

// SetBlob writes data to the remote bucket and local cache under the given key.
// If the context carries a TTL from WithCacheTTL, it overrides the bucket default.
func (bu *Bucket) SetBlob(ctx context.Context, key string, data []byte) error {
	key += ".zst"
	if bu.bucket != nil {
		w, err := bu.bucket.NewWriter(ctx, key, nil)
		if err != nil {
			return fmt.Errorf("failed to create bucket writer: %w", err)
		}
		zw, err := zstd.NewWriter(w)
		if err != nil {
			_ = w.Close()
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
		n, err := zw.Write(data)
		if err != nil {
			_ = zw.Close()
			_ = w.Close()
			return err
		}
		if n < len(data) {
			_ = zw.Close()
			_ = w.Close()
			return fmt.Errorf("violation of io.Writer interface: %d < %d", n, len(data))
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("failed to close zstd writer: %w", err)
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
			entry := badger.NewEntry([]byte(key), data).WithDiscard()
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

// GetBlob reads data by key, checking the local cache first, then the remote bucket.
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
		return &Blob{Data: cacheData, Source: "cache"}, nil
	}

	if bu.bucket != nil {
		r, err := bu.bucket.NewReader(ctx, key, nil)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				return nil, &NotFoundError{key}
			}
			return nil, fmt.Errorf("failed to create bucket reader: %w", err)
		}
		zr, err := zstd.NewReader(r)
		if err != nil {
			_ = r.Close()
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		// Cap decompressed size to prevent zip-bomb style memory exhaustion.
		const maxDecompressed = 500e6 // 500 MB
		data, err := io.ReadAll(io.LimitReader(zr, maxDecompressed))
		if err != nil {
			zr.Close()
			_ = r.Close()
			return nil, err
		}
		zr.Close()
		if err := r.Close(); err != nil {
			return nil, fmt.Errorf("failed to close bucket reader: %w", err)
		}
		if cacheData == nil && bu.cache != nil {
			err := bu.cache.Update(func(txn *badger.Txn) error {
				entry := badger.NewEntry([]byte(key), data).WithDiscard()
				if bu.cacheTTL > 0 {
					entry.WithTTL(bu.cacheTTL)
				}
				return txn.SetEntry(entry)
			})
			if err != nil {
				log.Err(err).Msg("failed to set cache")
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
	deleted := 0
	for _, k := range keys {
		err := bu.cache.Update(func(txn *badger.Txn) error {
			return txn.Delete(k)
		})
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return deleted, fmt.Errorf("failed to delete key: %w", err)
		}
		deleted++
	}
	return deleted, nil
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
