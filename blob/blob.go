package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
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

const defaultCacheDir = ".cache/limpet/blob/"
const defaultCacheTTL = 24 * time.Hour

// BucketConfig configures a Bucket.
type BucketConfig struct {
	CacheDir string
	NoCache  bool
	CacheTTL time.Duration // 0 means use default (24h), negative means no expiry
}

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
			cacheDir = defaultCacheDir
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
		err := bu.cache.Update(func(txn *badger.Txn) error {
			entry := badger.NewEntry(bu.cacheKey(key), data).WithDiscard()
			if bu.cacheTTL > 0 {
				entry.WithTTL(bu.cacheTTL)
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

type Blob struct {
	Data   []byte
	Source string
}

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
			item, err := txn.Get(bu.cacheKey(key))
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
				entry := badger.NewEntry(bu.cacheKey(key), data).WithDiscard()
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
	Key       string
	Size      int64
	ExpiresAt uint64 // Unix timestamp, 0 means no expiry
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

func (bu *Bucket) cacheKey(key string) []byte {
	return []byte(key)
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
