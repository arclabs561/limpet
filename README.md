# limpet - fetch, cache, and reuse web requests

[![Go package docs](https://pkg.go.dev/badge/github.com/arclabs561/limpet/badge.svg)](https://pkg.go.dev/github.com/arclabs561/limpet)
[![Build status](https://github.com/arclabs561/limpet/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/arclabs561/limpet/actions)

A Go library and CLI for fetching web pages with automatic caching. Supports plain HTTP and headless browser (Playwright/Chromium) requests. Can run as a caching HTTP proxy with HTTPS CONNECT tunneling.

## Features

- **HTTP + headless browser**: fetch pages via standard HTTP or Playwright-driven Chromium
- **Blob storage**: cache fetched pages to local filesystem or S3
- **Deterministic cache keys**: normalized URL+method+headers+body maps to a SHA-256 blob key, with options to exclude headers and query params
- **Conditional requests**: automatic ETag/If-Modified-Since revalidation in Transport avoids re-downloading unchanged content
- **Request deduplication**: concurrent Transport requests for the same URL coalesce via singleflight
- **Version history**: archive timestamped snapshots and diff pages to detect changes
- **Staleness hints**: check HTTP cache headers or time-based age via `Page.Stale()` / `Page.StaleAfter()`
- **Per-request cache TTL**: override the default TTL per request via context
- **Rate limiting**: configurable per-request rate limits with exponential backoff
- **Silent throttle detection**: detect and retry when a site silently serves captcha/block pages
- **HTTP proxy mode**: caching HTTP proxy with HTTPS CONNECT tunneling (SSRF-safe)

## CLI Usage

```sh
# Install
go install github.com/arclabs561/limpet/cmd/limpet@latest

# Fetch a URL (cached on subsequent calls)
limpet do https://example.com

# Force re-fetch, ignoring cache
limpet do -f https://example.com

# Include response headers in output
limpet do -i https://example.com

# Use headless browser (Playwright)
limpet do -B https://example.com

# HEAD request (implies -i)
limpet do -I https://example.com

# Custom HTTP method
limpet do -X POST https://example.com/api

# Run as a caching HTTP proxy
limpet proxy -a localhost:8080

# List cached entries
limpet cache ls

# List cached entries for a specific host
limpet cache ls example.com

# Read a cached page's response body
limpet cache get example.com/abc123.json

# Show page metadata (URL, status, fetch time)
limpet cache get --meta example.com/abc123.json

# Delete a cached entry
limpet cache rm example.com/abc123.json

# Purge all cached entries (or by host prefix)
limpet cache purge
limpet cache purge example.com
```

### Global flags

| Flag | Default | Description |
|------|---------|-------------|
| `-b`, `--bucket-url` | `file://<config>/bucket` | Blob storage URL (`file://` or `s3://`) |
| `--cache-dir` | `<config>/cache` | Local cache directory |
| `--no-cache` | `false` | Disable local caching |
| `--cache-ttl` | `24h` | Cache TTL (`0` or `forever` for no expiry) |
| `-L`, `--log-level` | `fatal` | Log level: `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `-F`, `--log-format` | `auto` | Log format: `auto`, `console` |
| `-c`, `--log-color` | `auto` | Log color: `auto`, `always`, `never` |

## Rate Limiting

Set the `LIMPET_RATE_LIMIT` environment variable:

```sh
LIMPET_RATE_LIMIT=100        # 100 requests/second (default)
LIMPET_RATE_LIMIT=10/1m      # 10 requests/minute
LIMPET_RATE_LIMIT=none       # Unlimited
```

Format: `<count>[/<duration>]`. Duration uses Go syntax (`1s`, `1m`, `1h`).

## Page Schema

Each fetched page is stored as JSON with three sections:

```
Page
+-- Meta
|   +-- Version      (uint16, currently 1)
|   +-- FetchedAt    (timestamp)
|   +-- FetchDur     (duration)
+-- Request
|   +-- URL
|   +-- RedirectedURL (if redirected)
|   +-- Method
|   +-- Header
|   +-- Body
+-- Response
    +-- StatusCode
    +-- ProtoMajor, ProtoMinor
    +-- Header
    +-- Body
    +-- ContentLength
    +-- TransferEncoding
    +-- Trailer
```

## Library Usage

```go
import (
    "context"
    "fmt"

    "github.com/arclabs561/limpet"
    "github.com/arclabs561/limpet/blob"
)

ctx := context.Background()
bucket, _ := blob.NewBucket(ctx, "file:///tmp/limpet-cache", nil)
defer bucket.Close()

cl, _ := limpet.NewClient(ctx, bucket)
defer cl.Close()

// Simple GET with convenience method
page, _ := cl.Get(ctx, "https://example.com")
fmt.Println(string(page.Response.Body))

// Second call returns cached result
page, _ = cl.Get(ctx, "https://example.com")
```

### Client options (construction time)

- `limpet.WithBrowser()` -- always use headless browser
- `limpet.WithChromiumSandbox(false)` -- disable Chromium OS sandbox (for CI containers)
- `limpet.WithRateLimit(10)` -- set programmatic rate limit
- `limpet.WithRequestBodyLimit(10e6)` -- max request body for cache key (default 10 MB, 0 = no limit)
- `limpet.WithResponseBodyLimit(100e6)` -- max response body to cache (default 100 MB, 0 = no limit)
- `limpet.WithIgnoreHeaders("User-Agent", "Accept-Encoding")` -- exclude headers from cache key (different browsers, same cache entry)
- `limpet.WithIgnoreParams("_t", "token", "utm_source")` -- exclude query params from cache key (auth tokens, tracking params)
- `limpet.WithUserAgent("limpet/0.1")` -- default User-Agent header (applied if not already set)
- `limpet.WithCacheStatuses(200, 301, 404)` -- cache non-200 responses (default: 200 only)
- `limpet.WithRetry(limpet.RetryConfig{Attempts: 3, MinWait: 2 * time.Second})` -- configure retry (zero fields keep defaults: 5 attempts, 1s min, 1m max, 1s jitter)

### Per-request options (DoConfig)

```go
page, _ := cl.Do(ctx, req, limpet.DoConfig{
    Replace:        true,                // force re-fetch, bypassing cache
    Browser:        true,                // use headless browser for this request
    Archive:        true,                // store a timestamped snapshot for version history
    SilentThrottle: regexp.MustCompile(`captcha`), // detect and retry throttled responses
    Limiter:        rateLimiter,         // per-request rate limiter
})
```

### Batch fetching

```go
// Fetch multiple URLs concurrently (up to 5 at a time)
err := cl.GetMany(ctx, urls, 5, limpet.DoConfig{}, func(url string, page *limpet.Page, err error) error {
    if err != nil {
        return err // stops remaining fetches
    }
    fmt.Printf("%s: %d bytes\n", url, len(page.Response.Body))
    return nil
})
```

### Error types

`Do` and `Get` return typed errors for non-200 responses and throttling:

```go
page, err := cl.Get(ctx, url)
var statusErr *limpet.StatusError
if errors.As(err, &statusErr) {
    fmt.Printf("HTTP %d\n", statusErr.Page.Response.StatusCode)
}

var throttleErr *limpet.ThrottledError
if errors.As(err, &throttleErr) {
    // site returned a captcha/block page matching SilentThrottle pattern
}
```

### Version history and change detection

```go
// Fetch with archive to build version history
page, _ := cl.Do(ctx, req, limpet.DoConfig{Archive: true, Replace: true})

// List all archived snapshots for a request
versions, _ := cl.Versions(ctx, req)
for _, v := range versions {
    fmt.Printf("%s  %s\n", v.FetchedAt, v.Key)
}

// Read a specific version and compare
old, _ := cl.Version(ctx, versions[0].Key)
new, _ := cl.Version(ctx, versions[len(versions)-1].Key)
diff := limpet.Diff(old, new)
fmt.Printf("changed=%v old_size=%d new_size=%d\n", diff.Changed, diff.OldSize, diff.NewSize)
```

### Staleness

```go
// Check HTTP cache headers (Cache-Control, Expires)
if page.Stale() {
    page, _ = cl.Get(ctx, url, limpet.DoConfig{Replace: true})
}

// Check time-based age (for targets with no cache headers)
if page.StaleAfter(24 * time.Hour) {
    page, _ = cl.Get(ctx, url, limpet.DoConfig{Replace: true})
}
```

### Transport (http.RoundTripper)

For integrating caching into any `http.Client`:

```go
bucket, _ := blob.NewBucket(ctx, "file:///tmp/cache", nil)
defer bucket.Close()

tr := limpet.NewTransport(bucket,
    limpet.TransportWithRateLimit(10),
    limpet.TransportWithRequestBodyLimit(10e6),   // 10 MB (default)
    limpet.TransportWithResponseBodyLimit(100e6), // 100 MB (default)
    limpet.TransportWithIgnoreHeaders("User-Agent", "Accept-Encoding"),
)

client := &http.Client{Transport: tr}

// First call fetches and caches. Second call returns from cache.
resp, _ := client.Get("https://example.com")
// resp.Header.Get("X-Limpet-Source") == "fetch", "cache", or "revalidated"
```

Per-request cache control via context:

```go
// Skip cache read, force fresh fetch (still caches the result)
ctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)

// Bypass cache entirely (no read, no write)
ctx = limpet.WithCachePolicy(ctx, limpet.CachePolicySkip)

// Per-request cache TTL (overrides bucket default)
ctx = limpet.WithCacheTTL(ctx, 7*24*time.Hour) // weekly
```

### Client vs Transport

Use **Transport** when you want transparent caching as a drop-in `http.RoundTripper` for any `http.Client`. Use **Client** when you also need retry with backoff, headless browser rendering, version history, or silent throttle detection. Transport is the caching primitive; Client composes it with higher-level scraping features.

## License

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).
