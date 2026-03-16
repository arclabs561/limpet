# limpet - fetch, cache, and reuse web requests

[![Go package docs](https://pkg.go.dev/badge/github.com/arclabs561/limpet/badge.svg)](https://pkg.go.dev/github.com/arclabs561/limpet)
[![Build status](https://github.com/arclabs561/limpet/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/arclabs561/limpet/actions)

A Go library and CLI for fetching web pages with automatic caching. Supports plain HTTP and headless browser (Playwright/Chromium) requests. Can run as a caching HTTP proxy with HTTPS CONNECT tunneling.

## Features

- **HTTP + headless browser**: fetch pages via standard HTTP or Playwright-driven Chromium
- **Blob storage**: cache fetched pages to local filesystem or S3
- **Request deduplication**: same URL+method+headers+body maps to a deterministic blob key (SHA-256)
- **Rate limiting**: configurable per-request rate limits with exponential backoff
- **Silent throttle detection**: detect and retry when a site silently serves captcha/block pages
- **HTTP proxy mode**: expose the fetcher as a caching HTTP proxy with HTTPS CONNECT tunneling

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
    +-- Header
    +-- Body
    +-- ContentLength
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

### Options

- `limpet.OptAlwaysBrowser()` - always use headless browser
- `limpet.OptRateLimit(10)` - set programmatic rate limit
- `&limpet.OptDoReplace{}` - force re-fetch, bypassing cache
- `&limpet.OptDoBrowser{}` - use browser for this request
- `&limpet.OptDoSilentThrottle{PageBytesRegexp: re}` - detect throttled responses
- `&limpet.OptDoLimiter{Limiter: lim}` - per-request rate limiter

### Transport (http.RoundTripper)

For integrating caching into any `http.Client`:

```go
bucket, _ := blob.NewBucket(ctx, "file:///tmp/cache", nil)
defer bucket.Close()

tr := limpet.NewTransport(bucket,
    limpet.TransportOptRateLimit(10),
)

client := &http.Client{Transport: tr}

// First call fetches and caches. Second call returns from cache.
resp, _ := client.Get("https://example.com")
// resp.Header.Get("X-Limpet-Source") == "fetch" or "cache"
```

Per-request cache control via context:

```go
// Skip cache read, force fresh fetch (still caches the result)
ctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)

// Bypass cache entirely (no read, no write)
ctx = limpet.WithCachePolicy(ctx, limpet.CachePolicySkip)
```

## License

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).
