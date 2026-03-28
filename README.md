# limpet - fetch, cache, and reuse web requests

[![Go package docs](https://pkg.go.dev/badge/github.com/arclabs561/limpet/badge.svg)](https://pkg.go.dev/github.com/arclabs561/limpet)
[![Build status](https://github.com/arclabs561/limpet/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/arclabs561/limpet/actions)

A Go library and CLI for fetching web pages with automatic caching. Supports plain HTTP, stealth transport (browser TLS fingerprint for Cloudflare bypass), and headless browser (Playwright/Chromium) requests. Can run as a caching HTTP proxy with HTTPS CONNECT tunneling.

Three transport modes: plain HTTP, stealth (browser TLS fingerprint for Cloudflare bypass), and headless browser (Playwright/Chromium). Caches to local filesystem or S3 with deterministic SHA-256 keys. Handles conditional requests, singleflight dedup, per-host and adaptive rate limiting, stale-while-revalidate (RFC 5861), version history, and silent throttle detection. Can run as a caching HTTP proxy with HTTPS CONNECT tunneling.

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

# Use stealth transport (browser TLS fingerprint, bypasses Cloudflare)
limpet do -S https://example.com

# Use headless browser (Playwright)
limpet do -B https://example.com

# HEAD request (implies -i)
limpet do -I https://example.com

# Custom HTTP method
limpet do -X POST https://example.com/api

# Custom headers
limpet do -H "Authorization: Bearer token" -H "Accept: application/json" https://api.example.com

# POST with body
limpet do -d '{"key":"value"}' https://api.example.com/data

# Request timeout
limpet do --timeout 10s https://slow.example.com

# Fetch multiple URLs concurrently
limpet do https://example.com https://example.org https://example.net

# Control concurrency (default: 4)
limpet do -j 8 url1 url2 url3 ...

# Write response body to file
limpet do -o page.html https://example.com

# Run as a caching HTTP proxy
limpet proxy -a localhost:8080

# Allow proxying to private/loopback addresses (dev use)
limpet proxy --allow-private

# List cached entries (shows URL, status, TTL)
limpet cache ls

# List as JSON (includes URL and status)
limpet cache ls --json

# Fast listing (keys only, skips reading page metadata)
limpet cache ls --keys-only

# List cached entries for a specific host
limpet cache ls example.com

# Read a cached page's response body
limpet cache get example.com/abc123.json

# Include response headers
limpet cache get -i example.com/abc123.json

# Show page metadata (URL, status, fetch time)
limpet cache get --meta example.com/abc123.json

# Delete a cached entry
limpet cache rm example.com/abc123.json

# Purge all cached entries (or by host prefix)
limpet cache purge
limpet cache purge example.com
```

Global flags: `--bucket-url` (blob storage, `file://` or `s3://`), `--cache-dir`, `--no-cache`, `--cache-ttl` (default 24h), `--log-level`. Run `limpet --help` for the full list.

## Rate Limiting

Set the `LIMPET_RATE_LIMIT` environment variable:

```sh
LIMPET_RATE_LIMIT=100        # 100 requests/second (default)
LIMPET_RATE_LIMIT=10/1m      # 10 requests/minute
LIMPET_RATE_LIMIT=none       # Unlimited
```

Format: `<count>[/<duration>]`. Duration uses Go syntax (`1s`, `1m`, `1h`).

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

Client options are set at construction time via `With*` functions: `WithBrowser()`, `WithStealth()`, `WithRateLimit(n)`, `WithPerHostRateLimit(n)`, `WithAdaptiveRate(true)`, `WithRetry(...)`, `WithStaleIfError(true)`, and others. See [pkg.go.dev](https://pkg.go.dev/github.com/arclabs561/limpet) for the full list.

### Per-request options (DoConfig)

```go
page, _ := cl.Do(ctx, req, limpet.DoConfig{
    Browser:        true,                // use headless browser for this request
    Stealth:        true,                // use stealth transport (mutually exclusive with Browser)
    Archive:        true,                // store a timestamped snapshot for version history
    SilentThrottle: regexp.MustCompile(`captcha`), // detect and retry throttled responses
    Limiter:        rate.NewLimiter(rate.Limit(2), 1), // per-request rate limiter
})
```

Client honors `CachePolicy` from context (same as Transport):

```go
// Force re-fetch, bypassing cache read (still caches result)
ctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
page, _ := cl.Do(ctx, req, limpet.DoConfig{})
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

// Convenience helper for status checks:
if limpet.IsStatus(err, 404) {
    fmt.Println("not found")
}

// Or access the full response via errors.As:
var statusErr *limpet.StatusError
if errors.As(err, &statusErr) {
    fmt.Printf("HTTP %d, body: %s\n", statusErr.StatusCode(), statusErr.Page.Response.Body)
}

var throttleErr *limpet.ThrottledError
if errors.As(err, &throttleErr) {
    fmt.Printf("throttled: %s\n", throttleErr.URL)
}
```

### Version history and change detection

```go
// Fetch with archive to build version history
rctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
page, _ := cl.Do(rctx, req, limpet.DoConfig{Archive: true})

// List all archived snapshots for a request
versions, _ := cl.Versions(ctx, req)
for _, v := range versions {
    fmt.Printf("%s  %s\n", v.FetchedAt, v.Key)
}

// Read a specific version
old, _ := cl.Version(ctx, versions[0].Key)
fmt.Printf("fetched=%s size=%d\n", old.Meta.FetchedAt, len(old.Response.Body))
```

### Staleness

```go
// Check HTTP cache headers (Cache-Control, Expires)
if page.Stale() {
    rctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
    page, _ = cl.Get(rctx, url)
}

// Check time-based age (for targets with no cache headers)
if page.StaleAfter(24 * time.Hour) {
    rctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
    page, _ = cl.Get(rctx, url)
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
    limpet.TransportWithIgnoreParams("_t", "utm_source"),
    limpet.TransportWithCacheStatuses(200, 301, 404),
    limpet.TransportWithUserAgent("mybot/1.0"),
    limpet.TransportWithRefreshPatterns(/* ... */),
    limpet.TransportWithStaleIfError(true),
    limpet.TransportWithPerHostRateLimit(2), // 2 req/s per domain
)

client := &http.Client{Transport: tr}

// First call fetches and caches. Second call returns from cache.
resp, _ := client.Get("https://example.com")
// resp.Header.Get("X-Limpet-Source") == "fetch", "cache", "remote", "revalidated", or "stale"
```

Cache performance counters:

```go
stats := tr.Stats()
fmt.Printf("hits=%d misses=%d revalidated=%d coalesced=%d stale=%d\n",
    stats.Hits, stats.Misses, stats.Revalidated, stats.Coalesced, stats.StaleServed)
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

Transport also supports `stale-while-revalidate` (RFC 5861): when a cached response has `Cache-Control: stale-while-revalidate=N` and is stale but within the revalidation window, Transport serves the stale response immediately and refreshes in the background.

Use **Transport** for transparent caching as an `http.RoundTripper`. Use **Client** when you also need retry, headless browser/stealth, version history, or adaptive rate limiting. Both share the same cache logic and honor `CachePolicy` from context. The local cache defaults to badger; replace it by implementing `blob.KVStore`.

## License

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).
