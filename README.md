# limpet

[![Go package docs](https://pkg.go.dev/badge/github.com/arclabs561/limpet/badge.svg)](https://pkg.go.dev/github.com/arclabs561/limpet)
[![Build status](https://github.com/arclabs561/limpet/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/arclabs561/limpet/actions)

Caching HTTP fetcher and proxy.

`limpet` fetches web pages through a Go client, CLI, or `http.RoundTripper`.
Responses are cached on the local filesystem or S3 with deterministic keys.
Conditional requests, request coalescing, rate limits, version history, and
stale-if-error are built into the fetch path.

## Install

```sh
go install github.com/arclabs561/limpet/cmd/limpet@latest
```

## CLI

```sh
limpet do https://example.com
limpet do -f https://example.com
limpet do -i https://example.com
limpet cache ls
limpet cache get example.com/abc123.json
limpet proxy -a localhost:8080
```

Global flags include `--bucket-url`, `--cache-dir`, `--no-cache`,
`--cache-ttl`, and `--log-level`. See [docs/cli.md](docs/cli.md) or
`limpet --help` for the full command list.

## Rate Limits

Set the `LIMPET_RATE_LIMIT` environment variable:

```sh
LIMPET_RATE_LIMIT=100        # 100 requests/second (default)
LIMPET_RATE_LIMIT=10/1m      # 10 requests/minute
LIMPET_RATE_LIMIT=none       # Unlimited
```

Format: `<count>[/<duration>]`. Duration uses Go syntax (`1s`, `1m`, `1h`).

## Go Client

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

page, _ := cl.Get(ctx, "https://example.com")
fmt.Println(string(page.Response.Body))
```

Per-request cache policy is set through context:

```go
ctx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
page, _ := cl.Do(ctx, req, limpet.DoConfig{})
```

`Do` and `Get` return typed errors for non-200 responses and throttling.

```go
page, err := cl.Get(ctx, url)
if limpet.IsStatus(err, 404) {
    fmt.Println("not found")
}
```

Client options are set at construction time via `With*` functions:
`WithBrowser()`, `WithStealth()`, `WithRateLimit(n)`,
`WithPerHostRateLimit(n)`, `WithAdaptiveRate(true)`, `WithRetry(...)`, and
`WithStaleIfError(true)`. See [pkg.go.dev](https://pkg.go.dev/github.com/arclabs561/limpet)
for the full API.

## Transport

```go
bucket, _ := blob.NewBucket(ctx, "file:///tmp/cache", nil)
defer bucket.Close()

tr := limpet.NewTransport(bucket, limpet.TransportWithRateLimit(10))
client := &http.Client{Transport: tr}

resp, _ := client.Get("https://example.com")
// resp.Header.Get("X-Limpet-Source") is "fetch", "cache", "revalidated", or "stale".
```

Use `Transport` for transparent caching behind any `http.Client`. Use `Client`
when you need retry, browser-backed fetches, version history, or adaptive rate
limiting.

## Scope

`limpet` is a fetch/cache layer. It is not an HTML parser, crawler, or browser
automation framework. Browser-backed fetches are available for pages that need
Chromium rendering.

## License

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).
