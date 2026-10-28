# limpet

[![Go package docs](https://pkg.go.dev/badge/github.com/arclabs561/limpet/badge.svg)](https://pkg.go.dev/github.com/arclabs561/limpet)

Caching HTTP fetcher and proxy.

`limpet` fetches web pages through a Go client, CLI, or `http.RoundTripper`.
Responses are cached on the local filesystem or S3 with deterministic keys.
It can revalidate cached responses, combine concurrent requests for the same
page, and serve stale content when a fetch fails.

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

## Rate limits

Set the `LIMPET_RATE_LIMIT` environment variable:

```sh
export LIMPET_RATE_LIMIT=100    # 100 requests/second (default)
export LIMPET_RATE_LIMIT=10/1m  # 10 requests/minute
export LIMPET_RATE_LIMIT=none   # No rate limit
```

Format: `<count>[/<duration>]`. Duration uses Go syntax (`1s`, `1m`, `1h`).

## Go client

```go
import (
    "context"
    "fmt"

    "github.com/arclabs561/limpet"
    "github.com/arclabs561/limpet/blob"
)

ctx := context.Background()
bucket, err := blob.NewBucket(ctx, "file:///tmp/limpet-cache", nil)
if err != nil {
    panic(err)
}
defer bucket.Close()

cl, err := limpet.NewClient(ctx, bucket)
if err != nil {
    panic(err)
}
defer cl.Close()

page, err := cl.Get(ctx, "https://example.com")
if err != nil {
    panic(err)
}
fmt.Println(string(page.Response.Body))
```

Per-request cache policy is set through context:

```go
refreshCtx := limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
page, err = cl.Get(refreshCtx, "https://example.com")
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

`WithStealth()` (CLI `-S`) sends requests through
[azuretls](https://github.com/Noooste/azuretls-client) with a browser TLS
fingerprint. It addresses TLS/JA3/JA4 fingerprint checks that reject Go's default
handshake; it is not a CAPTCHA or JavaScript-challenge solver.

## Cloudflare, proxies, and limits

- Use `-S` when a site rejects the TLS fingerprint. Use `-B` when the page needs
  JavaScript rendering. They are not interchangeable.
- Cloudflare's own documentation states that automated browsers are not
  supported for solving production challenges. Do not treat `-B` as a Turnstile
  or interstitial bypass. Use the site's official API, an authorized session,
  or a human-completed clearance flow.
- A `cf_clearance` cookie is bound to the visitor/device and rate-limited by the
  site; do not copy it between machines or bake it into a cache.
- Plain HTTP and stealth fetches honor `HTTPS_PROXY` / `HTTP_PROXY`. Browser
  requests are intercepted and fulfilled through the same fetch path, so the
  proxy must be able to reach the target. A proxy returning `504` or an SSL
  hostname mismatch is a transport failure, not a fingerprint problem.
- Respect the target's limits. Limpet's rate limit/cache is not permission to
  rotate proxies or parallelize around an origin limit.

## Transport

```go
bucket, err := blob.NewBucket(ctx, "file:///tmp/cache", nil)
if err != nil {
    panic(err)
}
defer bucket.Close()

tr := limpet.NewTransport(bucket, limpet.TransportWithRateLimit(10))
client := &http.Client{Transport: tr}

resp, err := client.Get("https://example.com")
if err != nil {
    panic(err)
}
defer resp.Body.Close()
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
