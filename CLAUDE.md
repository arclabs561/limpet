# CLAUDE.md -- limpet

## Module

`github.com/arclabs561/limpet` -- Go library + CLI for fetching web pages with persistent caching, version history, and change detection.

## Build and test

```sh
go build ./...                             # build all
go test ./...                              # unit tests (no external deps)
go test -tags test_all ./...               # integration tests (needs playwright)
go test -tags test_all -run TestDoBrowser   # browser test only
TEST_LIVE_HTTP=true go test -tags test_all  # live HTTP tests
```

## Testing conventions

- Prefer `t.Context()` over `context.Background()` and `t.TempDir()` over `os.MkdirTemp()` in tests (Go 1.24+). Reduces boilerplate and ensures cleanup.
- When upgrading golangci-lint or adding linters, check for deprecation warnings and replace with suggested alternatives.
- Shared `*Page` (from singleflight or cache) must use `Header.Clone()` when constructing `*http.Response` -- concurrent callers will write to the header map.
- TTL-based tests: use 5s+ TTL values to avoid flakiness under race detector.

## Accepted trade-offs

- `TestDoBrowser` fails without playwright binaries. Passes in CI.
- badger v3 (maintenance mode) -- abstracted behind `KVStore` interface; upgrade or swap when motivated.
- gocloud.dev pulls ~15 transitive deps for S3 support. Accepted for now.
- Async remote writes: `SetBlob` writes to local cache synchronously, remote bucket asynchronously. `Close()` and `DeleteBlob` wait for in-flight writes. Trade-off: caller gets success before remote write completes.

## Commit style

`limpet: <verb> <what>` -- one commit per logical change. Verb matches intent: add, fix, drop, rename, refactor.

## API conventions

- Construction options: `With*()` functional options (e.g., `WithBrowser()`, `WithRateLimit()`, `WithPerHostRateLimit()`, `WithAdaptiveRate()`)
- Per-request options: `DoConfig{}` struct. `DoConfig.Replace` is deprecated; use `WithCachePolicy(ctx, CachePolicyReplace)`.
- Per-request context: `WithCachePolicy(ctx, ...)`, `WithCacheTTL(ctx, ...)` -- works with both Client and Transport
- Per-request rate limiter: `DoConfig.Limiter` accepts `*rate.Limiter` from `golang.org/x/time/rate`
- Transport options: `TransportWith*()` (e.g., `TransportWithRateLimit()`, `TransportWithPerHostRateLimit()`)
- Error types: `StatusError` (with `StatusCode()` method), `ThrottledError` (with `URL` field)
- Error helpers: `IsStatus(err, 404, 500)` for status checks without `errors.As` boilerplate
- Cache policy: `CachePolicyDefault`, `CachePolicyReplace`, `CachePolicySkip` via context
- Source constants: `SourceHTTPPlain`, `SourceHTTPStealth`, `SourceHTTPBrowser`, `SourceStale`, `SourceRevalidated` (limpet package). `SourceCache`, `SourceRemote` in `blob` package.
- Cache key: normalized URL (lowercase host, sorted params via `path.Join`, stripped default ports) + method + headers + body

## Key types

- `cacheLayer` -- shared cache logic (read, write, key computation, refresh patterns, stale-if-error), embedded in both Client and Transport
- `Client` -- orchestrator: HTTP/stealth/browser fetch, retry, archive, adaptive rate limiting. Embeds cacheLayer.
- `Transport` -- `http.RoundTripper` with caching, singleflight dedup, conditional requests, stale-while-revalidate. Embeds cacheLayer.
- `blob.Bucket` -- two-tier storage (KVStore local + remote file/S3), async remote writes
- `blob.KVStore` -- interface for local cache backend (Get/Set/Delete/List/Close). Default: `BadgerStore`.
- `Page` -- cached request/response pair with metadata
- `DoConfig` -- per-request options (Browser, Stealth, Archive, SilentThrottle, Limiter)
- `CachePolicy` -- per-request cache behavior via context (Default, Replace, Skip)
- `RetryConfig` -- retry behavior (Attempts, MinWait, MaxWait, Jitter)
- `TransportStatsSnapshot` -- cache hit/miss/revalidation/coalesce counters (read via `Transport.Stats()`)

## CLI flags (do command)

- `-f` force refetch, `-i` include headers, `-I` HEAD request
- `-X METHOD` custom method, `-B` browser, `-S` stealth
- `-H "Key: Value"` custom header (repeatable), `-d "body"` POST data
- `-o file` write response to file, `-j N` concurrency for multi-URL
- `--timeout 30s` request deadline
