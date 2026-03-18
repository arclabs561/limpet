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

## Accepted trade-offs

- `TestDoBrowser` fails without playwright binaries. Passes in CI.
- badger v3 (maintenance mode) -- upgrade to v4 when motivated.
- gocloud.dev pulls ~15 transitive deps for S3 support. Accepted for now.
- `filepath.Join` in blob keys -- OS-dependent separator. Fine for single-OS use.

## Commit style

`limpet: <verb> <what>` -- one commit per logical change. Verb matches intent: add, fix, drop, rename, refactor.

## API conventions

- Construction options: `With*()` functional options (e.g., `WithBrowser()`, `WithRateLimit()`, `WithIgnoreHeaders()`)
- Per-request options: `DoConfig{}` struct (e.g., `DoConfig{Replace: true, Archive: true}`)
- Per-request context: `WithCachePolicy(ctx, ...)`, `WithCacheTTL(ctx, ...)`
- Transport options: `TransportWith*()` (e.g., `TransportWithRateLimit()`, `TransportWithIgnoreHeaders()`)
- Error types: `StatusError`, `ThrottledError` (no `Fetch` prefix)
- Cache policy: `CachePolicyDefault`, `CachePolicyReplace`, `CachePolicySkip` via context
- Cache key: normalized URL (lowercase host, sorted params, stripped default ports) + method + headers + body, with exclusion options

## Key types

- `Client` -- orchestrator: HTTP fetch, browser, retry, cache read/write, archive
- `Transport` -- `http.RoundTripper` with caching, singleflight dedup, conditional requests (standalone, no retry/browser)
- `blob.Bucket` -- two-tier storage (badger L1 + remote file/S3), with eviction and purge
- `Page` -- cached request/response pair with metadata
- `DoConfig` -- per-request options (Replace, Browser, Archive, SilentThrottle, Limiter)
- `RetryConfig` -- retry behavior (Attempts, MinWait, MaxWait, Jitter)
- `TransportStats` / `TransportStatsSnapshot` -- cache hit/miss/revalidation/coalesce counters
