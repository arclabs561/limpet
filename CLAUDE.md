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
- Per-request options: `DoConfig{}` struct (e.g., `cl.Do(ctx, req, DoConfig{Replace: true})`). `Do()` takes a single `DoConfig`; `Get()` accepts variadic for convenience.
- Per-request context: `WithCachePolicy(ctx, ...)`, `WithCacheTTL(ctx, ...)` -- works with both Client and Transport
- Transport options: `TransportWith*()` (e.g., `TransportWithRateLimit()`, `TransportWithIgnoreHeaders()`)
- Error types: `StatusError`, `ThrottledError` (no `Fetch` prefix)
- Cache policy: `CachePolicyDefault`, `CachePolicyReplace`, `CachePolicySkip` via context. `DoConfig.Replace` is equivalent to `CachePolicyReplace`.
- Source constants: `SourceHTTPPlain`, `SourceHTTPStealth`, `SourceHTTPBrowser`, `SourceCache`, `SourceRemote`, `SourceFetch`, `SourceStale`, `SourceRevalidated`
- Cache key: normalized URL (lowercase host, sorted params, stripped default ports) + method + headers + body, with exclusion options

## Key types

- `cacheLayer` -- shared cache logic (read, write, key computation, refresh patterns, stale-if-error), a named field in both Client and Transport
- `Client` -- orchestrator: HTTP/stealth/browser fetch, retry, archive. Embeds cacheLayer.
- `Transport` -- `http.RoundTripper` with caching, singleflight dedup, conditional requests. Embeds cacheLayer.
- `blob.Bucket` -- two-tier storage (badger L1 + remote file/S3), with eviction and purge
- `Page` -- cached request/response pair with metadata
- `DoConfig` -- per-request options (Replace, Browser, Stealth, Archive, SilentThrottle, Limiter)
- `CachePolicy` -- per-request cache behavior via context (Default, Replace, Skip). Works with both Client and Transport.
- `RetryConfig` -- retry behavior (Attempts, MinWait, MaxWait, Jitter)
- `TransportStatsSnapshot` -- cache hit/miss/revalidation/coalesce counters (read via `Transport.Stats()`)
