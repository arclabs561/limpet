# scraper - fetch, cache, and scrape web pages

[![Go package docs](https://pkg.go.dev/badge/github.com/henrywallace/scraper/badge.svg)](https://pkg.go.dev/github.com/henrywallace/scraper)
[![Build status](https://github.com/henrywallace/scraper/actions/workflows/main.yml/badge.svg?branch=main&event=push)](https://github.com/henrywallace/scraper/actions)

A Go library and CLI for fetching web pages with automatic caching and deduplication. Supports plain HTTP and headless browser (Playwright/Chromium) requests.

## Features

- **HTTP + headless browser**: fetch pages via standard HTTP or Playwright-driven Chromium
- **Blob storage**: cache fetched pages to local filesystem or S3
- **Request deduplication**: same URL+method+headers+body maps to a deterministic blob key (SHA-256)
- **Rate limiting**: configurable per-request rate limits with exponential backoff
- **Silent throttle detection**: detect and retry when a site silently serves captcha/block pages
- **TCP proxy mode**: expose the scraper as a transparent HTTP proxy

## CLI Usage

```sh
# Install
go install github.com/henrywallace/scraper/cmd/scraper@latest

# Fetch a URL (cached on subsequent calls)
scraper do https://example.com

# Force re-fetch, ignoring cache
scraper do -f https://example.com

# Include response headers in output
scraper do -i https://example.com

# Use headless browser (Playwright)
scraper do -B https://example.com

# HEAD request (implies -i)
scraper do -I https://example.com

# Custom HTTP method
scraper do -X POST https://example.com/api

# Run as a TCP proxy
scraper proxy -a localhost:8080
```

### Global flags

| Flag | Default | Description |
|------|---------|-------------|
| `-b`, `--bucket-url` | `file://<config>/bucket` | Blob storage URL (`file://` or `s3://`) |
| `--cache-dir` | `<config>/cache` | Local cache directory |
| `--no-cache` | `false` | Disable caching |
| `-L`, `--log-level` | `fatal` | Log level: `trace`, `debug`, `info`, `warn`, `error`, `fatal` |
| `-F`, `--log-format` | `auto` | Log format: `auto`, `console` |

## Rate Limiting

Set the `SCRAPER_RATE_LIMIT` environment variable:

```sh
SCRAPER_RATE_LIMIT=100        # 100 requests/second (default)
SCRAPER_RATE_LIMIT=10/1m      # 10 requests/minute
SCRAPER_RATE_LIMIT=none       # Unlimited
```

Format: `<count>[/<duration>]`. Duration uses Go syntax (`1s`, `1m`, `1h`).

## Page Schema

Each fetched page is stored as JSON with three sections:

```
Page
├── Meta
│   ├── Version      (uint16, currently 1)
│   ├── FetchedAt    (timestamp)
│   └── FetchDur     (duration)
├── Request
│   ├── URL
│   ├── RedirectedURL (if redirected)
│   ├── Method
│   ├── Header
│   └── Body
└── Response
    ├── StatusCode
    ├── Header
    ├── Body
    └── ContentLength
```

## Library Usage

```go
import (
    "context"
    "net/http"

    "github.com/henrywallace/scraper"
    "github.com/henrywallace/scraper/blob"
)

bucket, _ := blob.NewBucket(ctx, "file:///tmp/scraper-cache")
sc, _ := scraper.NewScraper(ctx, bucket)
defer sc.Close()

req, _ := http.NewRequest("GET", "https://example.com", nil)
page, _ := sc.Do(ctx, req)
// page.Response.Body contains the page content
// Second call with same request returns cached result
```

### Options

- `scraper.OptScraperAlwaysDoBrowser()` - always use headless browser
- `&scraper.OptDoReplace{}` - force re-fetch, bypassing cache
- `&scraper.OptDoBrowser{}` - use browser for this request
- `&scraper.OptDoSilentThrottle{PageBytesRegexp: re}` - detect throttled responses
- `&scraper.OptDoLimiter{Limiter: lim}` - per-request rate limiter

## License

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).
