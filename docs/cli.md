# limpet CLI

Install:

```sh
go install github.com/arclabs561/limpet/cmd/limpet@latest
```

Fetch:

```sh
limpet do https://example.com
limpet do -f https://example.com
limpet do -i https://example.com
limpet do -I https://example.com
limpet do -X POST https://example.com/api
limpet do -H "Authorization: Bearer token" -H "Accept: application/json" https://api.example.com
limpet do -d '{"key":"value"}' https://api.example.com/data
limpet do --timeout 10s https://slow.example.com
limpet do -o page.html https://example.com
```

Fetch multiple URLs:

```sh
limpet do https://example.com https://example.org https://example.net
limpet do -j 8 url1 url2 url3
```

Browser-backed transports:

```sh
limpet do -S https://example.com
limpet do -B https://example.com
```

Proxy:

```sh
limpet proxy -a localhost:8080
limpet proxy --allow-private
```

Cache:

```sh
limpet cache ls
limpet cache ls --json
limpet cache ls --keys-only
limpet cache ls example.com
limpet cache get example.com/abc123.json
limpet cache get -i example.com/abc123.json
limpet cache get --meta example.com/abc123.json
limpet cache rm example.com/abc123.json
limpet cache purge
limpet cache purge example.com
```

Global flags include `--bucket-url` (`file://` or `s3://`), `--cache-dir`,
`--no-cache`, `--cache-ttl`, and `--log-level`.
