package limpet

import (
	"regexp"
	"time"
)

// RefreshPattern maps a URL pattern to a cache TTL. When writing to cache,
// the first matching pattern determines the TTL. This is analogous to Squid's
// refresh_pattern directive.
type RefreshPattern struct {
	// Pattern is matched against the full request URL.
	Pattern *regexp.Regexp
	// MaxAge is the cache TTL for matching URLs. 0 means no expiry.
	MaxAge time.Duration
}

// matchRefreshTTL returns the TTL from the first matching RefreshPattern,
// or (0, false) if no pattern matches.
func matchRefreshTTL(patterns []RefreshPattern, url string) (time.Duration, bool) {
	for _, p := range patterns {
		if p.Pattern.MatchString(url) {
			return p.MaxAge, true
		}
	}
	return 0, false
}
