package limpet

import (
	"context"
	"sync"
)

// GetMany fetches multiple URLs concurrently with the given concurrency limit.
// The callback fn is called for each completed fetch (in arbitrary order).
// Stops early if ctx is cancelled. Returns the first non-nil error from fn,
// or nil if all callbacks return nil.
func (c *Client) GetMany(
	ctx context.Context,
	urls []string,
	concurrency int,
	cfg DoConfig,
	fn func(url string, page *Page, err error) error,
) error {
	if len(urls) == 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, concurrency)

	var (
		mu       sync.Mutex
		firstErr error
	)

	var wg sync.WaitGroup
	for _, u := range urls {
		// Check for early exit before launching more work.
		if ctx.Err() != nil {
			break
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			defer func() { <-sem }()

			page, fetchErr := c.Get(ctx, url, cfg)
			if cbErr := fn(url, page, fetchErr); cbErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = cbErr
				}
				mu.Unlock()
				cancel()
			}
		}(u)
	}

	wg.Wait()
	return firstErr
}
