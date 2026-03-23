package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/arclabs561/limpet"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var doCmd = &cobra.Command{
	Use:   "do [flags] url [url...]",
	Short: "Fetch the given url(s)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("requires at least one url")
		}
		return nil
	},
	RunE: doRunE,
}

func init() {
	doCmd.Flags().BoolP(
		"browser",
		"B",
		false,
		"whether to use browser automation",
	)
	doCmd.Flags().BoolP(
		"stealth",
		"S",
		false,
		"use stealth transport (browser TLS fingerprint, bypasses Cloudflare)",
	)
	doCmd.Flags().StringP(
		"method",
		"X",
		"GET",
		"HTTP method",
	)
	doCmd.Flags().BoolP(
		"force-refetch",
		"f",
		false,
		"whether to force refetch",
	)
	doCmd.Flags().BoolP(
		"include",
		"i",
		false,
		"include response headers in the output",
	)
	doCmd.Flags().BoolP(
		"head",
		"I",
		false,
		"send HEAD request, implies -i",
	)
	doCmd.Flags().StringP(
		"output",
		"o",
		"",
		"write response body to file instead of stdout",
	)
	doCmd.Flags().IntP(
		"concurrency",
		"j",
		4,
		"max concurrent fetches (when multiple URLs given)",
	)
}

func doRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cl, bucket, err := newClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer cl.Close()
	defer bucket.Close()

	method := mustFlagString(cmd, "method")
	browser := mustFlagBool(cmd, "browser")
	stealth := mustFlagBool(cmd, "stealth")
	forceRefetch := mustFlagBool(cmd, "force-refetch")
	head := mustFlagBool(cmd, "head")
	outputFile := mustFlagString(cmd, "output")
	concurrency := mustFlagInt(cmd, "concurrency")
	includeHeaders := mustFlagBool(cmd, "include")

	if head {
		method = "HEAD"
	}

	cfg := limpet.DoConfig{
		Browser: browser,
		Stealth: stealth,
	}
	if forceRefetch {
		ctx = limpet.WithCachePolicy(ctx, limpet.CachePolicyReplace)
	}

	// Single URL: fetch and output directly.
	if len(args) == 1 {
		return doFetchOne(ctx, cl, method, args[0], cfg, includeHeaders, head, outputFile)
	}

	if outputFile != "" {
		return fmt.Errorf("-o/--output cannot be used with multiple URLs")
	}

	// Multiple URLs: concurrent fetch via GetMany.
	return cl.GetMany(ctx, args, concurrency, cfg, func(url string, page *limpet.Page, fetchErr error) error {
		if fetchErr != nil {
			var notOK *limpet.StatusError
			if errors.As(fetchErr, &notOK) {
				fmt.Fprintf(os.Stderr, "%d %s\n", notOK.Page.Response.StatusCode, url)
				return nil
			}
			fmt.Fprintf(os.Stderr, "ERR %s: %v\n", url, fetchErr)
			return nil
		}
		fmt.Fprintf(os.Stderr, "%d %s (%d bytes)\n", page.Response.StatusCode, url, len(page.Response.Body))
		return nil
	})
}

func doFetchOne(
	ctx context.Context,
	cl *limpet.Client,
	method, url string,
	cfg limpet.DoConfig,
	includeHeaders, head bool,
	outputFile string,
) error {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	log.Info().Str("url", url).Msg("fetching")
	page, err := cl.Do(ctx, req, cfg)
	if err != nil {
		var notOK *limpet.StatusError
		if errors.As(err, &notOK) {
			page = notOK.Page
			log.Warn().Msgf("non-200 status code: %d", page.Response.StatusCode)
		} else {
			return fmt.Errorf("failed to fetch: %w", err)
		}
	}

	if includeHeaders || head {
		for k, v := range page.Request.Header {
			fmt.Fprintf(os.Stderr, "> %s: %s\n", k, strings.Join(v, ", "))
		}
		if len(page.Request.Header) > 0 {
			fmt.Fprintln(os.Stderr)
		}
		for k, v := range page.Response.Header {
			fmt.Fprintf(os.Stderr, "< %s: %s\n", k, strings.Join(v, ", "))
		}
		if len(page.Response.Header) > 0 {
			fmt.Fprintln(os.Stderr)
		}
	}

	if head {
		return nil
	}

	body := page.Response.Body

	// Write to file if -o specified.
	if outputFile != "" {
		return os.WriteFile(outputFile, body, 0o644)
	}

	// Write raw bytes to stdout. No TrimSpace -- preserves binary content.
	// Add a trailing newline only for text content on a TTY.
	_, _ = os.Stdout.Write(body)
	if term.IsTerminal(int(os.Stdout.Fd())) && len(body) > 0 && body[len(body)-1] != '\n' {
		_, _ = os.Stdout.Write([]byte("\n"))
	}
	return nil
}
