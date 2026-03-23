package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/arclabs561/limpet"
)

var cacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "Inspect the local cache",
}

var cacheLsCmd = &cobra.Command{
	Use:   "ls [prefix]",
	Short: "List cached keys",
	Args:  cobra.MaximumNArgs(1),
	RunE:  cacheLsRunE,
}

var cacheGetCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Read a cached page and print its response body",
	Args:  cobra.ExactArgs(1),
	RunE:  cacheGetRunE,
}

var cacheRmCmd = &cobra.Command{
	Use:   "rm <key>",
	Short: "Delete a cached entry",
	Args:  cobra.ExactArgs(1),
	RunE:  cacheRmRunE,
}

var cachePurgeCmd = &cobra.Command{
	Use:   "purge [prefix]",
	Short: "Delete all cached entries (or those matching a prefix)",
	Args:  cobra.MaximumNArgs(1),
	RunE:  cachePurgeRunE,
}

func init() {
	cacheLsCmd.Flags().BoolP("json", "j", false, "output as JSON")
	cacheLsCmd.Flags().BoolP("keys-only", "k", false, "show cache keys only (skip reading page metadata)")
	cacheGetCmd.Flags().BoolP("headers", "i", false, "include response headers")
	cacheGetCmd.Flags().Bool("meta", false, "print page metadata instead of body")

	cacheCmd.AddCommand(cacheLsCmd)
	cacheCmd.AddCommand(cacheGetCmd)
	cacheCmd.AddCommand(cacheRmCmd)
	cacheCmd.AddCommand(cachePurgeCmd)
}

func cacheLsRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	bucket, err := newBucket(cmd)
	if err != nil {
		return err
	}
	defer bucket.Close()

	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}

	entries, err := bucket.ListCache(prefix)
	if err != nil {
		return fmt.Errorf("failed to list cache: %w", err)
	}
	if entries == nil {
		return fmt.Errorf("cache is disabled (--no-cache)")
	}

	asJSON := mustFlagBool(cmd, "json")
	keysOnly := mustFlagBool(cmd, "keys-only")
	enc := json.NewEncoder(cmd.OutOrStdout())

	for _, e := range entries {
		expiry := "never"
		if e.ExpiresAt > 0 {
			t := time.Unix(int64(e.ExpiresAt), 0)
			remaining := time.Until(t).Round(time.Second)
			if remaining > 0 {
				expiry = remaining.String()
			} else {
				expiry = "expired"
			}
		}

		// Read the page URL and status from the blob unless --keys-only.
		// Uses a lightweight struct that skips body base64 decoding.
		url := ""
		status := 0
		if !keysOnly {
			if b, err := bucket.GetBlob(ctx, e.Key); err == nil {
				var summary struct {
					Request  struct{ URL string `json:"url"` }       `json:"request"`
					Response struct{ StatusCode int `json:"status_code"` } `json:"response"`
				}
				if err := json.Unmarshal(b.Data, &summary); err == nil {
					url = summary.Request.URL
					status = summary.Response.StatusCode
				}
			}
		}

		if asJSON {
			row := map[string]any{
				"key":  e.Key,
				"size": e.Size,
				"ttl":  expiry,
			}
			if url != "" {
				row["url"] = url
				row["status"] = status
			}
			_ = enc.Encode(row)
		} else if url != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%d\t%s\tttl=%s\n", e.Key, status, url, expiry)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\tsize=%d\tttl=%s\n", e.Key, e.Size, expiry)
		}
	}
	if len(entries) == 0 {
		log.Info().Msg("cache is empty")
	}
	return nil
}

func cacheGetRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	bucket, err := newBucket(cmd)
	if err != nil {
		return err
	}
	defer bucket.Close()

	key := args[0]
	b, err := bucket.GetBlob(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to get blob %q: %w", key, err)
	}

	var page limpet.Page
	if err := json.Unmarshal(b.Data, &page); err != nil {
		// Not a limpet page -- print raw data
		fmt.Fprint(cmd.OutOrStdout(), string(b.Data))
		return nil
	}

	showMeta := mustFlagBool(cmd, "meta")
	if showMeta {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"source":     b.Source,
			"version":    page.Meta.Version,
			"fetched_at": page.Meta.FetchedAt,
			"fetch_dur":  page.Meta.FetchDur.String(),
			"url":        page.Request.URL,
			"method":     page.Request.Method,
			"status":     page.Response.StatusCode,
			"body_bytes": len(page.Response.Body),
		})
	}

	showHeaders := mustFlagBool(cmd, "headers")
	if showHeaders {
		for k, v := range page.Response.Header {
			for _, vv := range v {
				fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", k, vv)
			}
		}
		fmt.Fprintln(cmd.OutOrStdout())
	}
	fmt.Fprint(cmd.OutOrStdout(), string(page.Response.Body))
	return nil
}

func cacheRmRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	bucket, err := newBucket(cmd)
	if err != nil {
		return err
	}
	defer bucket.Close()

	key := args[0]
	if err := bucket.DeleteBlob(ctx, key); err != nil {
		return fmt.Errorf("failed to delete %q: %w", key, err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "deleted %s\n", key)
	return nil
}

func cachePurgeRunE(cmd *cobra.Command, args []string) error {
	bucket, err := newBucket(cmd)
	if err != nil {
		return err
	}
	defer bucket.Close()

	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}

	n, err := bucket.PurgeCache(prefix)
	if err != nil {
		return fmt.Errorf("failed to purge cache: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "purged %d entries\n", n)
	return nil
}
