package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/arclabs561/limpet"
	"github.com/arclabs561/limpet/blob"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var rootCmd = &cobra.Command{
	Use:   "limpet",
	Short: "limpet is a caching HTTP fetcher and proxy",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		ctx := setupLogger(cmd, args)
		cmd.SetContext(ctx)
		return nil
	},
}

func init() {
	configDir := getConfigDir()
	defaultBucketURL := fmt.Sprintf("file://%s", filepath.Join(configDir, "bucket"))
	defaultCacheDir := filepath.Join(configDir, "cache")

	rootCmd.PersistentFlags().StringP(
		"bucket-url",
		"b",
		defaultBucketURL,
		"supported protocols (no scheme is rel file path): file|s3://",
	)
	rootCmd.PersistentFlags().String(
		"cache-dir",
		defaultCacheDir,
		"directory to cache files",
	)
	rootCmd.PersistentFlags().Bool(
		"no-cache",
		false,
		"disable the local cache",
	)
	rootCmd.PersistentFlags().String(
		"cache-ttl",
		"24h",
		"cache TTL duration (e.g. 24h, 7d, 0 for no expiry)",
	)
	rootCmd.PersistentFlags().StringP(
		"log-level",
		"L",
		"fatal",
		"logging level",
	)
	rootCmd.PersistentFlags().StringP(
		"log-format",
		"F",
		"auto",
		"logging format",
	)
	rootCmd.PersistentFlags().StringP(
		"log-color",
		"c",
		"auto",
		"logging color",
	)

	rootCmd.AddCommand(doCmd)
	rootCmd.AddCommand(proxyCmd)
	rootCmd.AddCommand(cacheCmd)
}

func setupLogger(cmd *cobra.Command, _ []string) context.Context {
	logLevel, err := zerolog.ParseLevel(mustFlagString(cmd, "log-level"))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	zerolog.SetGlobalLevel(logLevel)

	logFormat := mustFlagString(cmd, "log-format")
	logColor := mustFlagString(cmd, "log-color")

	lg := zerolog.New(os.Stderr).With().
		Timestamp().
		Stack().
		Caller().
		Logger()
	lg.Level(logLevel)

	doConsole := false
	out := os.Stderr
	isTerm := term.IsTerminal(int(out.Fd()))
	switch strings.TrimSpace(strings.ToLower(logFormat)) {
	case "", "auto":
		doConsole = isTerm
	case "console":
		doConsole = true
	default:
		lg.Fatal().Msgf("unknown log format: %q", logFormat)
	}

	if doConsole {
		doColor := false
		switch strings.ToLower(logColor) {
		case "", "auto":
			doColor = isTerm
		case "always":
			doColor = true
		case "never":
			doColor = false
		}
		lg = lg.Output(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
			w.Out = out
			w.NoColor = !doColor
		}))
	}

	log.Logger = lg
	return lg.WithContext(cmd.Context())
}

func newBucket(cmd *cobra.Command) (*blob.Bucket, error) {
	ctx := cmd.Context()
	bucketURL := mustFlagString(cmd, "bucket-url")
	cacheDir := mustFlagString(cmd, "cache-dir")
	noCache := mustFlagBool(cmd, "no-cache")
	cacheTTLStr := mustFlagString(cmd, "cache-ttl")

	var cacheTTL time.Duration
	if cacheTTLStr == "0" || cacheTTLStr == "infinite" || cacheTTLStr == "forever" {
		cacheTTL = -1 // no expiry
	} else {
		var err error
		cacheTTL, err = time.ParseDuration(cacheTTLStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse --cache-ttl=%q: %w", cacheTTLStr, err)
		}
	}

	return blob.NewBucket(ctx, bucketURL, &blob.BucketConfig{
		CacheDir: cacheDir,
		NoCache:  noCache,
		CacheTTL: cacheTTL,
	})
}

// newClient creates a Client and its backing Bucket. The caller must close
// both: cl.Close() for browser resources, bucket.Close() for the cache.
func newClient(cmd *cobra.Command) (*limpet.Client, *blob.Bucket, error) {
	bucket, err := newBucket(cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create bucket: %w", err)
	}
	ctx := cmd.Context()
	cl, err := limpet.NewClient(ctx, bucket)
	if err != nil {
		bucket.Close()
		return nil, nil, fmt.Errorf("failed to create client: %w", err)
	}
	return cl, bucket, nil
}

const appName = "limpet"

func getConfigDir() string {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	switch runtime.GOOS {
	case "linux":
		if xdgConfig := os.Getenv("XDG_CONFIG_HOME"); xdgConfig != "" {
			return filepath.Join(xdgConfig, appName)
		}
		return filepath.Join(homedir, ".config", appName)
	case "darwin":
		return filepath.Join(homedir, "Library", "Preferences", appName)
	default:
		return filepath.Join(homedir, ".config", appName)
	}
}

func mustFlagString(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	return val
}

func mustFlagBool(cmd *cobra.Command, name string) bool {
	val, err := cmd.Flags().GetBool(name)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	return val
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
