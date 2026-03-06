package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/henrywallace/scraper"
	"github.com/henrywallace/scraper/blob"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/samber/mo"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "scraper",
	Short: "Scraper is a tool to scrape a url",
	RunE:  rootRunE,
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
		"whether to use the cache",
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
	rootCmd.PersistentFlags().BoolP(
		"log-color-always",
		"C",
		false,
		"whether to always log with color",
	)

	rootCmd.AddCommand(doCmd)
	rootCmd.AddCommand(proxyCmd)

}

func setupLogger(
	cmd *cobra.Command,
	args []string,
) context.Context {
	logLevel, err := zerolog.ParseLevel(mustFlagString(cmd, "log-level"))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	logFormat := mustFlagString(cmd, "log-format")
	logColor := mustFlagString(cmd, "log-color")
	if mustFlagBool(cmd, "log-color-always") {
		logColor = "always"
	}
	ctx := cmd.Context()
	opts := loggerOptions{
		Level:  mo.Some(logLevel),
		Format: mo.Some(logFormat),
		Color:  mo.Some(logColor),
	}
	return initGlobalLogger(ctx, opts)
}

var _, lg = initLogger(context.Background(), loggerOptions{}) //nolint:unused

type loggerOptions struct {
	Level  mo.Option[zerolog.Level]
	Format mo.Option[string]
	Color  mo.Option[string]
}

func initGlobalLogger(
	ctx context.Context,
	opts loggerOptions,
) context.Context {
	logLvl := opts.Level.OrElse(zerolog.FatalLevel)
	zerolog.SetGlobalLevel(logLvl)
	opts.Level = mo.Some(logLvl)
	ctx, lg := initLogger(ctx, opts)
	log.Logger = lg
	return ctx
}

func initLogger(
	ctx context.Context,
	opts loggerOptions,
) (context.Context, zerolog.Logger) {
	// zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	lg := zerolog.New(os.Stderr).With().
		Timestamp().
		Stack().
		Caller().
		Logger()
	lg.Level(opts.Level.OrElse(zerolog.FatalLevel))

	doConsole := false
	logFmt := opts.Format.OrElse("auto")
	out := os.Stderr
	isTerm := isatty.IsTerminal(out.Fd())
	switch strings.TrimSpace(strings.ToLower(logFmt)) {
	case "", "auto":
		doConsole = isTerm
	case "console":
		doConsole = true
	default:
		lg.Fatal().Msgf("unknown log format: %q", logFmt)

	}

	if doConsole {
		doColor := false
		switch strings.ToLower(opts.Color.OrElse("auto")) {
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

	return lg.WithContext(ctx), lg
}

func rootRunE(cmd *cobra.Command, args []string) error {
	setupLogger(cmd, args)
	return nil
}

func newScraper(
	cmd *cobra.Command,
	args []string,
) (*scraper.Scraper, error) {
	ctx := cmd.Context()
	bucketURL := mustFlagString(cmd, "bucket-url")
	cacheDir := mustFlagString(cmd, "cache-dir")
	noCache := mustFlagBool(cmd, "no-cache")
	var opts []blob.BucketOption
	if cacheDir != "" {
		opts = append(opts, &blob.OptBucketCacheDir{CacheDir: cacheDir})
	}
	if noCache {
		opts = append(opts, &blob.OptBucketNoCache{})
	}
	blob, err := blob.NewBucket(ctx, bucketURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}
	sc, err := scraper.NewScraper(ctx, blob)
	if err != nil {
		return nil, fmt.Errorf("failed to create scraper: %w", err)
	}
	return sc, nil
}

const appName = "scraper"

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
	case "darwin": // macOS
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
