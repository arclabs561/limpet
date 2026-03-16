package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/arclabs561/limpet"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var doCmd = &cobra.Command{
	Use:   "do",
	Short: "Fetch the given url(s)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("requires a url")
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
}

func doRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cl, err := newClient(cmd, args)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	method := mustFlagString(cmd, "method")
	browser := mustFlagBool(cmd, "browser")
	forceRefetch := mustFlagBool(cmd, "force-refetch")
	head := mustFlagBool(cmd, "head")
	if head {
		method = "HEAD"
	}
	req, err := http.NewRequest(method, args[0], nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	var opts []limpet.DoOption
	if browser {
		opts = append(opts, &limpet.OptDoBrowser{})
	}
	if forceRefetch {
		opts = append(opts, &limpet.OptDoReplace{})
	}
	log.Info().Interface("opts", opts).Msgf("fetching %s", args[0])
	page, err := cl.Do(ctx, req, opts...)
	if err != nil {
		return fmt.Errorf("failed to fetch: %w", err)
	}
	if page.Response.StatusCode >= 400 {
		log.Error().Msgf("non-200 status code: %d", page.Response.StatusCode)
	} else {
		log.Info().Msgf("status code: %d", page.Response.StatusCode)
	}
	includeHeaders := mustFlagBool(cmd, "include")
	if includeHeaders || head {
		for k, v := range page.Request.Header {
			fmt.Printf("> %s: %s\n", k, strings.Join(v, ", "))
		}
		if len(page.Request.Header) > 0 {
			fmt.Println()
		}
		for k, v := range page.Response.Header {
			fmt.Printf("< %s: %s\n", k, strings.Join(v, ", "))
		}
		if len(page.Response.Header) > 0 {
			fmt.Println()
		}
	}
	out := strings.TrimSpace(string(page.Response.Body))
	if out != "" && !head {
		fmt.Println(out)
	}
	return nil
}
