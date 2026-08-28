package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

const progressDefaultInterval = "30s"

func registerProgressFlag(cmd *cobra.Command, target *string) {
	cmd.Flags().StringVar(target, "progress", "",
		"Periodically print progress (roots done/total, copied/deleted rows, remaining, ETA) to stdout. "+
			"Attach an interval with '=': --progress=10s (bare --progress = 30s; bare integer = seconds; minimum 1s).")
	cmd.Flags().Lookup("progress").NoOptDefVal = progressDefaultInterval
	cmd.Args = progressAwareNoArgs
}

// parseProgressInterval resolves the flag value. Zero means disabled.
func parseProgressInterval(cmd *cobra.Command, raw string) (time.Duration, error) {
	if !cmd.Flags().Changed("progress") {
		return 0, nil
	}
	if raw == "" {
		return 0, fmt.Errorf("--progress= needs an interval (e.g. --progress=10s) or use bare --progress for %s", progressDefaultInterval)
	}
	var d time.Duration
	if _, err := strconv.Atoi(raw); err == nil {
		// Parse the suffixed value directly so duration overflow is rejected
		// instead of wrapping during integer multiplication.
		parsed, perr := time.ParseDuration(raw + "s")
		if perr != nil {
			return 0, fmt.Errorf("invalid --progress interval %q: %v", raw, perr)
		}
		d = parsed
	} else if parsed, perr := time.ParseDuration(raw); perr == nil {
		d = parsed
	} else {
		return 0, fmt.Errorf("invalid --progress interval %q: use a Go duration (10s, 1m30s) or a whole number of seconds", raw)
	}
	if d < time.Second {
		return 0, fmt.Errorf("--progress interval %q is too small: must be at least 1s", raw)
	}
	return d, nil
}

func progressAwareNoArgs(cmd *cobra.Command, args []string) error {
	err := cobra.NoArgs(cmd, args)
	if err == nil {
		return nil
	}
	if cmd.Flags().Changed("progress") && len(args) == 1 && looksLikeInterval(args[0]) {
		return fmt.Errorf("%s (interval values must be attached with '=': --progress=%s)", err.Error(), args[0])
	}
	return err
}

func looksLikeInterval(s string) bool {
	if _, err := strconv.Atoi(s); err == nil {
		return true
	}
	_, err := time.ParseDuration(s)
	return err == nil
}
