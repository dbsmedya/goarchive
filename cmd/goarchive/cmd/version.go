package cmd

import (
	"runtime"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Long:  `Display detailed version information including build details.`,
	Run:   runVersion,
}

func init() {
	rootCmd.AddCommand(versionCmd)
}

func runVersion(cmd *cobra.Command, args []string) {
	cmd.Printf("goarchive version %s\n", Version)
	cmd.Printf("  Commit: %s\n", Commit)
	cmd.Printf("  Go version: %s\n", runtime.Version())
	cmd.Printf("  OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}
