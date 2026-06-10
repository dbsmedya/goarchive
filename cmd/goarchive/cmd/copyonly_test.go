package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyOnlyCommandStructure(t *testing.T) {
	assert.NotNil(t, copyOnlyCmd)
	assert.Equal(t, "copy-only", copyOnlyCmd.Use)
	assert.NotNil(t, copyOnlyCmd.RunE)
}

func TestCopyOnlyCommandFlags(t *testing.T) {
	flags := copyOnlyCmd.Flags()
	jobFlag := flags.Lookup("job")
	assert.NotNil(t, jobFlag)
	assert.Equal(t, "j", jobFlag.Shorthand)
}

func TestCopyOnlyIsAddedToRoot(t *testing.T) {
	found := false
	for _, c := range rootCmd.Commands() {
		if c.Name() == "copy-only" {
			found = true
			break
		}
	}
	assert.True(t, found, "copy-only command should be added to root command")
}
