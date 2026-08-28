package cmd

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func newProgressTestCmd() (*cobra.Command, *string) {
	var val string
	c := &cobra.Command{Use: "t", RunE: func(*cobra.Command, []string) error { return nil }}
	registerProgressFlag(c, &val)
	return c, &val
}

func TestParseProgressInterval(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    time.Duration
		wantErr string
	}{
		{name: "absent means disabled", args: nil, want: 0},
		{name: "bare flag uses default 30s", args: []string{"--progress"}, want: 30 * time.Second},
		{name: "duration value", args: []string{"--progress=10s"}, want: 10 * time.Second},
		{name: "compound duration", args: []string{"--progress=1m30s"}, want: 90 * time.Second},
		{name: "bare integer means seconds", args: []string{"--progress=10"}, want: 10 * time.Second},
		{name: "explicit empty is an error", args: []string{"--progress="}, wantErr: "interval"},
		{name: "below 1s floor", args: []string{"--progress=500ms"}, wantErr: "at least 1s"},
		{name: "zero", args: []string{"--progress=0"}, wantErr: "at least 1s"},
		{name: "negative", args: []string{"--progress=-5"}, wantErr: "at least 1s"},
		{name: "integer overflow rejected not wrapped", args: []string{"--progress=18446744075"}, wantErr: "interval"},
		{name: "garbage", args: []string{"--progress=fast"}, wantErr: "interval"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, val := newProgressTestCmd()
			assert.NoError(t, c.ParseFlags(tt.args))
			got, err := parseProgressInterval(c, *val)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProgressArgsValidator(t *testing.T) {
	c, _ := newProgressTestCmd()
	c.SetArgs([]string{"--progress", "10s"})
	err := c.Execute()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "--progress=10s")

	c2, _ := newProgressTestCmd()
	c2.SetArgs([]string{"10s"})
	err2 := c2.Execute()
	assert.Error(t, err2)
	assert.NotContains(t, err2.Error(), "--progress=")

	c3, _ := newProgressTestCmd()
	c3.SetArgs([]string{"--progress=5s"})
	assert.NoError(t, c3.Execute())
}

func TestProgressFlagRegisteredOnExactlyThreeCommands(t *testing.T) {
	want := map[string]bool{"archive": true, "copy-only": true, "purge": true}
	for _, cmd := range rootCmd.Commands() {
		has := cmd.Flags().Lookup("progress") != nil
		assert.Equal(t, want[cmd.Name()], has,
			"command %q progress flag presence", cmd.Name())
		if has {
			assert.Equal(t, "30s", cmd.Flags().Lookup("progress").NoOptDefVal)
			assert.NotNil(t, cmd.Args, "Args validator installed on %q", cmd.Name())
		}
	}
}
