package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// The functions in this file expose the 2.x validators to the 3.0 job model
// (internal/jobspec), which decodes the same shared blocks but validates its own jobs.
// They wrap the unexported validators, so 2.x behaviour is unchanged.

// ValidateShared checks every block except jobs and verification: source, destination,
// replication, processing, safety and the global logging
// block. jobspec owns 3.0's verification, one value (R42).
func (c *Config) ValidateShared() error {
	var errs ValidationErrors
	errs = append(errs, c.validateDatabase("source", &c.Source)...)
	errs = append(errs, c.validateDatabase("destination", &c.Destination)...)
	errs = append(errs, c.validateReplication()...)
	errs = append(errs, c.validateProcessing()...)
	errs = append(errs, c.validateSafety()...)
	errs = append(errs, validateLoggingConfig(c.Logging, "logging")...)
	errs = append(errs, ValidateControlConfig("control", c.Control)...)
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// Normalize applies the per-server replication defaults that Load applies after decoding.
func (c *Config) Normalize() {
	c.normalizeReplication()
}

// ValidateProcessingConfig checks a job's effective (merged) processing settings.
func ValidateProcessingConfig(prefix string, p ProcessingConfig) ValidationErrors {
	return (&Config{}).validateProcessingConfig(prefix, &p)
}

// ValidateLoggingConfig checks a job's effective (merged) logging settings.
func ValidateLoggingConfig(prefix string, lc LoggingConfig) ValidationErrors {
	return validateLoggingConfig(lc, prefix)
}

// ValidateControlConfig checks a control block, global or a job's effective one: socket_dir, when
// set, is an absolute path. A path that begins with ~ is refused with its own message, since the file
// expands no ~; a relative one is refused because it would depend on the directory a run was started
// from, and cron and a shell differ there. The block is judged whether or not it is enabled, so a typo
// is caught before it is switched on. Whether the directory exists, and the job-name and path-length
// rules, are the run's and the loader's.
func ValidateControlConfig(prefix string, c ControlConfig) ValidationErrors {
	dir := c.SocketDir
	switch {
	case dir == "":
		return nil
	case strings.HasPrefix(dir, "~"):
		return ValidationErrors{{Field: prefix + ".socket_dir", Message: fmt.Sprintf(
			"socket_dir %q begins with ~, which the file does not expand: write the absolute path of the directory", dir)}}
	case !filepath.IsAbs(dir):
		return ValidationErrors{{Field: prefix + ".socket_dir", Message: fmt.Sprintf(
			"socket_dir %q is a relative path; it must be absolute, because a relative path depends on the directory the run was started from", dir)}}
	}
	return nil
}
