package config

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
