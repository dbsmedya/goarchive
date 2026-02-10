// Package logger provides structured logging for GoArchive using zap.
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/internal/config"
)

// Logger wraps zap.SugaredLogger with context methods.
type Logger struct {
	*zap.SugaredLogger
	base *zap.Logger
}

// New creates a new Logger from configuration.
func New(cfg *config.LoggingConfig) (*Logger, error) {
	level := parseLevel(cfg.Level)
	encoder := buildEncoder(cfg.Format)
	writers := buildWriters(cfg.Output)

	core := zapcore.NewCore(encoder, writers, level)
	baseLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return &Logger{
		SugaredLogger: baseLogger.Sugar(),
		base:          baseLogger,
	}, nil
}

// NewDefault creates a Logger with default settings (info level, text format, stdout).
func NewDefault() *Logger {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "stdout",
	}
	logger, _ := New(cfg)
	return logger
}

// parseLevel converts string level to zapcore.Level.
func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info", "":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// buildEncoder creates the appropriate encoder based on format.
func buildEncoder(format string) zapcore.Encoder {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	if format == "json" {
		return zapcore.NewJSONEncoder(encoderConfig)
	}

	// Text format with colored output
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// buildWriters creates the output writers based on configuration.
func buildWriters(output string) zapcore.WriteSyncer {
	switch output {
	case "stdout", "":
		return zapcore.AddSync(os.Stdout)
	case "stderr":
		return zapcore.AddSync(os.Stderr)
	default:
		// File output
		file, err := os.OpenFile(output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			// Fall back to stdout
			return zapcore.AddSync(os.Stdout)
		}
		// Write to both file and stdout
		return zapcore.NewMultiWriteSyncer(
			zapcore.AddSync(file),
			zapcore.AddSync(os.Stdout),
		)
	}
}

// WithJob returns a Logger with job context.
func (l *Logger) WithJob(jobName string) *Logger {
	return &Logger{
		SugaredLogger: l.SugaredLogger.With("job", jobName),
		base:          l.base,
	}
}

// WithBatch returns a Logger with batch context.
func (l *Logger) WithBatch(batchNum int) *Logger {
	return &Logger{
		SugaredLogger: l.SugaredLogger.With("batch", batchNum),
		base:          l.base,
	}
}

// WithTable returns a Logger with table context.
func (l *Logger) WithTable(tableName string) *Logger {
	return &Logger{
		SugaredLogger: l.SugaredLogger.With("table", tableName),
		base:          l.base,
	}
}

// WithFields returns a Logger with additional fields.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &Logger{
		SugaredLogger: l.SugaredLogger.With(args...),
		base:          l.base,
	}
}

// Sync flushes any buffered log entries.
func (l *Logger) Sync() error {
	return l.base.Sync()
}
