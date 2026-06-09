// Package logger provides structured logging for GoArchive using zap.
package logger

import (
	"errors"
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/internal/config"
)

// Logger wraps zap.SugaredLogger with context methods.
type Logger struct {
	*zap.SugaredLogger
	base      *zap.Logger
	logFile   *os.File
	closeOnce *sync.Once
}

// nopSyncWriter wraps a console stream so Sync is a no-op. fsync on a
// terminal or pipe fails with EINVAL on Linux, and zap does not buffer
// console writes, so there is nothing to flush.
type nopSyncWriter struct{ io.Writer }

func (nopSyncWriter) Sync() error { return nil }

// New creates a new Logger from configuration.
func New(cfg *config.LoggingConfig) (*Logger, error) {
	level := parseLevel(cfg.Level)

	var cores []zapcore.Core
	var logFile *os.File

	switch cfg.Output {
	case "stdout", "":
		cores = append(cores, zapcore.NewCore(buildEncoder(cfg.Format, true), nopSyncWriter{os.Stdout}, level))
	case "stderr":
		cores = append(cores, zapcore.NewCore(buildEncoder(cfg.Format, true), nopSyncWriter{os.Stderr}, level))
	default:
		// File output, plain encoder (no ANSI color codes)
		file, err := os.OpenFile(cfg.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		logFile = file
		cores = append(cores, zapcore.NewCore(buildEncoder(cfg.Format, false), zapcore.AddSync(file), level))
		if !cfg.FileOnly {
			cores = append(cores, zapcore.NewCore(buildEncoder(cfg.Format, true), nopSyncWriter{os.Stdout}, level))
		}
	}

	baseLogger := zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return &Logger{
		SugaredLogger: baseLogger.Sugar(),
		base:          baseLogger,
		logFile:       logFile,
		closeOnce:     &sync.Once{},
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

// buildEncoder creates the appropriate encoder based on format. Colored
// level output is only used for text format on console streams; file
// outputs get a plain encoder so log files stay free of ANSI codes.
func buildEncoder(format string, console bool) zapcore.Encoder {
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

	// Text format: colored levels on console, plain in files
	if console {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// WithJob returns a Logger with job context.
func (l *Logger) WithJob(jobName string) *Logger {
	return &Logger{
		SugaredLogger: l.With("job", jobName),
		base:          l.base,
		logFile:       l.logFile,
		closeOnce:     l.closeOnce,
	}
}

// WithBatch returns a Logger with batch context.
func (l *Logger) WithBatch(batchNum int) *Logger {
	return &Logger{
		SugaredLogger: l.With("batch", batchNum),
		base:          l.base,
		logFile:       l.logFile,
		closeOnce:     l.closeOnce,
	}
}

// WithTable returns a Logger with table context.
func (l *Logger) WithTable(tableName string) *Logger {
	return &Logger{
		SugaredLogger: l.With("table", tableName),
		base:          l.base,
		logFile:       l.logFile,
		closeOnce:     l.closeOnce,
	}
}

// WithFields returns a Logger with additional fields.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &Logger{
		SugaredLogger: l.With(args...),
		base:          l.base,
		logFile:       l.logFile,
		closeOnce:     l.closeOnce,
	}
}

// Sync flushes any buffered log entries.
func (l *Logger) Sync() error {
	return l.base.Sync()
}

// Close flushes buffered logs and closes owned file output, if any.
func (l *Logger) Close() error {
	if l.closeOnce == nil {
		return l.base.Sync()
	}

	var closeErr error
	l.closeOnce.Do(func() {
		syncErr := l.base.Sync()
		var fileErr error
		if l.logFile != nil {
			fileErr = l.logFile.Close()
		}
		closeErr = errors.Join(syncErr, fileErr)
	})
	return closeErr
}
