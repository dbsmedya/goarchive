// Package logger provides structured logging for GoArchive using zap.
package logger

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// Logger wraps zap.SugaredLogger with context methods.
type Logger struct {
	*zap.SugaredLogger
	base      *zap.Logger
	always    *zap.SugaredLogger // the same streams, every level (Always)
	logFile   *os.File
	filePath  string
	console   bool // the log stream is stdout or stderr
	json      bool
	stderr    bool // the log stream is stderr
	closeOnce *sync.Once
}

// Routing marks (spec §3.3). A mark is a field the console tee reads; every core drops it, so
// it never reaches a log line. Live: an INFO entry the tee shows (a hold or a pause ending, a
// replay starting). FileOnly: an entry the tee never shows (the closing entry, a banner's log
// record).
const (
	liveKey     = "goarchive.live"
	fileOnlyKey = "goarchive.file_only"
)

var (
	Live     = zap.Bool(liveKey, true)
	FileOnly = zap.Bool(fileOnlyKey, true)
)

// Options are New's stream choices. Stdout and Stderr default to the process's; a command
// passes its own streams, so tests can capture them. Tee turns on a file log's console tee
// (with --progress).
type Options struct {
	Stdout, Stderr io.Writer
	Tee            bool
}

// nopSyncWriter wraps a console stream so Sync is a no-op. fsync on a
// terminal or pipe fails with EINVAL on Linux, and zap does not buffer
// console writes, so there is nothing to flush.
type nopSyncWriter struct{ io.Writer }

func (nopSyncWriter) Sync() error { return nil }

// New creates a new Logger from configuration. A console log (output stdout, stderr or empty)
// writes to that stream in its format. A file log writes the file; with opts.Tee and without
// file_only, its warnings, errors and Live entries also reach stderr as text (spec §3.2).
func New(cfg *config.LoggingConfig, opts Options) (*Logger, error) {
	level := parseLevel(cfg.Level)
	stdout, stderr := opts.Stdout, opts.Stderr
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	l := &Logger{json: cfg.Format == "json", closeOnce: &sync.Once{}}
	var cores, always []zapcore.Core
	add := func(ws zapcore.WriteSyncer) {
		cores = append(cores, stripCore{zapcore.NewCore(buildEncoder(cfg.Format), ws, level)})
		always = append(always, stripCore{zapcore.NewCore(buildEncoder(cfg.Format), ws, zapcore.DebugLevel)})
	}

	switch cfg.Output {
	case "stdout", "":
		l.console = true
		add(nopSyncWriter{stdout})
	case "stderr":
		l.console, l.stderr = true, true
		add(nopSyncWriter{stderr})
	default:
		// File output. A new file is readable by its owner only, since the log names tables,
		// keys and servers (GH #5). OpenFile leaves an existing file's mode alone, so an
		// operator whose log reader runs as another user creates the file first with the mode
		// it needs. On Windows the mode decides only the read-only attribute, which 0600 leaves
		// unset; the file takes its folder's permissions.
		file, err := os.OpenFile(cfg.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		l.logFile, l.filePath = file, cfg.Output
		add(zapcore.AddSync(file))
		if opts.Tee && !cfg.FileOnly {
			cores = append(cores, &teeCore{level: level, w: stderr})
		}
	}

	l.base = zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	l.SugaredLogger = l.base.Sugar()
	l.always = zap.New(zapcore.NewTee(always...), zap.AddCaller()).Sugar()
	return l, nil
}

// NewDefault creates a Logger with default settings (info level, text format, stdout).
func NewDefault() *Logger {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "stdout",
	}
	logger, _ := New(cfg, Options{})
	return logger
}

// Always writes past the level filter, to the log's own streams (never the tee): the output
// --progress asks for, the R45 banners, and an error on a JSON stderr (spec §3.4a).
func (l *Logger) Always() *zap.SugaredLogger { return l.always }

// Console reports a log on stdout or stderr; ConsoleJSON one in JSON (setup C); ConsoleStderr
// one on stderr. File is the log file's path, empty for a console log.
func (l *Logger) Console() bool       { return l.console }
func (l *Logger) ConsoleJSON() bool   { return l.console && l.json }
func (l *Logger) ConsoleStderr() bool { return l.stderr }
func (l *Logger) File() string        { return l.filePath }

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

// buildEncoder creates the encoder for format: JSON, or plain text with the level as a word.
// No colour codes on any stream: a character-device check is not a terminal check, so colour
// reached files and docker logs as escape sequences (spec §3.2 B).
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
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// with derives a Logger carrying args on both its ordinary and its Always logger.
func (l *Logger) with(args ...interface{}) *Logger {
	c := *l
	c.SugaredLogger = l.With(args...)
	c.always = l.always.With(args...)
	return &c
}

// WithJob returns a Logger with job context.
func (l *Logger) WithJob(jobName string) *Logger { return l.with("job", jobName) }

// WithBatch returns a Logger with batch context.
func (l *Logger) WithBatch(batchNum int) *Logger { return l.with("batch", batchNum) }

// WithTable returns a Logger with table context.
func (l *Logger) WithTable(tableName string) *Logger { return l.with("table", tableName) }

// WithFields returns a Logger with additional fields.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return l.with(args...)
}

// stripCore drops the routing marks before the wrapped core encodes an entry.
type stripCore struct{ zapcore.Core }

func (c stripCore) With(f []zapcore.Field) zapcore.Core { return stripCore{c.Core.With(stripMarks(f))} }

func (c stripCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c stripCore) Write(e zapcore.Entry, f []zapcore.Field) error {
	return c.Core.Write(e, stripMarks(f))
}

// teeCore is a file log's console tee (spec §3.3): warnings and errors the configured level
// keeps, and Live entries whatever the level, one text line each; never a FileOnly entry, never
// the job field. It takes every INFO and above for inspection, since a Live mark is a field
// only Write can see.
type teeCore struct {
	level  zapcore.LevelEnabler // the log's configured level, for ordinary entries
	w      io.Writer
	fields []zapcore.Field
}

func (t *teeCore) Enabled(l zapcore.Level) bool { return l >= zapcore.InfoLevel || t.level.Enabled(l) }

func (t *teeCore) With(f []zapcore.Field) zapcore.Core {
	c := *t
	c.fields = append(slices.Clip(t.fields), dropKey(stripMarks(f), "job")...)
	return &c
}

func (t *teeCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if t.Enabled(e.Level) {
		return ce.AddCore(e, t)
	}
	return ce
}

func (t *teeCore) Write(e zapcore.Entry, f []zapcore.Field) error {
	switch {
	case hasKey(f, fileOnlyKey):
		return nil
	case hasKey(f, liveKey):
	case e.Level < zapcore.WarnLevel || !t.level.Enabled(e.Level):
		return nil
	}
	_, err := io.WriteString(t.w, renderText(e, append(slices.Clip(t.fields), dropKey(stripMarks(f), "job")...)))
	return err
}

func (t *teeCore) Sync() error { return nil }

// renderText is one tee line: "15:04:05 WARN  message key=value ...", fields sorted, control
// characters escaped so a value cannot forge a second line.
func renderText(e zapcore.Entry, fields []zapcore.Field) string {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %-5s %s", e.Time.Format("15:04:05"), e.Level.CapitalString(), escape(e.Message))
	for _, k := range slices.Sorted(maps.Keys(enc.Fields)) {
		fmt.Fprintf(&b, " %s=%s", k, escape(fmt.Sprint(enc.Fields[k])))
	}
	b.WriteByte('\n')
	return b.String()
}

func escape(s string) string {
	if strings.IndexFunc(s, unicode.IsControl) < 0 {
		return s
	}
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}

func stripMarks(f []zapcore.Field) []zapcore.Field {
	if !hasKey(f, liveKey) && !hasKey(f, fileOnlyKey) {
		return f
	}
	return slices.DeleteFunc(slices.Clone(f), func(x zapcore.Field) bool { return x.Key == liveKey || x.Key == fileOnlyKey })
}

func dropKey(f []zapcore.Field, k string) []zapcore.Field {
	if !hasKey(f, k) {
		return f
	}
	return slices.DeleteFunc(slices.Clone(f), func(x zapcore.Field) bool { return x.Key == k })
}

func hasKey(f []zapcore.Field, k string) bool {
	return slices.ContainsFunc(f, func(x zapcore.Field) bool { return x.Key == k })
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
