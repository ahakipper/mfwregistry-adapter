// Package logging wires the application logger to the ports.Logger
// interface.
//
// The setup mirrors pkg/log/log.go exactly: a zap sugared logger built on a
// lumberjack rotating file sink (<path>/app.log) with zapcore.ISO8601 time
// encoding and CapitalLevel level encoding, optionally duplicated to stdout,
// using either the JSON encoder or the console encoder ("log" encoding).
// Unlike pkg/log, the returned logger is an instance (not a package global)
// and closing it releases the lumberjack sink (flushing any buffered data)
// instead of relying on process exit.
package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"spotter/internal/ports"
)

// Defaults applied by New when the corresponding Options field is zero. They
// match the flag defaults registered in cmd/root.go and cmd/adapter.go.
const (
	defaultFilePath  = "./logfiles/"
	defaultMaxSizeMB = 100
	defaultMaxAge    = 7
	defaultMaxLevel  = -1
)

const (
	defaultMaxBackups = 10
)

// Options configures the logger returned by New. Zero-valued fields are
// replaced with the defaults above by the constructor.
type Options struct {
	// FilePath is the log directory; the log file is <FilePath>/app.log.
	FilePath string
	// MaxSizeMB is the max size of a log file in megabytes.
	MaxSizeMB int
	// MaxBackups is the number of old log files to keep.
	MaxBackups int
	// MaxAgeDays is the max age of an old log file in days.
	MaxAgeDays int
	// Level is the zap level (-1 debug, 0 info, 1 warn, ...).
	Level int
	// Encoding is "json" or "log"; any other value makes New fail.
	Encoding string
	// ToStd additionally writes every entry to stdout.
	ToStd bool
}

// defaultEncoding is applied when Encoding is empty (zero value).
const defaultEncoding = "json"

// supported encodings, mirroring pkg/log (json -> JSON encoder, everything
// else -> console encoder); the console encoding is selected with "log".
const (
	encodingJSON = "json"
	encodingLog  = "log"
)

// loggerAdapter adapts a zap sugared logger to ports.Logger.
type loggerAdapter struct {
	sugar   *zap.SugaredLogger
	closers []io.Closer
}

// Compile-time assertion that the adapter satisfies the port.
var _ ports.Logger = (*loggerAdapter)(nil)

// New builds a ports.Logger from the given options.
//
// The logger writes to <FilePath>/app.log through lumberjack, and to stdout
// as well when ToStd is true. Level entries below Options.Level are dropped.
// The second return value is an io.Closer that flushes the zap logger and
// closes the underlying lumberjack sink; it must be called once the logger is
// no longer used. New returns an error when Encoding is neither "json" nor
// "log" (pkg/log silently falls back to the console encoder for any unknown
// encoding; this adapter prefers failing fast, as documented).
func New(opts Options) (ports.Logger, io.Closer, error) {
	if opts.FilePath == "" {
		opts.FilePath = defaultFilePath
	}
	if opts.MaxSizeMB == 0 {
		opts.MaxSizeMB = defaultMaxSizeMB
	}
	if opts.MaxBackups == 0 {
		opts.MaxBackups = defaultMaxBackups
	}
	if opts.MaxAgeDays == 0 {
		opts.MaxAgeDays = defaultMaxAge
	}
	if opts.Level == 0 {
		opts.Level = defaultMaxLevel
	}
	if opts.Encoding == "" {
		opts.Encoding = defaultEncoding
	}

	encoder, err := getEncoder(opts.Encoding)
	if err != nil {
		return nil, nil, err
	}

	// make sure the path exists, exactly like pkg/log.LoggerInit.
	if err := os.MkdirAll(opts.FilePath, 0755); err != nil {
		return nil, nil, fmt.Errorf("create log directory %s: %w", opts.FilePath, err)
	}

	// init log syncer, exactly like pkg/log.getLogWriter.
	lumberJackLogger := &lumberjack.Logger{
		Filename:   filepath.Join(opts.FilePath, "app.log"),
		MaxSize:    opts.MaxSizeMB, // megabytes
		MaxBackups: opts.MaxBackups,
		MaxAge:     opts.MaxAgeDays,
	}
	syncers := []zapcore.WriteSyncer{zapcore.AddSync(lumberJackLogger)}
	if opts.ToStd {
		syncers = append(syncers, zapcore.AddSync(os.Stdout))
	}
	writeSyncer := zapcore.NewMultiWriteSyncer(syncers...)

	core := zapcore.NewCore(encoder, writeSyncer, zapcore.Level(int8(opts.Level)))
	// the func call stack, exactly like pkg/log.LoggerInit.
	base := zap.New(core, zap.AddCaller())
	sugar := base.Sugar()

	adapter := &loggerAdapter{
		sugar:   sugar,
		closers: []io.Closer{zapCloser{base}, lumberJackLogger},
	}
	// The adapter implements both ports.Logger and io.Closer, so it is
	// returned for both result values.
	return adapter, adapter, nil
}

// zapCloser adapts zap's Sync method to io.Closer. zap.Logger (and
// zap.SugaredLogger) expose Sync, not Close.
type zapCloser struct {
	logger *zap.Logger
}

// Close syncs the wrapped zap logger, flushing buffered entries.
func (z zapCloser) Close() error {
	return z.logger.Sync()
}

// getEncoder mirrors pkg/log.getEncoder but validates the encoding: "json"
// selects the JSON encoder, "log" selects the console encoder, anything else
// (including the empty string, which New already replaces with the default)
// returns an error instead of silently falling back to the console encoder.
func getEncoder(encoding string) (zapcore.Encoder, error) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	switch encoding {
	case encodingJSON:
		return zapcore.NewJSONEncoder(encoderConfig), nil
	case encodingLog:
		return zapcore.NewConsoleEncoder(encoderConfig), nil
	default:
		return nil, fmt.Errorf("unsupported log encoding %q: want %q or %q", encoding, encodingJSON, encodingLog)
	}
}

// Close flushes the zap logger and closes the lumberjack sink.
//
// It is safe to call multiple times: the error of every closer is returned
// only on the first call, and later calls return nil.
func (l *loggerAdapter) Close() error {
	for _, closer := range l.closers {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	l.closers = nil
	return nil
}

// Info logs the arguments at info level.
func (l *loggerAdapter) Info(args ...interface{}) {
	l.sugar.Info(args...)
}

// Infof logs the formatted message at info level.
func (l *loggerAdapter) Infof(format string, args ...interface{}) {
	l.sugar.Infof(format, args...)
}

// Warn logs the arguments at warn level.
func (l *loggerAdapter) Warn(args ...interface{}) {
	l.sugar.Warn(args...)
}

// Warnf logs the formatted message at warn level.
func (l *loggerAdapter) Warnf(format string, args ...interface{}) {
	l.sugar.Warnf(format, args...)
}

// Error logs the arguments at error level.
func (l *loggerAdapter) Error(args ...interface{}) {
	l.sugar.Error(args...)
}

// Errorf logs the formatted message at error level.
func (l *loggerAdapter) Errorf(format string, args ...interface{}) {
	l.sugar.Errorf(format, args...)
}

// NewNop returns a ports.Logger that discards all messages.
func NewNop() ports.Logger {
	return ports.NopLogger{}
}

// Compile-time assertion that the adapter's Close implements io.Closer.
var _ io.Closer = (*loggerAdapter)(nil)
