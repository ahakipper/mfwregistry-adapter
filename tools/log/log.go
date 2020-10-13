package log

import (
	"go.uber.org/zap"
)

// Declare the global logger
var logger *zap.SugaredLogger

// Init logger
func init() {
	cfg := zap.Config{
		Level:         zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:   true,
		Encoding:      "json",
		EncoderConfig: zap.NewDevelopmentEncoderConfig(),
		OutputPaths:   []string{"stdout"},
	}

	var err error
	Logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer Logger.Sync()

	logger = Logger.Sugar()
}

// Debug uses zap.SugaredLogger to construct and log a message.
func Debug(args ...interface{}) {
	logger.Debug(args)
}

// Info uses zap.SugaredLogger to construct and log a message.
func Info(args ...interface{}) {
	logger.Info(args)
}

// Warn uses zap.SugaredLogger to construct and log a message.
func Warn(args ...interface{}) {
	logger.Warn(args)
}

// Error uses zap.SugaredLogger to construct and log a message.
func Error(args ...interface{}) {
	logger.Error(args)
}

// Infof uses zap.SugaredLogger to log a templated message.
func Infof(template string, args ...interface{}) {
	logger.Infof(template, args)
}

// Errorf uses zap.SugaredLogger to log a templated message.
func Errorf(template string, args ...interface{}) {
	logger.Errorf(template, args)
}
