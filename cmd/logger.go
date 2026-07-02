package cmd

import (
	"net/url"
	"os"
	"sync"

	"github.com/bruin-data/bruin/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// dynStdoutSink writes to the current os.Stdout at write time, so zap output
// routes through the masking pipe that logOutput swaps in (not the raw fd).
type dynStdoutSink struct{}

func (dynStdoutSink) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (dynStdoutSink) Sync() error                 { return nil }
func (dynStdoutSink) Close() error                { return nil }

var registerSinkOnce sync.Once

func makeLogger(isDebug bool) logger.Logger {
	registerSinkOnce.Do(func() {
		_ = zap.RegisterSink("bruinstdout", func(*url.URL) (zap.Sink, error) {
			return dynStdoutSink{}, nil
		})
	})

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling:    nil,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"bruinstdout://"},
		ErrorOutputPaths: []string{"bruinstdout://"},
	}

	if isDebug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		config.Development = true
		config.EncoderConfig.CallerKey = "caller"
	}

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}
