package cmd

import (
	"github.com/bruin-data/bruin/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func makeLogger(isDebug bool) logger.Logger {
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
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stdout"},
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
