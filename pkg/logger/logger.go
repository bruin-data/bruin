package logger

type Logger interface {
	Debug(args ...interface{})
	Debugf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
	Error(args ...interface{})
}
