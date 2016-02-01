package dagr

import "log"

type Logger interface {
	Printf(string, ...interface{})
}

type defaultLogger struct{}

func (defaultLogger) Printf(f string, v ...interface{}) {
	log.Printf(f, v...)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}

var (
	DiscardLogger        = discardLogger{}
	StdLogger            = defaultLogger{}
	Log           Logger = DiscardLogger
)
