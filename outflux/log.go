package outflux

import (
	"io"
	"runtime"
	"sync/atomic"
)

type logFunc func(format string, args ...interface{})

// Logger is a basic logging interface that outflux uses to handle its logging behavior.
type Logger interface {
	Printf(format string, args ...interface{})
}

// pkglog is the Log used by outflux. If nil, outflux will not log anything.
//
// Access to this is controlled by ReplaceLogger.
var pkglog atomic.Value // storedLog

type storedLog struct {
	Logger
}

func (l storedLog) printer() logFunc {
	if l.Logger == nil {
		return nil
	}
	return l.Logger.Printf
}

func init() {
	pkglog.Store(storedLog{Logger: nil})
}

// logger returns the current logging function.
// If the current Logger is nil, it returns a nil function.
func logger() logFunc {
	return pkglog.Load().(storedLog).printer()
}

func logclose(c io.Closer, desc string) error {
	err := c.Close()
	if log := logger(); log != nil && err != nil {
		if _, file, line, ok := runtime.Caller(1); ok {
			log("%s (%T): %v", desc, c, err)
		} else {
			log("%s (%T:%s:%d): %v", desc, c, file, line, err)
		}
	}
	return err
}

// ReplaceLogger sets the current Logger for outflux and returns the previous Logger.
//
// This function is safe to call from multiple goroutines.
func ReplaceLogger(log Logger) Logger {
	got := pkglog.Load().(storedLog)
	pkglog.Store(storedLog{Logger: log})
	return got.Logger
}
