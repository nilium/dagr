package outflux

import (
	"fmt"
	"io"
	"log"
)

// Logger is a basic logging interface that outflux uses to handle its logging behavior. You can set the logger by
// modifying the Log package variable. This is not safe for concurrent modification by virtue of it being a package
// variable, so you should only set it once at program startup, before you've given outflux a reason to log anything.
type Logger interface {
	Print(...interface{})
}

// Log is the Logger used by outflux. If nil, outflux will not log anything. All outflux log messages are prefixed with
// "outflux: " to identify them. Log is not thread-safe and should not be modified after program startup.
var Log Logger

// stdlog is an empty struct that represents logging from the standard, global logger.
type stdlog struct{}

func (stdlog) Print(args ...interface{}) { log.Print(args...) }

// Stdlog is a Logger that uses the standard log package's logger.
var Stdlog = stdlog{}

func logf(format string, args ...interface{}) {
	if Log == nil {
		return
	}

	Log.Print("outflux: ", fmt.Sprintf(format, args...))
}

func logclose(c io.Closer) error {
	err := c.Close()
	if err != nil {
		logf("Error closing %T: %v", c, err)
	}
	return err
}
