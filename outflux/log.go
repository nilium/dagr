package outflux

import (
	"fmt"
	"io"
	"log"
)

type Logger interface {
	Print(...interface{})
}

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
