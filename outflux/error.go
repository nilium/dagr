package outflux

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrNotInfluxError is returned by (*BadStatusError).InfxluError when an error body does not appear
// to describe an InfluxDB JSON error message.
var ErrNotInfluxError = errors.New("outflux: error is not an InfluxDB error")

// InfluxError is a generic error message from InfluxDB.
type InfluxError struct {
	Error string `json:"error"`
}

// BadStatusError is any error that occurs as a result of a request failing. It includes the
// response code, body, and any error that occurred as a result of reading the body (never EOF).
type BadStatusError struct {
	Code int
	Body []byte
	Err  error
}

func (e *BadStatusError) Error() string {
	if e == nil {
		return "<nil>"
	}

	const (
		errorlimit = 300
		fmtpre     = "outflux: bad status returned: code=%d"
		fmtbody    = " len=%d body=%q%s"
		fmterr     = " err=%q"
	)

	var (
		body   = e.Body
		n      = len(body)
		suffix = ""
		err    = e.Err
	)
	if n > errorlimit {
		suffix = " (truncated)"
		body = body[:errorlimit]
	}

	switch {
	case n == 0 && err == nil:
		return fmt.Sprintf(fmtpre, e.Code)
	case n == 0 && err != nil:
		return fmt.Sprintf(fmtpre+fmterr, e.Code, err)
	case err == nil:
		return fmt.Sprintf(fmtpre+fmtbody, e.Code, n, body, suffix)
	default:
		return fmt.Sprintf(fmtpre+fmtbody+fmterr, e.Code, n, body, suffix, err)
	}
}

// InfluxError attempts to parse and return the BadStatusError's body as an InfluxDB error (i.e.,
// JSON of {"error":"description"}). If there is an error parsing it as an InfluxError, it will
// return either a JSON-specific error or ErrNotInfluxError if the body was empty or not a JSON
// object.
func (e *BadStatusError) InfluxError() (*InfluxError, error) {
	if len(e.Body) == 0 || e.Body[0] != '{' {
		return nil, ErrNotInfluxError
	}

	var ie InfluxError
	if err := json.Unmarshal(e.Body, &ie); err != nil {
		return nil, err
	}
	return &ie, nil
}
