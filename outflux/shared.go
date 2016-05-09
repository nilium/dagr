package outflux

import (
	"sync"
	"time"

	"go.spiff.io/dagr"
)

var writerlock sync.RWMutex
var sharedWriter *Proxy

// SetWriter replaces the current Proxy with w. w may be nil, causing all subsequent writes via
// shared Proxy functions to be silently discarded.
func SetWriter(w *Proxy) {
	writerlock.Lock()
	defer writerlock.Unlock()
	sharedWriter = w
}

// Write writes an arbitrary sequence of bytes to the shared outflux Proxy. It returns the number of
// bytes written and any error that occurred. If b is nil or empty, the call is a no-op.
func Write(b []byte) (int, error) {
	if b == nil {
		return 0, nil
	}

	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return sharedWriter.Write(b)
}

// WriteMeasurements writes multiple dagr.Measurements to the shared outflux Proxy. It returns the
// number of bytes written and any error that occurred in writing the measurements. If ms is nil or
// empty, it is a no-op.
func WriteMeasurements(ms ...dagr.Measurement) (int64, error) {
	if len(ms) == 0 {
		return 0, nil
	}

	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return dagr.WriteMeasurements(sharedWriter, ms...)
}

// WriteMeasurement writes a single dagr.Measurement to the shared outflux Proxy. It returns the
// number of bytes written and any error that occurred in writing the measurement. If m is nil, it
// is a no-op.
func WriteMeasurement(m dagr.Measurement) (int64, error) {
	if m == nil {
		return 0, nil
	}

	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return dagr.WriteMeasurement(sharedWriter, m)
}

// WritePoint writes a single point to the shared outflux Proxy. It returns the number of bytes
// written and any error that occurred in writing it.
func WritePoint(key string, when time.Time, tags dagr.Tags, fields dagr.Fields) (n int64, err error) {
	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return sharedWriter.WritePoint(key, when, tags, fields)
}
