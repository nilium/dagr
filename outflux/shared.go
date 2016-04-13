package outflux

import (
	"sync"
	"time"

	"go.spiff.io/dagr"
)

var writerlock sync.RWMutex
var sharedWriter *Proxy

func SetWriter(w *Proxy) {
	writerlock.Lock()
	defer writerlock.Unlock()
	sharedWriter = w
}

func Write(b []byte) (int, error) {
	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return sharedWriter.Write(b)
}

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

func WritePoint(key string, when time.Time, tags dagr.Tags, fields dagr.Fields) (n int64, err error) {
	writerlock.RLock()
	defer writerlock.RUnlock()

	if sharedWriter == nil {
		return 0, nil
	}

	return sharedWriter.WritePoint(key, when, tags, fields)
}
