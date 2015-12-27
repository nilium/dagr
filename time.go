package dagr

import "time"

// timeSource is primarily here as a test facility, since it's necessary to override time.Now for testing, as otherwise
// we never receive a consistent time.
type timeSource interface {
	Now() time.Time
}

type defaultClock struct{}

func (defaultClock) Now() time.Time { return time.Now() }

// This is set to a fixed time in time_test.go
var clock timeSource = defaultClock{}
