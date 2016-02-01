//+build !realtime

package dagr

import "time"

type testClock time.Time

func (t testClock) Now() time.Time { return time.Time(t) }

var (
	// Not exactly the reference time since timezones are irrelevant with timestamps.
	testTime      = time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)
	testTimeStamp = testTime.UnixNano()
)

func init() {
	clock = testClock(testTime)
}
