package dagr

import (
	"math"
	"sync/atomic"
)

// Counter is a simple float64 counter that is atomically modified. All methods
// on Counter are safe to call concurrently.
type Counter struct {
	value uint64
}

// NewCounter allocates and returns a new Counter.
func NewCounter() *Counter {
	return new(Counter)
}

// ptr returns a uint64 pointer to the counter c's internal storage. This
// simply returns a pointer to the value method, though if the format of the
// structure changes, this is the only function that needs to be updated to
// ensure atomic operations in Get, Add, and any other methods still work,
// provided the counter's storage is persistent.
func (c *Counter) ptr() *uint64 {
	return &c.value
}

// Get returns the float64 value of the counter c.
func (c *Counter) Get() float64 {
	return math.Float64frombits(atomic.LoadUint64(c.ptr()))
}

// Add atomically adds n to the counter c.
func (c *Counter) Add(n float64) {
	for p := c.ptr(); ; {
		ui := atomic.LoadUint64(p)
		next := math.Float64bits(math.Float64frombits(ui) + n)
		if atomic.CompareAndSwapUint64(c.ptr(), ui, next) {
			return
		}
	}
}
