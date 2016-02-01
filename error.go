package dagr

// Error is any error code that is returned by a dagr function or method or might be worth catching a panic on.
type Error int

const (
	ErrNoFields    = Error(1 + iota) // Returned by WriteMeasurement(s) when a measurement has no fields
	ErrEmptyKey                      // Used to panic when attempting to allocate a point with an empty key
	ErrNoAllocator                   // Used to panic when attempting to allocate a PointSet with a nil allocator
)

func (e Error) Error() string {
	if msg, ok := errDescs[e]; ok {
		return msg
	}
	return "unknown error"
}

var errDescs = map[Error]string{
	ErrNoFields:    "measurement has no fields",
	ErrEmptyKey:    "NewPoint: key is empty",
	ErrNoAllocator: "allocator is nil",
}
