package dagr

// IntAdder defines anything that can have an int64 added to itself (e.g., an Int).
type IntAdder interface {
	Add(int64)
}

// FloatAdder defines anything that can have a float64 added to itself (e.g., a Float).
type FloatAdder interface {
	Add(float64)
}

// AddInt adds incr to any field that implements either IntAdder or FloatAdder (by converting incr to a float64) and
// returns true. If the field doesn't implement either interface, it returns false. This can be used to interact with
// elements of Fields without requiring you to perform type assertions. If field is nil, it returns false.
func AddInt(field Field, incr int64) bool {
	if field == nil {
		return false
	}
	switch f := field.(type) {
	case IntAdder:
		f.Add(incr)
	case FloatAdder:
		f.Add(float64(incr))
	default:
		return false
	}
	return true
}

// AddFloat adds incr to any field that implements either FloatAdder or IntAdder (by converting incr to an int64) and
// returns true. If the field doesn't implement either interface, it returns false. If field is nil, it returns false.
func AddFloat(field Field, incr float64) bool {
	if field == nil {
		return false
	}
	switch f := field.(type) {
	case IntAdder:
		f.Add(int64(incr))
	case FloatAdder:
		f.Add(incr)
	default:
		return false
	}
	return true
}
