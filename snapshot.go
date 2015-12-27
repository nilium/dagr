package dagr

import "time"

// Snapshotter is an interface that any Field may implement that returns a more-or-less frozen instance of a field.
type SnapshotField interface {
	Field
	Snapshot() Field
}

type SnapshotMeasurement interface {
	Measurement
	Snapshot() TimeMeasurement
}

// timePoint is a general-purpose measurement with a time attached, rather than being based off the current clock's
// time. All members are considered immutable, though there are no safeguards in place to ensure it.
type timePoint struct {
	key    string
	when   time.Time
	tags   map[string]string
	fields map[string]Field
}

var _ = TimeMeasurement(timePoint{})

func (t timePoint) Time() time.Time           { return t.when }
func (t timePoint) Key() string               { return t.key }
func (t timePoint) Fields() map[string]Field  { return t.fields }
func (t timePoint) Tags() map[string]string   { return t.tags }
func (t timePoint) Snapshot() TimeMeasurement { return t }

// Snapshot creates and returns a new measurement that implements TimeMeasurement. This Measurement is detached from its
// original source and is intended to be fixed in time. This is roughly the same as duplicating a point and its fields.
func Snapshot(m Measurement) TimeMeasurement {
	switch m := m.(type) {
	case timePoint:
		return m
	case SnapshotMeasurement:
		// Allow the metric to take its own snapshot, if it supports that. This is necessary for compiled
		// points, for example, since they don't return anything for their key, tags, or fields.
		return m.Snapshot()
	}
	if m, ok := m.(timePoint); ok {
		return m
	}

	var when time.Time
	if m, ok := m.(TimeMeasurement); ok {
		when = m.Time()
	} else {
		when = clock.Now()
	}

	key := m.Key()
	tags := m.Tags()
	fields := m.Fields()

	for name, field := range fields {
		switch f := field.(type) {
		case SnapshotField:
			field = f.Snapshot()
		default:
			field = f.Dup()
		}
		fields[name] = field
	}

	return timePoint{key, when, tags, fields}
}
