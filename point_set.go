package dagr

import (
	"io"
	"sync"
)

// PointAllocator is used to prepare a point from an identifier. Given the identifier, the PointAllocator must return
// a key, tags, and fields for a new Point to use. The PointAllocator can signal that an identifier should not receive
// a point by returning an empty key or no fields.
//
// The map of tags and fields are copied, so you may return prefabricated maps of tags or fields (though usually you'll
// want at least one of a unique tag or field per point).
//
// The opaque value has no effect on the identifier, but can be used to pass specific information to affect the result
// of the allocator (e.g., an *http.Request that might contain a path to include a tag).
type PointAllocator interface {
	AllocatePoint(identifier string, opaque interface{}) (key string, tags Tags, fields Fields)
}

// StaticPointAllocator is a simple PointAllocator that reuses a single key and set of tags and fields.
//
// If fields is
// nil, IdentifierField must be non-empty, otherwise it will never produce a valid point.
type StaticPointAllocator struct {
	Key    string
	Tags   Tags
	Fields Fields // All fields will be duplicated to ensure that they're unique among points

	// If either IdnetifierTag or IdentifierField is set, the tag/field with that name will be assigned the
	// identifier passed to the StaticPointAllocator.
	IdentifierTag   string
	IdentifierField string
}

func (s StaticPointAllocator) AllocatePoint(identifier string, _ interface{}) (key string, tags Tags, fields Fields) {
	tags = s.Tags
	if s.IdentifierTag != "" {
		if tags == nil {
			tags = make(Tags, 1)
		}
		tags = s.Tags.Dup()
		tags[s.IdentifierTag] = identifier
	}

	fields = s.Fields.Dup(true)
	if s.IdentifierField != "" {
		if fields == nil {
			fields = make(Fields, 1)
		}
		identField := new(String)
		identField.Set(identifier)
		fields[s.IdentifierField] = identField
	}

	return s.Key, tags, fields
}

type PointAllocFunc func(identifier string, opaque interface{}) (key string, tags Tags, fields Fields)

func (fn PointAllocFunc) AllocatePoint(identifier string, opaque interface{}) (key string, tags Tags, fields Fields) {
	return fn(identifier, opaque)
}

type taggedMetric struct {
	Measurement
	fields Fields
}

func (t taggedMetric) GetFields() Fields {
	return t.fields.Dup(false)
}

// PointSet is a simple collection of (compiled) points that supports dynamic allocation and revocation of points. It is
// intended for situations where you have a single point form varying across a tag or field, such as a request path or
// some other varying data.
type PointSet struct {
	allocator PointAllocator
	m         sync.RWMutex // controls metrics
	metrics   map[string]taggedMetric
}

// NewPointSet allocates a new PointSet with the given allocator. If allocator is nil, the function panics with
// ErrNoAllocator. You shouldn't bother recovering from this because there is no recovering from a PointSet without an
// allocator.
func NewPointSet(allocator PointAllocator) *PointSet {
	if allocator == nil {
		panic(ErrNoAllocator)
	}

	return &PointSet{
		allocator: allocator,
		metrics:   make(map[string]taggedMetric),
	}
}

func (p *PointSet) delete(ident string) {
	p.m.Lock()
	defer p.m.Unlock()

	delete(p.metrics, ident)
}

// alloc allocates a new point and stores it in the PointSet, returning a copy of the taggedMetric and whether the
// allocation was successful. It will always check, first, whether the point was allocated prior to the lock being
// acquired (i.e., if an alloc for the same identifier was waiting elsewhere) and return that if one was found.
//
// It is possible to significantly degrate PointSet performance by using an allocator that returns an empty key or
// fields map for frequently used identifiers. In that case, it will always take a write lock and fail each time the
// allocator returns nil. This isn't a bug, but it is something to consider when writing allocators.
func (p *PointSet) alloc(ident string, opaque interface{}) (m taggedMetric, ok bool) {
	p.m.Lock()
	defer p.m.Unlock()

	if m, ok := p.metrics[ident]; ok {
		return m, true
	}

	key, tags, fields := p.allocator.AllocatePoint(ident, opaque)
	if key == "" || len(fields) == 0 {
		return m, false
	}

	pt := NewPoint(key, tags, fields)
	compiled := pt.Compiled()
	if compiled == nil {
		return m, false
	}

	m = taggedMetric{
		Measurement: compiled,
		fields:      pt.GetFields(),
	}
	p.metrics[ident] = m

	return m, true
}

// lookup tries to find an existing metric for ident. If one is found, it returns the taggedMetric and true (otherwise,
// nothing and false).
//
// lookup takes a read lock on the PointSet. This is the common case when getting a metric out of the PointSet.
func (p *PointSet) lookup(ident string) (m taggedMetric, ok bool) {
	p.m.RLock()
	defer p.m.RUnlock()

	if m, ok := p.metrics[ident]; ok {
		return m, true
	}

	return taggedMetric{}, false
}

// FieldsForID returns the fields for a particular identifier. If no point is found and no point can be allocated for
// the identifier, it returns nil. The identifier may be an empty string.
func (p *PointSet) FieldsForID(identifier string, opaque interface{}) Fields {
	if m, ok := p.lookup(identifier); ok {
		return m.GetFields()
	}

	if m, ok := p.alloc(identifier, opaque); ok {
		return m.GetFields()
	}

	return nil
}

// Clear erases all points held by the PointSet.
func (p *PointSet) Clear() {
	p.m.Lock()
	defer p.m.Unlock()

	// Retain current length as new capacity, since presumably we'll end up with the same -- you can obviously
	// double-clear to completely zero out the initial capacity.
	capacity := len(p.metrics)
	p.metrics = make(map[string]taggedMetric, capacity)
}

func (p *PointSet) Remove(identifier string) {
	p.delete(identifier)
}

func (p *PointSet) WriteTo(w io.Writer) (int64, error) {
	buf := getBuffer(w)
	defer putBuffer(buf)

	p.m.RLock()
	defer p.m.RUnlock()

	for _, m := range p.metrics {
		head := buf.Len()
		if _, err := WriteMeasurement(buf, m.Measurement); err == ErrNoFields {
			buf.Truncate(head)
		} else if err != nil {
			buf.Truncate(int(buf.head))
			return 0, err
		}
	}

	return buf.WriteTo(w)
}

// The following prevents the PointSet from looking like a valid point to anything but WriteMeasurement(s), since
// WriteMeasurement(s) will see that it's a io.WriterTo and use that.

// Key returns an empty string, as a PointSet is a collection of points and relies on its WriterTo implementation for
// encoding its output.
func (p *PointSet) GetKey() string {
	return ""
}

func (p *PointSet) GetFields() Fields {
	return nil
}

func (p *PointSet) GetTags() Tags {
	return nil
}
