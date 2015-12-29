package dagr

import (
	"encoding/json"
	"io"
	"sort"
	"sync"
	"time"
)

type Tags map[string]string

// Dup clones the Tags map, t. If t is nil, it returns nil.
func (t Tags) Dup() Tags {
	if t == nil {
		return nil
	}

	d := make(Tags, len(t))
	for name, tag := range t {
		d[name] = tag
	}
	return d
}

type Fields map[string]Field

// Dup clones the Fields map. If deep is true, it will also duplicate the fields held, creating new field instances,
// otherwise it retains its references to the Fields held by fs. If fs is nil, it returns nil.
func (fs Fields) Dup(deep bool) Fields {
	if fs == nil {
		return nil
	}

	d := make(Fields, len(fs))
	for name, field := range fs {
		if deep {
			field = field.Dup()
		}
		d[name] = field
	}
	return d
}

// Measurement defines the minimum interface for a measurement that could be passed to InfluxDB. All measurements must
// have a key and a minimum of one field. Tags are optional.
//
// Unless stated otherwise, the results of Tags and Fields are considered immutable.
type Measurement interface {
	Key() string
	Tags() Tags
	Fields() Fields
}

// TimeMeasurement is a measurement that has a known, known, fixed time. It can be considered a measurement that is
// fixed to a specific point in time. TimeMeasurements are not necessarily read-only and may, for example, return
// time.Now(). In general, it is better to not implement TimeMeasurement if your measurement is returning time.Now().
type TimeMeasurement interface {
	Time() time.Time

	Measurement
}

type fixedField struct {
	prefix []byte
	fields []fixedField
}

// Point is a single Measurement as understood by InfluxDB.
type Point struct {
	key        string
	tagOrder   []string
	fieldOrder []string
	tags       map[string]string
	fields     map[string]Field
	m          sync.RWMutex
}

var _ = Measurement((*Point)(nil))

// NewPoint allocates a new Point with the given key, tags, and fields. If key is empty, NewPoint panics. If fields is
// empty, the point cannot be written until it has at least one field.
func NewPoint(key string, tags Tags, fields Fields) *Point {
	if key == "" {
		panic("dagr.NewPoint: key is empty")
	}

	p := &Point{
		key:    key,
		tags:   make(map[string]string, len(tags)),
		fields: make(map[string]Field, len(fields)),
	}

	for name, tag := range tags {
		p.addTag(name, tag)
	}

	for name, field := range fields {
		p.addField(name, field)
	}

	return p
}

// WriteTo writes the point to the given writer, w. If an error occurs while building the point, it writes nothing and
// return an error. If the point has no fields, it returns the error ErrNoFields.
func (p *Point) WriteTo(w io.Writer) (int64, error) {
	buf := getBuffer(w)
	p.m.RLock()
	defer func() {
		p.m.RUnlock()
		putBuffer(buf)
	}()

	buf.WriteString(tagEscaper.Replace(p.key))
	writeTags(buf, p.tags, p.tagOrder)

	buf.WriteByte(' ')
	if err := writeFields(buf, p.fields, p.fieldOrder); err != nil {
		buf.Truncate(int(buf.head))
		return 0, err
	}

	buf.WriteByte(' ')
	writeTimestamp(buf, clock.Now())
	buf.WriteByte('\n')

	return buf.WriteTo(w)
}

// Key returns the point's key.
func (p *Point) Key() string {
	return p.key
}

// Compiled returns a compiled form of the point. The resulting Measurement is immutable except for its field values.
// New fields may not be added and it will not return a key, tags, or values. The compiled form of a point is only
// useful to improve write times on points when necessary. If the point has no fields, it returns nil, as the point is
// not valid to write.
func (p *Point) Compiled() Measurement {
	p.m.RLock()
	p.m.RUnlock()

	if len(p.fieldOrder) == 0 {
		return nil
	}
	return p.compile()
}

func (p *Point) compile() compiledPoint {
	var c compiledPoint
	buf := getBuffer(nil)
	defer putBuffer(buf)

	// Write key
	buf.WriteString(tagEscaper.Replace(p.key))
	// Write tags
	writeTags(buf, p.tags, p.tagOrder)
	c.lead = buf.Len()
	// Write field names
	var pre byte = ' '
	fields := make([]compiledField, len(p.fieldOrder))
	for i, name := range p.fieldOrder {
		field := p.fields[name]

		from := buf.Len()
		buf.WriteByte(pre)
		pre = ','

		buf.WriteString(tagEscaper.Replace(name))
		buf.WriteByte('=')

		to := buf.Len()
		if i == 0 {
			from = to
			c.lead = to
		}
		fields[i] = compiledField{from, to, field}
	}

	c.prefix = append([]byte(nil), buf.Bytes()...)
	c.fields = fields

	return c
}

// Tags

func (p *Point) addTag(name, value string) {
	_, exists := p.tags[name]
	p.tags[name] = value
	if exists {
		return
	}
	p.tagOrder = insertOrderedString(p.tagOrder, name)
}

// SetTag sets a tag on the point with the given name and value. If the value is empty, the tag is removed. If the name
// is empty, the call is a no-op. It is safe to call SetTag from concurrent goroutines.
func (p *Point) SetTag(name, value string) {
	if name == "" {
		return
	} else if value == "" {
		p.RemoveTag(name)
		return
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.addTag(name, value)
}

// RemoveTag removes the tag with the given name from the point. If name is empty, the call is a no-op. It is safe to
// call from concurrent goroutines.
func (p *Point) RemoveTag(name string) {
	if name == "" {
		return
	}

	p.m.Lock()
	defer p.m.Unlock()

	if _, ok := p.tags[name]; !ok {
		// No tag
		return
	}

	delete(p.tags, name)
	for i, tname := range p.tagOrder {
		if name == tname {
			copy(p.tagOrder[i:], p.tagOrder[i+1:])
			p.tagOrder = p.tagOrder[:len(p.tagOrder)-1]
			break
		}
	}
}

// Tags returns a copy of the point's tags as a map of names to values. Names and values are not escaped. It is safe to
// copy and modify the result of this method.
func (p *Point) Tags() Tags {
	p.m.RLock()
	defer p.m.RUnlock()
	return Tags(p.tags).Dup()
}

// Fields

// insertOrderedString inserts str in the ordered slice of strings. It assumes that all elements of slice are unique,
// though this isn't necessarily important except that having non-unique names would result in Point having a memory
// leak.
func insertOrderedString(slice []string, str string) []string {
	n := len(slice)
	if idx := sort.SearchStrings(slice, str); idx < n {
		slice = append(slice, "")
		copy(slice[idx+1:], slice[idx:n])
		slice[idx] = str
	} else {
		slice = append(slice, str)
	}
	return slice
}

func (p *Point) addField(name string, value Field) {
	_, exists := p.fields[name]
	p.fields[name] = value
	if exists {
		return
	}
	p.fieldOrder = insertOrderedString(p.fieldOrder, name)
}

// SetField sets a field with the given name and value. If name is empty, the call is a no-op. If value is nil, it
// removes the field.
func (p *Point) SetField(name string, value Field) {
	if name == "" {
		return
	} else if value == nil {
		p.RemoveField(name)
		return
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.addField(name, value)
}

// RemoveField removes a field with the given name. If name is empty, the call is a no-op. It is safe to call
// RemoveField from concurrent goroutines.
func (p *Point) RemoveField(name string) {
	if name == "" {
		return
	}

	p.m.Lock()
	defer p.m.Unlock()

	if _, ok := p.fields[name]; !ok {
		// No field
		return
	}

	delete(p.fields, name)
	for i, fname := range p.fieldOrder {
		if name == fname {
			copy(p.fieldOrder[i:], p.fieldOrder[i+1:])
			p.fieldOrder = p.fieldOrder[:len(p.fieldOrder)-1]
			break
		}
	}
}

// Fields returns a map of the point's fields. This map is a copy of the point's state and may be modified without
// affecting the point. The fields themselves are those held by the point, however, and modifying them will modify the
// point's state.
func (p *Point) Fields() Fields {
	p.m.RLock()
	defer p.m.RUnlock()
	return Fields(p.fields).Dup(false)
}

func (p *Point) MarshalJSON() ([]byte, error) {
	p.m.RLock()
	defer p.m.RUnlock()
	jsonPoint := struct {
		Key       string
		Timestamp int64 `json:",string"`
		Tags      jsonFields
		Fields    jsonFields
	}{
		p.key,
		clock.Now().UnixNano(),
		makeJSONFields(p.fields, p.fieldOrder),
		makeJSONTags(p.tags, p.tagOrder),
	}

	return json.Marshal(jsonPoint)
}
