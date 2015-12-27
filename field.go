package dagr

import (
	"encoding/json"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
)

// Field is any field value an InfluxDB measurement may hold. Fields must be duplicate-able (e.g., for snapshotting and
// such). Methods that modify field state must be safe for use across multiple goroutines. If the type implementing
// Field does not have mutable state, it is considered read-only.
//
// In order to keep fields and their names separate, Field is not responsible for writing its name, and should not
// attempt to write a name, only its value.
type Field interface {
	Dup() Field
	io.WriterTo
}

// Bool is a Field that stores an InfluxDB boolean value. When written, it is encoded as either T or F, depending on its
// value. Its zero value is false.
type Bool uint32

var _ = Field((*Bool)(nil))
var _ = json.Marshaler((*Bool)(nil))
var _ = json.Unmarshaler((*Bool)(nil))

func (b *Bool) ptr() *uint32 {
	return (*uint32)(b)
}

func (b *Bool) sample() bool {
	return atomic.LoadUint32(b.ptr()) != 0
}

// Set sets the Bool's value to new.
func (b *Bool) Set(new bool) {
	var i uint32
	if new {
		i = 1
	}
	atomic.StoreUint32(b.ptr(), i)
}

func (b *Bool) Snapshot() Field {
	return fixedBool(b.sample())
}

func (b *Bool) Dup() Field {
	q := Bool(atomic.LoadUint32(b.ptr()))
	return &q
}

func (b *Bool) WriteTo(w io.Writer) (int64, error) {
	return fixedBool(b.sample()).WriteTo(w)
}

func (b *Bool) MarshalJSON() ([]byte, error) {
	if b.sample() {
		return []byte{'t', 'r', 'u', 'e'}, nil
	} else {
		return []byte{'f', 'a', 'l', 's', 'e'}, nil
	}
}

func (b *Bool) UnmarshalJSON(js []byte) error {
	var new bool
	if err := json.Unmarshal(js, new); err != nil {
		return err
	}
	b.Set(new)
	return nil
}

// Int is a Field that stores an InfluxDB integer value. When written, it's encoded as a 64-bit integer with the 'i'
// suffix, per InfluxDB documentation (e.g., "123456i" sans quotes).
type Int int64

var _ = Field((*Int)(nil))
var _ = json.Marshaler((*Int)(nil))
var _ = json.Unmarshaler((*Int)(nil))

func (n *Int) ptr() *int64 {
	return (*int64)(n)
}

func (n *Int) sample() int64 {
	return atomic.LoadInt64(n.ptr())
}

// Add adds incr to the value held by the Int.
func (n *Int) Add(incr int64) {
	atomic.AddInt64(n.ptr(), incr)
}

// Set sets the value held by the Int.
func (n *Int) Set(new int64) {
	atomic.StoreInt64(n.ptr(), new)
}

func (n *Int) Snapshot() Field {
	return fixedInt(n.sample())
}

func (n *Int) Dup() Field {
	i := Int(n.sample())
	return &i
}

func (n *Int) WriteTo(w io.Writer) (int64, error) {
	return fixedInt(n.sample()).WriteTo(w)
}

func (n *Int) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.sample())
}

func badJSONValue(in []byte) string {
	switch in[0] {
	case '{':
		return "object"
	case '[':
		return "array"
	case 'n':
		return "null"
	case 't':
		return "true"
	case 'f':
		return "false"
	case '"':
		return "string"
	default:
		return "number " + string(in)
	}
}

func (n *Int) UnmarshalJSON(in []byte) error {
	if len(in) == 0 {
		return &json.UnmarshalTypeError{"empty JSON", reflect.TypeOf(n), 0}
	}

	var err error
	var next int64
	switch in[0] {
	case 'n', 't', 'f', '{', '[':
		return &json.UnmarshalTypeError{badJSONValue(in), reflect.TypeOf(n), 0}
	case '"':
		var new json.Number
		err = json.Unmarshal(in, &new)
		if err == nil {
			next, err = new.Int64()

			if err != nil {
				err = &json.UnmarshalTypeError{"quoted number " + new.String(), reflect.TypeOf(n), 0}
			}
		}
	default:
		err = json.Unmarshal(in, &next)
	}

	if err == nil {
		n.Set(next)
	}

	return nil
}

// Float is a Field that stores an InfluxDB float value. When written, it's encoded as a float64 using as few digits as
// possible (i.e., its precision is -1 when passed to FormatFloat). Different behavior may be desirable, in which case
// it's necessary to implement your own float field. Updates to Float are atomic.
type Float uint64

var _ = Field((*Float)(nil))
var _ = json.Marshaler((*Float)(nil))
var _ = json.Unmarshaler((*Float)(nil))

func (f *Float) ptr() *uint64 {
	return (*uint64)(f)
}

func (f *Float) sample() float64 {
	return math.Float64frombits(atomic.LoadUint64(f.ptr()))
}

func (f *Float) tryAdd(incr float64) bool {
	p := f.ptr()
	old := atomic.LoadUint64(p)
	new := math.Float64bits(math.Float64frombits(old) + incr)
	return atomic.CompareAndSwapUint64(p, old, new)
}

// Add adds incr to the value held by Float.
func (f *Float) Add(incr float64) {
	for !f.tryAdd(incr) {
	}
}

// Set sets the Float's value to new.
func (f *Float) Set(new float64) {
	atomic.StoreUint64(f.ptr(), math.Float64bits(new))
}

func (f *Float) Snapshot() Field {
	return fixedFloat(f.sample())
}

func (f *Float) Dup() Field {
	n := Float(atomic.LoadUint64(f.ptr()))
	return &n
}

func (f *Float) WriteTo(w io.Writer) (int64, error) {
	return fixedFloat(f.sample()).WriteTo(w)
}

func (f *Float) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.sample())
}

func (f *Float) UnmarshalJSON(in []byte) error {
	if len(in) == 0 {
		return &json.UnmarshalTypeError{"number", reflect.TypeOf(f), 0}
	}

	var err error
	var next float64
	switch in[0] {
	case 'n', 't', 'f', '{', '[':
		return &json.UnmarshalTypeError{badJSONValue(in), reflect.TypeOf(f), 0}
	case '"':
		var new json.Number
		err = json.Unmarshal(in, &new)
		if err == nil {
			next, err = new.Float64()

			if err != nil {
				err = &json.UnmarshalTypeError{"quoted number " + new.String(), reflect.TypeOf(f), 0}
			}
		}
	default:
		err = json.Unmarshal(in, &next)
	}

	if err == nil {
		f.Set(next)
	}

	return err
}

// String is a Field that stores an InfluxDB string value.
type String struct {
	value atomic.Value
}

var (
	stringUnescaper = strings.NewReplacer(`\"`, `"`)
	stringEscaper   = strings.NewReplacer(`"`, `\"`)

	_ = Field((*String)(nil))
	_ = json.Marshaler((*String)(nil))
	_ = json.Unmarshaler((*String)(nil))
)

// Set sets the String's value to new.
func (s *String) Set(new string) {
	// Limit stored values to 64kb -- this should still point to the same memory backing the string.
	if len(new) > 64000 {
		new = new[:64000]
	}
	s.value.Store([]byte(`"` + stringEscaper.Replace(new) + `"`))
}

func (s *String) sample() []byte {
	b, _ := s.value.Load().([]byte)
	return b
}

func (s *String) Snapshot() Field {
	return fixedString(s.sample())
}

func (s *String) Dup() Field {
	b := s.sample()
	s = new(String)
	s.value.Store(b)
	return s
}

func (s *String) WriteTo(w io.Writer) (int64, error) {
	// I could store this encoded and it might make a small difference, but I'm treating sets as the common case and
	// writes as the uncommon case. Basically, I expect write-outs of fields to only occur as often as once every
	// 500ms at its most frequent (you could do more, but strings are already pretty unlikely to be used, I figure).
	// More likely, you'll be writing once every 5s or 10s, in which case, the time taken to do a replacement is
	// fairly low in comparison, assuming the string itself is small enough.
	n, err := w.Write(s.sample())
	return int64(n), err
}

func (s *String) MarshalJSON() ([]byte, error) {
	b := s.sample()
	b = b[1 : len(b)-1]
	return json.Marshal(stringUnescaper.Replace(string(b)))
}

func (s *String) UnmarshalJSON(in []byte) error {
	var new string
	if err := json.Unmarshal(in, &new); err != nil {
		return err
	}
	s.Set(new)
	return nil
}

// Fixed types
// These are used primarily for snapshotting, since

type (
	fixedBool   bool
	fixedFloat  float64
	fixedInt    int64
	fixedString []byte
)

func (f fixedBool) Dup() Field { return f }

func (f fixedBool) MarshalJSON() ([]byte, error) {
	if f {
		return []byte{'t', 'r', 'u', 'e'}, nil
	} else {
		return []byte{'f', 'a', 'l', 's', 'e'}, nil
	}
}

func (f fixedBool) WriteTo(w io.Writer) (n int64, err error) {
	var c byte = 'F'
	if f {
		c = 'T'
	}

	err = writeByte(w, c)
	if err == nil {
		n = 1
	}
	return n, err
}

func (f fixedInt) Dup() Field { return f }

func (f fixedInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(f))
}

func (f fixedInt) WriteTo(w io.Writer) (int64, error) {
	var buf [20]byte
	b := append(strconv.AppendInt(buf[0:0], int64(f), 10), 'i')
	wn, err := w.Write(b)
	return int64(wn), err
}

func (f fixedFloat) Dup() Field { return f }

func (f fixedFloat) MarshalJSON() ([]byte, error) {
	return json.Marshal(float64(f))
}

func (f fixedFloat) WriteTo(w io.Writer) (int64, error) {
	var buf [32]byte
	b := strconv.AppendFloat(buf[0:0], float64(f), 'f', -1, 64)
	n, err := w.Write(b)
	return int64(n), err
}

func (f fixedString) Dup() Field { return f }

func (s fixedString) MarshalJSON() ([]byte, error) {
	return json.Marshal(stringUnescaper.Replace(string(s[1 : len(s)-1])))
}

func (s fixedString) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(s))
	return int64(n), err
}
