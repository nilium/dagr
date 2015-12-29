package dagr

import (
	"io"
	"time"
)

// RawPoint is a simple Field implementation that has no read/write concurrency guarantees. It's useful as a read-only
// point that can be written out and quickly discarded. It's a good idea to make use of the raw field types for this as
// well to avoid the overhead of atomics.
type RawPoint struct {
	Key    string
	Tags   Tags
	Fields Fields
	Time   time.Time
}

func (p RawPoint) GetKey() string {
	return p.Key
}

func (p RawPoint) GetTags() Tags {
	return p.Tags
}

func (p RawPoint) GetFields() Fields {
	return p.Fields
}

func (p RawPoint) GetTime() time.Time {
	if p.Time.IsZero() {
		return clock.Now()
	}
	return p.Time
}

type RawString string

var _ = Field(RawString(""))

func (s RawString) WriteTo(w io.Writer) (int64, error) {
	escaped := stringEscaper.Replace(string(s))
	n, err := io.WriteString(w, `"`+escaped+`"`)
	return int64(n), err
}

func (s RawString) Dup() Field      { return s }
func (s RawString) Snapshot() Field { return s }
