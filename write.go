package dagr

import (
	"bytes"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	minBufferCapacity = 128
	maxBufferCapacity = 65000
)

func allocMinimumBuffer() *tempBuffer {
	return &tempBuffer{bytes.NewBuffer(make([]byte, 0, minBufferCapacity)), true, 0}
}

type tempBuffer struct {
	*bytes.Buffer
	owned bool
	head  int64
}

var _ = (io.Writer)((*tempBuffer)(nil))
var _ = (io.WriterTo)((*tempBuffer)(nil))

func (t *tempBuffer) WriteTo(w io.Writer) (int64, error) {
	diffWriters := true
	if b, ok := w.(*bytes.Buffer); ok {
		diffWriters = b != t.Buffer
	}

	if diffWriters {
		return t.Buffer.WriteTo(w)
	}

	// If we're writing to an unowned buffer, just return how much we wrote to the buffer.
	n := int64(t.Len())
	if n < t.head {
		// Panic if the buffer was truncated to somewhere before head. This case is supposedly impossible for
		// owned buffers, since their heads are always 0 and their lengths are never < 0.
		panic("tempBuffer: head > buffer length - buffer was truncated during write")
	}

	return n - t.head, nil
}

var tempBuffers = sync.Pool{
	New: func() interface{} { return allocMinimumBuffer() },
}

func getBuffer(w io.Writer) *tempBuffer {
	if b, ok := w.(*bytes.Buffer); b != nil && ok {
		return &tempBuffer{b, false, int64(b.Len())}
	}

	if b, ok := tempBuffers.Get().(*tempBuffer); ok {
		return b
	}

	// Bizzaro case: tempBuffers.New didn't work? Something should've panicked by now.
	return allocMinimumBuffer()
}

func putBuffer(b *tempBuffer) {
	if !b.owned {
		return
	}

	b.head = 0
	b.Reset()

	tempBuffers.Put(b)
}

var tagEscaper = strings.NewReplacer(
	` `, `\ `,
	`=`, `\=`,
	`,`, `\,`,
)

func writeFields(buf *tempBuffer, fields Fields, names []string) error {
	for i, name := range names {
		field := fields[name]
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(tagEscaper.Replace(name))
		buf.WriteByte('=')

		// On the off chance that the field reports an error writing, we have to be careful with it and truncate
		// the buffer we got back to where the write started so we can leave the buffer sort of intact.
		if _, err := field.WriteTo(buf); err != nil {
			// This has the potential to panic IFF the buffer is being messed with from multiple goroutines.
			return err
		}
	}

	return nil
}

func writeTags(buf *tempBuffer, tags Tags, names []string) {
	for _, name := range names {
		tag := tags[name]
		buf.WriteByte(',') // Because tags must necessarily follow a key or tag, always include the comma
		buf.WriteString(tagEscaper.Replace(name))
		buf.WriteByte('=')
		buf.WriteString(tagEscaper.Replace(tag))
	}
}

// WriteMeasurement writes a single measurement, m, to w. It returns the number of bytes written and any error that
// occurred when writing the measurement.
//
// When writing tags and fields, both are sorted by name in ascending order. So, a tag named "pid" will precede a tag
// named "version", and a field name "depth" will precede a field named "value".
//
// If the measurement has no fields, it returns 0 and ErrNoFields.
//
// If the measurement implements io.WriterTo, this simply calls that instead of WriteMeasurement.
func WriteMeasurement(w io.Writer, m Measurement) (n int64, err error) {
	buf := getBuffer(w)
	defer putBuffer(buf)

	if mw, ok := m.(io.WriterTo); ok {
		n, err := mw.WriteTo(buf)
		if err != nil {
			return n, err
		}
		return buf.WriteTo(w)
	}

	var when time.Time
	if m, ok := m.(TimeMeasurement); ok {
		when = m.Time()
	} else {
		when = clock.Now()
	}

	tags := m.Tags()
	fields := m.Fields()

	if len(fields) == 0 {
		return 0, ErrNoFields
	}

	// Write key
	buf.WriteString(m.Key())

	nameLen := len(tags)
	if l := len(fields); l > nameLen {
		nameLen = l
	}
	names := make([]string, 0, nameLen)

	// Write tags
	if len(tags) > 0 {
		for name := range tags {
			names = append(names, name)
		}
		sort.Strings(names)
		writeTags(buf, tags, names)

		// Reset names slice so we can sort field names
		names = names[0:0]
	}

	// Write fields
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	buf.WriteByte(' ')
	if err := writeFields(buf, fields, names); err != nil {
		buf.Truncate(int(buf.head))
		return 0, err
	}

	buf.WriteByte(' ')
	writeTimestamp(buf, when)
	buf.WriteByte('\n')

	return buf.WriteTo(w)
}

func writeTimestamp(w io.Writer, ts time.Time) (n int64, err error) {
	var buf [20]byte
	tsb := strconv.AppendInt(buf[0:0], ts.UnixNano(), 10)
	in, err := w.Write(tsb)
	return int64(in), err
}

func writeByte(w io.Writer, b byte) error {
	if bw, ok := w.(io.ByteWriter); ok {
		return bw.WriteByte(b)
	}

	buf := [1]byte{b}
	n, err := w.Write(buf[0:1])
	if err == nil && n == 0 {
		err = io.ErrShortWrite
	}
	return err
}
