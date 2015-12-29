package dagr

import (
	"io"
	"time"
)

type compiledField struct {
	from, to int
	value    Field
}

type compiledPoint struct {
	prefix []byte
	lead   int
	fields []compiledField
}

var _ = Measurement(compiledPoint{})
var _ = SnapshotMeasurement(compiledPoint{})

func (c compiledPoint) WriteTo(w io.Writer) (int64, error) {
	buf := getBuffer(w)
	defer putBuffer(buf)

	buf.Write(c.prefix[:c.lead])
	for _, f := range c.fields {
		if f.from < f.to {
			buf.Write(c.prefix[f.from:f.to])
		}

		if _, err := f.value.WriteTo(buf); err != nil {
			buf.Truncate(int(buf.head))
			return 0, err
		}
	}

	buf.WriteByte(' ')
	writeTimestamp(buf, clock.Now())
	buf.WriteByte('\n')

	return buf.WriteTo(w)
}

// compiledPoints are strictly for io.WriterTo usage and don't support regular Measurement options

func (c compiledPoint) GetKey() string {
	return ""
}

func (c compiledPoint) GetFields() Fields {
	return nil
}

func (c compiledPoint) GetTags() Tags {
	return nil
}

type fixedCompiledPoint struct {
	compiledPoint
	when time.Time
}

func (f fixedCompiledPoint) GetTime() time.Time {
	return f.when
}

func (c compiledPoint) Snapshot() TimeMeasurement {
	return fixedCompiledPoint{c, clock.Now()}
}
