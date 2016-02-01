package dagr

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestWriteEmptyMeasurement(t *testing.T) {
	defer prepareLogger(t)()

	var rb bytes.Buffer
	m := NewPoint(
		"balderdash.things_baldered",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		nil,
	)

	if _, err := WriteMeasurement(&rb, m); err != ErrNoFields {
		t.Errorf("WriteMeasurements(m) error:\nGot %#v\nExpected %#v", err, ErrNoFields)
	}

	if rb.Len() != 0 {
		t.Errorf("rb.Len: got=%d expected=%d", rb.Len(), 0)
	}
}

func TestWriteMeasurement(t *testing.T) {
	const required = `balderdash.things_baldered,host=example.local,pid=1234 on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(t)()

	integer := new(Int)
	boolean := new(Bool)

	boolean.Set(true)
	integer.Set(123)

	var rb bytes.Buffer
	m := NewPoint(
		"balderdash.things_baldered",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "on": boolean},
	)

	_, err := WriteMeasurement(&rb, m)
	if err != nil {
		t.Fatal(err)
	}

	if bs := rb.String(); bs != required {
		t.Errorf("Expected ---\n%s\n---\n\nGot ---\n%s\n---", required, bs)
	}
}

func TestWriteMeasurements(t *testing.T) {
	const required = `balderdash.things_baldered,host=example.local,pid=1234 on=T,value=123i 1136214245000000000` + "\n" +
		`system.cache_flustered,host=example.local,pid=5678 source=456i 1136214245000000000` + "\n"

	defer prepareLogger(t)()

	integer := new(Int)
	otherInt := new(Int)
	boolean := new(Bool)

	boolean.Set(true)
	integer.Set(123)
	otherInt.Set(456)

	var rb bytes.Buffer
	ms := []Measurement{
		NewPoint(
			"balderdash.things_baldered",
			Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
			Fields{"value": integer, "on": boolean},
		),
		NewPoint(
			"system.cache_flustered",
			Tags{"pid": fmt.Sprint(5678), "host": "example.local"},
			nil,
		),
		NewPoint(
			"system.cache_flustered",
			Tags{"pid": fmt.Sprint(5678), "host": "example.local"},
			Fields{"source": otherInt},
		),
	}

	_, err := WriteMeasurements(&rb, ms...)
	if err != nil {
		t.Fatal(err)
	}

	if bs := rb.String(); bs != required {
		t.Errorf("Expected ---\n%s\n---\n\nGot ---\n%s\n---", required, bs)
	}
}

func TestCompiledPoint(t *testing.T) {
	const required = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(t)()

	integer := new(Int)
	boolean := new(Bool)
	float := new(Float)
	str := new(String)

	integer.Set(123)
	boolean.Set(true)
	float.Set(123.456)
	str.Set(`a "string" of sorts`)

	var rb bytes.Buffer
	m := NewPoint(
		"service.some_event",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "depth": float, "on": boolean, "msg": str},
	)

	if _, err := m.WriteTo(&rb); err != nil {
		t.Fatal(err)
	}

	normal := rb.String()
	if normal != required {
		t.Errorf("[N] Expected %q\nGot %q", required, normal)
	}

	c := m.compile()
	var cb bytes.Buffer
	if _, err := c.WriteTo(&cb); err != nil {
		t.Fatal(err)
	}

	compiled := cb.String()
	if compiled != required {
		t.Errorf("[C] Expected %q\nGot %q", required, compiled)
	}

	if compiled != normal {
		t.Errorf("[CN] Expected %q\nGot %q", normal, compiled)
	}
}

func BenchmarkWriteMeasurement(b *testing.B) {
	const result = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(b)()

	integer := new(Int)
	boolean := new(Bool)
	float := new(Float)
	str := new(String)

	integer.Set(123)
	boolean.Set(true)
	float.Set(123.456)
	str.Set(`a "string" of sorts`)

	m := NewPoint(
		"service.some_event",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "depth": float, "on": boolean, "msg": str},
	)

	for i := b.N; i > 0; i-- {
		WriteMeasurement(ioutil.Discard, m)
	}
}

func BenchmarkWriteMeasurement_Compiled(b *testing.B) {
	const result = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(b)()

	integer := new(Int)
	boolean := new(Bool)
	float := new(Float)
	str := new(String)

	integer.Set(123)
	boolean.Set(true)
	float.Set(123.456)
	str.Set(`a "string" of sorts`)

	m := NewPoint(
		"service.some_event",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "depth": float, "on": boolean, "msg": str},
	).Compiled()

	for i := b.N; i > 0; i-- {
		WriteMeasurement(ioutil.Discard, m)
	}
}

func BenchmarkWriteMeasurement_Parallel(b *testing.B) {
	const result = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(b)()

	integer := new(Int)
	boolean := new(Bool)
	float := new(Float)
	str := new(String)

	integer.Set(123)
	boolean.Set(true)
	float.Set(123.456)
	str.Set(`a "string" of sorts`)

	m := NewPoint(
		"service.some_event",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "depth": float, "on": boolean, "msg": str},
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			WriteMeasurement(ioutil.Discard, m)
		}
	})
}

func BenchmarkWriteMeasurement_ParallelCompiled(b *testing.B) {
	const result = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

	defer prepareLogger(b)()

	integer := new(Int)
	boolean := new(Bool)
	float := new(Float)
	str := new(String)

	integer.Set(123)
	boolean.Set(true)
	float.Set(123.456)
	str.Set(`a "string" of sorts`)

	m := NewPoint(
		"service.some_event",
		Tags{"pid": fmt.Sprint(1234), "host": "example.local"},
		Fields{"value": integer, "depth": float, "on": boolean, "msg": str},
	).Compiled()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			WriteMeasurement(ioutil.Discard, m)
		}
	})
}
