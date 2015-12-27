package dagr

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestCompiledPoint(t *testing.T) {
	const required = `service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000` + "\n"

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
