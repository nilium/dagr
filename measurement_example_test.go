package dagr

import (
	"fmt"
	"os"
)

func ExampleWriteMeasurement() {
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

	WriteMeasurement(os.Stdout, m)
	// Output:
	// service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000
}

// Compiled measurements can be used to speed up encoding if you're frequently writing a large number of measurements.
// In most cases, this is entirely unnecessary. Compiled measurements only update their field values and are otherwise
// immutable.
func ExampleWriteMeasurement_compiled() {
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

	WriteMeasurement(os.Stdout, m)
	// Output:
	// service.some_event,host=example.local,pid=1234 depth=123.456,msg="a \"string\" of sorts",on=T,value=123i 1136214245000000000
}
