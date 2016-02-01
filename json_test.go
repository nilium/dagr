package dagr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

func ExamplePoint_MarshalJSON() {
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

	bs, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s", bs)
	// Output:
	// {
	//   "Key": "service.some_event",
	//   "Timestamp": "1136214245000000000",
	//   "Tags": {
	//     "depth": 123.456,
	//     "msg": "a \"string\" of sorts",
	//     "on": true,
	//     "value": 123
	//   },
	//   "Fields": {
	//     "host": "example.local",
	//     "pid": "1234"
	//   }
	// }
}

func BenchmarkJSONMeasurement(b *testing.B) {
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

	enc := json.NewEncoder(ioutil.Discard)
	for i := b.N; i > 0; i-- {
		if err := enc.Encode(m); err != nil {
			b.Fatal(err)
		}
	}
}
