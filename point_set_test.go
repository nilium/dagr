package dagr

import (
	"bytes"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPointSet(t *testing.T) {
	var output = []string{
		`http_request,host=example.local,path=/api/v1/kittens,pid=1234 count=2i,time_taken=1.7 1136214245000000000`,
		`http_request,host=example.local,path=/api/v1/puppies,pid=1234 count=1i,time_taken=0.1 1136214245000000000`,
	}

	defer prepareLogger(t)()

	p := NewPointSet(StaticPointAllocator{
		Key:           "http_request",
		Tags:          Tags{"host": "example.local", "pid": "1234"},
		IdentifierTag: "path",
		Fields:        Fields{"count": new(Int), "time_taken": new(Float)},
	})

	recordRequest := func(path string, elapsed time.Duration) {
		fields := p.FieldsForID(path, nil)
		t.Logf("Fields for identifier %q: %#+ v", path, fields)
		fields["count"].(*Int).Add(1)
		fields["time_taken"].(*Float).Add(elapsed.Seconds())
	}

	// Someone took extra long petting kittens
	recordRequest("/api/v1/kittens", time.Second+time.Millisecond*200)
	recordRequest("/api/v1/kittens", time.Millisecond*500)
	// PUPPIES WERE FOUND WANTING. ಠ_ಠ
	recordRequest("/api/v1/puppies", time.Millisecond*100)

	var buf bytes.Buffer
	if _, err := WriteMeasurement(&buf, p); err != nil {
		t.Errorf("Error writing PointSet: %v", err)
	}

	out := buf.String()
	if out == "" {
		t.Fatal("Output from WriteMeasurement is empty")
	}
	out = out[:len(out)-1]
	lines := strings.Split(out, "\n")
	sort.Strings(lines)
	if !reflect.DeepEqual(lines, output) {
		t.Errorf("Expected ---\n%s\n------------\nGot --------\n%s\n------------",
			strings.Join(lines, "\n"),
			strings.Join(output, "\n"),
		)
	}

	t.Logf("OUT=\n%s", out)
}

type testAllocator struct{}

func TestPointSetAllocator(t *testing.T) {
	var output = []string{
		`http_request,host=example.local,path=/api/v1/kittens,pid=1234 count=2i,time_taken=1.7 1136214245000000000`,
		`http_request,host=example.local,path=/api/v1/puppies,pid=1234 count=1i,time_taken=0.1 1136214245000000000`,
	}

	testAllocator := PointAllocFunc(func(ident string, _ interface{}) (key string, tags Tags, fields Fields) {
		if dir, base := path.Split(ident); dir == "/api/v1/" && base == "turtles" {
			// What? No, turtles aren't a thing.
			return "", nil, nil
		}

		return "http_request",
			Tags{
				"host": "example.local",
				"pid":  "1234",
				"path": ident,
			},
			Fields{
				"count":      new(Int),
				"time_taken": new(Float),
			}
	})

	defer prepareLogger(t)()

	p := NewPointSet(testAllocator)

	recordRequest := func(path string, elapsed time.Duration) {
		fields := p.FieldsForID(path, nil)
		t.Logf("Fields for identifier %q: %#+ v", path, fields)
		if fields == nil {
			return
		}
		fields["count"].(*Int).Add(1)
		fields["time_taken"].(*Float).Add(elapsed.Seconds())
	}

	// Someone took extra long petting kittens
	recordRequest("/api/v1/kittens", time.Second+time.Millisecond*200)
	recordRequest("/api/v1/kittens", time.Millisecond*500)
	// PUPPIES WERE FOUND WANTING. ಠ_ಠ
	recordRequest("/api/v1/puppies", time.Millisecond*100)
	// We don't record turtles but someone is very insistent that we ought to.
	for i := 0; i < 5; i++ {
		recordRequest("/api/v1/turtles", time.Hour*78839)
	}

	var buf bytes.Buffer
	if _, err := WriteMeasurement(&buf, p); err != nil {
		t.Errorf("Error writing PointSet: %v", err)
	}

	out := buf.String()
	if out == "" {
		t.Fatal("Output from WriteMeasurement is empty")
	}
	out = out[:len(out)-1]
	lines := strings.Split(out, "\n")
	sort.Strings(lines)
	if !reflect.DeepEqual(lines, output) {
		t.Errorf("Expected ---\n%s\n------------\nGot --------\n%s\n------------",
			strings.Join(lines, "\n"),
			strings.Join(output, "\n"),
		)
	}

	t.Logf("OUT=\n%s", out)
}
