package dagr

import (
	"bytes"
	"encoding/json"
)

type jsonField struct {
	name  string
	value interface{}
}

type jsonFields []jsonField

func makeJSONFields(fields map[string]Field, order []string) jsonFields {
	fs := make([]jsonField, len(order))
	for i, name := range order {
		fs[i] = jsonField{name, fields[name]}
	}
	return fs
}

func makeJSONTags(tags map[string]string, order []string) jsonFields {
	fs := make([]jsonField, len(order))
	for i, name := range order {
		fs[i] = jsonField{name, tags[name]}
	}
	return fs
}

func (fs jsonFields) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')

	var err error
	enc := json.NewEncoder(&buf)
	write := func(v interface{}) {
		if err != nil {
			return
		}

		n := buf.Len()
		if err = enc.Encode(v); err != nil {
			return
		}

		if l := buf.Len(); l > n {
			buf.Truncate(l - 1)
		}
	}

	for i, f := range fs {
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		write(f.name)
		buf.WriteByte(':')
		write(f.value)
	}

	if err != nil {
		return nil, err
	}
	buf.WriteByte('}')

	return buf.Bytes(), nil
}
