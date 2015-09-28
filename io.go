package dagr

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// A mess of IO functions that may or may not be used at this time. Most of
// this is incomplete and should be regarded with suspicion, since it's
// definitely not likely to be sane.

var versionHeader = []byte{'d', 'g', 'r', 1}
var versionPrefix = versionHeader[0:3]

const sigBits uint64 = (^uint64(0)) >> 12
const expBits uint64 = ^sigBits

var ErrBadVersionHeader = errors.New("Version header didn't have the required prefix")
var ErrUnsupportedVersion = errors.New("Unsupported version code encountered")

type byteReader interface {
	io.Reader
	io.ByteReader
}

type failReader struct {
	r    io.Reader
	read int
	err  error
}

func (f *failReader) Read(out []byte) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err = f.r.Read(out)
	f.err = err
	f.read += n
	return n, err
}

func (f *failReader) ReadByte() (byte, error) {
	if f.err != nil {
		return 0, f.err
	}

	switch r := f.r.(type) {
	case io.ByteReader:
		b, err := r.ReadByte()
		if err == nil {
			f.read += 1
		}
		f.err = err
		return b, err
	}

	var store [1]byte
	_, err := f.Read(store[:])
	return store[0], err
}

type failWriter struct {
	w       io.Writer
	written int
	err     error
}

func (f *failWriter) Write(data []byte) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err = f.w.Write(data)
	f.err = err
	f.written += n
	return n, err
}

func writeI64(w io.Writer, i int64) (int, error) {
	return writeU64(w, uint64(i))
}

func writeI32(w io.Writer, i int32) (int, error) {
	return writeU32(w, uint32(i))
}

func writeI16(w io.Writer, i int16) (int, error) {
	return writeU16(w, uint16(i))
}

func writeU64(w io.Writer, i uint64) (n int, err error) {
	var b [8]byte
	b[0] = byte(i & 0xff)
	b[1] = byte((i >> 8) & 0xff)
	b[2] = byte((i >> 16) & 0xff)
	b[3] = byte((i >> 24) & 0xff)
	b[4] = byte((i >> 32) & 0xff)
	b[5] = byte((i >> 40) & 0xff)
	b[6] = byte((i >> 48) & 0xff)
	b[7] = byte((i >> 56) & 0xff)
	return w.Write(b[0:8])
}

func writeU32(w io.Writer, i uint32) (n int, err error) {
	var b [4]byte
	b[0] = byte(i & 0xff)
	b[1] = byte((i >> 8) & 0xff)
	b[2] = byte((i >> 16) & 0xff)
	b[3] = byte((i >> 24) & 0xff)
	return w.Write(b[0:4])
}

func writeU16(w io.Writer, i uint16) (n int, err error) {
	var b [2]byte
	b[0] = byte(i & 0xff)
	b[1] = byte((i >> 8) & 0xff)
	return w.Write(b[0:2])
}

func writeF64(w io.Writer, f float64) (n int, err error) {
	// TODO: Write floats with a prefix byte? indicating whether it's
	// a two-part varint or a regular IEEE-754 float64 (i.e.:)
	// return writeU64(w, math.Float64bits(f))

	u := math.Float64bits(f)
	sig := u & sigBits
	exp := (u & expBits) >> 52
	n, err = writeUvarint(w, sig)
	if err == nil {
		var vn int
		vn, err = writeUvarint(w, exp)
		n += vn
	}
	return n, err
}

func writeUvarint(w io.Writer, i uint64) (int, error) {
	var buf [binary.MaxVarintLen64]byte
	max := binary.PutUvarint(buf[:], i)
	return w.Write(buf[0:max])
}

func writeVarint(w io.Writer, i int64) (int, error) {
	var buf [binary.MaxVarintLen64]byte
	max := binary.PutVarint(buf[:], i)
	return w.Write(buf[0:max])
}

func readU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := r.Read(b[0:8]); err != nil && err != io.EOF {
		return 0, err
	} else {
		u := uint64(b[0]) |
			uint64(b[0])<<8 |
			uint64(b[0])<<16 |
			uint64(b[0])<<24 |
			uint64(b[0])<<32 |
			uint64(b[0])<<40 |
			uint64(b[0])<<48 |
			uint64(b[0])<<56
		return u, err
	}
}

func readF64(r byteReader) (f float64, err error) {
	// Reads a two-part float composed of a varint for both the mantissa
	// and exponent. This has the unusual property of usually reducing the
	// size of the written float enough that it's not unreasonable.

	// TODO: Fallback to regular F64 reads when they're smaller -- this
	// will likely require some flag indicated by the first byte of the
	// float or something equally weird.
	// if u, err := readU64(r); err != nil {
	// 	return 0, err
	// } else {
	// 	return math.Float64frombits(u), nil
	// }

	var exp, sig uint64

	if sig, err = binary.ReadUvarint(r); err != nil {
		return 0, err
	}

	if exp, err = binary.ReadUvarint(r); err != nil && err != io.EOF {
		return 0, err
	}

	exp = exp << 52

	f = math.Float64frombits(sig | exp)
	return f, err
}

// A VarString is defined as any string that begins with a uvarint describing
// its length followed by the string's data. E.g.,
// struct {
//      length varint
//      data [length]byte
// }

func writeVarString(w io.Writer, s string) (n int, err error) {
	n, err = writeUvarint(w, uint64(len(s)))
	if err == nil {
		var sn int
		sn, err = io.WriteString(w, s)
		n += sn
	}
	return n, err
}

func readVarString(r byteReader) (string, error) {
	length, err := binary.ReadUvarint(r)
	if err != nil && err != io.EOF {
		return "", err
	} else if err == io.EOF && length > 0 {
		return "", io.ErrUnexpectedEOF
	}

	if length == 0 {
		return "", err
	}

	b := make([]byte, int(length))
	n, err := r.Read(b)
	if n > 0 && uint64(n) <= length {
		return string(b[:n]), err
	}
	return "", err
}
