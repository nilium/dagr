package outflux

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

var errDoubleClose = errors.New("outflux: buffer already closed")

type closerfunc func() error

func (fn closerfunc) Close() error { return fn() }

type bufferchain struct {
	buffers chan *bytes.Buffer
	all     []*bytes.Buffer
	sz      int64
	m       sync.RWMutex
}

func newBufferchain(n, cap int) *bufferchain {
	if n <= 0 {
		panic("outflux: bufferchain length must be >= 1")
	}

	b := &bufferchain{
		buffers: make(chan *bytes.Buffer, n),
		all:     make([]*bytes.Buffer, n),
	}
	for i := range b.all {
		buf := new(bytes.Buffer)
		if cap > 0 {
			buf.Grow(cap)
		}
		b.all[i] = buf
		b.buffers <- buf
	}

	return b
}

func (b *bufferchain) Len() int {
	b.m.RLock()
	n := atomic.LoadInt64(&b.sz)
	b.m.RUnlock()
	return int(n)
}

func (b *bufferchain) take() *lockcloser {
	b.m.RLock()
	var (
		ch   = b.buffers
		buf  = <-ch
		done int32
	)
	close := func() error {
		if !atomic.CompareAndSwapInt32(&done, 0, 1) {
			return errDoubleClose
		}
		b.m.RUnlock()
		ch <- buf
		return nil
	}
	return &lockcloser{buf, &b.sz, closerfunc(close)}
}

func (b *bufferchain) Write(data []byte) (n int, err error) {
	buf := b.take()
	defer buf.Close()
	return buf.Write(data)
}

func (b *bufferchain) WriteString(str string) (n int, err error) {
	buf := b.take()
	defer buf.Close()
	return buf.WriteString(str)
}

func (b *bufferchain) WriteByte(oct byte) (err error) {
	buf := b.take()
	defer buf.Close()
	return buf.WriteByte(oct)
}

func (b *bufferchain) ReadFrom(r io.Reader) (n int64, err error) {
	buf := b.take()
	defer buf.Close()
	return buf.ReadFrom(r)
}

func (b *bufferchain) Writer() io.WriteCloser {
	return b.take()
}

func (b *bufferchain) flush() []byte {
	defer b.m.Unlock()
	b.m.Lock()

	n := 0
	for _, b := range b.all {
		n += b.Len()
	}

	if n == 0 {
		return nil
	}

	i := 0
	data := make([]byte, n)
	for _, b := range b.all {
		if n := b.Len(); n > 0 {
			i += copy(data[i:], b.Bytes())
			b.Reset()
		}
	}

	atomic.StoreInt64(&b.sz, 0)

	return data
}

// lockcloser is a wrapper around a bytes.Buffer that, upon closing, releases a lock.
type lockcloser struct {
	buf    *bytes.Buffer
	size   *int64
	closer io.Closer
}

func (b *lockcloser) Len() int {
	if b.buf == nil {
		return 0
	}
	return b.buf.Len()
}

func (b *lockcloser) Grow(cap int) {
	if b.buf != nil {
		b.buf.Grow(cap)
	}
}

func (b *lockcloser) Cap() int {
	if b.buf == nil {
		return 0
	}
	return b.buf.Cap()
}

func (b *lockcloser) Write(data []byte) (n int, err error) {
	if b.buf == nil {
		return 0, errDoubleClose
	}
	n, err = b.buf.Write(data)
	atomic.AddInt64(b.size, int64(n))
	return n, err
}

func (b *lockcloser) WriteString(s string) (n int, err error) {
	if b.buf == nil {
		return 0, errDoubleClose
	}
	n, err = b.buf.WriteString(s)
	atomic.AddInt64(b.size, int64(n))
	return n, err
}

func (b *lockcloser) WriteByte(oct byte) (err error) {
	if b.buf == nil {
		return errDoubleClose
	}

	err = b.buf.WriteByte(oct)
	atomic.AddInt64(b.size, 1)
	return err
}

func (b *lockcloser) ReadFrom(r io.Reader) (int64, error) {
	if b.buf == nil {
		return 0, errDoubleClose
	}
	n, err := b.buf.ReadFrom(r)
	atomic.AddInt64(b.size, n)
	return n, err
}

func (b *lockcloser) Close() error {
	if b.buf == nil {
		return errDoubleClose
	}

	err := b.closer.Close()
	*b = lockcloser{}
	return err
}
