// dubb is a package defining a naive double-buffer.
package dubb

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

var ErrClosed = errors.New("outflux: buffer is closed")

type (
	ByteBuffer interface {
		Truncate(n int)
		Grow(n int)
		Len() int
	}

	Reader interface {
		io.Reader
		io.WriterTo
		ByteBuffer
	}

	Writer interface {
		io.Writer
		io.ByteWriter
		WriteString(string) (int, error)

		ByteBuffer
	}

	ReadCloser interface {
		Reader
		io.Closer
	}

	WriteCloser interface {
		Writer
		io.Closer
	}

	// unlockCloser is a wrapper around a lockBuffer that unlocks its mutex upon close. Subsequent calls to Close
	// are no-ops and return ErrClosed.
	unlockCloser struct {
		*lockBuffer
		sig uint32
	}
)

var (
	_ = ReadCloser((*unlockCloser)(nil))
	_ = WriteCloser((*unlockCloser)(nil))
)

func (c *unlockCloser) Close() error {
	if atomic.CompareAndSwapUint32(&c.sig, 0, 1) {
		c.lockBuffer.m.Unlock()
		return nil
	}
	return ErrClosed
}

func (c *unlockCloser) check(shouldPanic bool) error {
	if atomic.LoadUint32(&c.sig) != 0 {
		if shouldPanic {
			panic("outflux: op on closed buffer")
		}
		return ErrClosed
	}
	return nil
}

func (c *unlockCloser) Write(b []byte) (n int, err error) {
	if err = c.check(false); err != nil {
		return 0, err
	}
	return c.lockBuffer.buf.Write(b)
}

func (c *unlockCloser) WriteByte(b byte) (err error) {
	if err = c.check(false); err != nil {
		return err
	}
	return c.lockBuffer.buf.WriteByte(b)
}

func (c *unlockCloser) WriteString(s string) (n int, err error) {
	if err = c.check(false); err != nil {
		return 0, err
	}
	return c.lockBuffer.buf.WriteString(s)
}

func (c *unlockCloser) Read(o []byte) (n int, err error) {
	if err = c.check(false); err != nil {
		return 0, err
	}
	return c.lockBuffer.buf.Read(o)
}

func (c *unlockCloser) Len() int {
	c.check(true)
	return c.lockBuffer.buf.Len()
}

func (c *unlockCloser) Grow(n int) {
	c.check(true)
	c.lockBuffer.buf.Grow(n)
}

func (c *unlockCloser) Truncate(n int) {
	c.check(true)
	c.lockBuffer.buf.Truncate(n)
}

func (c *unlockCloser) WriteTo(w io.Writer) (n int64, err error) {
	if err = c.check(false); err != nil {
		return 0, err
	}
	return c.lockBuffer.buf.WriteTo(w)
}

// lockBuffer is buffer that locks for every read/write operation.
type lockBuffer struct {
	buf bytes.Buffer
	m   sync.Mutex
}

func (lb *lockBuffer) Write(b []byte) (n int, err error) {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.Write(b)
}

func (lb *lockBuffer) WriteByte(b byte) error {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.WriteByte(b)
}

func (lb *lockBuffer) WriteString(s string) (n int, err error) {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.WriteString(s)
}

func (lb *lockBuffer) Read(o []byte) (n int, err error) {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.Read(o)
}

func (lb *lockBuffer) Len() int {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.Len()
}

func (lb *lockBuffer) Grow(n int) {
	lb.m.Lock()
	defer lb.m.Unlock()
	lb.buf.Grow(n)
}

func (lb *lockBuffer) Truncate(n int) {
	lb.m.Lock()
	defer lb.m.Unlock()
	lb.buf.Truncate(n)
}

func (lb *lockBuffer) WriteTo(w io.Writer) (n int64, err error) {
	lb.m.Lock()
	defer lb.m.Unlock()
	return lb.buf.WriteTo(w)
}

// Buffer is a double-buffered writer intended for concurrent accumulation into a buffer and writing the opposite buffer
// to an output stream. All writes and reads are locked to their specific buffers. Buffer necessarily implements both
// the Reader and Writer interfaces.
type Buffer struct {
	// face is the current writer, while 1-face is the current reader -- may only be accessed via atomics.
	face  uint32
	sides [2]*lockBuffer
}

func NewBuffer(capacity int) *Buffer {
	if capacity < 0 {
		panic("outflux: buffer capacity < 0")
	}

	buf := new(Buffer)
	for i := range buf.sides {
		buf.sides[i] = new(lockBuffer)
		if capacity > 0 {
			buf.sides[i].Grow(capacity)
		}
	}

	return buf
}

func (b *Buffer) compareAndSwap() bool {
	i := atomic.LoadUint32(&b.face)
	next := 1 - i&0x1
	return atomic.CompareAndSwapUint32(&b.face, i, next)
}

func (b *Buffer) Swap() {
	for i := range b.sides {
		b.sides[i].m.Lock()
		defer b.sides[i].m.Unlock()
	}

	for !b.compareAndSwap() {
	}
}

func (b *Buffer) reader() *lockBuffer {
	i := atomic.LoadUint32(&b.face) & 0x1
	return b.sides[i]
}

// Reader returns a fixed pointer to the underlying read buffer currently held. As with Writer, you should only take the
// Reader when you are in control of swaps on both the read and write buffers and can guarantee that Reader and Writer
// are not swapped while held.
func (b *Buffer) Reader() ReadCloser {
	r := b.reader()
	r.m.Lock()
	return &unlockCloser{lockBuffer: r}
}

func (b *Buffer) writer() *lockBuffer {
	i := atomic.LoadUint32(&b.face) & 0x1
	return b.sides[1-i]
}

func (b *Buffer) Len() int {
	// Returns the length of the writer
	return b.writer().Len()
}

// Writer returns a locked, fixed pointer to a writer. It must be closed when no longer in use to unlock the writer.
func (b *Buffer) Writer() WriteCloser {
	w := b.writer()
	w.m.Lock()
	return &unlockCloser{lockBuffer: w}
}

func (b *Buffer) Write(v []byte) (n int, err error) {
	return b.writer().Write(v)
}

func (b *Buffer) WriteByte(v byte) error {
	return b.writer().WriteByte(v)
}

func (b *Buffer) WriteString(s string) (n int, err error) {
	return b.writer().WriteString(s)
}

func (b *Buffer) Read(o []byte) (n int, err error) {
	return b.reader().Read(o)
}

func (b *Buffer) WriteTo(w io.Writer) (n int64, err error) {
	return b.reader().WriteTo(w)
}
