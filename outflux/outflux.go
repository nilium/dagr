package outflux

import (
	"errors"
	"io"
	"net/url"
	"runtime"
	"sync"
	"time"

	"golang.org/x/net/context"

	"go.spiff.io/dagr"
)

// A taskqueue is a simple queue that is used to signal the start and end of a request. It must
// implement begin() and end() methods. The former must block until there are resources available to
// perform a task (the queue is assumed to be aware of this without being given a concrete task) and
// block subsequent tasks while resources are unavailable.
//
// For a taskqueue with a capacity of 1, this would require only one task to begin and block others
// until that task called end(), then repeating the same for another task.
type taskqueue interface {
	begin()
	end()
}

// WriteFunc is any transactional function that accepts an io.Writer. The writer received by the
// WriteFunc is only valid until it returns.
type WriteFunc func(io.Writer) error

// Proxy is a basic InfluxDB line protocol proxy. You may write measurements to it either using dagr or just via
// functions such as fmt.Fprintf. Writes are accumulated for a given duration then POST-ed to the URL the Proxy was
// configured with. It is safe to write to the Proxy while it is sending. Concurrent writes to the Proxy are safe, but
// you should ensure that all writes are atomic and contain all necessary data or occur inside of a Transaction call to
// ensure that nothing slips in between writes.
type Proxy struct {
	sender Sender
	buffer *bufferchain

	timeout   time.Duration
	flushSize int
	flushlock sync.Mutex

	director Director
	requests taskqueue

	seq       int64
	retries   int
	delayfunc BackoffFunc

	startOnce sync.Once
	flush     chan flushop
}

var (
	ErrNoSender = errors.New("outflux: proxy sender not configured")
	ErrNoURL    = errors.New("outflux: URI is nil")
	ErrNoWriter = errors.New("outflux: writer is nil")
)

func newProxy(ctx context.Context, sender Sender, opts ...Option) *Proxy {
	buffers := 6
	if n := (runtime.NumCPU() * 17) / 10; n > buffers {
		buffers = n
	}
	proxy := &Proxy{
		sender:   sender,
		buffer:   newBufferchain(buffers, 4000),
		requests: noRequestLimit{},
		flush:    make(chan flushop),
	}

	DefaultBackoffFunc.configure(proxy)
	DefaultRetries.configure(proxy)
	proxy.configure(ctx, opts...)

	return proxy
}

// NewURL allocates a new Proxy with a given context, HTTP client, and URL. If the URL is nil, NewURL panics. If the
// context is nil, a new background context is allocated specifically for the Proxy.
//
// Additional configuration can be provided by passing Option values, such as Timeout and FlushSize.
func NewURL(ctx context.Context, destURL *url.URL, opts ...Option) (*Proxy, error) {
	if destURL == nil {
		return nil, ErrNoURL
	}

	sender, err := allocSender(ctx, destURL)
	if err != nil {
		return nil, err
	} else if sender == nil {
		return nil, ErrNoSender
	}

	return newProxy(ctx, sender, opts...), nil
}

// NewWriter allocates a new proxy that writes its buffered output to dst. This will only return ErrNoWriter if dst is
// nil.
func NewWriter(ctx context.Context, dst io.Writer, opts ...Option) (*Proxy, error) {
	if dst == nil {
		return nil, ErrNoWriter
	}

	return newProxy(ctx, newWriterClient(ctx, dst), opts...), nil
}

// New allocates a new Proxy with the given context, HTTP client, and URL. Unlike NewURL, this will parse the URL first.
// If the URL is empty, New panics. See NewURL for further information.
func New(ctx context.Context, destURL string, opts ...Option) (*Proxy, error) {
	if destURL == "" {
		return nil, ErrNoURL
	}

	du, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}

	return NewURL(ctx, du, opts...)
}

func (w *Proxy) configure(ctx context.Context, opts ...Option) {
	for _, opt := range opts {
		if po, ok := opt.(proxyOption); ok {
			po.configure(w)
		}

		if so, ok := opt.(SenderOption); ok {
			so.Configure(ctx, w.sender)
		}
	}
}

// flushExcess attempts to flush the proxy's write buffer to InfluxDB if it exceeds the current flush size.
func (w *Proxy) flushExcess() {
	max := w.flushSize
	if max <= 0 {
		return
	}

	var (
		unlock func()
		once   sync.Once
		length int
	)

loop:
	length = w.buffer.Len()
	if length < max {
		if unlock != nil {
		}
		return
	}

	if unlock == nil {
		w.flushlock.Lock()
		unlock = func() { once.Do(w.flushlock.Unlock) }
		defer unlock()
		goto loop
	}

	if flerr := w.flushWithCapacity(context.Background(), max, unlock); flerr != nil {
		logf("Flush failed after reaching capacity=%d: %v", max, flerr)
	}
}

// Write writes the byte slice b to the write buffer of the Proxy. WriteMeasurements should be preferred to ensure that
// the writer is correctly sending InfluxDB line protocol messages, but may be used as a raw writer to the underlying
// Proxy buffers.
func (w *Proxy) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n, err := w.buffer.Write(b)
	w.flushExcess()
	return n, err
}

// Writer returns a locked writer for the Proxy's write buffer. It must be closed to release the lock. Changes to the
// Writer are not counted against the flush size, as the writer is not tracked by the Proxy.
func (w *Proxy) Writer() io.WriteCloser {
	wc := w.buffer.take()
	if w.flushSize > 0 {
		closer := wc.closer
		wc.closer = closerfunc(func() error {
			if err := closer.Close(); err != nil {
				return err
			}
			w.flushExcess()
			return nil
		})
	}
	return wc
}

// Transaction locks the Proxy's write buffer and passes it to fn. Once fn completes, the lock is released. This is
// shorthand for just doing that yourself, in the event that you have a function to pass a writer to but want to avoid
// writing extra code. For example:
//
//      err := proxy.Transaction(func(w io.Writer) {
//              _, err := buf.WriteTo(w)
//              return err
//      })
//
//      if err != nil {
//              // ...
//      }
//
// The WriteFunc given may return an error. This has no effect on the outcome of the transaction and is entirely for
// convenience. If the Proxy is closed, it will return the context error for its closure.
func (w *Proxy) Transaction(fn WriteFunc) (err error) {
	wx := w.Writer()
	defer func() {
		if clerr := logclose(wx); err == nil {
			err = clerr
		}
		w.flushExcess()
	}()
	return fn(wx)
}

// WriteMeasurements writes all measurements in measurements to the Proxy, effectively queueing them for delivery.
func (w *Proxy) WriteMeasurements(measurements ...dagr.Measurement) (n int64, err error) {
	if len(measurements) == 0 {
		return 0, nil
	}

	return dagr.WriteMeasurements(w, measurements...)
}

// WriteMeasurement writes a single measurement to the Proxy.
func (w *Proxy) WriteMeasurement(measurement dagr.Measurement) (n int64, err error) {
	return dagr.WriteMeasurement(w, measurement)
}

// WritePoint writes a single point to the Proxy.
func (w *Proxy) WritePoint(key string, when time.Time, tags dagr.Tags, fields dagr.Fields) (n int64, err error) {
	if key == "" {
		logf("Empty key in point")
		return 0, dagr.ErrEmptyKey
	} else if len(fields) == 0 {
		logf("No fields in point %q", key)
		return 0, dagr.ErrNoFields
	}

	if when.IsZero() {
		when = time.Now()
	}

	return dagr.WriteMeasurement(w, dagr.RawPoint{key, tags, fields, when})
}

// Start creates a goroutine that POSTs buffered data at the given interval. If interval is not a positive duration, the
// Proxy will only send data when you call Flush or if the Proxy has been configured to send when exceeding a certain
// buffer size. The context passed may be used to signal cancellation or provide a hard deadline for the proxy to stop
// by.
//
// The context may not be nil.
func (w *Proxy) Start(ctx context.Context, interval time.Duration) {
	if ctx == nil {
		panic("outflux: context is nil")
	}

	w.startOnce.Do(func() {
		go w.sendEveryInterval(ctx, interval)
	})
}

func (w *Proxy) sendEveryInterval(ctx context.Context, interval time.Duration) {
	defer func() {
		// Flush on close -- use a different context, though.
		ctx := context.Background()
		if timeout := w.timeout; timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		w.swapAndSend(flushop{ctx: ctx, capacity: -1})
	}()

	var tick <-chan time.Time
	var timer *time.Timer
	if interval > 0 {
		timer = time.NewTimer(interval)
		tick = timer.C
		defer timer.Stop()
	}

	done := ctx.Done()
	for {
		select {
		case <-done: // Dead
			return
		case <-tick: // Send after interval (ticker is nil if interval <= 0)
			w.swapAndSend(flushop{ctx: ctx, capacity: -1})
		case op := <-w.flush: // Send forced
			w.swapAndSend(op)
			continue
		}

		// Reset timer if we're using that and not just flushing manually.
		if timer != nil {
			timer.Reset(interval)
		}
	}
}

type flushop struct {
	ctx      context.Context
	err      chan<- error
	capacity int

	// swapFunc is a callback executed once the buffer has been swapped for a flush. This can be
	// used to allow other goroutines to fail out of a swap if the buffer hasn't exceeded its
	// limit.
	swapFunc func()
}

func (f *flushop) swapped() {
	if f.swapFunc != nil {
		f.swapFunc()
	}
}

func (f *flushop) reply(err error) {
	if f.err != nil {
		select {
		case <-f.ctx.Done():
		case f.err <- err:
		}
	}
}

// Flush forces the Proxy to send out all buffered measurement data as soon as possible. It returns once the flush has
// been received by the Proxy or the Proxy is closed. This only works after Start() has been called.
//
// Flush will block until the write completes and return any relevant error that occurred during the send.
//
// The context may not be nil.
func (w *Proxy) Flush(ctx context.Context) error {
	return w.flushWithCapacity(ctx, -1, nil)
}

func (w *Proxy) flushWithCapacity(ctx context.Context, capacity int, swapped func()) error {
	if ctx == nil {
		logf("outflux: flushWithCapacity: context is nil")
		ctx = context.TODO()
	}

	var (
		errch = make(chan error, 1)
		done  = ctx.Done()
		op    = flushop{ctx, errch, capacity, swapped}
	)
	select {
	case <-done:
		op.swapped()
		return ctx.Err()
	case w.flush <- op:
	}

	select {
	case err := <-errch:
		return err
	case <-done:
		return ctx.Err()
	}
}

func (w *Proxy) swapAndSend(op flushop) {
	if op.capacity < 0 {
		// Always Flush
	} else if buflen := w.buffer.Len(); buflen < op.capacity {
		// flushExcess: buffer too small
		op.swapped()
		op.reply(nil)
		return
	}

	data := w.buffer.flush()
	op.swapped()

	if len(data) == 0 {
		// Nothing to do.
		op.reply(nil)
		return
	}

	go func() {
		var err error
		defer func() { op.reply(err) }()

		err = w.sendData(op.ctx, data, w.retries)
	}()
}

func (w *Proxy) sendData(ctx context.Context, data []byte, retries int) error {
	var (
		try = func(ctx context.Context) (retry bool, err error) {
			w.requests.begin()
			defer w.requests.end()

			if timeout := w.timeout; timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			return w.sender.Send(ctx, data)
		}

		done  = ctx.Done()
		retry bool
		err   error
	)

retryLoop:
	for i := 0; i <= retries; i++ {
		if err = ctx.Err(); err != nil {
			// Whatever is returned here will necessarily all future retries.
			break retryLoop
		}

		retry, err = try(ctx)
		if err == nil {
			return nil
		}

		if !retry || err == context.Canceled {
			logf("Failed sending payload of size=%d via %v - will not retry: %v", len(data), w.sender, err)
			break retryLoop
		}

		next := w.delayfunc(i+1, retries)
		if next <= 0 {
			// Send now. If there's a context error, it'll be caught by send().
			continue retryLoop
		}

		logf("Error sending payload of size=%d via %v - will retry in %v: %v", len(data), w.sender, next, err)
		select {
		case <-time.After(next):
		case <-done:
			if err == nil {
				err = ctx.Err()
			}
			break retryLoop
		}
	}

	if err != nil {
		logf("Failed to send payload of size=%d via %v: %v", len(data), w.sender, err)
	}

	return err
}
