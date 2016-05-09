package outflux

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

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
	destURL *url.URL
	buffer  *bufferchain
	client  *http.Client

	timeout   time.Duration
	flushSize int

	director Director
	requests taskqueue

	retries   int
	delayfunc BackoffFunc

	startOnce sync.Once
	flush     chan flushop
}

// NewURL allocates a new Proxy with a given context, HTTP client, and URL. If the URL is nil, NewURL panics. If the
// context is nil, a new background context is allocated specifically for the Proxy.
//
// Additional configuration can be provided by passing Option values, such as Timeout and FlushSize.
//
// If the HTTP client given is nil, NewURL will use http.DefaultClient.
func NewURL(client *http.Client, destURL *url.URL, opts ...Option) *Proxy {
	if destURL == nil {
		panic("outflux: destination url is nil")
	}

	if client == nil {
		client = http.DefaultClient
	}

	buffers := 6
	if n := (runtime.NumCPU() * 17) / 10; n > buffers {
		buffers = n
	}
	proxy := &Proxy{
		destURL:  destURL,
		buffer:   newBufferchain(buffers, 4000),
		client:   client,
		requests: noRequestLimit{},
		flush:    make(chan flushop),
	}

	DefaultBackoffFunc.configure(proxy)
	DefaultRetries.configure(proxy)

	for _, opt := range opts {
		opt.configure(proxy)
	}

	return proxy
}

// New allocates a new Proxy with the given context, HTTP client, and URL. Unlike NewURL, this will parse the URL first.
// If the URL is empty, New panics. See NewURL for further information.
func New(client *http.Client, destURL string, opts ...Option) *Proxy {
	if destURL == "" {
		panic("outflux: destination url is nil")
	}

	du, err := url.Parse(destURL)
	if err != nil {
		panic(fmt.Sprintf("outflux: error parsing url: %v", err))
	}

	return NewURL(client, du, opts...)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// flushExcess attempts to flush the proxy's write buffer to InfluxDB if it exceeds the current flush size.
func (w *Proxy) flushExcess() {
	max := w.flushSize
	if max <= 0 || w.buffer.Len() < max {
		return
	}
	go func() {
		if flerr := w.Flush(context.Background()); flerr != nil {
			logf("Flush failed after reaching capacity=%d: %v", max, flerr)
		}
	}()
}

// Write writes the byte slice b to the write buffer of the Proxy. WriteMeasurements should be preferred to ensure that
// the writer is correctly sending InfluxDB line protocol messages, but may be used as a raw writer to the underlying
// Proxy buffers.
func (w *Proxy) Write(b []byte) (int, error) {
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

		w.swapAndSend(ctx, nil)
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
			w.swapAndSend(ctx, nil)
		case op := <-w.flush: // Send forced
			w.swapAndSend(op.ctx, op.err)
		}

		// Reset timer if we're using that and not just flushing manually.
		if timer != nil {
			timer.Reset(interval)
		}
	}
}

type flushop struct {
	ctx context.Context
	err chan<- error
}

// Flush forces the Proxy to send out all buffered measurement data as soon as possible. It returns once the flush has
// been received by the Proxy or the Proxy is closed. This only works after Start() has been called.
//
// Flush will block until the write completes and return any relevant error that occurred during the send.
//
// The context may not be nil.
func (w *Proxy) Flush(ctx context.Context) error {
	if ctx == nil {
		panic("outflux: context is nil")
	}
	errch := make(chan error, 1)
	done := ctx.Done()
	select {
	case <-done:
		return ctx.Err()
	case w.flush <- flushop{ctx, errch}:
		select {
		case err := <-errch:
			return err
		case <-done:
			return ctx.Err()
		}
	}
}

func (w *Proxy) swapAndSend(ctx context.Context, out chan<- error) {
	data := w.buffer.flush()
	if len(data) == 0 {
		// Nothing to do.
		return
	}

	go func() {
		err := w.sendData(ctx, data, w.retries)
		if out != nil {
			out <- err
		}
	}()
}

func (w *Proxy) sendData(ctx context.Context, data []byte, retries int) error {
	var (
		done  = ctx.Done()
		retry bool
		err   error
	)

retryLoop:
	for i := 0; i <= retries; i++ {
		retry, err = w.send(ctx, bytes.NewReader(data))
		if err == nil {
			return nil
		}

		if !retry {
			break retryLoop
		}

		next := w.delayfunc(i+1, retries)
		if next <= 0 {
			// Send now. If there's a context error, it'll be caught by send().
			continue
		}

		select {
		case <-time.After(next):
		case <-done:
			break retryLoop
		}
	}

	if err != nil {
		logf("Failed to send payload of size=%d to %s: %v", len(data), w.destURL.Host, err)
	}

	return err
}

func (w *Proxy) send(ctx context.Context, body *bytes.Reader) (retry bool, err error) {
	if err = ctx.Err(); err != nil {
		// Whatever is returned here will necessarily all future retries.
		return false, err
	}

	if timeout := w.timeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	w.requests.begin()
	defer w.requests.end()

	req, err := http.NewRequest("POST", w.destURL.String(), body)
	if err != nil {
		logf("Error creating request: %v", err)
		// Error occurred creating the request itself, we can't do anything
		return false, err
	}

	req.Header.Set("Content-Type", "")
	if w.director != nil {
		if err := w.director(req); err != nil {
			return false, err
		}
	}

	resp, err := ctxhttp.Do(ctx, w.client, req)
	if err != nil {
		if err == context.Canceled {
			return false, err
		}

		logf("Error posting to InfluxDB: %v", err)
		return true, err
	}
	defer func() {
		// Discard response.
		if _, copyerr := io.Copy(ioutil.Discard, resp.Body); copyerr != nil {
			logf("Error discarding InfluxDB response body: %v", copyerr)
		}
		logclose(resp.Body)
	}()

	// Per InfluxDB docs (0.11-ish I think, but possibly earlier), anything other than 204,
	// including status 200, is an error.
	if resp.StatusCode != 204 {
		var sterr = &BadStatusError{Code: resp.StatusCode}
		sterr.Body, sterr.Err = ioutil.ReadAll(resp.Body)

		logf("Bad response from InfluxDB: %v", sterr)
		return false, sterr // InfluxDB rejected the response, so discard it.
	}

	return false, nil
}
