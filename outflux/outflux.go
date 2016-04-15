package outflux

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"go.spiff.io/dagr"
	"go.spiff.io/dagr/outflux/internal/dubb"
)

type BadStatusError int

func (e BadStatusError) Error() string {
	return fmt.Sprintf("outflux: bad status returned: %d", int(e))
}

type WriteFunc func(io.Writer) error

type Option interface {
	configure(*Proxy)
}

// FlushSize controls the minimum size to exceed before the Proxy will auto-flush itself.
type FlushSize int

func (sz FlushSize) configure(p *Proxy) {
	p.flushSize = int(sz)
}

// Timeout controls the timeout for InfluxDB requests. If the timeout is <= 0, soft timeouts are disabled. This does not
// affect client / transport and server timeouts.
type Timeout time.Duration

func (d Timeout) configure(p *Proxy) {
	if d < 0 {
		d = 0
	}
	p.timeout = time.Duration(d)
}

// Proxy is a basic InfluxDB line protocol proxy. You may write measurements to it either using dagr or just via
// functions such as fmt.Fprintf. Writes are accumulated for a given duration then POST-ed to the URL the Proxy was
// configured with. It is safe to write to the Proxy while it is sending. Concurrent writes to the Proxy are safe, but
// you should ensure that all writes are atomic and contain all necessary data or occur inside of a Transaction call to
// ensure that nothing slips in between writes.
type Proxy struct {
	destURL   *url.URL
	buffer    *dubb.Buffer
	client    *http.Client
	timeout   time.Duration
	flushSize int

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

	proxy := &Proxy{
		destURL: destURL,
		buffer:  dubb.NewBuffer(16000),
		client:  client,
		flush:   make(chan flushop),
	}

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
			logf("Flush failed after reaching capacity %d: %v", max, flerr)
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

type proxyWriter struct {
	dubb.WriteCloser
	p   *Proxy
	err error
}

func (w *proxyWriter) Close() error {
	if w.p != nil {
		p := w.p
		*w = proxyWriter{err: w.WriteCloser.Close()}
		p.flushExcess()
	}
	return w.err
}

// Writer returns a locked writer for the Proxy's write buffer. It must be closed to release the lock.
func (w *Proxy) Writer() io.WriteCloser {
	if w.flushSize > 0 {
		return &proxyWriter{WriteCloser: w.buffer.Writer(), p: w}
	}
	return w.buffer.Writer()
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
func (w *Proxy) Transaction(fn WriteFunc) error {
	wx := w.Writer()
	defer func() {
		logclose(wx)
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
		w.swapAndSend(context.Background(), nil)
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
	buf := w.buffer

	buf.Swap()
	rd := buf.Reader()
	defer func() {
		rd.Truncate(0)
		logclose(rd)
	}()

	if rd.Len() == 0 {
		// Nothing to do.
		return
	}

	go func(body io.ReadCloser, length int64) {
		err := w.send(ctx, body, length)
		if err != nil {
			logf("Error sending request: %v", err)
		}

		if out != nil {
			out <- err
		}
	}(w.coder(rd))
}

func (w *Proxy) coder(rd dubb.Reader) (rc io.ReadCloser, length int64) {
	var buf bytes.Buffer
	N, _ := rd.WriteTo(&buf) // rd is now free for use elsewhere.
	return ioutil.NopCloser(&buf), N
}

func (w *Proxy) send(ctx context.Context, body io.ReadCloser, contentLength int64) error {
	if timeout := w.timeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, err := http.NewRequest("POST", w.destURL.String(), body)
	if err != nil {
		logclose(body)
		logf("Error creating request: %v", err)
		return err
	}

	req.ContentLength = contentLength
	req.Header.Set("Content-Type", "")

	resp, err := ctxhttp.Do(ctx, w.client, req)
	if err != nil {
		logf("Error posting to InfluxDB: %v", err)
		return err
	}
	defer func() {
		// Discard response.
		if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
			logf("Error discarding InfluxDB response body: %v", err)
		}
		logclose(resp.Body)
	}()

	// Ideally we'll get status 204, but we discard anything from InfluxDB that's regarded as a success.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		const copiedSize = 400
		var buf bytes.Buffer
		buf.Grow(int(copiedSize))
		if _, err := io.CopyN(&buf, resp.Body, copiedSize); err != nil && err != io.EOF {
			logf("Unable to copy body in measurement send: %v", err)
			return err
		}

		logf("Bad response from InfluxDB (%d): %s", resp.StatusCode, buf.Bytes())
		return BadStatusError(resp.StatusCode)
	}

	return nil
}
