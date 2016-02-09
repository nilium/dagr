package outflux

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/nilium/dagr"
	"github.com/nilium/dagr/outflux/internal/dubb"
)

type BadStatusError int

func (e BadStatusError) Error() string {
	return fmt.Sprintf("outflux: bad status returned: %d", int(e))
}

type WriteFunc func(io.Writer) error

type Proxy struct {
	destURL *url.URL
	buffer  *dubb.Buffer
	client  *http.Client
	timeout time.Duration

	startOnce sync.Once
	flush     chan chan<- error
	cancel    context.CancelFunc
	ctx       context.Context
}

func NewURL(ctx context.Context, timeout time.Duration, client *http.Client, destURL *url.URL) *Proxy {
	if destURL == nil {
		panic("outflux: destination url is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	if client == nil {
		client = http.DefaultClient
	}

	return &Proxy{
		destURL: destURL,
		buffer:  dubb.NewBuffer(64000),
		client:  client,
		timeout: timeout,
		flush:   make(chan chan<- error),
		cancel:  cancel,
		ctx:     ctx,
	}
}

func New(ctx context.Context, timeout time.Duration, client *http.Client, destURL string) *Proxy {
	if destURL == "" {
		panic("outflux: destination url is nil")
	}

	du, err := url.Parse(destURL)
	if err != nil {
		panic(fmt.Sprintf("outflux: error parsing url: %v", err))
	}

	return NewURL(ctx, timeout, client, du)
}

// Close stops the Proxy's runloop, if it was ever started. The Proxy is no longer usable if closed.
func (w *Proxy) Close() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	w.cancel()
	return nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// Write writes the byte slice b to the write buffer of the Proxy. WriteMeasurements should be preferred to ensure that
// the writer is correctly sending InfluxDB line protocol messages, but may be used as a raw writer to the underlying
// Proxy buffers.
func (w *Proxy) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	return w.buffer.Write(b)
}

// Writer returns a locked writer for the Proxy's write buffer. It must be closed to release the lock.
func (w *Proxy) Writer() io.WriteCloser {
	if err := w.ctx.Err(); err != nil {
		return nopWriteCloser{ioutil.Discard}
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
	if err := w.ctx.Err(); err != nil {
		return err
	}

	wx := w.Writer()
	defer logclose(wx)
	return fn(wx)
}

// WriteMeasurements writes all measurements in measurements to the Proxy, effectively queueing them for delivery.
func (w *Proxy) WriteMeasurements(measurements ...dagr.Measurement) (n int64, err error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	} else if len(measurements) == 0 {
		return 0, nil
	}

	return dagr.WriteMeasurements(w, measurements...)
}

func (w *Proxy) WriteMeasurement(measurement dagr.Measurement) (n int64, err error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	return dagr.WriteMeasurement(w, measurement)
}

// WritePoint writes a single point to the Proxy.
func (w *Proxy) WritePoint(key string, when time.Time, tags dagr.Tags, fields dagr.Fields) (n int64, err error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

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

func (w *Proxy) Start(interval time.Duration) context.CancelFunc {
	w.startOnce.Do(func() {
		go w.sendEveryInterval(interval)
	})

	return w.cancel
}

func (w *Proxy) sendEveryInterval(interval time.Duration) {
	defer func() {
		// Flush on close
		w.swapAndSend(nil)
		w.destURL = nil
		w.buffer = nil
		w.client = nil
	}()

	var tick <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		tick = ticker.C
	}

	for {
		select {
		case <-w.ctx.Done(): // Dead
			return
		case <-tick: // Send after interval (ticker is nil if interval <= 0)
			w.swapAndSend(nil)
		case errch := <-w.flush: // Send forced
			w.swapAndSend(errch)
		}
	}
}

// Flush forces the Proxy to send out all buffered measurement data as soon as possible. It returns once the flush has
// been received by the Proxy or the Proxy is closed. This only works after Start() has been called.
//
// Flush will block until the write completes and return any relevant error that occurred during the send.
func (w *Proxy) Flush() error {
	errch := make(chan error)
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	case w.flush <- errch:
		return <-errch
	}
}

func (w *Proxy) swapAndSend(out chan<- error) {
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

	body, encoding := w.coder(rd)
	go func() {
		err := w.send(body, encoding)
		if err != nil {
			logf("Error sending request: %v", err)
		}

		if out != nil {
			out <- err
		}
	}()
}

func (w *Proxy) coder(rd dubb.Reader) (rc io.ReadCloser, encoding string) {
	var buf bytes.Buffer
	rd.WriteTo(&buf) // rd is now free for use elsewhere.

	if buf.Len() <= 1000 {
		// We truncate and close the dubb reader elsewhere, so make it a nop here.
		return ioutil.NopCloser(&buf), ""
	}

	pr, pw := io.Pipe()
	enc := gzip.NewWriter(pw)
	go func() {
		if _, err := buf.WriteTo(enc); err != nil {
			logf("Error writing measurements: %v", err)
		}
		if err := enc.Close(); err != nil {
			logf("Error closing gzip encoder: %v", err)
		}
		if err := pw.Close(); err != nil {
			logf("Error closing pipe writer: %v", err)
		}
	}()

	return pr, "gzip"
}

func (w *Proxy) send(body io.ReadCloser, encoding string) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}

	ctx := w.ctx
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

	req.Header.Set("Content-Type", "")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}

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
		const copiedSize int64 = 400
		var buf bytes.Buffer
		buf.Grow(copiedSize)
		if _, err := io.CopyN(&buf, resp.Body, copiedSize); err != nil && err != io.EOF {
			logf("Unable to copy body in measurement send: %v", err)
			return err
		}

		logf("Bad response from InfluxDB (%d): %s", resp.StatusCode, buf.Bytes())
		return BadStatusError(resp.StatusCode)
	}

	return nil
}
