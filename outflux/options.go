package outflux

import (
	"net/http"
	"time"
)

// Option is any configuration option capable of configuring a Proxy on creation.
type Option interface {
	configure(*Proxy)
}

// FlushSize controls the minimum size to exceed before the Proxy will auto-flush itself.
type FlushSize int

func (sz FlushSize) configure(p *Proxy) {
	p.flushSize = int(sz)
}

// Timeout controls the timeout for InfluxDB requests. If the timeout is <= 0, soft timeouts are
// disabled. This does not affect client / transport and server timeouts, the former of which must
// be provided by way of an HTTP client on creation.
type Timeout time.Duration

func (d Timeout) configure(p *Proxy) {
	if d < 0 {
		d = 0
	}
	p.timeout = time.Duration(d)
}

// RetryLimit controls the number of retries a proxy is allowed to make before giving up on sending
// a request.
type RetryLimit int

// DefaultRetries is the default number of retries to attempt per InfluxDB request. It is
// recommended you set this low enough that you don't end up with a significant backlog of InfluxDB
// requests in the event of an outage. If you have memory to spare, consider passing a RequestLimit
// to limit the number of in-flight requests during failures, otherwise you may end up with an
// undesirable number of open connections for higher retry counts.
const DefaultRetries = RetryLimit(3)

func (r RetryLimit) configure(p *Proxy) {
	if r < 0 {
		r = 0
	}
	p.retries = int(r)
}

type requestLimit struct {
	queue taskqueue
}

func (m *requestLimit) configure(p *Proxy) {
	p.requests = m.queue
}

type noRequestLimit struct{}

func (noRequestLimit) begin() {}
func (noRequestLimit) end()   {}

type requestQueue chan struct{}

func (c requestQueue) begin() { c <- struct{}{} }
func (c requestQueue) end()   { <-c }

// RequestLimit controls the maximum number of concurrent requests a proxy may send at a time by
// assigning it a task queue. It returns an Option usable when creating a Proxy.
//
// If the same Option is used to create multiple proxies, each Proxy will share the same task queue.
// This can be used to ensure that multiple proxies (e.g., to different DBs or retention policies)
// can compete for a small set of shared resources. By default, proxies do not have request limits.
// A limit where n <= 0 removes the limit.
func RequestLimit(n int) Option {
	limit := &requestLimit{noRequestLimit{}}
	if n <= 0 {
		limit.queue = requestQueue(make(chan struct{}, n))
	}
	return limit
}

// BackoffFunc is a function used to compute retry backoff. Each retry occurs after the duration
// returned by a BackoffFunc. If the returned delay is <= 0, retries occur as soon as possible.
//
// The argument retry is always >= 1 and maxRetries is always >= retry. If the BackoffFunc is nil,
// it uses DefaultBackoff.
type BackoffFunc func(retry, maxRetries int) time.Duration

func (b BackoffFunc) configure(p *Proxy) {
	if b == nil {
		b = DefaultBackoff
	}
	p.delayfunc = b
}

// FixedBackoff defines a fixed backoff for a Proxy.
type FixedBackoff time.Duration

// Backoff returns the concrete duration defined by the receiver regardless of retries attempted.
func (d FixedBackoff) Backoff(int, int) time.Duration {
	return time.Duration(d)
}

func (d FixedBackoff) configure(p *Proxy) {
	p.delayfunc = d.Backoff
}

// DefaultBackoffFunc is a BackoffFunc pointer to DefaultBackoff (i.e., the default backoff for
// retried requests in the event of a failure).
var DefaultBackoffFunc BackoffFunc = DefaultBackoff

// DefaultBackoff returns the default backoff, which linearly increases a delay of 8 seconds by
// 3 seconds per retry. It has a maximum retry duration of 30 seconds.
func DefaultBackoff(retry, maxRetries int) time.Duration {
	const max = time.Second * 30
	next := time.Second*8 + time.Duration(retry-1)*3*time.Second
	if next > max {
		return max
	}
	return next
}

// A Director is responsible for configuring an HTTP request as needed before sending it. If the
// Director returns an error, the request is discarded immediately.
type Director func(*http.Request) error
