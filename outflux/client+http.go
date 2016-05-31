package outflux

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

var ErrBadProtocol = errors.New("outflux: bad protocol")

type httpclient struct {
	destURL  *url.URL
	client   *http.Client
	director Director
}

func newHTTPClient(_ context.Context, u *url.URL) (Sender, error) {
	switch u.Scheme {
	case "http", "https":
	default:
		return nil, ErrBadProtocol
	}

	dup := new(url.URL)
	*dup = *u
	return &httpclient{destURL: dup}, nil
}

func init() {
	RegisterSenderType("http", newHTTPClient)
	RegisterSenderType("https", newHTTPClient)
}

func (c *httpclient) Close() error { return nil }

type withRequestContext interface {
	WithContext(context.Context) *http.Request
}

func (c *httpclient) Send(ctx context.Context, body []byte) (retry bool, err error) {
	var (
		dest = *c.destURL
		req  = &http.Request{
			Method:     "POST",
			URL:        &dest,
			Host:       dest.Host,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header: http.Header{
				http.CanonicalHeaderKey("Content-Type"): []string{""},
			},
			ContentLength: int64(len(body)),
			Body:          ioutil.NopCloser(bytes.NewReader(body)),
		}
		// TODO: Assign request context in Go 1.7
	)

	if err = c.director.direct(req); err != nil {
		return false, err
	}

	resp, err := ctxhttp.Do(ctx, c.client, req)
	if err != nil {
		if ne, ok := err.(net.Error); ok {
			return ne.Temporary(), err
		}
		return err != context.Canceled, err
	}

	defer func(body io.ReadCloser) {
		if _, copyerr := io.Copy(ioutil.Discard, body); copyerr != nil {
			if log := logger(); log != nil {
				log("Error discarding %s response body: %v", dest.Host, copyerr)
			}
		}
		logclose(body, "outflux response body")
	}(resp.Body)

	if resp.StatusCode != 204 {
		var sterr = &BadStatusError{Code: resp.StatusCode}
		sterr.Body, sterr.Err = ioutil.ReadAll(resp.Body)
		err = sterr
	}

	return false, err
}
