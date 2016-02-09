package outflux

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/nilium/dagr"
)

func gzipMeasurementsBody(measurements ...dagr.Measurement) (encoding string, length int64, body io.ReadCloser, err error) {
	const minGZIPLength = 1000 // 1kb

	var buf bytes.Buffer
	if _, err = dagr.WriteMeasurements(&buf, measurements...); err != nil {
		return "", 0, nil, err
	}

	if N := buf.Len(); N < minGZIPLength {
		return "", int64(buf.Len()), ioutil.NopCloser(&buf), nil
	}

	// GZIP-encode the body
	r, w := io.Pipe()
	enc := gzip.NewWriter(w)
	go func() {
		if _, werr := buf.WriteTo(enc); werr != nil {
			logf("Error writing measurements: %v", werr)
		}
		if cerr := enc.Close(); cerr != nil {
			logf("Error closing gzip encoder: %v", cerr)
		}
		if cerr := w.Close(); cerr != nil {
			logf("Error closing pipe writer: %v", cerr)
		}
	}()

	return encoding, 0, r, nil
}

// SendMeasurements sends the dagr Measurements to the given URL as a POST request. If an error occurs, that error is
// returned.
//
// The request is GZIP-encoded if it exceeds 1kb (1000 bytes) in length.
func SendMeasurements(ctx context.Context, url *url.URL, client *http.Client, measurements ...dagr.Measurement) (err error) {
	if ctx == nil {
		panic("outflux: SendMeasurements: context is nil")
	} else if err = ctx.Err(); err != nil {
		return err
	}

	if url == nil {
		panic("outflux: SendMeasurements: url is nil")
	}

	encoding, length, body, err := gzipMeasurementsBody(measurements...)
	if err != nil {
		return err
	}

	requrl := *url
	req := http.Request{
		Method:        "POST",
		URL:           &requrl,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          body,
		Host:          requrl.Host,
		ContentLength: length,
	}

	if err != nil {
		logclose(body)
		logf("Error creating request: %v", err)
		return err
	}

	req.Header.Set("Content-Type", "")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}

	resp, err := ctxhttp.Do(ctx, client, &req)
	if err != nil {
		logf("Error posting to InfluxDB: %v", err)
		return err
	}
	defer func() {
		// Discard entire response so we can let the client reuse connections if it's set up to do so.
		_, copyerr := io.Copy(ioutil.Discard, resp.Body)
		if copyerr != nil && copyerr != io.EOF {
			logf("Error copying response body to /dev/null: %v", err)
		}
		logclose(resp.Body)
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		const copiedSize = 400
		var buf bytes.Buffer
		buf.Grow(copiedSize)
		if _, copyerr := io.CopyN(&buf, resp.Body, copiedSize); copyerr != nil && copyerr != io.EOF {
			logf("Error copying response from response body, discarding: %v", copyerr)
			return err
		}

		logf("Bad response from InfluxDB (%d): %s", resp.StatusCode, buf.Bytes())
		return err
	}

	return err
}
