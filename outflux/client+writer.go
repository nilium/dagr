package outflux

import (
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"
)

func boolopt(v string) bool {
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	return err != nil && b
}

type writerclient struct {
	w      io.Writer
	closed bool
	m      sync.Mutex
}

const (
	fdScheme   = "fd"
	fileScheme = "file"
)

// ErrBadFDPath is returned when creating an fd:/ client with a path and no FD integer.
var ErrBadFDPath = errors.New("outflux: invalid fd path")

func init() {
	// file:///foo/bar/baz
	// file:/../foo
	// file:///C:/Windows/System32/User32.dll
	RegisterSenderType(fileScheme, newFileClient)
	// fd:1234 (extracts FD from Opaque)
	// fd:/1234 (extracts from Path)
	RegisterSenderType(fdScheme, newFileClient)
}

func newWriterClient(ctx context.Context, w io.Writer) Sender {
	return &writerclient{w: w}
}

func newFDClient(ctx context.Context, uri *url.URL) (Sender, error) {
	var (
		fdstr string
		fd    uint64
		name  string
		err   error
		w     io.Writer
		opts  = uri.Query()
	)

	switch {
	case uri.Opaque != "":
		fdstr = uri.Opaque
	case uri.Path != "":
		fdstr = uri.Path
		if fdstr[0] == '/' {
			fdstr = fdstr[1:]
		}

		if fdstr == "" {
			return nil, ErrBadFDPath
		}
	}

	if fd, err = strconv.ParseUint(fdstr, 10, 64); err != nil {
		return nil, err
	}

	if name = opts.Get("name"); name == "" {
		name = uri.String()
	}

	w = os.NewFile(uintptr(fd), name)

	return newWriterClient(ctx, w), nil
}

func newFileClient(ctx context.Context, uri *url.URL) (Sender, error) {
	var (
		w   io.Writer
		err error
	)

	// If /dev/null or similar (depending on OS), just use the discard writer
	if uri.Path == os.DevNull {
		return newWriterClient(ctx, ioutil.Discard), nil
	}

	// Try to forward this to newFDClient
	const devFDPrefix = "/dev/fd/"
	if strings.HasPrefix(uri.Path, devFDPrefix) {
		fduri := *uri
		fduri.Scheme, fduri.Opaque = "fd", uri.Path[len(devFDPrefix):]
		if sender, err := newFDClient(ctx, &fduri); err == nil {
			return sender, nil
		}
	}

	opts := uri.Query()
	if boolopt(opts.Get("append")) {
		w, err = os.OpenFile(uri.Path, os.O_APPEND|os.O_CREATE, os.ModeAppend)
	} else {
		w, err = os.Create(uri.Path)
	}

	if err != nil {
		return nil, err
	}

	return newWriterClient(ctx, w), nil
}

// Close points the client at a discard buffer and closes its current writer if it's an
// io.Closer. If the writer's already closed, it returns ErrClosed.
func (w *writerclient) Close() error {
	w.m.Lock()
	defer w.m.Unlock()

	if w.closed {
		return ErrClosed
	}

	var err error
	if c, ok := w.w.(io.Closer); ok {
		err = c.Close()
	}
	w.w, w.closed = ioutil.Discard, true

	return err
}

func (w *writerclient) Send(ctx context.Context, msg []byte) (retry bool, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	_, err = w.w.Write(msg)
	return false, err
}
