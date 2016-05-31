package outflux

import (
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/context"
)

type udpclient struct {
	conn     *net.UDPConn
	m        sync.Mutex
	closer   sync.Once
	closeErr error
}

func newUDPClient(ctx context.Context, uri *url.URL) (Sender, error) {
	addr, err := net.ResolveUDPAddr(uri.Scheme, uri.Opaque)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP(uri.Scheme, nil, addr)
	if err != nil {
		return nil, err
	}

	return &udpclient{conn: conn}, nil
}

func init() {
	RegisterSenderType("udp", newUDPClient)
	RegisterSenderType("udp4", newUDPClient)
	RegisterSenderType("udp6", newUDPClient)
}

func (c *udpclient) Close() error {
	c.closer.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}

func (c *udpclient) Send(ctx context.Context, body []byte) (retry bool, err error) {
	c.m.Lock()
	defer c.m.Unlock()

	var (
		sz = len(body)
		n  int
	)

	if deadline, ok := ctx.Deadline(); ok {
		err = c.conn.SetWriteDeadline(deadline)
	} else {
		err = c.conn.SetWriteDeadline(time.Time{})
	}

	if err != nil {
		// Failed to set deadline - something probably very wrong here
		return false, err
	}

	if n, err = c.conn.Write(body); err == nil && n < sz {
		// Undecided if handling n > sz is sane
		err = io.ErrShortWrite
	} else if ne, ok := err.(net.Error); ok && ne != nil {
		// Retry if the send failed on a temporary error and nothing was reported written
		// Discard buffers of partial writes
		retry = ne.Temporary() && n == 0
	}

	return retry, err
}
