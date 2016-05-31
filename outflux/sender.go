package outflux

import (
	"fmt"
	"io"
	"net/url"

	"golang.org/x/net/context"
)

// A sender is anything that may send data from a Proxy to some kind of recipient. send ought to
// block until the send is complete. It should attempt to send the entire msg without breaking it up
// into chunks.
//
// If sending the message results in an error, the sender should return whether it ought to be
// retried and the error that occurred. If the error is context.Canceled, however, the message will
// not be re-sent.
//
// All senders must implement io.Closer, where calls to Close must be idempotent.
type Sender interface {
	io.Closer

	Send(ctx context.Context, msg []byte) (retry bool, err error)
}

type NewSenderFunc func(context.Context, *url.URL) (Sender, error)

var namedSenders = make(map[string]NewSenderFunc)

func RegisterSenderType(protocol string, alloc NewSenderFunc) {
	if _, ok := namedSenders[protocol]; ok {
		panic(fmt.Sprintf("outflux: conflict on sender protocol %q", protocol))
	}

	namedSenders[protocol] = alloc
}

func allocSender(ctx context.Context, uri *url.URL) (Sender, error) {
	fn := namedSenders[uri.Scheme]
	if fn == nil {
		return nil, ErrNoSender
	}
	return fn(ctx, uri)
}
