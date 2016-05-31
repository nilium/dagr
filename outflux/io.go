package outflux

import "io"

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
