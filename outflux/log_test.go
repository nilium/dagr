package outflux

type testingLogger interface {
	Log(...interface{})
}

type testLogger struct{ log testingLogger }

func (t testLogger) Print(args ...interface{}) {
	t.log.Log(args...)
}

func logtest(t testingLogger) func() {
	last := Log
	Log = testLogger{log}
	return func() { Log = last }
}
