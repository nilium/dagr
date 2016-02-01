package dagr

type tLogf func(string, ...interface{})

func (fn tLogf) Printf(f string, v ...interface{}) { fn(f, v...) }

type testLogger interface {
	Logf(string, ...interface{})
}

func prepareLogger(t testLogger) func() {
	temp := Log
	Log = tLogf(t.Logf)
	return func() {
		Log = temp
	}
}
