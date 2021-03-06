# Dagr

[![CircleCI](https://circleci.com/gh/nilium/dagr/tree/master.svg?style=svg)](https://circleci.com/gh/nilium/dagr/tree/master)

	$ go get go.spiff.io/dagr

Dagr is a simple library for tracking measurements and writing them out in an InfluxDB
line-protocol-friendly format.

***Note: the Dagr and outflux APIs are currently unstable and may change during v0 development.***
I'll try to make these breaks infrequent, but keep in mind that these are being developed while
being used, so I'll inevitably discover quirks in the API that I just have to fix and break
something in the process. When this happens, the commit should make a note of the break.


## Usage

As I see it, there are roughly two categories of measurements when dealing with a program: long-term
stats and events.  The next two sections attempt to elaborate on this, but they're roughly the same
and just use different types and send models.

### Stats

A **long-term stat** is something like the number of requests sent to an endpoint over its lifetime.
Typically, you don't need to record each individual request, you just need to know how many requests
have come in since the last time a measurement was sent. So, you keep a counter:

```go
// Setup
counter := new(dagr.Int)
measurement := dagr.NewPoint("http_request",
	dagr.Tags{"host": "example.local", "method": "GET", "path": "/v1/parrots"},
	dagr.Fields{"count": counter},
)

// Handler of some kind
func (h Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Atomically increment the counter
	counter.Add(1)
}

// Every now and then, write it to a stream that somehow gets over to InfluxDB:
go func() {
	for range time.Tick(time.Second) {
		// Ignoring any error that could be returned by WriteMeasurement
		WriteMeasurement(os.Stdout, measurement) // Replace os.Stdout with any io.Writer
		// Output:
		// http_event,host=example.local,method=GET,path=/v1/parrots count=123i 1136214245000000000
	}
}()
```

The above will keep a count for the number of times `/v1/parrots` is accessed, assuming the
handler only handles that particular path. As a long-term thing, this is fairly useful, because we'd
like to know how many people want a list of parrots. The important thing is that updates to the
regular Dagr types, Int, Float, String, and Bool, are atomic: you can increment from multiple
goroutines and the increment will only block for a minimal amount of time. This also means that you
can update these fields mid-write without interrupting the write or causing a data race (but you may
occasionally end up with slightly out of sync fields between two writes).

While the Int and Float types are useful as accumulators, you can also use Bool and String to keep
global process state up to date in a Point, allowing you to periodically send whether the process is
sleeping or what its current stage is (e.g., started vs. listener started vs. stopped).

Of course, you could also have short-time stats and allocate and expire them as needed. Dagr doesn't
really enforce this model, it's just one way I think of things.


### Events

An **event**, on the other hand, is something short-lived but worth recording, like a really
important error. For example:

```go
// Setup
event := dagr.RawPoint{
	Key:    "recv_error",
	Tag:    dagr.Tags{"host": "example.local"},
	Fields: dagr.Fields{
		"fatal":   dagr.RawBool(true),
		"message": dagr.RawString("Parrot has been scritched"),
	},
	Time:   time.Now(), // if omitted, WriteMeasurement will use time.Now() anyway
}

// And just send it once (again, ignoring errors)
WriteMeasurement(os.Stdout, event)
// Output:
// http_event,host=example.local fatal=T,message="Parrot has been scritched" 1136214245000000000
//
// Note that RawEvent does not guarantee tag or field order, unlike Point, so the above may not be
// exactly the same every time.
```

A RawPoint is just a simple Measurement implementation that satisfies the bare minimum needed to
write a point in line protocol format. It also serves as a decent example for how to begin writing
your own points, if necessary. More advanced usage can be seen in both Point and PointSet.

Naturally, you may want to alert off of such errors, because a hole in the space time continuum is
known to create bunny people and generally leads to all sorts of chaos. Point is, these are two
different use-cases Dagr was built to accommodate, and there are likely more it could handle.


## Contributing

If you want to contribute changes, create an issue to discuss what you want to do ahead of time.

Anything from bug fixes to documentation to tests to just correcting typos is welcome. Removing code
is accepted with justification since the API is currently in breakable-when-good mode. Adding
features requires the most justification just because dagr already contains code that could be
removed.

Changes are reviewed on <https://git.spiff.io>, so you'll need to be given access to it before you
push any changes. If you're not familiar with Gerrit, its user guide (at
<https://gerrit-documentation.storage.googleapis.com/Documentation/2.12.2/intro-user.html>) is
a good resource to check out first. If you need help with it, we'll work through it. You're welcome
to submit a pull request while working on something if you're looking for cursory feedback before
squashing it for review on git.spiff.io.

**The only documentation requirement to contribute** is that you must add your real name (i.e., the
one you'd introduce yourself with, not the one on a birth certficiate), an email address you can be
contacted at, and optionally a handle or alias to AUTHORS.md. If this places an undue burden on you,
please send me an email at <ncower@gmail.com> or create a new issue so we can talk about it. This
should be part of your first changeset if you're not already credited.


## License

Dagr is licensed under the 2-clause BSD license. You should have received a copy of this license
with Dagr in the LICENSE.txt file.
