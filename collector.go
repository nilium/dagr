package dagr

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math"
	"time"
)

type Collector struct {
	// counters is a map of counters to their metric keys
	counters map[string]*Counter
	// captures is a map of captured counter data relative to the last
	// capture time (walking backwards by interval) -- snapshot intervals
	// without captured data are given a NaN value
	captures map[string][]float64
	// lastCapture is the last capture time, rounded to interval.
	lastCapture time.Time

	interval time.Duration // How often to take capture snapshots
	timespan time.Duration // The maximum timespan for the collector to keep snapshots for.
	spanSize int

	nanSpan []float64 //

	stop chan struct{}
}

func NewCollector(interval, span time.Duration) *Collector {
	spanSize := int(span / interval)
	nans := make([]float64, spanSize)
	nan := math.NaN()
	for i := range nans {
		nans[i] = nan
	}

	return &Collector{
		counters:    map[string]*Counter{},
		captures:    map[string][]float64{},
		lastCapture: time.Now().Round(interval),

		interval: interval,
		timespan: span,
		spanSize: spanSize,

		nanSpan: nans,

		stop: make(chan struct{}),
	}
}

func (c *Collector) WatchCounter(counter *Counter, metric string) {
	c.counters[metric] = counter
	c.captures[metric] = make([]float64, c.spanSize)
}

func (c *Collector) captureSnapshot(whence time.Time) {
	snapshot := c.metricSnapshot()

	now := whence.Round(c.interval)
	delta := now.Sub(c.lastCapture)
	if delta%c.interval > c.interval/2 {
		delta += (c.interval * 5) / 10
	}
	log.Println(delta)
	shift := int(delta / c.interval)

	caps := c.captures

	switch {
	case shift >= c.spanSize:
		nans := c.nanSpan
		for _, c := range caps {
			copy(c, nans)
		}
	case shift > 0:
		nans := c.nanSpan
		for _, c := range caps {
			// Move everything back by `shift` elements
			copy(c[shift:], c)
			// Overwrite unshifted remains with nans
			copy(c[:shift], nans)
		}
	}

	for _, m := range snapshot {
		count := m.Count
		for _, k := range m.Keys {
			caps[k][0] = count
		}
	}

	c.lastCapture = now
}

func (c *Collector) waitUntil(t time.Time) bool {
	now := time.Now()
	if now.After(t) {
		return true
	}
	timer := time.NewTimer(now.Sub(t))
	select {
	case <-timer.C:
		return true
	case <-c.stop:
		timer.Stop()
		return false
	}
}

func (c *Collector) GatherSnapshots() {
	c.waitUntil(time.Now().Add((c.interval * 15) / 10).Round(c.interval))
	tick := time.NewTicker(c.interval)
runloop:
	for {
		select {
		case t := <-tick.C:
			now := time.Now()
			c.captureSnapshot(t)
			log.Println("Snapshot at", t.Round(c.interval).Format(time.StampNano), "took", time.Since(now))
		case <-c.stop:
			break runloop
		}
	}
	tick.Stop()
}

func (c *Collector) metricSnapshot() MetricSet {
	metrics := map[*Counter]Metric{}

	for key, counter := range c.counters {
		m, ok := metrics[counter]
		if !ok {
			m.Count = counter.Get()
		}
		m.Keys = append(m.Keys, key)
		metrics[counter] = m
	}

	result := make([]Metric, len(metrics))
	for _, m := range metrics {
		result = append(result, m)
	}
	return result
}

type Metric struct {
	Count float64
	Keys  []string
}

type MetricSet []Metric

func (ms MetricSet) KVMap() MetricMap {
	r := make(map[string]float64)
	for _, m := range ms {
		count := m.Count
		for _, k := range m.Keys {
			r[k] = count
		}
	}
	return r
}

type MetricMap map[string]float64

func (mm MetricMap) WriteTo(w io.Writer) (n int, err error) {
	fw := failWriter{w: w}

	m := map[string]float64(mm)
	fw.Write(versionHeader)
	writeUvarint(&fw, uint64(len(m)))
	for k, c := range mm {
		writeF64(&fw, c)
		writeVarString(&fw, k)
	}

	return fw.written, fw.err
}

func (mm MetricMap) ReadFrom(r io.Reader) (n int, err error) {
	fr := failReader{r: r}

	var vh [4]byte
	n, err = fr.Read(vh[:])
	if n != 4 || err != nil {
		return fr.read, fr.err
	}

	if !bytes.Equal(versionPrefix, vh[0:3]) {
		return fr.read, ErrBadVersionHeader
	}

	switch vh[3] {
	case 1:
		return fr.read, mm.readV1From(&fr)
	default:
		return fr.read, ErrUnsupportedVersion
	}
}

// v1 metric map reading

func (mm MetricMap) readV1From(r byteReader) error {
	numPairs, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	for ; numPairs > 0; numPairs-- {
		if err := mm.readV1Pair(r); err != nil {
			return err
		}
	}

	return nil
}

func (mm MetricMap) readV1Pair(r byteReader) error {
	count, err := readF64(r)
	if err != nil {
		return err
	}

	name, err := readVarString(r)
	if err != nil && err != io.EOF {
		return err
	}

	mm[name] = count

	return nil
}
