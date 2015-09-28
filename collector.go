package dagr

import (
	"math"
	"time"
)

type Collector struct {
	// counters is a map of counters to their metric keys
	counters map[string]CounterGet
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
		counters:    map[string]CounterGet{},
		captures:    map[string][]float64{},
		lastCapture: time.Now().Round(interval),

		interval: interval,
		timespan: span,
		spanSize: spanSize,

		nanSpan: nans,

		stop: make(chan struct{}),
	}
}

func (c *Collector) WatchCounter(counter CounterGet, metric string) {
	c.counters[metric] = counter
	c.captures[metric] = make([]float64, c.spanSize)
}

func (c *Collector) captureSnapshot(whence time.Time) {
	snapshot := c.metricSnapshot()

	delta := whence.Sub(c.lastCapture)
	if delta%c.interval > c.interval/2 {
		delta += (c.interval * 5) / 10
	}
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

	c.lastCapture = whence
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
			c.captureSnapshot(t)
		case <-c.stop:
			break runloop
		}
	}
	tick.Stop()
}

func (c *Collector) metricSnapshot() MetricSet {
	metrics := map[CounterGet]Metric{}

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
