// Package dagr is an InfluxDB measurement library.
//
// You can use dagr to keeping track of and write primitive types understood by InfluxDB. Measurements are written in
// line protocol format, per InfluxDB 0.9.x docs:
//
// - Line Protocol:        https://influxdb.com/docs/v0.9/write_protocols/line.html
// - Line Protocol Syntax: https://influxdb.com/docs/v0.9/write_protocols/write_syntax.html
//
// dagr has a handful of types, Int, Float, Bool, and String, that allow atomic updates to their values from concurrent
// goroutines, and a few provisions for writing measurements in line protocol format.
package dagr
