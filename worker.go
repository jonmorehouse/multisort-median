package main

import (
	"time"
)

type Worker interface {
	Start()
	Write(Metric)
	Stop()
}

// a buffered worker is a worker which will buffer metrics and then flush them at once to the database
type BufferedWorker struct {
	metricCh      chan Metric
	quitCh        chan bool
	flushInterval time.Duration
	bufferSize    int
	database      Database
}

func NewBufferedWorker(bufferSize int, flushInterval time.Duration, database Database) *BufferedWorker {
	return &BufferedWorker{
		metricCh:      make(chan Metric),
		quitCh:        make(chan bool),
		flushInterval: flushInterval,
		bufferSize:    bufferSize,
		database:      database,
	}
}

func (b *BufferedWorker) Start() {
	// start the background worker
	go func() {
		b.worker()
	}()
}

func (b *BufferedWorker) Stop() {
	// dispatch a method to the internal worker to flush any messages found
	b.quitCh <- true

	// the internal worker loop will wait for a ping back from the
	// goroutine to ensure all messages were flushed to the database before
	<-b.quitCh
	close(b.quitCh)
	close(b.metricCh)
}

func (b *BufferedWorker) Write(metric Metric) {
	// write is a threadsafe method which prevents unsafe access to writing
	// metrics to the worker
	b.metricCh <- metric
}

func (b *BufferedWorker) worker() {
	// worker is a background process that handles the actual buffering and
	// flushing of metrics to the datastore. Specifically, this method will
	// buffer all metrics for a max time or until a bufferSize max is hit.
	// Once either situation happens, a series of "aggregate" metrics will
	// be written in bulk to the database.

	// set the first time that data should be flushed. This is reused later for further flushes
	nextFlush := time.Now().Add(b.flushInterval)
	buffer := make(map[int]*BulkMetric, b.bufferSize)
	count := 0

	// bulk flushes data to the database
	flush := func() {
		// first we build an array of all known bulkMetrics
		metrics := make([]*BulkMetric, 0, len(buffer))

		for _, metric := range buffer {
			metrics = append(metrics, metric)
		}

		// now we have a sorted slice of *BulkMetric objects flush to
		// the database.
		// NOTE: we call the write method in another
		// goroutine to ensure that if the BulkWrite method blocks we
		// don't block this channel.
		go func() {
			b.database.BulkWrite(metrics)
		}()

		// reset the state to start rebuffering metrics again
		buffer = make(map[int]*BulkMetric, b.bufferSize)
		count = 0
		nextFlush = time.Now().Add(b.flushInterval)
	}

	// writes a single metric into the local buffer
	handle := func(metric Metric) {
		bulkMetric, ok := buffer[metric.Value()]
		if !ok {
			buffer[metric.Value()] = NewBulkMetric(metric.Value())
			return
		}

		bulkMetric.Incr()
	}

	// loop and select on both channels until we grab a message, flush or quit
	for {
		select {
		case metric := <-b.metricCh:
			handle(metric)
		case <-b.quitCh:
			flush()
			// ping the channel back acknowledging that we received
			// the message and are finished flushing
			b.quitCh <- true
			return
		default:
			// flush if it has been too long or if we have buffered enough
			// data
			if time.Now().After(nextFlush) || count >= b.bufferSize {
				flush()
			}
			continue
		}

	}
}
