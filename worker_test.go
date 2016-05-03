package main

import (
	"testing"
	"time"
)

type mockCallback func([]*BulkMetric)
type mockDatabase struct {
	cb mockCallback
	t  *testing.T
}

func newMockDatabase(t *testing.T, cb mockCallback) *mockDatabase {
	return &mockDatabase{
		cb: cb,
		t:  t,
	}
}

// mock methods that aren't used
func (d *mockDatabase) Open() {
	d.t.Fail()
}

func (d mockDatabase) Close() {
	d.t.Fail()
}

func (d mockDatabase) GetMedian() int {
	d.t.Fail()
	return 0
}

func (d mockDatabase) BulkWrite(metrics []*BulkMetric) {
	d.cb(metrics)
}

func TestBufferedWorker(t *testing.T) {
	done := make(chan bool)
	metrics := 100

	go func() {
		var worker Worker
		called := 0

		cb := func(bulkMetrics []*BulkMetric) {
			// this is expected, because we flush upon close
			if called == 1 {
				return
			}

			called += 1
			for _, metric := range bulkMetrics {
				metrics -= metric.Count()
			}

			// close the worker once we've processed everything
			worker.Stop()
			done <- true
			close(done)
		}

		db := newMockDatabase(t, cb)
		worker = NewBufferedWorker(10000, time.Second, db)
		worker.Start()
		// write a bunch of arbitrary metrics
		for i := 0; i < metrics; i++ {
			worker.Write(NewIntMetric(i))
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout")
	}

	// block the main thread until the callback has completed
	<-done
}
