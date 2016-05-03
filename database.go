package main

import (
	"sort"
	"sync/atomic"
)

type Database interface {
	BulkWrite([]*BulkMetric)
	Open()
	Close()
	GetMedian() int
}

type MedianDatabase struct {
	writeCh chan []*BulkMetric
	quitCh  chan bool

	// used to keep the left and right in sync!
	left   []BulkMetric
	right  []BulkMetric
	size   int
	median int32
}

func NewMedianDatabase() *MedianDatabase {
	return &MedianDatabase{
		writeCh: make(chan []*BulkMetric),
		quitCh:  make(chan bool),
		median:  0,
	}
}

func (m *MedianDatabase) Open() {
	go func() {
		m.worker()
	}()
}

func (m *MedianDatabase) Close() {
	// signal the database to close itself
	m.quitCh <- true

	// wait for the database to finish processing
	<-m.quitCh
	close(m.quitCh)
	close(m.writeCh)
}

func (m MedianDatabase) GetMedian() int {
	// load the median value, ensuring that if this happens at the same
	// time an update is happening that invalid data isn't accidentally
	// returned
	median := atomic.LoadInt32(&(m.median))

	// by default int will correspond to the size of the current OS in go.
	// Thus, it is safe to assume that this call will operate normally
	// unless we are on a 16bit system (which is likely to cause much
	// bigger problems elsewhere)
	return int(median)
}

func (m *MedianDatabase) BulkWrite(bulkMetrics []*BulkMetric) {
	// NOTE this is totally slow an unoptimized in every way possible; this
	// is the quickest implementation to sort this sort of set and we want
	// to keep it out of the critical path of `worker`.
	sortedKeys := make([]int, 0, len(bulkMetrics))
	keyToMetrics := make(map[int]*BulkMetric)

	for _, bulkMetric := range bulkMetrics {
		sortedKeys = append(sortedKeys, bulkMetric.Value())
		keyToMetrics[bulkMetric.Value()] = bulkMetric
	}
	sort.Ints(sortedKeys)

	// TODO implement a nicer type around `[]*BulkMetric` which implements
	// the Sort interface
	for i, key := range sortedKeys {
		bulkMetrics[i] = keyToMetrics[key]
	}

	m.writeCh <- bulkMetrics
}

func (m *MedianDatabase) worker() {
	// in order to efficiently sort and maintain a median for a high volume
	// of data we effectively organize data into two arrays, the left and
	// right side. In normal, balanced circumstances, when len(left) ==
	// len(right) we can guarantee that the median is either left[-1] or
	// average(left[-1], right[0]).
	left := make([]*BulkMetric, 0, 1000)
	right := make([]*BulkMetric, 0, 1000)
	totalLength := 0

	// accepts a list of BulkMetrics and inserts them into specified array
	insert := func(metrics []*BulkMetric, output []*BulkMetric) (int, []*BulkMetric, []*BulkMetric) {

		// this will input elements in an already existent array of metrics
		if len(output) == 0 {
			return 0, metrics, output
		}

		offset := 0
		index := 0
		for _, metric := range metrics {
			for {
				// we've gotten to the end of the output and can't place anymore.
				// Specifically, we don't append to the end of an array because we'd like to handle that downstream to simplify this part
				if index >= len(output) {
					return offset, metrics, output
				}

				current := output[index]
				// if the current metric exists already, simply increment it and continue
				if current.Value() == metric.Value() {
					totalLength = totalLength + metric.Count()
					current.IncrBy(metric.Count())
					offset = offset + metric.Count()
					index = index + 1
					break
				} else if current.Value() > metric.Value() {
					// our metric should go in front of the existing metric
					// both lists are sorted, so this means that our metrics is "behind" the output
					totalLength = totalLength + metric.Count()
					offset = offset + metric.Count()

					// there's an edge case where we need to create a new array if this is the first item
					if index == 0 {
						// prepend the item
						output = append([]*BulkMetric{metric}, output...)
					} else {
						// inject the item at this place in the array
						head := append(output[:index], metric)
						output = append(head, output[index:]...)
					}
					break
				} else {
					// keep looking, the current metric is greater than the value we're at in the existing array
					index = index + 1
				}
			}
		}

		// if we get to this level, it means that we've inserted all the metrics into the database
		return offset, []*BulkMetric{}, output
	}

	recalculate := func() {
		if len(left) == 0 {
			return
		}

		leftTail := left[len(left)-1].Value()

		// if total is odd then we grab the last item from the left array
		if totalLength%2 == 1 {
			m.median = int32(leftTail)
			return
		}

		rightTail := right[0].Value()
		median := (rightTail + leftTail) / 2
		m.median = int32(median)
	}

	// take items from left and move them right until the two arrays are balanced!
	rebalanceRight := func(l, r []*BulkMetric, offset int) ([]*BulkMetric, []*BulkMetric) {
		for {
			if offset < 1 || len(l) < 1 {
				break
			}

			// if the head of right is greater than offset then we
			// can't pop it completely so we split it between the two lists until they are equal
			lTail := l[len(l)-1]
			if lTail.Count() > offset {
				// remove the offset from the rHead
				lTail.DecrBy(offset)

				// create a new tail for l
				rHead := NewBulkMetric(lTail.Value())
				// bulkMetric defaults to 1 so we need to account for that when setting its value
				rHead.IncrBy(offset - 1)

				// finally we add this as the new head of the r array
				r = append([]*BulkMetric{rHead}, r...)
				break
			}

			// the head of r is less than the offset, so we can pop the whole node off of r and put it on l
			offset -= lTail.Count()
			r = append([]*BulkMetric{lTail}, r...)
			l = l[:len(l)-1]
		}

		return l, r
	}

	// take items from right and move them left until the two arrays are balanced!
	rebalanceLeft := func(l, r []*BulkMetric, offset int) ([]*BulkMetric, []*BulkMetric) {
		for {
			if offset < 1 || len(r) < 1 {
				break
			}

			// if the head of right is greater than offset then we
			// can't pop it completely so we split it between the two lists until they are equal
			if r[0].Count() > offset {
				// remove the offset from the rHead
				r[0].DecrBy(offset)

				// create a new tail for l
				lTail := NewBulkMetric(r[0].Value())
				// bulkMetric defaults to 1 so we need to account for that when setting its value
				lTail.IncrBy(offset - 1)

				// finally we add this new tail to the l array
				l = append(l, lTail)
				break
			}

			// the head of r is less than the offset, so we can pop the whole node off of r and put it on l
			offset -= r[0].Count()
			l = append(l, r[0])
			r = r[1:]
		}

		return l, r
	}

	write := func(bulkMetrics []*BulkMetric) {
		if len(bulkMetrics) == 0 {
			return
		}

		// write as many elements as we can into the left side
		leftOffset, remaining, newLeft := insert(bulkMetrics, left)
		left = newLeft

		// write as many elements as we can into the right side
		rightOffset, remaining, newRight := insert(bulkMetrics, right)
		right = newRight

		// at this point, only elements that were greater than the
		// right most value and can be inserted naively to the end of
		// the right list
		for _, metric := range remaining {
			rightOffset = rightOffset + metric.Count()
			totalLength += metric.Count()
		}
		right = append(right, remaining...)

		// now we need to rebalance the arrays to take care of the offset
		if leftOffset > rightOffset { // we put more elements on the left side, move some right
			left, right = rebalanceRight(left, right, (leftOffset-rightOffset)/2)
		} else if leftOffset < rightOffset { // we put more elements on the right side, move some left
			left, right = rebalanceLeft(left, right, (rightOffset-leftOffset)/2)
		}

		recalculate()
	}

	for {
		select {
		case bulkMetrics := <-m.writeCh:
			write(bulkMetrics)
		case <-m.quitCh:
			m.quitCh <- true
			return
		}
	}
}
