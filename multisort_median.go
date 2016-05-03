package main

type Metric interface {
	Value() int
}

type IntMetric struct {
	value int
}

func NewIntMetric(value int) *IntMetric {
	return &IntMetric{
		value: value,
	}
}

func (i IntMetric) Value() int {
	return i.value
}

type BulkMetric struct {
	value int
	count int
}

func (b BulkMetric) Value() int {
	return b.value
}

func (b BulkMetric) Count() int {
	return b.count
}

func (b *BulkMetric) Incr() {
	b.count = b.count + 1
}

func (b *BulkMetric) IncrBy(value int) {
	b.count = b.count + value
}

func (b *BulkMetric) DecrBy(value int) {
	b.count = b.count - value
}

func NewBulkMetric(value int) *BulkMetric {
	return &BulkMetric{
		value: value,
		count: 1,
	}
}

func main() {
	panic("not implemented; use `go test -v` or `go test -benchmark=.` instead")
}
