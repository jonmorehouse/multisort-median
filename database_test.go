package main

import (
	"testing"
)

func buildBulkMetrics(h, t int) []*BulkMetric {
	metrics := make([]*BulkMetric, 0, t-h)

	for i := h; i < t; i++ {
		metric := NewBulkMetric(i)
		metrics = append(metrics, metric)
	}

	return metrics
}

func TestMedianDatabaseRebalancing(t *testing.T) {
	database := NewMedianDatabase()
	database.Open()
	defer database.Close()

	// [0 1 2 3 |4 5 6 7 ]
	database.BulkWrite(buildBulkMetrics(0, 9))

	// [0 1 2 3 4 5 5 | 6 6 7 7 8 8 9 9 ]
	database.BulkWrite(buildBulkMetrics(5, 9))

	// TODO: verify medians are ballpark correct
}
