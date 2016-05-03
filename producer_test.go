package main

// NOTE this is a test only method as its not part of the public interface because it generates randomized data
type Producer struct {
	workers []*BufferedWorker
}

func NewProducer(workers int, db Database) {
	// create a bunch of workers
}
