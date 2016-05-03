# MultiSort Median DB

An in memory database for storing metrics and calculating a running median against them. This isn't production code and is just a "toy" implementation.

## Problem

Given an array with many duplicates, its somewhat trivial to optimize for space efficiency with a "run length" like encoding.

For instance, let's say you have the following array:

```python
[1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3]
```

You can optimize for space by storing this as an associative array, instead:

```python
# (value, occurrence)
[(1,4), (2, 5), (3, 2)]
```

In the naive instance, we could create a median by flattening the list and finding the most middle value. Here, `MedianDatabase` strives to improve upon the time complexity of that algorithm.

## MultiSort Median Database

This package delivers a `MedianDatabase` type for storing naive metrics and tallying a running `median` against them. Data is stored as depicted in the above structures.

`MedianDatabase` is a threadsafe database and allows for bulk writing associated arrays as above into the database. Specifically, `BufferedWorker` implements an asynchronous, threadsafe worker which consumes raw metrics, aggregates and flushes them periodically to the database for "bulk imports".

### Median Algorithm
 
In order to keep a running median we maintain two "balanced" lists of the associated types mentioned before. By keeping two perfectly balanced lists, we can easily calculate the median as the two inner elements of the arrays (eg: the left array's tail and the right array's head).

It's worth inspecting `database.go` to find the details, but here's some pseudo code explaining the write path of the algorithm:

```python
# bulk_write
left = [ ] 
right = [ ]

def write(bulk_metrics):
     offset = 0

     # write metrics into the right place
     for metric in bulk_metrics:
           stored_metric, container = metric from database
           stored_metric += metric

  
      # balance out the left and right arrays
      while True:
             if offset < -1:
                  move metrics from left array to the right array
              elif offset > 1:
                  move metrics from the right array to the left array
              else:
                  break 


while True:
      new_bulk_metrics = receive()
      write(new_bulk_metrics)
```

Specifically, we insert metrics into their corresponding lists left, or right (based upon ordering) while maintaining an "offset" so we can easily shift the arrays to be the same length. Once we've inserted all metrics, we equilibrize the two arrays.

Once the metrics have been inserted, finding the median is as simple as inspecting two elements:

```python
if total length is odd:
  median = tail of the left array
else:
  median = average of left tail and right head
```

## Testing

The `./run.sh` script executes a benchmarking suite which attempts to "load test" the implementation.

In order to test the viability of the database in an asynchronous environment, we spin up several instances of `BufferedWorker` to consume messages and buffer messages to write into the datastore. We create "dummy" metrics with a naive `Producer` type which simply emits arbitrary values. The `BufferedWorker` consumes messages, buffers them and flushes them into the `Database`.

## Setup

A go runtime environment is bootstrapped and accessible in the included `Vagrant` virtual machine. If not familiar with Vagrant, please refer to the installation [directions](https://www.vagrantup.com/docs/installation/).

To download provision and login to a virtual machine with the Go runtime:

```bash
$ vagrant up 
$ vagrant ssh
```

Now, change directories to `/opt/multisort-median` and execute the `run.sh` script

```bash
$ cd /opt/multisort-median
$ ./run.sh
```

Alternatively, if you have a `go` compiler installed locally you don't need to install `vagrant` and can just run `./run.sh` locally (assuming you are in a bash-friendly environment).
