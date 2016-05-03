#!/bin/bash

# if in the VM we can guarantee where the project code is at /opt/multisort-median
# in the vm, we also add the go binaries to our path
if [ "$HOSTNAME" = multisort-median.vagrant ]; then
  cd /opt/
  GOPATH=/opt/go
  PATH=/opt/go/bin:$PATH
fi

# run tests
go test -v .

# run benchmarks 
go test -bench=.
