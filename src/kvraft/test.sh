#!/bin/bash  

echo "Start..."

for i in {1..5}  
do  
    echo "loop$i"
    go test -race  -count=1 -run TestSnapshotUnreliableRecover3B
done  

echo "All Is Done..."

