#!/bin/bash  

echo "Start..."

for i in {1..50}  
do  
    echo "loop$i"
    go test -race -count=1 .
done  

echo "All Is Done..."

