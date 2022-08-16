#!/bin/bash  

echo "Start..."

for i in {1..5}  
do  
    echo "loop$i"
    go test -race -run 2D
done  

echo "All Is Done..."

