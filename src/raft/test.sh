#!/bin/bash  

echo "Start..."

for i in {1..2}  
do  
    echo "loop$i"
    go test -race -run 2D
    go test -race -run 2C
done  

echo "All Is Done..."

