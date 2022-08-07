#!/bin/bash  

echo "Start..."

for i in {1..5}  
do  
    echo "loop$i"
    go test -race
done  

echo "All Is Done..."

