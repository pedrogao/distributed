#!/bin/bash  

echo "Start..."

for i in {1..10}  
do  
echo "loop$i"
go test -race -run TestFailPut  
done  

echo "All Is Done..."

