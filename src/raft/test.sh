#!/bin/bash  

echo "Start..."

for i in {1..20}  
do  
    echo "loop$i"
    go test -race -run TestBackup2B
done  

echo "All Is Done..."

