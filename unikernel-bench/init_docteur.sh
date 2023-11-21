#!/bin/bash
## To initialize files at specific sizes within a git repository for Docteur's computation

min=500
max=100000
step=10000
offset=0

for ((size = "$min"; size <= "$max"; size += "$step"))
do
  n_size=$(echo "$size + $offset" | bc)
  megabytes=$(echo "scale=0; $n_size / 1024" | bc)
  dd if=/dev/random of=$size bs=1024 count=$megabytes
  offset=$(echo "$offset + 200" | bc)
done

