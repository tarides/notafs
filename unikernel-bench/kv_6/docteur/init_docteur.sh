#!/bin/bash
## To initialize files at specific sizes within a git repository for Docteur's computation

min=1000
max=100000
step=10000

for ((size = "$min"; size <= "$max"; size += "$step"))
do
  dd if=/dev/random of=$size count=$size bs=1000 iflag=count_bytes
done

min=$max
max=1000000
step=100000

for ((size = "$min"; size <= "$max"; size += "$step"))
do
  dd if=/dev/random of=$size count=$size bs=1000 iflag=count_bytes
done
