#!/bin/sh

for i in {6..10}
do
    length=$(echo "100000 * $i" | bc -l )
    echo $length
	python main.py --command generate --filename list.txt --list-length $length

	echo "Performing Python test over $length"
    python main.py --command sorting  --filename list.txt
    python main.py --command sorting  --filename list.txt --max-concurrency 2 --threaded True
    python main.py --command sorting  --filename list.txt --max-concurrency 4 --threaded True
    python main.py --command sorting  --filename list.txt --max-concurrency 8 --threaded True
    python main.py --command sorting  --filename list.txt --max-concurrency 2 --threaded False
    python main.py --command sorting  --filename list.txt --max-concurrency 4 --threaded False

	echo "Performing Python test over $length"
    go run main.go --filename list.txt
    go run main.go --filename list.txt --max-concurrency 2
    go run main.go --filename list.txt --max-concurrency 4
    go run main.go --filename list.txt --max-concurrency 8

done