## GoLang vs Python

A Golang vs Python single and multicore msort_merge-sort implementation comparison. For more details, read my blog post [here](https://made2591.github.io/blog/go-py-benchmark).

### Dependencies
For Python, there are no dependencies: I use Python 2.7, it should work fine.

For GoLang, there is one dependencies: [argparse](https://github.com/akamensky/argparse) package by akamensky.

### Usages
Open a shell and run the following:

```shell
go get github.com/akamensky/argparse
git clone https://github.com/made2591/go-py-benchmark.git
cd go-py-benchmark
```

#### Step 1 - Generate a common list
Generate a list of random integers for benchmarking, run:

```shell
python main.py --command generate --filename list.txt --list-length 100000
```

#### Step 2 - Python run

- To run a simple merge sort over the generated list:

```shell
python main.py --command sorting --filename list.txt
```

- To run a concurrent merge sort limited to _n_ = 4 over the generated list:

```shell
python main.py --command sorting --filename list.txt --max-concurrency 4
```

#### Step 3 - GoLang run

- To run a simple merge sort over the generated list:

```shell
go run main.go --filename list.txt
```

- To run a concurrent merge sort limited to _n_ = 4 over the generated list:

```shell
go run main.go --filename list.txt --max-concurrency 8
```

### Thanks
Sources: [1](https://devopslog.wordpress.com/2012/04/15/mergesort-example-using-python-multiprocessing/), [2](https://medium.com/@_orcaman/when-too-much-concurrency-slows-you-down-golang-9c144ca305a)