#!/usr/bin/python
import os, sys, time, math, random, argparse
from multiprocessing import Process, Manager, Pool
from multiprocessing.pool import ThreadPool
from threading import Thread, Semaphore, BoundedSemaphore

def merge_sort_parallel_fastest(array, concurrentRoutine, threaded):

    # create a pool of concurrent threaded or process routine
    if threaded:
        pool = ThreadPool(concurrentRoutine)
    else:
        pool = Pool(concurrentRoutine)

    # size of partitions
    size = int(math.ceil(float(len(array)) / concurrentRoutine))

    # partitioning
    data = [array[i * size:(i + 1) * size] for i in range(concurrentRoutine)]

    # mapping each partition to one worker, using the standard merge sort
    data = pool.map(msort_sort, data)

    # go ahead until the number of partition are reduced to one (workers end respective ordering job)
    while len(data) > 1:

        # extra partition if there's a odd number of worker
        extra = data.pop() if len(data) % 2 == 1 else None

        # prepare couple of ordered partition for merging
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]

        # use the same number of worker to merge partitions
        data = pool.map(msort_merge, data) + ([extra] if extra else [])

    # return result
    return data[0]

def merge_sort_parallel_golike(array, bufferedChannel, results):

    # if array length is 1, is ordered : return
    if len(array) <= 1:
        return array

    # compute length
    n = len(array) / 2

    # append thread for subroutine
    ts = []

    # try to acquire channel
    if bufferedChannel.acquire(blocking=False):

        # if yes, setup call on the first half
        ts.append(Thread(target=merge_sort_parallel_golike, args=(array[:n], bufferedChannel, results,)))

    else:

        # else call directly the merge sort over the first halft
        results.append(msort_sort(array[:n]))

    # the same, in the second half
    if bufferedChannel.acquire(blocking=False):

        ts.append(Thread(target=merge_sort_parallel_golike, args=(array[n:], bufferedChannel, results,)))

    else:

        results.append(msort_sort(array[n:]))

    # start thread
    for t in ts:
        t.start()

    # wait for finish
    for t in ts:
        t.join()

    # append results
    results.append(msort_merge(results.pop(0), results.pop(0)))

    # unlock the semaphore for another threads for next call to merge_sort_parallel_golike
    # try is to prevent arise of exception in the end
    try:
        bufferedChannel.release()
    except:
        pass

def msort_sort_multi(array):
    """Returns the result of a multi processed merge sort - the sort part - over the passed list"""
    responses.append(msort_sort(array))

def msort_merge_multi(left, right):
    """Returns the result of a multi processed merge sort - the merge part - over the passed lists"""
    responses.append(msort_merge(left, right))

def msort_sort(array):
    """Returns the result of a merge sort - the sort part - over the passed list"""

    n = len(array)
    # if len of list is zero or 1, the list is ordered.
    if n <= 1:

        # return the list
        return array

    # split the list [length takes O(1) https://wiki.python.org/moin/TimeComplexity
    # but removing computation is still more efficient]
    left = array[:n / 2]
    right = array[n / 2:]

    # return the merged result of merge sort call over left and right parts - divide and conquer
    return msort_merge(msort_sort(left), msort_sort(right))

def msort_merge(*args):
    """Returns the result of a merge sort - the merge part - over the passed lists"""

    # get left and right both as separated and in a single tuple argument
    left, right = args[0] if len(args) == 1 else args

    # create the final list
    a = []

    # while there are elements in left and right part
    while left or right:

        # if there are no elements in left part
        if not left:

            # append the first element of right part to final result
            a.append(right.pop())

        # if there are no elements in right part OR the last element of left is
        # bigger than last element of right
        elif not right or left[-1] > right[-1]:

            # append the first element of left part to final result
            a.append(left.pop())

        # if here: left is empty, right is empty or not.
        else:

            # in any case continue pop from right part
            a.append(right.pop())

    # revert order
    a.reverse()

    # return a
    return a

def generate(filename, quantity):
    """Generate a random sequence of quantity integers in [0, quantity) interval"""

    # open the output filename
    with open(filename, "w") as f:

        # for quantity times
        for i in range(0, quantity):

            # write a new random integer in [0, quantity)
            f.write(str(random.randint(0, quantity))+"\n")

def sorting(filename, quantity, random = False, routinesNumber = 1, threaded = False):
    """Run merge sort over a random generated list or a list of integers provided in
       filename. Save statistics to output file and prints logs during computation"""

    # sign start time for reading
    startTime = time.time()

    # read list of integers from file
    if not random:

        try:
            with open(filename, "r") as f:
                a = f.readlines()
        except Exception as e:
            sys.exit("Error: %s does not exist. Exception: %s" % (filename, e))

        print "List length : ", len(a)
        print "Random list readed in ", time.time() - startTime

    # create random list
    else:

        a = [random.randint(0, quantity) for n in range(0, quantity)]

        print "List length : ", quantity
        print "Random list generated in ", time.time() - startTime

    # start time creation
    startTime = time.time()

    # single routine elapsed time
    single = msort_sort(a)

    # single routine elapsed time
    singleRoutineTime = time.time() - startTime

    # sort algorithm from standard library
    aSorted = a[:]
    aSorted.sort()

    print "Verification of sorting algorithm", aSorted == single
    print "Single routine: %4.6f sec" % singleRoutineTime

    # if routines number > 1
    if routinesNumber > 1:

        mode = "threads-" if threaded else "process-"
        # write result to file
        f = open("py-mergesort-max-" + mode + str(routinesNumber) +"-list-" +str(len(a))+ ".txt", "a")
        print "Starting %d-routine" % routinesNumber

        # save start time
        startTime = time.time()

        ################################
        # sem = BoundedSemaphore(routinesNumber)
        # merge_sort_parallel_golike(a, sem, responses)
        # a = responses.pop(0)
        ################################

        a = merge_sort_parallel_fastest(a, routinesNumber, threaded)

        multiRoutineCore = time.time() - startTime

        print "Sorted arrays equal: ", (a == single)
        print "%d-routine ended: %4.6f sec" % (routinesNumber, multiRoutineCore)

        f.write("%d %4.3f %4.3f %4.2f\n" %
                (len(a),
                 singleRoutineTime,
                 multiRoutineCore,
                 multiRoutineCore/singleRoutineTime)
        )

        f.close()

if __name__ == "__main__":

    # msort_merge([2, 9, 12, 17, 21], [1, 10, 13, 19])

    # create function mapping
    functionMapping = { "generate" : generate, "sorting" : sorting }

    # manager to handle routine response
    manager = Manager() 
    responses = manager.list()

    # parser to get argument from command line
    parser = argparse.ArgumentParser(
        description="Create a random column of integers and save the to file. \
                     Sort integers reading from a column file or randomly generated")

    # add argument to command line
    parser.add_argument("--command",         default="generate", type=str,  help="Command to run: sorting, generate",
                        choices=functionMapping.keys())
    parser.add_argument("--random",          default=False,      type=bool, help="Generate random list at runtime.")
    parser.add_argument("--filename",        default="list.txt", type=str,  help="Name of file for the output generated list.")
    parser.add_argument("--list-length",     default=100000,     type=int,  help="Number of integers in the list.")
    parser.add_argument("--max-concurrency", default=1,          type=int,  help="Max number of concurrent routine to run in the sorting.")
    parser.add_argument("--threaded",        default=False,      type=bool, help="Spawn new threads instead of processes.")

    # parse all available program arguments
    args = parser.parse_args()

    if args.command == "generate":
        # generate command
        functionMapping[args.command](args.filename, args.list_length)
    elif args.command == "sorting":
        # sorting command
        functionMapping[args.command](args.filename, args.list_length, args.random, args.max_concurrency, args.threaded)
    else:
        sys.exit("%s not available." % args.command)






