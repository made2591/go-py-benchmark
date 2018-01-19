#!/usr/bin/python
import os, sys, time, math, random, argparse
from multiprocessing import Process, Manager

def msort_sort_multi(a):
    """Returns the result of a multi processed merge sort - the sort part - over the passed list"""
    responses.append(msort_sort(a))

def msort_merge_multi(l, r):
    """Returns the result of a multi processed merge sort - the merge part - over the passed lists"""
    responses.append(msort_merge(l, r))

def msort_sort(a):
    """Returns the result of a merge sort - the sort part - over the passed list"""

    # if len of list is zero or 1, the list is ordered.
    if len(a) <= 1:
    
        # return the list
        return a
    
    # split the list 
    m = int(math.floor(len(a)/2))

    # return the merged result of merge sort call over left and right parts - divide and conquer
    return msort_merge(msort_sort(a[0:m]), msort_sort(a[m:]))

def msort_merge(l, r):
    """Returns the result of a merge sort - the merge part - over the passed lists"""

    # create the final list
    a = []

    # while there are elements in the list
    while len(l) > 0 or len(r) > 0:

        # if both list are not empty
        if len(l) > 0 and len(r) > 0:

            # append to a the smallest of the two first element of left and right part
            if l[0] <= r[0]:

                # append and remove first element of left part
                a.append(l.pop(0))
            else:

                # append and remove first element of right part
                a.append(r.pop(0))

        elif len(l) > 0:

            # append left part
            a.extend(l)
            break

        elif len(r) > 0:

            # append right part
            a.extend(r)
            break

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

def sorting(filename, quantity, random = False, processesNumber = 1):
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

    # single core elapsed time
    single = msort_sort(a)

    # single core elapsed time
    singleCoreTime = time.time() - startTime

    # sort algorithm from standard library
    aSorted = a[:]
    aSorted.sort()

    print "Verification of sorting algorithm", aSorted == single
    print "Single Core: %4.6f sec" % singleCoreTime

    # if processes number > 1
    if processesNumber > 1:

        # write result to file
        f = open("py-mergesort-" + str(processesNumber) + ".txt", "a")
        print "Starting %d-process" % processesNumber

        # save start time
        startTime = time.time()

        # compute number of steps for specified number of process
        step = int(math.floor(len(a) / processesNumber))

        # accumulate process
        processes = []

        # assign each process a portion of sorting
        for n in range(0, processesNumber):

            # assign to the last process the remaining part of list to order
            if n < processesNumber - 1:
                process = Process(target=msort_sort_multi, args=(a[n * step:(n + 1) * step],))
            else:
                process = Process(target=msort_sort_multi, args=(a[n * step:],))

            # accumulate
            processes.append(process)

        # start process
        for process in processes:
            process.start()

        # wait for each of them to end
        for process in processes:
            process.join()

        # use more processes to handle the final sort
        print "Performing final msort_merge..."
        startTimeFinalMergeStep = time.time()

        # accumulate process
        processes = []

        # merge each part of list and pull the result in the response (handled by manager list)
        if len(responses) > 2:

            # while there are at least a response in the list
            while len(responses) > 0:
                # assign first two part
                process = Process(target=msort_merge_multi, args=(responses.pop(0), responses.pop(0)))
                processes.append(process)

            # start process
            for process in processes:
                process.start()

            # wait for each of them to end
            for process in processes:
                process.join()

        # perform final merge step
        a = msort_merge(responses[0], responses[1])

        # final merge time
        finalMergeTime = time.time() - startTimeFinalMergeStep

        # final merge
        print "Final msort_merge duration: ", finalMergeTime
        multiProcessCore = time.time() - startTime

        print "Sorted arrays equal: ", (a == single)
        print "%d-process ended: %4.6f sec" % (processesNumber, multiProcessCore)

        f.write("%d %4.3f %4.3f %4.2f %4.3f\n" %
                (len(a),
                 singleCoreTime,
                 multiProcessCore,
                 multiProcessCore/singleCoreTime,
                 finalMergeTime)
        )

        f.close()

if __name__ == "__main__":

    # create function mapping
    functionMapping = { "generate" : generate, "sorting" : sorting }

    # manager to handle process response
    manager = Manager() 
    responses = manager.list()

    # parser to get argument from command line
    parser = argparse.ArgumentParser(
        description="Create a random column of integers and save the to file. \
                     Sort integers reading from a column file or randomly generated")

    # add argument to command line
    parser.add_argument("--command",      default="generate", type=str,  help="Command to run: sorting, generate",
                        choices=functionMapping.keys())
    parser.add_argument("--random",       default=False,      type=bool, help="Random list generation.")
    parser.add_argument("--filename",     default="list.txt", type=str,  help="Name of file for the list.")
    parser.add_argument("--list-length",  default=100000,     type=int,  help="Number of integers in the list.")
    parser.add_argument("--cores-number", default=1,          type=int,  help="Number of cores to use in the sorting.")

    # parse all available program arguments
    args = parser.parse_args()

    if args.command == "generate":
        # generate command
        functionMapping[args.command](args.filename, args.list_length)
    elif args.command == "sorting":
        # sorting command
        functionMapping[args.command](args.filename, args.list_length, args.random, args.cores_number)
    else:
        sys.exit("%s not available." % args.command)






