#!/usr/bin/python
import os, sys, time
import math, random
from multiprocessing import Process, Manager
 
def merge_sort_multi(list_part):
    responses.append(merge_sort(list_part))
 
def merge_multi(list_part_left, list_part_right):
    responses.append(merge(list_part_left, list_part_right))
 
def merge_sort(a):
    length_a = len(a)
    if length_a <= 1: return a
    m = int(math.floor(length_a / 2))
    a_left = a[0:m]
    a_right = a[m:]
    a_left = merge_sort(a_left)
    a_right = merge_sort(a_right)
    return merge(a_left, a_right)

def merge(left, right):
    a = []
    while len(left) > 0 or len(right) > 0:
        if len(left) > 0 and len(right) > 0:
            if left[0] <= right[0]:
                a.append(left.pop(0))
            else:
                a.append(right.pop(0))
        elif len(left) > 0:
            a.extend(left)
            break
        elif len(right) > 0:
            a.extend(right)
            break
    return a
 
if __name__ == '__main__':
    try:
        cores = int(sys.argv[1])
        if cores > 1:
            if cores % 2 != 0:
                cores -= 1
        print 'Using %d cores'%cores
    except:
        cores = 1

    manager = Manager() 
    responses = manager.list()

    makeRandom = False
    if not makeRandom:
        start_time = time.time()
        with open('list.csv', 'r') as f:
            a = f.readlines()
        print 'List length : ', len(a)
        print 'Random list readed in ', time.time() - start_time
    else:
        l = random.randint(3*10**4, 3*10**5)
        print 'List length : ', l
        start_time = time.time()
        a = [ random.randint(0, n*100) for n in range(0, l) ]
        print 'Random list generated in ', time.time() - start_time

    start_time = time.time()
    single = merge_sort(a)
    single_core_time = time.time() - start_time
    a_sorted = a[:]
    a_sorted.sort()
    print 'Verification of sorting algorithm', a_sorted == single
    print 'Single Core: %4.6f sec'%( single_core_time )
    if cores > 1:
        f = open('py-mergesort-'+str(cores)+'.dat', 'a')
        print 'Starting %d-core process'%cores
        start_time = time.time()
        step = int( math.floor( l / cores ) )
        offset = 0
        p = []
        for n in range(0, cores):
            if n < cores - 1:
                proc = Process( target=merge_sort_multi, args=( a[n*step:(n+1)*step], ) )
            else:
                proc = Process( target=merge_sort_multi, args=( a[n*step:], ) )
            p.append(proc)

        for proc in p:
            proc.start()
        for proc in p:
            proc.join()
        print 'Performing final merge...'
        start_time_final_merge = time.time()
        p = []
        if len(responses) > 2:
            while len(responses) > 0:
                proc = Process( target=merge_multi, args=(responses.pop(0),responses.pop(0)) )
                p.append( proc )
            for proc in p:
                proc.start()
            for proc in p:
                proc.join()
        a = merge(responses[0], responses[1])
        final_merge_time = time.time() - start_time_final_merge
        print 'Final merge duration : ', final_merge_time
        multi_core_time = time.time() - start_time
        print 'Sorted arrays equal : ', (a == single)
        print '%d-Core ended: %4.6f sec'%(cores, multi_core_time)
        f.write("%d %4.3f %4.3f %4.2f %4.3f\n"%(l, single_core_time, multi_core_time, multi_core_time/single_core_time, final_merge_time))
        f.close()