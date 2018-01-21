package main

import (
    "bufio"
    "fmt"
    "github.com/akamensky/argparse"
    "io/ioutil"
    "math"
    "math/rand"
    "os"
    "sort"
    "strconv"
    "sync"
    "time"
)

// Returns the result of a merge sort - the sort part - over the passed list
func merge_sort_multi(s []int, sem chan struct{}) []int {

    // return ordered 1 element array
    if len(s) <= 1 {
        return s
    }

    // split length
    n := len(s) / 2

    // create a wait group to wait for both goroutine call before final merge step
    wg := sync.WaitGroup{}
    wg.Add(2)

    // result of goroutine
    var l []int
    var r []int

    // check if passed buffered channel is full
    select {

    // check if you can acquire a slot
    case sem <- struct{}{}:

        // call another goroutine worker over the first half
        go func() {
            l = merge_sort_multi(s[:n], sem)

            // free a slot
            <-sem

            // unlock one semaphore
            wg.Done()
        }()
    default:
        l = msort_sort(s[:n])
        wg.Done()
    }

    // the same over the second half
    select {
    case sem <- struct{}{}:
        go func() {
            r = merge_sort_multi(s[n:], sem)
            <-sem
            wg.Done()
        }()
    default:
        r = msort_sort(s[n:])
        wg.Done()
    }

    // wait for go subroutine
    wg.Wait()

    // return
    return msort_merge(l, r)

}

// Returns the result of a merge sort - the sort part - over the passed list
func msort_sort(a []int) []int {

    // if len of list is zero or 1, the list is ordered.
    if len(a) <= 1 {

        // return the list
        return a
    }

    // split the list
    m := int(math.Floor(float64(len(a)) / 2))

    // return the merged result of merge sort call over l and r parts - divide and conquer
    return msort_merge(msort_sort(a[0:m]), msort_sort(a[m:]))

}

// Returns the result of a merge sort - the merge part - over the passed lists
func msort_merge(l []int, r []int) []int {

    a := []int{}

    // while there are elements in the list
    for len(l) > 0 || len(r) > 0 {
        // if left part is empty
        if len(l) == 0 {
            // append the first element of right part to final result
            a = append(a, r[len(r)-1])
            if len(r) > 1 {
                r = r[:len(r)-1]
            } else {
                r = []int{}
            }
        } else {
            // if right part is empty or last element of
            // left is bigger than last element of right part
            if len(r) == 0 || (l[len(l)-1] > r[len(r)-1]) {
                // append the first element of left part to final result
                a = append(a, l[len(l)-1])
                if len(l) > 1 {
                    l = l[:len(l)-1]
                } else {
                    l = []int{}
                }
            } else {
                if len(r) > 0 {
                    // append the first element of right part to final result
                    a = append(a, r[len(r)-1])
                    if len(r) > 1 {
                        r = r[:len(r)-1]
                    } else {
                        r = []int{}
                    }
                }
            }
        }
    }

    a = reverse(a)

    // return a
    return a

}

func reverse(s []int) []int {
    for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
        s[i], s[j] = s[j], s[i]
    }
    return s
}

// Return true if the passed list are equals
func testEq(a, b []int) bool {

    if a == nil && b == nil {
        return true
    }

    if a == nil || b == nil {
        fmt.Println(a, b)
        return false
    }

    if len(a) != len(b) {
        fmt.Println("!0 len ", len(a), len(b))
        return false
    }

    for i := range a {
        if a[i] != b[i] {
            fmt.Print(a[:10], b[:10])
            fmt.Println("a[i] != b[i] ", a[i], b[i])
            return false
        }
    }

    return true
}

// Convert duration in seconds
func fmtDuration(d time.Duration) string {

    return fmt.Sprintf("%f", d.Seconds())

}

// Read lines of column file to a slice of integer
func readLines(path string) (a []int, err error) {

    // read the file
    file, err := os.Open(path)

    // if err, panic
    if err != nil {
        return a, err
    }

    // defer close when function return
    defer file.Close()

    // scan file
    scanner := bufio.NewScanner(file)

    // while EOF
    for scanner.Scan() {

        // convert next line to int
        num, err := strconv.Atoi(scanner.Text())

        // if err, ignore
        if err == nil {
            a = append(a, num)
        }

    }

    // return list and err
    return a, scanner.Err()

}

// Run merge sort over a random generated list or a list of integers provided in
// filename. Save statistics to output file and prints logs during computation
func sorting(f string, q int, r bool, c int) {

    // init of var
    a := []int{}
    var err error

    // sign start time for reading
    startTime := time.Now()

    // read list of integers from file
    if !r {

        a, err = readLines(f)

        if err != nil {
            panic(err)
        }

        fmt.Printf("List length: %d\n", len(a))
        fmt.Printf("Random list readed in %s\n", fmtDuration(time.Now().Sub(startTime)))

    } else { // create random list

        a = rand.Perm(q)

        fmt.Printf("List length: %d\n", q)
        fmt.Printf("Random list generated in %s\n", fmtDuration(time.Now().Sub(startTime)))

    }

    if c > 1 {
        if math.Mod(float64(c), 2) != 0 {
            c = c - 1
        }
    }

    // channel process response
    responses := make(chan struct{}, c)

    // start time creation
    startTime = time.Now()

    // single core elapsed time
    single := msort_sort(a)

    // single core elapsed time
    singleCoreTime := time.Now().Sub(startTime)

    // sort algorithm from standard library
    aSorted := a[:]
    sort.Ints(aSorted)

    fmt.Printf("Verification of sorting algorithm %v\n", testEq(aSorted, single))
    fmt.Printf("Single go-routine: %s sec\n", fmtDuration(singleCoreTime))

    // if processes number > 1
    if c > 1 {

        // write result to file
        d1 := []byte("")
        err := ioutil.WriteFile("./go-mergesort-max-goroutine-" + strconv.Itoa(c) +"-list-" +strconv.Itoa(len(a))+ ".txt", d1, 0755)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Starting %d-go-routine process\n", c)

        //  save start time
        startTime = time.Now()

        // start a multi_sort_with a maximum number of concurrent thread
        a := merge_sort_multi(a, responses)

        // final merge
        multiCoreTime := time.Now().Sub(startTime)

        fmt.Printf("Sorted arrays equal : %v\n", (testEq(a, single)))
        fmt.Printf("%d-process ended: %s sec\n", c, fmtDuration(multiCoreTime))

        d1 = []byte(fmt.Sprintf("%d %s %s %s\n",
            len(a),
            fmtDuration(singleCoreTime),
            fmtDuration(multiCoreTime),
            fmtDuration(multiCoreTime/singleCoreTime)))

        err = ioutil.WriteFile("./go-mergesort-max-goroutine-" + strconv.Itoa(c) +"-list-" +strconv.Itoa(len(a))+ ".txt", d1, 0755)

        if err != nil {
            panic(err)
        }

    }

}

func main() {

    // parser to get argument from command line
    parser := argparse.NewParser("Sorting", "Create a random column of integers and save the to file. "+
        "Sort integers reading from a column file or randomly generated")

    // add argument to command line
    fileNameStr := parser.String("f", "filename", &argparse.Options{Required: false, Help: "Name of file for the list."})
    randomFlag := parser.Flag("r", "random", &argparse.Options{Required: false, Help: "Random list generation."})
    listLengthStr := parser.String("l", "list-length", &argparse.Options{Required: false, Help: "Name of person to greet."})
    coresNumberStr := parser.String("c", "max-concurrency", &argparse.Options{Required: false, Help: "Number of cores to use in the sorting."})

    err := parser.Parse(os.Args)
    if err != nil {
        fmt.Println(err.Error())
    }

    fileName := "list.txt"
    listLength, coresNumber := 1, 1

    if len(*fileNameStr) > 0 {
        fileName = *fileNameStr
    }
    if len(*listLengthStr) > 0 {
        listLength, err = strconv.Atoi(*listLengthStr)
    }
    if len(*coresNumberStr) > 0 {
        coresNumber, err = strconv.Atoi(*coresNumberStr)
    }
    if err != nil {
        panic(err)
    }

    //msort_merge([]int{2, 9, 12, 17, 21}, []int{1, 10, 13, 19})

    sorting(fileName, listLength, *randomFlag, coresNumber)

}
