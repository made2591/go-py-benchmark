package main

import (
    "os"
    "fmt"
    "time"
    "math"
    "sort"
    "bufio"
    "strconv"
    "math/rand"
    "io/ioutil"
    "github.com/akamensky/argparse"
)

// Returns the result of a multi processed merge sort - the sort part - over the passed list
func msort_sort_multi(a []int, responses chan []int) {
    responses <- msort_sort(a)
}

// Returns the result of a multi processed merge sort - the merge part - over the passed lists
func msort_merge_multi(l []int, r []int, responses chan []int) {
    responses <- msort_merge(l, r)
}

// Returns the result of a merge sort - the sort part - over the passed list
func msort_sort(a []int) []int {

    // if len of list is zero or 1, the list is ordered.
    if len(a) <= 1 {

        // return the list
        return a
    }

    // split the list 
    m := int(math.Floor(float64(len(a))/2))
    
    // return the merged result of merge sort call over l and r parts - divide and conquer
    return msort_merge(msort_sort(a[0:m]), msort_sort(a[m:]))

}

// Returns the result of a merge sort - the merge part - over the passed lists
func msort_merge(l []int, r []int) (a []int) {

    // while there are elements in the list
    for len(l) > 0 || len(r) > 0 {

        // if both list are not empty
        if len(l) > 0 && len(r) > 0 {

            // append to a the smallest of the two first element of left and right part
            if l[0] <= r[0] {

                // append and remove first element of left part
                a = append(a, l[0])
                l = l[1:]

            } else {

                // append and remove first element of right part
                a = append(a, r[0])
                r = r[1:]

            }

        } else {

            // append left part
            if len(l) > 0 {

                // append right part
                a = append(a, l...)
                break

            } else {

                if len(r) > 0 {

                    // append right part
                    a = append(a, r...)
                    break
                }

            }
        }
    }

    // return a
    return a

}

// Return true if the passed list are equals
func testEq(a, b []int) bool {

    if a == nil && b == nil {
        return true;
    }

    if a == nil || b == nil {
        fmt.Println(a, b)
        return false;
    }

    if len(a) != len(b) {
        fmt.Println(len(a), len(b))
        return false
    }

    for i := range a {
        if a[i] != b[i] {
            fmt.Println(a[i], b[i])
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

    // channel process response
    responses := make(chan []int)

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
        d1 := []byte("go-mergesort-"+strconv.Itoa(c)+".txt")
        err := ioutil.WriteFile("./go-mergesort-"+strconv.Itoa(c)+".dat", d1, 0755)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Starting %d-go-routine process\n", c)

        //  save start time
        startTime = time.Now()

        // compute number of steps for specified number of process
        step := int(math.Floor(float64(len(a)/c)))

        // assign each go routine a portion of sorting
        for n := 0; n < c; n++ {

            // assign to the last go-routine the remaining part of list to order
            if n < c-1 {
                go msort_sort_multi(a[n*step:(n+1)*step], responses)
            } else {
                go msort_sort_multi(a[n*step:], responses)
            }

        }

        // use the same number of go-routine to handle the final sort
        fmt.Printf("Performing final msort_merge...\n")
        startTimeFinalMerge := time.Now()

        // merge each part of list and pull the result in the response (handled by channel)
        if len(responses) > 2 {

            // while there are at least a response in the list
            for len(responses) > 0 {

                // assign first two part
                go msort_merge_multi(<-responses, <-responses, responses)

            }

        }

        // perform final merge step
        a = msort_merge(<-responses, <-responses)

        // final merge time
        finalMergeTime := time.Now().Sub(startTimeFinalMerge)

        // final merge
        fmt.Printf("Final msort_merge duration: %s\n", fmtDuration(finalMergeTime))

        // final merge
        multiCoreTime := time.Now().Sub(startTime)

        fmt.Printf("Sorted arrays equal : %v\n", (testEq(a, single)))
        fmt.Printf("%d-process ended: %s sec\n", c, fmtDuration(multiCoreTime))

        d1 = []byte(fmt.Sprintf("%d %s %s %s %s\n",
            len(a),
            fmtDuration(singleCoreTime),
            fmtDuration(multiCoreTime),
            fmtDuration(multiCoreTime/singleCoreTime),
            fmtDuration(finalMergeTime)))


        err = ioutil.WriteFile("./go-mergesort-"+strconv.Itoa(c)+".txt", d1, 0755)

        if err != nil {
            panic(err)
        }

    }

}

func main() {

    // parser to get argument from command line
    parser := argparse.NewParser("Sorting", "Create a random column of integers and save the to file. " +
                        "Sort integers reading from a column file or randomly generated")

    // add argument to command line
    fileName       := parser.String("f","filename",    &argparse.Options{Required: false, Help: "Name of file for the list."})
    randomFlag     := parser.Flag(  "r","random",      &argparse.Options{Required: false, Help: "Random list generation."})
    listLengthStr  := parser.String("l","listLength",  &argparse.Options{Required: false, Help: "Name of person to greet."})
    coresNumberStr := parser.String("c","coresnumber", &argparse.Options{Required: false, Help: "Number of cores to use in the sorting."})

    err := parser.Parse(os.Args)
    if err != nil {
        fmt.Println(err.Error())
    }

    listLength, err  := strconv.Atoi(*listLengthStr)
    coresNumber, err := strconv.Atoi(*coresNumberStr)

    if err != nil {
        panic(err)
    }

    sorting(*fileName, listLength, *randomFlag, coresNumber)

}














