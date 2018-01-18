package main

import (
    "fmt"
    "time"
    "math"
    "os"
    "strconv"
    "math/rand"
    "sort"
    "io/ioutil"
    "bufio"
)

func merge_sort_multi(list_part []int, responses chan []int) {
    responses <- merge_sort(list_part)
}

func merge_multi(list_part_left []int, list_part_right []int, responses chan []int) {
    responses <- merge(list_part_left, list_part_right)
}

func merge_sort(a []int) []int {
    length_a := float64(len(a))
    if length_a <= 1 {
        return a
    }
    m := int(math.Floor(length_a / 2))
    a_left := a[0:m]
    a_right := a[m:]
    a_left = merge_sort(a_left)
    a_right = merge_sort(a_right)
    return merge(a_left, a_right)
}

func merge(left []int, right []int) (a []int) {
    for len(left) > 0 || len(right) > 0 {
        if len(left) > 0 && len(right) > 0 {
            if left[0] <= right[0] {
                a = append(a, left[0])
                left = left[1:]
            } else {
                a = append(a, right[0])
                right = right[1:]
            }
        } else {
            if len(left) > 0 {
                a = append(a, left...)
                break
            } else {
                if len(right) > 0 {
                    a = append(a, right...)
                    break
                }
            }
        }
    }
    return a
}

func testEq(a, b []int) bool {

    if a == nil && b == nil {
        return true;
    }

    if a == nil || b == nil {
        return false;
    }

    if len(a) != len(b) {
        return false
    }

    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }

    return true
}

func fmtDuration(d time.Duration) string {
    return fmt.Sprintf("%f", d.Seconds())
}

func readLines(path string) ([]int, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var lines []int
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        num, err := strconv.Atoi(scanner.Text())
        if err == nil {
            lines = append(lines, num)
        }
    }
    return lines, scanner.Err()
}

func main() {

    cores := 1
    var err error = nil
    if len(os.Args) == 2 {
        cores, err = strconv.Atoi(os.Args[1])
        if err == nil {
            if cores > 1 {
                if math.Mod(float64(cores), 2) != 0 {
                    cores = cores - 1
                }
            }
        } else {
            cores = 1
        }
    }
    fmt.Printf("Using %d cores\n", cores)

    responses := make(chan []int)

    random := false
    a := []int{}
    start_time := time.Now()
    if !random {
        start_time := time.Now()
        a, err = readLines("list.csv")
        if err != nil {
            panic(err)
        }
        fmt.Printf("Random list readed in %s\n", fmtDuration(time.Now().Sub(start_time)))
        fmt.Printf("List length: %d\n", len(a))
    } else {
        dummy := rand.Intn(300000)
        l := dummy*5 + rand.Intn(300000)
        fmt.Printf("List length: %d\n", l)
        a = rand.Perm(l)
        fmt.Printf("Random list generated in %s\n", fmtDuration(time.Now().Sub(start_time)))
    }
    l := len(a)
    start_time = time.Now()
    single := merge_sort(a)
    single_core_time := time.Now().Sub(start_time)
    a_sorted := a[:]
    sort.Ints(a_sorted)
    fmt.Printf("Verification of sorting algorithm %v\n", testEq(a_sorted, single))
    fmt.Printf("Single Core: %s sec\n", fmtDuration(single_core_time))
    if cores > 1 {

        d1 := []byte("go-mergesort-"+strconv.Itoa(cores)+".dat")
        err := ioutil.WriteFile("./go-mergesort-"+strconv.Itoa(cores)+".dat", d1, 0755)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Starting %d-core process\n", cores)
        start_time = time.Now()
        step := int(math.Floor(float64(l / cores)))
        for n := 0; n < cores; n++ {
            if n < cores-1 {
                go merge_sort_multi(a[n*step:(n+1)*step], responses)
            } else {
                go merge_sort_multi(a[n*step:], responses)
            }
        }
        fmt.Printf("Performing final merge...\n")
        start_time_final_merge := time.Now()
        if len(responses) > 2 {
            for len(responses) > 0 {
                go merge_multi(<-responses, <-responses, responses)
            }
        }
        a = merge(<-responses, <-responses)
        final_merge_time := time.Now().Sub(start_time_final_merge)
        fmt.Printf("Final merge duration: %s\n", fmtDuration(final_merge_time))
        multi_core_time := time.Now().Sub(start_time)
        fmt.Printf("Sorted arrays equal : %v\n", (testEq(a, single)))
        fmt.Printf("%d-Core ended: %s sec\n", cores, fmtDuration(multi_core_time))
        d1 = []byte(fmt.Sprintf("%d %s %s %s %s\n", l,
            fmtDuration(single_core_time),
            fmtDuration(multi_core_time),
            fmtDuration(multi_core_time/single_core_time),
            fmtDuration(final_merge_time)))
        err = ioutil.WriteFile("./go-mergesort-"+strconv.Itoa(cores)+".dat", d1, 0755)
        if err != nil {
            panic(err)
        }

    }
}



















