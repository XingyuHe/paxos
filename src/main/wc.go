package main

import (
	"container/list"
	"fmt"
	"mapreduce"
	"os"
	"strings"
	"unicode"
	"strconv"
)

/*

our simplified version of MapReduce does not supply a
key to the Map function, as in the paper; only a value,
which is a part of the input file contents

inputs
	value string: a part of the text
outputs
	list.List: a linked list where each value contains a KeyValue type

*/

func Map(value string) *list.List {

	sep := func(c rune) bool {
		return !unicode.IsLetter(c)
	}

	tokens := strings.FieldsFunc(value, sep)

	// start counting
	counter := make(map[string]int)
	for _, tkn := range tokens {
		counter[tkn]++
	}


	ans := list.New()
	for tkn, cnt := range counter {
		ans.PushBack(mapreduce.KeyValue{Key: tkn,
		Value: strconv.Itoa(cnt)})
	}

	return ans
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	ans := 0
    for e := values.Front(); e != nil; e = e.Next() {
		str_val := e.Value.(string)
		val, err := strconv.Atoi(str_val)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
		ans += val
	}
	return strconv.Itoa(ans)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
        fmt.Printf("%s: see usage comments in file\n", os.Args[0])
    } else if os.Args[1] == "master" {
        if os.Args[3] == "sequential" {
            mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
        } else {
            mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
            // Wait until MR is done
            <- mr.DoneChannel
        }
    } else {
        mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
    }
}
