package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for unit := range in {
		num, ok := unit.(int)
		if !ok {
			panic("type assertion failed")
		}
		data := strconv.Itoa(num)
		wg.Add(1)
		go func(data string) {
			defer wg.Done()
			var md5 string
			func() {
				mu.Lock()
				defer mu.Unlock()
				md5 = DataSignerMd5(data)
			}()
			ch2 := make(chan string)
			go func() {
				ch2 <- DataSignerCrc32(md5)
			}()
			out <- DataSignerCrc32(data) + "~" + <-ch2
		}(data)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	for unit := range in {
		data, ok := unit.(string)
		if !ok {
			panic("type assertion failed")
		}
		wg.Add(1)
		go func(data string) {
			defer wg.Done()
			const numHashes int = 6
			var multiRes [numHashes]string
			wgIn := sync.WaitGroup{}
			wgIn.Add(numHashes)
			for i := 0; i < numHashes; i++ {
				go func(i int) {
					defer wgIn.Done()
					multiRes[i] = DataSignerCrc32(strconv.Itoa(i) + data)
				}(i)
			}
			wgIn.Wait()
			out <- strings.Join(multiRes[:], "")
		}(data)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result []string
	for unit := range in {
		data, ok := unit.(string)
		if !ok {
			panic("type assertion failed")
		}
		result = append(result, data)
	}
	sort.Strings(result)
	out <- strings.Join(result, "_")
}

func ExecutePipeline(jobs ...job) {
	out := make(chan interface{})
	firstJob := jobs[0]
	jobs = jobs[1:]
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(out)
		firstJob(nil, out)
	}()
	inChan := out
	for _, j := range jobs {
		outChan := make(chan interface{})
		wg.Add(1)
		go func(worker job, chIn, chOut chan interface{}) {
			defer wg.Done()
			defer close(chOut)
			worker(chIn, chOut)
		}(j, inChan, outChan)
		inChan = outChan
	}
	wg.Wait()
}
