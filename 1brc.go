package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

// TODO: try moving to SSD

type StationMeasure struct {
	StationName string
	MinTemp     float64
	AvgTemp     float64
	MaxTemp     float64
	count       int
}

const MB = 1024 * 1024

func main() {
	start := time.Now()
	fmt.Println("Starting 1brc")

	// get file name
	// fileName := "measurements100lines.txt" // basic testing
	// fileName := "measurements2andhalfmb.txt" // testing for division of file into chunks
	// fileName := "measurementslast500klines.txt" // testing for division of file into chunks
	fileName := "measurements.txt" // real file

	// process file, get results
	results, err := processFileNaive(fileName)
	if err != nil {
		panic(fmt.Sprintf("Error running 1brc: %v", err))
	}

	// print results
	fmt.Print("{")
	for station, results := range results {
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", station, results.MinTemp, results.AvgTemp, results.MaxTemp) // TODO: round to 1 decimal
	}
	// TODO: remove last ,
	fmt.Print("}")

	fmt.Println("")
	elapsed := time.Since(start)
	fmt.Println("1brc took ", elapsed.Seconds(), " seconds")
}

func processFileNaive(fileName string) (map[string]*StationMeasure, error) {
	// open file
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// divide file into chunks
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %w", err)
	}
	fileSize := fileInfo.Size()
	offsets := make([]int64, 0)
	chunkSize := int64(128 * MB)
	n := int64(0)
	for n < fileSize {
		offsets = append(offsets, int64(n))
		n += chunkSize
	}
	// fmt.Println("offsets", offsets)

	const numMergeWorkers = 15

	measuresChannel := make(chan map[string]*StationMeasure, len(offsets))
	var wg sync.WaitGroup

	// process segment of data
	// stationMeasures := make([]map[string]StationMeasure, len(offsets))
	for _, offset := range offsets {
		// fmt.Println("Processing chunk", i)
		wg.Add(1)
		buf := make([]byte, chunkSize+128)
		go func(offset int64) {
			defer wg.Done()
			err := parseChunk(file, buf, offset, chunkSize, measuresChannel)
			if err != nil {
				fmt.Println("error parsing chunk: %w", err)
			}
		}(offset)

		// stationMeasures[i] = stationMeasure
	}

	go func() {
		wg.Wait()
		close(measuresChannel)
	}()

	var resultMutex sync.Mutex
	result := make(map[string]*StationMeasure)

	var workerPool sync.WaitGroup
	workerPool.Add(numMergeWorkers)
	for i := 0; i < numMergeWorkers; i++ {
		go func() {
			defer workerPool.Done()
			for stationMeasure := range measuresChannel {
				// fmt.Println("received station measure")
				// merge data into results
				for stationName, measure := range stationMeasure {
					resultMutex.Lock()
					if existingMeasure, ok := result[stationName]; ok {
						existingMeasure.MinTemp = getNewMin(existingMeasure.MinTemp, measure.MinTemp)
						existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, measure.AvgTemp, existingMeasure.count+measure.count)
						existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, measure.MaxTemp)
						existingMeasure.count += measure.count
					} else {
						result[stationName] = measure
					}
					resultMutex.Unlock()
				}
				// fmt.Println("finished merging chunk")
			}
		}()
	}

	workerPool.Wait()

	// for stationMeasure := range measuresChannel {
	// 	// go func(stationMeasure map[string]*StationMeasure) {
	// 	// 	fmt.Println("received station measure")
	// 	// 	for stationName, measure := range stationMeasure {
	// 	// 		resultMutex.Lock()
	// 	// 		defer resultMutex.Unlock()
	// 	// 		if existingMeasure, ok := result[stationName]; ok {
	// 	// 			existingMeasure.MinTemp = getNewMin(existingMeasure.MinTemp, measure.MinTemp)
	// 	// 			existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, measure.AvgTemp, existingMeasure.count+measure.count)
	// 	// 			existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, measure.MaxTemp)
	// 	// 			existingMeasure.count += measure.count
	// 	// 		} else {
	// 	// 			result[stationName] = measure
	// 	// 		}
	// 	// 	}
	// 	// }(stationMeasure)

	// 	// fmt.Println("received station measure")
	// 	// merge data into results
	// 	for stationName, measure := range stationMeasure {
	// 		if existingMeasure, ok := result[stationName]; ok {
	// 			existingMeasure.MinTemp = getNewMin(existingMeasure.MinTemp, measure.MinTemp)
	// 			existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, measure.AvgTemp, existingMeasure.count+measure.count)
	// 			existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, measure.MaxTemp)
	// 			existingMeasure.count += measure.count
	// 		} else {
	// 			result[stationName] = measure
	// 		}
	// 	}
	// 	// fmt.Println("finished merging chunk")
	// }

	// return
	return result, nil
}

func parseChunk(file *os.File, buffer []byte, offset int64, chunkSize int64, outputChannel chan map[string]*StationMeasure) error {
	// read chunk
	n, err := file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading chunk: %w", err)
	}
	// fmt.Println("read n bytes in chunk:", n)

	stationMeasures := make(map[string]*StationMeasure)
	index := 0

	// handle if offset is not 0
	if offset != 0 {
		// find first newline
		for {
			if buffer[index] == '\n' {
				index++
				break
			}
			index++
		}
	}

	for {
		// read line
		nameStart := index
		var nameEnd int
		var valueStart int
		var valueEnd int
		for {
			if buffer[index] == ';' {
				nameEnd = index
				valueStart = index + 1
			}

			if buffer[index] == '\n' {
				valueEnd = index
				index++
				break
			}

			index++
		}

		name := string(buffer[nameStart:nameEnd])
		value, err := strconv.ParseFloat(string(buffer[valueStart:valueEnd]), 64)
		if err != nil {
			fmt.Println("error parsing value:", string(buffer[valueStart:valueEnd]), "namestart: ", nameStart, "nameend: ", nameEnd, "valuestart: ", valueStart, "valueend: ", valueEnd, "index: ", index)
			fmt.Println("nearbuffer", string(buffer[int(math.Max(float64(index-10), float64(0))):index+10]))
			return fmt.Errorf("error parsing value: %w", err)
		}

		stationMeasure := StationMeasure{
			StationName: name,
			MinTemp:     value,
			AvgTemp:     value,
			MaxTemp:     value,
			count:       1,
		}
		stationMeasures = merge(stationMeasures, stationMeasure)

		if int64(index) >= chunkSize || index >= n {
			break
		}
	}

	outputChannel <- stationMeasures

	return nil
}

func merge(existingMeasures map[string]*StationMeasure, newMeasure StationMeasure) map[string]*StationMeasure {
	stationName := newMeasure.StationName
	if existingMeasure, ok := existingMeasures[stationName]; ok {
		existingMeasure.MinTemp = getNewMin(existingMeasure.MinTemp, newMeasure.MinTemp)
		existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, newMeasure.AvgTemp, existingMeasure.count+newMeasure.count)
		existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, newMeasure.MaxTemp)
		existingMeasure.count += newMeasure.count
		// existingMeasures[stationName] = existingMeasure
	} else {
		existingMeasures[stationName] = &newMeasure
	}
	return existingMeasures
}

func getNewAverage(currentAverage, newValue float64, count int) float64 {
	return currentAverage + ((newValue - currentAverage) / float64(count))
}

func getNewMin(currentMin, newValue float64) float64 {
	return min(currentMin, newValue)
}

func getNewMax(currentMax, newValue float64) float64 {
	return max(currentMax, newValue)
}
