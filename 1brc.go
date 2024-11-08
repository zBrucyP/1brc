package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
	"math/rand/v2"
	_ "net/http/pprof"
)

// ideas
// randomize / vary chunk size so merging thread isn't waiting and hit all at once
// limit processing threads to ~number of cores

type StationMeasure struct {
	AvgTemp     float64
	Count       int
	MaxTemp     float64
	MinTemp     float64
	StationName string
	Sum         float64
}

type Chunk struct {
	Offset int64
	ChunkSize int64
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
	results, err := processFile(fileName)
	if err != nil {
		panic(fmt.Sprintf("Error running 1brc: %v", err))
	}

	// print results
	fmt.Print("{")
	for station, results := range results {
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", station, results.MinTemp, results.Sum/float64(results.Count), results.MaxTemp)
	}
	// TODO: remove last ,
	fmt.Print("}")

	fmt.Println("")
	elapsed := time.Since(start)
	fmt.Println("1brc took ", elapsed.Seconds(), " seconds")
}

func processFile(fileName string) (map[string]*StationMeasure, error) {
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
	offsets := make([]Chunk, 0)
	chunkSizeMin := int64(16 * MB)
	chunkSizeMax := int64(48 * MB)
	n := int64(0)
	for n < fileSize {
		chunkSize := rand.Int64N(chunkSizeMax-chunkSizeMin) + chunkSizeMin
		offsets = append(offsets, Chunk{
			Offset: n,
			ChunkSize: chunkSize,
		})
		n += chunkSize
	}
	fmt.Println("offsets", len(offsets))

	const numMergeWorkers = 1

	measuresChannel := make(chan map[string]*StationMeasure, len(offsets))
	var wg sync.WaitGroup

	// process segment of data
	// stationMeasures := make([]map[string]StationMeasure, len(offsets))
	for _, offset := range offsets {
		// fmt.Println("Processing chunk", i)
		wg.Add(1)
		buf := make([]byte, offset.ChunkSize+128)
		go func(offset Chunk) {
			defer wg.Done()
			err := parseChunk(file, buf, offset.Offset, offset.ChunkSize, measuresChannel)
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
						// existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, measure.AvgTemp, existingMeasure.Count+measure.Count)
						existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, measure.MaxTemp)
						existingMeasure.Sum = getNewSum(existingMeasure.Sum, measure.Sum)
						existingMeasure.Count += measure.Count
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
			Sum:         value,
			Count:       1,
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
		// existingMeasure.AvgTemp = getNewAverage(existingMeasure.AvgTemp, newMeasure.AvgTemp, existingMeasure.Count+newMeasure.Count)
		existingMeasure.Sum = getNewSum(existingMeasure.Sum, newMeasure.Sum)
		existingMeasure.MaxTemp = getNewMax(existingMeasure.MaxTemp, newMeasure.MaxTemp)
		existingMeasure.Count += newMeasure.Count
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

func getNewSum(currentSum, newValue float64) float64 {
	return currentSum + newValue
}
