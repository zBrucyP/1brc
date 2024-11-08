package main

import (
	"fmt"
	"os"
	"testing"
)

func Test1brc(t *testing.T) {
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(fmt.Sprintf("Error running 1brc: %v", err))
	}
	defer file.Close()

	chunkSize := int64(10000)
	buf := make([]byte, chunkSize+128)
	file.ReadAt(buf, 268437504)

	fmt.Println(string(buf))

}
