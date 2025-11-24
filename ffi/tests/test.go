// How to run:
// go run test.go
// Or compile and run:
// go build -o test-go test.go && ./test-go

package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"
)

func formatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func main() {
	filePath := "./test.csv.gz"

	fmt.Printf("Reading: %s\n", filePath)

	start := time.Now()

	// Open gzipped file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v\n", err)
		os.Exit(1)
	}
	defer gzReader.Close()

	// Create CSV reader
	csvReader := csv.NewReader(gzReader)
	csvReader.Comment = '#'
	csvReader.ReuseRecord = true // Reuse the same slice for better performance

	var count uint64 = 0

	// Read header (first non-comment line)
	_, err = csvReader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading header: %v\n", err)
		os.Exit(1)
	}

	// Count rows
	for {
		_, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading record: %v\n", err)
			os.Exit(1)
		}
		count++
	}

	duration := time.Since(start).Seconds()

	fmt.Printf("\nParsed %s rows in %.2fs\n", formatNumber(count), duration)
	fmt.Printf("Rate: %s rows/sec\n", formatNumber(uint64(float64(count)/duration)))
}
