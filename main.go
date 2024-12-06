package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	bucketName = "wagner-test-bucket"
	region     = "us-east-1"
	outputKey  = "merged.csv"
)

func main() {
	// Simulate retrieving parts of the file
	fileParts := []string{"part1", "part2", "part3"}

	// Stream merge and upload directly to S3
	err := streamMergeAndUpload(fileParts, bucketName, outputKey)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("CSV files merged and uploaded to S3 successfully.")
	}
}

// streamMergeAndUpload streams CSV merging and uploads directly to S3.
func streamMergeAndUpload(fileParts []string, bucket, key string) error {
	// Create an AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %v", err)
	}

	uploader := s3manager.NewUploader(sess)

	// Create an io.Pipe for streaming
	pr, pw := io.Pipe()

	// Use a WaitGroup to wait for the writer goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	// Start writing merged CSV data to the pipe
	go func() {
		defer wg.Done()
		err := writeMergedCSVToPipe(fileParts, pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	// Upload the stream directly to S3
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr, // PipeReader is directly streamed
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %v", err)
	}

	// Wait for the writing goroutine to finish
	wg.Wait()

	return nil
}

// writeMergedCSVToPipe writes merged CSV data to the provided pipe writer.
func writeMergedCSVToPipe(fileParts []string, writer *io.PipeWriter) error {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	headerWritten := false

	for _, part := range fileParts {
		// Simulate retrieving a part of the file
		data, err := retrieveFilePart(part)
		if err != nil {
			return fmt.Errorf("failed to retrieve file part %s: %v", part, err)
		}

		reader := csv.NewReader(strings.NewReader(data))
		rows, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("failed to parse CSV data from part %s: %v", part, err)
		}

		for i, row := range rows {
			// Write the header only once
			if i == 0 && !headerWritten {
				csvWriter.Write(row)
				headerWritten = true
			} else if i > 0 {
				csvWriter.Write(row)
			}
		}
	}
	return nil
}

// retrieveFilePart simulates retrieving a part of the file from an external system.
func retrieveFilePart(part string) (string, error) {
	// Simulate a delay to mimic an external call
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))

	// Mock CSV data for the file part
	mockData := map[string]string{
		"part1": "id,name,age\n1,John,30\n2,Jane,25",
		"part2": "id,name,age\n3,Mike,35\n4,Sara,28",
		"part3": "id,name,age\n5,Paul,40\n6,Linda,32",
	}

	data, exists := mockData[part]
	if !exists {
		return "", fmt.Errorf("file part %s not found", part)
	}

	return data, nil
}
