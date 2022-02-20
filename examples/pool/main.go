package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/devfacet/worker"
)

func main() {
	// Create a worker pool
	numberOfWorkers := 4 // default runtime.NumCPU()
	jobsBuffer := 0      // 0 unbuffered, > 0 buffered
	wp, err := worker.NewPool(context.Background(), numberOfWorkers, jobsBuffer)
	if err != nil {
		log.Fatalf("failed to create a worker pool: %s", err)
	}
	// Run the worker pool in the background
	go func() {
		if err := wp.Run(); err != nil {
			log.Fatalf("failed to run the worker pool: %s", err)
		}
	}()

	// Add jobs to the worker pool
	numOfJobs := 100
	for i := 0; i < numOfJobs; i++ {
		go wp.AddJob(worker.Job{
			Descriptor: worker.Descriptor{
				ID: fmt.Sprintf("%03d", i+1),
				Metadata: map[string]interface{}{
					"Created": time.Now(),
				},
			},
			Fn: sha256Checksum, // function name
			Args: []interface{}{ // function arguments
				[]byte(fmt.Sprintf("job-%d", i)),
			},
		})
	}

	// Check job results
	for i := 0; i < numOfJobs; i++ {
		result := <-wp.Results()
		if result.Error != nil {
			log.Fatalf("job %q failed due to %s", result.Descriptor.ID, result.Error)
		}
		fmt.Printf("job %q returned: %v\n", result.Descriptor.ID, result.Value)
	}
}

// sha256Checksum returns the SHA256 checksum of the data.
func sha256Checksum(ctx context.Context, args ...interface{}) (interface{}, error) {
	//time.Sleep(500 * time.Millisecond) // artificial delay for tests

	if len(args) == 0 {
		return nil, errors.New("missing data")
	}
	b, ok := args[0].([]byte)
	if !ok {
		return nil, errors.New("invalid data")
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
