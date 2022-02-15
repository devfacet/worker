// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/devfacet/worker"
)

func TestWorkerRun(t *testing.T) {
	jobs, results := jobResGen("sum", 2)

	var tests = []struct {
		name    string
		jobs    []worker.Job
		results map[string]worker.Result
	}{
		{
			name:    "worker run",
			jobs:    jobs,
			results: results,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numJobs := len(tt.jobs)
			jobs := make(chan worker.Job, numJobs)
			results := make(chan worker.Result, numJobs)
			w := worker.Worker{}
			go w.Run(ctx, jobs, results)

			for _, job := range tt.jobs {
				jobs <- job
			}
			close(jobs)

			for i := 0; i < numJobs; i++ {
				result := <-results
				if want := tt.results[result.Descriptor.ID].Value; want != result.Value {
					t.Errorf("got %v, want %v", result.Value, want)
				}
			}
		})
	}
}

func TestWorkerRunCtx(t *testing.T) {
	jobs := make(chan worker.Job)
	defer close(jobs)
	results := make(chan worker.Result)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	w := worker.Worker{}
	go w.Run(ctx, jobs, results)
	result := <-results
	if !errors.Is(result.Error, context.DeadlineExceeded) {
		t.Errorf("got %v, want %v", result.Error, context.DeadlineExceeded)
	}
}

func BenchmarkWorkerDefault(b *testing.B) {
	jobs := make(chan worker.Job)
	defer close(jobs)
	results := make(chan worker.Result)
	job := worker.Job{
		Descriptor: jobDescriptor("01"),
		Fn:         jobFn("benchmark"),
		Args:       []interface{}{struct{}{}},
	}
	ctx := context.Background()
	w := worker.Worker{}
	go w.Run(ctx, jobs, results)
	for i := 0; i < b.N; i++ {
		jobs <- job
		<-results
	}
}
