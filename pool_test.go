// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/devfacet/worker"
)

func TestPoolNew(t *testing.T) {
	var tests = []struct {
		name            string
		numberOfWorkers int
		jobsBuffer      int
		err             error
	}{
		{
			name: "new worker pool (simple)",
			err:  nil,
		},
		{
			name:            "new worker pool (invalid number of workers)",
			numberOfWorkers: -1,
			err:             errors.New("number of workers must be greater than 0 and less than 1000"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp, err := worker.NewPool(context.Background(), tt.numberOfWorkers, 0)
			if tt.err != nil {
				if errors.Is(err, tt.err) {
					t.Errorf("got %v, want %v", err, tt.err)
				}
			} else if wp == nil {
				t.Error("got nil worker pool, want not nil")
			}
		})
	}
}

func TestPool(t *testing.T) {
	jobs, results := jobResGen("sum", 20)

	var tests = []struct {
		name            string
		numberOfWorkers int
		jobsBuffer      int
		jobs            []worker.Job
		results         map[string]worker.Result
	}{
		{
			name:    "worker pool (default)",
			jobs:    jobs,
			results: results,
		},
		{
			name:            "worker pool (single worker)",
			numberOfWorkers: 1,
			jobs:            jobs,
			results:         results,
		},
		{
			name:       "worker pool (no jobs buffer)",
			jobsBuffer: 0,
			jobs:       jobs,
			results:    results,
		},
		{
			name:            "worker pool (multiple workers and jobs buffer)",
			numberOfWorkers: 10,
			jobsBuffer:      10,
			jobs:            jobs,
			results:         results,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp, err := worker.NewPool(context.Background(), tt.numberOfWorkers, tt.jobsBuffer)
			if err != nil {
				t.Errorf("failed to create a new worker pool: %s", err)
				return
			}
			go func() {
				if err := wp.Run(); err != nil {
					t.Errorf("failed to run the worker pool: %s", err)
				}
			}()
			for _, j := range tt.jobs {
				go wp.AddJob(j)
			}
			resultCounter := 0
			for result := range wp.Results() {
				resultCounter++
				if want := tt.results[result.Descriptor.ID].Value; want != result.Value {
					t.Errorf("got %v, want %v", result.Value, want)
				}
				if resultCounter == len(tt.results) {
					if err := wp.Stop(); err != nil {
						t.Errorf("failed to stop the worker pool: %s", err)
					}
				}
			}
		})
	}
}

func TestPoolStart(t *testing.T) {
	wp := worker.Pool{}
	if err := wp.Start(); err != nil {
		t.Errorf("failed to start the worker pool: %s", err)
	}
}

func TestPoolRun(t *testing.T) {
	wp := worker.Pool{}
	go func() {
		if err := wp.Run(); err != nil {
			t.Errorf("failed to start the worker pool: %s", err)
		}
	}()
}

func BenchmarkPoolDefault(b *testing.B) {
	wp, err := worker.NewPool(context.Background(), 0, 0)
	if err != nil {
		b.Errorf("failed to create a new worker pool: %s", err)
		return
	}
	go func() {
		if err := wp.Run(); err != nil {
			b.Errorf("failed to run the worker pool: %s", err)
		}
	}()
	job := worker.Job{
		Descriptor: jobDescriptor("01"),
		Fn:         jobFn("benchmark"),
		Args:       []interface{}{struct{}{}},
	}
	for i := 0; i < b.N; i++ {
		wp.AddJob(job)
		<-wp.Results()
	}
}
