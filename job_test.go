// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/devfacet/worker"
)

func TestJobExec(t *testing.T) {
	var tests = []struct {
		name string
		job  worker.Job
		want worker.Result
	}{
		{
			name: "successful job",
			job: worker.Job{
				Descriptor: jobDescriptor("sum"),
				Fn:         jobFn("sum"),
				Args:       []interface{}{1, 2},
			},
			want: worker.Result{
				Descriptor: jobDescriptor("sum"),
				Value:      interface{}(3),
				Error:      nil,
			},
		},
		{
			name: "failed job",
			job: worker.Job{
				Descriptor: worker.Descriptor{},
				Fn:         jobFn("failed"),
				Args:       nil,
			},
			want: worker.Result{
				Descriptor: worker.Descriptor{},
				Value:      nil,
				Error:      errors.New("failed"),
			},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.job.Exec(ctx)
			if !reflect.DeepEqual(got.Descriptor, tt.want.Descriptor) ||
				got.Value != tt.want.Value ||
				errors.Is(got.Error, errors.New("failed")) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkNewJobWithExec(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		j := worker.Job{
			Descriptor: jobDescriptor("1"),
			Fn:         jobFn("benchmark"),
			Args:       []interface{}{struct{}{}},
		}
		j.Exec(ctx)
	}
}

func BenchmarkJobExecOnly(b *testing.B) {
	ctx := context.Background()
	j := worker.Job{
		Descriptor: jobDescriptor("1"),
		Fn:         jobFn("benchmark"),
		Args:       []interface{}{struct{}{}},
	}
	for i := 0; i < b.N; i++ {
		j.Exec(ctx)
	}
}

func jobFn(choice string) func(ctx context.Context, args ...interface{}) (interface{}, error) {
	switch choice {
	case "sum":
		return func(ctx context.Context, args ...interface{}) (interface{}, error) {
			var sum int
			for _, v := range args {
				switch v := v.(type) {
				case int:
					sum += v
				default:
					return nil, fmt.Errorf("invalid arg type: %T", v)
				}
			}
			return sum, nil
		}
	case "failed":
		return func(ctx context.Context, args ...interface{}) (interface{}, error) {
			return nil, errors.New("failed")
		}
	case "benchmark":
		return func(ctx context.Context, args ...interface{}) (interface{}, error) {
			return struct{}{}, nil
		}
	default:
		return nil
	}
}

func jobDescriptor(id string) worker.Descriptor {
	return worker.Descriptor{
		ID:       id,
		Metadata: map[string]interface{}{"Name": "test"},
	}
}

func jobResGen(choice string, n int) ([]worker.Job, map[string]worker.Result) {
	var jobs []worker.Job
	results := map[string]worker.Result{}
	for i := 0; i < n; i++ {
		jobID := fmt.Sprintf("%03d", i+1)
		jobs = append(jobs, worker.Job{
			Descriptor: jobDescriptor(jobID),
			Fn:         jobFn(choice),
			Args:       []interface{}{i + 1, i + 2},
		})
		switch choice {
		case "sum":
			results[jobID] = worker.Result{
				Value: interface{}(i + i + 3),
			}
		case "failed":
			results[jobID] = worker.Result{
				Error: errors.New("failed"),
			}
		}
	}
	return jobs, results
}
