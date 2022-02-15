// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

// Package worker provides functions for running tasks.
package worker

import (
	"context"
)

// Worker represents a worker which runs jobs.
type Worker struct {
}

// Run runs the worker by the given context, job and result channels.
func (worker *Worker) Run(ctx context.Context, jobs <-chan Job, results chan<- Result) {
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				// Channel is closed and empty
				return
			}
			results <- job.Exec(ctx)
		case <-ctx.Done():
			results <- Result{
				Error: ctx.Err(),
			}
			return
		}
	}
}
