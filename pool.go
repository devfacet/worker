// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

// NewPool creates a new worker Pool instance by the given options.
func NewPool(ctx context.Context, numberOfWorkers int, jobsBuffer int) (*Pool, error) {
	if numberOfWorkers < 0 || numberOfWorkers > 1000 {
		return nil, errors.New("number of workers must be greater than 0 and less than 1000")
	}

	// Init the pool instance
	pool := Pool{
		ctx:             ctx,
		jobsBuffer:      jobsBuffer,
		numberOfWorkers: numberOfWorkers,
	}
	pool.init()

	return &pool, nil
}

// Pool represents a worker pool which runs concurrent workers and jobs.
type Pool struct {
	ctx             context.Context
	isRunning       bool
	isStopped       bool
	isStopping      bool
	jobs            chan Job
	jobsBuffer      int
	jobsClosed      bool
	numberOfWorkers int
	results         chan Result
	resultsClosed   bool
	wg              sync.WaitGroup
	workers         []Worker
}

// init initializes the pool variables.
func (pool *Pool) init() {
	if pool.ctx == nil {
		pool.ctx = context.Background()
	}
	if pool.numberOfWorkers == 0 {
		pool.numberOfWorkers = runtime.NumCPU()
	}
	if pool.jobs == nil {
		pool.jobs = make(chan Job, pool.jobsBuffer)
	}
	if pool.results == nil {
		pool.results = make(chan Result, pool.jobsBuffer)
	}
}

// Run runs the worker pool and waits for it to complete.
func (pool *Pool) Run() error {
	if pool.isStopped {
		return errors.New("pool is stopped, initialize a new pool")
	} else if pool.isRunning {
		return errors.New("pool is already running")
	}
	pool.isRunning = true // protect from dup calls
	pool.init()

	// Initialize workers
	for i := 0; i < pool.numberOfWorkers; i++ {
		w := Worker{}
		pool.workers = append(pool.workers, w)
		pool.wg.Add(1)
		go func() {
			w.Run(pool.ctx, pool.jobs, pool.results)
			pool.wg.Done()
			// At this point jobs channel is closed and empty.
		}()
	}
	pool.wg.Wait()

	return nil
}

// Start starts the worker pool but does not wait for it to complete.
func (pool *Pool) Start() error {
	ch := make(chan error, 1)
	go func() {
		if err := pool.Run(); err != nil {
			ch <- err
		}
		close(ch)
	}()
	select {
	case err := <-ch:
		if err != nil {
			return err
		}
	case <-time.After(1 * time.Second): // enough time for waiting worker pool error
		break
	}
	return nil
}

// Stop stops the worker pool.
func (pool *Pool) Stop() error {
	if pool.isStopping || !pool.isRunning {
		return nil
	}
	pool.isStopping = true
	pool.closeJobsChannel()
	pool.closeResultsChannel()
	pool.isRunning = false
	pool.isStopping = false

	return nil
}

// AddJob adds a new job to the worker pool.
func (pool *Pool) AddJob(job Job) {
	if pool.jobs != nil && !pool.jobsClosed {
		pool.jobs <- job
	}
}

// Results returns the results channel which is populated by job results.
func (pool *Pool) Results() <-chan Result {
	return pool.results
}

// closeJobsChannel closes the jobs channel.
func (pool *Pool) closeJobsChannel() {
	if pool.jobs != nil && !pool.jobsClosed {
		close(pool.jobs)
		pool.jobsClosed = true
	}
}

// closeResultsChannel closes the results channel.
func (pool *Pool) closeResultsChannel() {
	if pool.results != nil && !pool.resultsClosed {
		close(pool.results)
		pool.resultsClosed = true
	}
}
