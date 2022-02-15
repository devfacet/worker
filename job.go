// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker

import (
	"context"
)

// Job represents a worker job which executes the given function.
type Job struct {
	Descriptor Descriptor
	Fn         func(ctx context.Context, args ...interface{}) (interface{}, error)
	Args       []interface{}
}

// Exec executes the job function and returns it's result.
func (j Job) Exec(ctx context.Context) Result {
	result := Result{
		Descriptor: j.Descriptor,
	}
	if j.Fn != nil {
		result.Value, result.Error = j.Fn(ctx, j.Args...)
	}
	return result
}
