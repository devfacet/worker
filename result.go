// Worker
// For the full copyright and license information, please view the LICENSE.txt file.

package worker

// Result represents a result.
type Result struct {
	Descriptor Descriptor
	Value      interface{}
	Error      error
}
