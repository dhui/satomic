package atomictest

import (
	atomic "github.com/dhui/satomic"
)

// ErrsEq determines if the two *atomic.Errors are equal
func ErrsEq(a, b *atomic.Error) bool {
	if a == b {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}

// NewError creates a new *atomic.Error
func NewError(err, atomicErr error) *atomic.Error {
	return &atomic.Error{Err: err, Atomic: atomicErr}
}
