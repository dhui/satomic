package satomictest

import (
	"github.com/dhui/satomic"
)

// ErrsEq determines if the two *satomic.Errors are equal
func ErrsEq(a, b *satomic.Error) bool {
	if a == b {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}

// NewError creates a new *satomic.Error
func NewError(err, atomicErr error) *satomic.Error {
	return &satomic.Error{Err: err, Atomic: atomicErr}
}
