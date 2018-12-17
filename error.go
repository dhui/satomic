package satomic

import (
	"fmt"
)

// Error implements the error interface and is used to differentiate between Querier.Atomic() errors
// and Querier.Atomic() callback function errors
type Error struct {
	// Err is an error returned from Querier.Atomic()'s callback function
	Err error
	// Atomic is an error from within Querier.Atomic()'s implementation.
	// Usually such an error is the result of an improperly configured/created Querier or a DB error.
	Atomic error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		if e.Atomic == nil {
			return "Err: <nil> Atomic: <nil>"
		}
		return fmt.Sprintf("Err: <nil> Atomic: %q", e.Atomic.Error())
	} else if e.Atomic == nil {
		return fmt.Sprintf("Err: %q Atomic: <nil>", e.Err.Error())
	}
	return fmt.Sprintf("Err: %q Atomic: %q", e.Err.Error(), e.Atomic.Error())
}

func newError(err, dbErr error) *Error {
	return &Error{Err: err, Atomic: dbErr}
}
