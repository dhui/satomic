package atomic_test

import (
	"errors"
	"testing"
)

import (
	atomic "github.com/dhui/satomic"
	"github.com/dhui/satomic/atomictest"
)

func TestError(t *testing.T) {
	testCases := []struct {
		name        string
		err         *atomic.Error
		expectedStr string
	}{
		{name: "nil Error", err: nil, expectedStr: ""},
		{name: "nil Err and Atomic", err: atomictest.NewError(nil, nil),
			expectedStr: `Err: <nil> Atomic: <nil>`},
		{name: "nil Err, non-nil Atomic", err: atomictest.NewError(nil, errors.New("atomic")),
			expectedStr: `Err: <nil> Atomic: "atomic"`},
		{name: "non-nil Err, nil Atomic", err: atomictest.NewError(errors.New("err"), nil),
			expectedStr: `Err: "err" Atomic: <nil>`},
		{name: "non-nil Err and Atomic", err: atomictest.NewError(errors.New("err"), errors.New("atomic")),
			expectedStr: `Err: "err" Atomic: "atomic"`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if str := tc.err.Error(); str != tc.expectedStr {
				t.Error("Didn't get expected Error string:", str, "!=", tc.expectedStr)
			}
		})
	}
}
