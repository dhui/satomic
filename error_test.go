package satomic_test

import (
	"errors"
	"testing"
)

import (
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/satomictest"
)

func TestError(t *testing.T) {
	testCases := []struct {
		name        string
		err         *satomic.Error
		expectedStr string
	}{
		{name: "nil Error", err: nil, expectedStr: ""},
		{name: "nil Err and Atomic", err: satomictest.NewError(nil, nil),
			expectedStr: `Err: <nil> Atomic: <nil>`},
		{name: "nil Err, non-nil Atomic", err: satomictest.NewError(nil, errors.New("atomic")),
			expectedStr: `Err: <nil> Atomic: "atomic"`},
		{name: "non-nil Err, nil Atomic", err: satomictest.NewError(errors.New("err"), nil),
			expectedStr: `Err: "err" Atomic: <nil>`},
		{name: "non-nil Err and Atomic", err: satomictest.NewError(errors.New("err"), errors.New("atomic")),
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
