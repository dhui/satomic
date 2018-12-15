package savepointers_test

import (
	"testing"
)

import (
	"github.com/dhui/satomic/savepointers"
)

func TestGenSavepointName(t *testing.T) {
	expectedLen := 22
	if name := savepointers.GenSavepointName(); len(name) != expectedLen {
		t.Error("Generated savepoint name doesn't have the expected length:", len(name), "!=",
			expectedLen)
	}
}
