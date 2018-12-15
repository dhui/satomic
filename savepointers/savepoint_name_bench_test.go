package savepointers_test

import (
	"testing"
)

import (
	"github.com/dhui/satomic/savepointers"
)

func BenchmarkGenSavepointName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		savepointers.GenSavepointName()
	}
}
