package savepointers

import (
	"encoding/base64"
	"math/rand"
)

import (
	"github.com/dhui/dhrand"
)

// number of bytes used to generate a random savepoint name. 16 bytes is plenty since it's the same size as a uuid
const savepointNumBytes = 16

var _rand *rand.Rand

func init() {
	seed, err := dhrand.GenSeed()
	if err != nil {
		panic(err)
	}
	_rand = rand.New(dhrand.NewLockedSource(seed)) //nolint:gosec
}

// GenSavepointName quickly generates a unique savepoint name
func GenSavepointName() string {
	b := make([]byte, savepointNumBytes)
	// rand.Read() always returns len(p) and a nil err, so it's save to ignore the error.
	// https://golang.org/pkg/math/rand/#Rand.Read
	_rand.Read(b)
	return base64.RawStdEncoding.EncodeToString(b)
}
