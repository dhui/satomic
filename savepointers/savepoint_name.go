package savepointers

import (
	"encoding/base64"
	"encoding/binary"
	"math/rand/v2"
)

// number of bytes used to generate a random savepoint name. 16 bytes is plenty since it's the same size as a uuid
const savepointNumBytes = 16

// GenSavepointName quickly generates a unique savepoint name
func GenSavepointName() string {
	b := make([]byte, savepointNumBytes)

	// 2 64-bit ints is 16 bytes
	binary.NativeEndian.PutUint64(b, rand.Uint64())
	binary.NativeEndian.PutUint64(b[8:], rand.Uint64())

	return base64.RawStdEncoding.EncodeToString(b)
}
