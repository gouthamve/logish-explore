package chunkenc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemChunk(t *testing.T) {
	b, err := ioutil.ReadFile("NASA_access_log_Aug95")
	require.NoError(t, err)

	lines := bytes.Split(b, []byte("\n"))

	for _, enc := range []Encoding{EncGZIP, EncZLIB, EncSnappy, EncZSTD, EncBZIP2} {
		for _, blockSize := range []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024} {
			testName := fmt.Sprintf("%s-%d", enc.String(), blockSize/1024)
			t.Run(testName, func(t *testing.T) {
				chk := NewMemChunk(enc)
				chk.blockSize = blockSize

				app, err := chk.Appender()
				require.NoError(t, err)

				for i, l := range lines {
					app.Append(int64(i), string(l))
				}

				fmt.Println(float64(len(b))/(1024*1024), float64(len(chk.Bytes()))/(1024*1024))

				it := chk.Iterator()
				require.NoError(t, err)

				for i, l := range lines {
					require.True(t, it.Next())

					ts, str := it.At()
					require.Equal(t, int64(i), ts)
					require.Equal(t, string(l), str)
				}
			})
		}
	}
}
