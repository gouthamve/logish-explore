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
		t.Run(enc.String(), func(t *testing.T) {
			chk := NewMemChunk(enc)
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
