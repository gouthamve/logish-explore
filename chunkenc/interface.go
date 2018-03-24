package chunkenc

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

// The different available encodings.
const (
	EncGZIP Encoding = iota
	EncLZ4
	EncZLIB
	EncSnappy
	EncZSTD
	EncBZIP2
)

func (e Encoding) String() string {
	switch e {
	case EncGZIP:
		return "gzip"
	case EncLZ4:
		return "lz4"
	case EncZLIB:
		return "zlib"
	case EncSnappy:
		return "snappy"
	case EncZSTD:
		return "zstd"
	case EncBZIP2:
		return "bzip2"
	default:
		return "unknown"
	}
}

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte
	Encoding() Encoding
	Appender() (Appender, error)
	Iterator() Iterator
	NumSamples() int

	Close() error
}

// Appender is used to add samples to the chunk.
type Appender interface {
	Append(int64, string)
	Close() error
}

// Iterator is the sample iterator that can only stream forward.
type Iterator interface {
	Seek(int64) bool
	At() (int64, string)
	Next() bool

	Err() error
}

// CompressionWriter is the writer that compresses the data passed to it.
type CompressionWriter interface {
	Write(p []byte) (int, error)
	Close() error
}

// CompressionReader reads the compressed data.
type CompressionReader interface {
	Read(p []byte) (int, error)
}
