package chunkenc

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

// The different available encodings.
const (
	EncGZIP Encoding = iota
)

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
