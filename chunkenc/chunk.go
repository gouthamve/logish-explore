package chunkenc

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/dsnet/compress/bzip2"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

// MemChunk implements compressed log chunks.
type MemChunk struct {
	blockSize int
	blocks    []block

	memBlock *block

	app *memAppender

	sync.Mutex // Acquire the lock before modifying blocks.

	encoding Encoding
	cw       func(w io.Writer) CompressionWriter
	cr       func(r io.Reader) (CompressionReader, error)
}

type block struct {
	b          []byte
	entries    []entry
	numEntries int
	size       int // size of uncompressed bytes.

	mint, maxt int64
}

type entry struct {
	t int64
	s string
}

// NewMemChunk returns a new in-mem chunk.
func NewMemChunk(enc Encoding) *MemChunk {
	c := &MemChunk{
		blockSize: 32 * 1024, // The blockSize in bytes.
		blocks:    []block{},

		memBlock: &block{},
	}

	switch enc {
	case EncGZIP:
		c.cw = func(w io.Writer) CompressionWriter { return gzip.NewWriter(w) }
		c.cr = func(r io.Reader) (CompressionReader, error) { return gzip.NewReader(r) }
	case EncLZ4:
		c.cw = func(w io.Writer) CompressionWriter {
			cw := lz4.NewWriter(w)
			cw.Header.BlockMaxSize = 64 * 1024
			cw.Header.HighCompression = true
			cw.Header.BlockDependency = false

			return cw
		}

		c.cr = func(r io.Reader) (CompressionReader, error) { return lz4.NewReader(r), nil }

	case EncZLIB:
		c.cw = func(w io.Writer) CompressionWriter {
			return zlib.NewWriter(w)
		}

		c.cr = func(r io.Reader) (CompressionReader, error) { return zlib.NewReader(r) }

	case EncSnappy:
		c.cw = func(w io.Writer) CompressionWriter { return snappy.NewWriter(w) }
		c.cr = func(r io.Reader) (CompressionReader, error) { return snappy.NewReader(r), nil }

	case EncZSTD:
		c.cw = func(w io.Writer) CompressionWriter { return noopFlushingWriter{zstd.NewWriter(w)} }
		c.cr = func(r io.Reader) (CompressionReader, error) { return zstd.NewReader(r), nil }

	case EncBZIP2:
		c.cw = func(w io.Writer) CompressionWriter { cw, _ := bzip2.NewWriter(w, nil); return noopFlushingWriter{cw} }
		c.cr = func(r io.Reader) (CompressionReader, error) { return bzip2.NewReader(r, nil) }

	default:
		panic("unknown encoding")
	}

	c.app = newMemAppender(c)

	return c
}

// Bytes implements Chunk.
func (c *MemChunk) Bytes() []byte {
	c.Lock()
	defer c.Unlock()

	l := 0
	for _, b := range c.blocks {
		l += len(b.b)
	}

	l += len(c.memBlock.b)

	totBytes := make([]byte, l)
	off := 0
	for _, b := range c.blocks {
		n := copy(totBytes[off:], b.b)
		off += n
	}

	n := copy(totBytes[off:], c.memBlock.b)
	off += n

	return totBytes[:off]
}

// Encoding implements Chunk.
func (c *MemChunk) Encoding() Encoding {
	return c.encoding
}

// Appender implements Chunk.
func (c *MemChunk) Appender() (Appender, error) {
	return c.app, nil
}

// Iterator implements Chunk.
func (c *MemChunk) Iterator() Iterator {
	return newMemIterator(append(c.blocks, *c.memBlock), c.cr)
}

// NumSamples implements Chunk.
func (c *MemChunk) NumSamples() int {
	return 0
}

// Close implements Chunk.
// TODO: Fix this shit to check edge cases.
func (c *MemChunk) Close() error {
	return c.app.Close()
}

type memAppender struct {
	block *block

	chunk *MemChunk

	writer CompressionWriter
	buffer *bytes.Buffer

	encBuf []byte
}

func newMemAppender(chunk *MemChunk) *memAppender {
	buf := bytes.NewBuffer(chunk.memBlock.b)
	return &memAppender{
		block: chunk.memBlock,
		chunk: chunk,

		buffer: buf,
		writer: chunk.cw(buf),

		encBuf: make([]byte, binary.MaxVarintLen64),
	}
}

func (a *memAppender) Append(t int64, s string) {
	n := binary.PutVarint(a.encBuf[:], t)
	_, err := a.writer.Write(a.encBuf[:n])
	if err != nil {
		fmt.Println("WZTF", err)
	}
	a.block.size += n

	n = binary.PutVarint(a.encBuf[:], int64(len(s)))
	_, err = a.writer.Write(a.encBuf[:n])
	if err != nil {
		fmt.Println("WZTF", err)
	}
	a.block.size += n

	_, err = a.writer.Write([]byte(s))
	if err != nil {
		fmt.Println("WZTF", err)
	}
	a.block.size += len(s)

	a.block.entries = append(a.block.entries, entry{t, s})

	if a.block.mint > t {
		a.block.mint = t
	}
	a.block.maxt = t

	a.block.numEntries++

	a.writer.Flush()

	if a.buffer.Len() > a.chunk.blockSize {
		a.cut()
	}
}

// cut a new block.
func (a *memAppender) cut() {
	a.chunk.Lock()
	defer a.chunk.Unlock()

	err := a.writer.Close()
	if err != nil {
		fmt.Println("WZTF", err)
	}
	a.block.b = a.buffer.Bytes()

	b := make([]byte, len(a.block.b))
	copy(b, a.block.b)
	a.chunk.blocks = append(a.chunk.blocks, block{
		b:          b,
		numEntries: a.block.numEntries,
		mint:       a.block.mint,
		maxt:       a.block.maxt,
		size:       a.block.size,
	})

	// Reset the block.
	a.block.entries = a.block.entries[:0]
	a.block.b = a.block.b[:0]
	a.block.mint = math.MaxInt64
	a.block.maxt = 0
	a.block.numEntries = 0
	a.block.size = 0

	a.buffer = bytes.NewBuffer(a.block.b)
	a.writer = a.chunk.cw(a.buffer)
}

func (a *memAppender) Close() error {
	a.writer.Close()

	a.block.entries = nil
	a.block.b = a.buffer.Bytes()

	b := make([]byte, len(a.block.b))
	copy(b, a.block.b)
	a.chunk.blocks = append(a.chunk.blocks, block{
		b:          b,
		numEntries: a.block.numEntries,
		mint:       a.block.mint,
		maxt:       a.block.maxt,
		size:       a.block.size,
	})

	return nil
}

type memIterator struct {
	blocks []block

	it  *listIterator
	cur entry

	cr func(io.Reader) (CompressionReader, error)
}

func newMemIterator(blocks []block, cr func(io.Reader) (CompressionReader, error)) *memIterator {
	// TODO: Handle nil blocks.
	it := newListIterator(blocks[0], cr)
	return &memIterator{
		blocks: blocks[1:],
		it:     it,

		cr: cr,
	}
}

func (gi *memIterator) Seek(int64) bool {
	return false
}

func (gi *memIterator) Next() bool {
	if gi.it.Next() {
		gi.cur.t, gi.cur.s = gi.it.At()
		return true
	}

	if len(gi.blocks) > 0 {
		gi.it = newListIterator(gi.blocks[0], gi.cr)
		gi.blocks = gi.blocks[1:]

		return gi.Next()
	}

	return false
}

func (gi *memIterator) At() (int64, string) {
	return gi.cur.t, gi.cur.s
}

func (gi *memIterator) Err() error {
	return nil
}

type listIterator struct {
	entries []entry

	cur entry
}

func newListIterator(b block, cr func(io.Reader) (CompressionReader, error)) *listIterator {
	if len(b.entries) > 0 {
		// TODO: Don't make it copy this. Do something about it!
		// Also, race!
		return &listIterator{
			entries: b.entries[:],
		}
	}

	// big alloc!
	buf := make([]byte, b.size+10)
	entries := make([]entry, 0, b.numEntries)

	r, _ := cr(bytes.NewBuffer(b.b))

	toRead := b.size
	buf2 := buf[:]
	for toRead > 0 {
		n, err := r.Read(buf2)
		if err != nil && err != io.EOF {
			fmt.Println("WTF", err)
		}
		toRead -= n
		buf2 = buf2[n:]
	}

	for i := 0; i < b.numEntries; i++ {
		t, n := binary.Varint(buf)
		buf = buf[n:]

		len, n := binary.Varint(buf)
		buf = buf[n:]

		str := string(buf[:len])
		buf = buf[len:]

		entries = append(entries, entry{t, str})
	}

	return &listIterator{
		entries: entries,
	}
}

func (li *listIterator) Seek(int64) bool {
	return false
}

func (li *listIterator) Next() bool {
	if len(li.entries) > 0 {
		li.cur = li.entries[0]
		li.entries = li.entries[1:]

		return true
	}

	return false
}

func (li *listIterator) At() (int64, string) {
	return li.cur.t, li.cur.s
}

func (li *listIterator) Err() error {
	return nil
}

type noopFlushingWriter struct {
	io.WriteCloser
}

func (noopFlushingWriter) Flush() error {
	return nil
}
