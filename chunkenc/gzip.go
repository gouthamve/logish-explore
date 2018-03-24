package chunkenc

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
)

// GZIPChunk implements GZIP compressed log chunks.
type GZIPChunk struct {
	blockSize int
	blocks    []block

	memBlock *block

	app *gzipAppender

	sync.Mutex // Acquire the lock before modifying blocks.
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

// NewGZIPChunk returns a new GZIP chunk.
func NewGZIPChunk() *GZIPChunk {
	c := &GZIPChunk{
		blockSize: 32 * 1024, // The blockSize in bytes.
		blocks:    []block{},

		memBlock: &block{},
	}

	c.app = newGzipAppender(c)

	return c
}

// Bytes implements Chunk.
func (c *GZIPChunk) Bytes() []byte {
	c.Lock()
	defer c.Unlock()

	totBytes := make([]byte, (len(c.blocks)+1)*c.blockSize)
	off := 0
	for _, b := range c.blocks {
		n := copy(totBytes[off:], b.b)
		off += n
	}

	return totBytes[:off]
}

// Encoding implements Chunk.
func (c *GZIPChunk) Encoding() Encoding {
	return EncGZIP
}

// Appender implements Chunk.
func (c *GZIPChunk) Appender() (Appender, error) {
	return c.app, nil
}

// Iterator implements Chunk.
func (c *GZIPChunk) Iterator() Iterator {
	return newGzipIterator(append(c.blocks, *c.memBlock))
}

// NumSamples implements Chunk.
func (c *GZIPChunk) NumSamples() int {
	return 0
}

// Close implements Chunk.
// TODO: Fix this shit to check edge cases.
func (c *GZIPChunk) Close() error {
	return c.app.Close()
}

type gzipAppender struct {
	block *block

	chunk *GZIPChunk

	writer *gzip.Writer
	buffer *bytes.Buffer

	encBuf []byte
}

func newGzipAppender(chunk *GZIPChunk) *gzipAppender {
	buf := bytes.NewBuffer(chunk.memBlock.b)
	return &gzipAppender{
		block: chunk.memBlock,
		chunk: chunk,

		buffer: buf,
		writer: gzip.NewWriter(buf),

		encBuf: make([]byte, binary.MaxVarintLen64),
	}
}

func (a *gzipAppender) Append(t int64, s string) {
	n := binary.PutVarint(a.encBuf[:], t)
	a.writer.Write(a.encBuf[:n])
	a.block.size += n

	n = binary.PutVarint(a.encBuf[:], int64(len(s)))
	a.writer.Write(a.encBuf[:n])
	a.block.size += n

	a.writer.Write([]byte(s))
	a.block.size += len(s)

	a.block.entries = append(a.block.entries, entry{t, s})

	if a.block.mint > t {
		a.block.mint = t
	}
	a.block.maxt = t

	a.block.numEntries++

	if a.buffer.Len() > a.chunk.blockSize {
		a.cut()
	}
}

// cut a new block.
func (a *gzipAppender) cut() {
	a.chunk.Lock()
	defer a.chunk.Unlock()

	a.writer.Close()
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
	a.writer = gzip.NewWriter(a.buffer)
}

func (a *gzipAppender) Close() error {
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

type gzipIterator struct {
	blocks []block

	it  *listIterator
	cur entry
}

func newGzipIterator(blocks []block) *gzipIterator {
	// TODO: Handle nil blocks.
	it := newListIterator(blocks[0])
	return &gzipIterator{
		blocks: blocks[1:],
		it:     it,
	}
}

func (gi *gzipIterator) Seek(int64) bool {
	return false
}

func (gi *gzipIterator) Next() bool {
	if gi.it.Next() {
		gi.cur.t, gi.cur.s = gi.it.At()
		return true
	}

	if len(gi.blocks) > 0 {
		gi.it = newListIterator(gi.blocks[0])
		gi.blocks = gi.blocks[1:]

		return gi.Next()
	}

	return false
}

func (gi *gzipIterator) At() (int64, string) {
	return gi.cur.t, gi.cur.s
}

func (gi *gzipIterator) Err() error {
	return nil
}

type listIterator struct {
	entries []entry

	cur entry
}

func newListIterator(b block) *listIterator {
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

	r, _ := gzip.NewReader(bytes.NewBuffer(b.b))

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
