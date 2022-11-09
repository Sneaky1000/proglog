package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// Width refers to the number of bytes that make up each entry.
var (
	// Offset width
	offWidth uint64 = 4
	// Position width
	posWidth uint64 = 8
	// Entry width
	entWidth = offWidth + posWidth
)

// The index struct comprises of a persisted file and a memory-mapped file.
type index struct {
	file *os.File
	mmap gommap.MMap
	// This gives the size of the index and where to write the next
	// entry appended to the index.
	size uint64
}

// Creates a new index for the given file. The function creates the index
// saves the current size to track the amount of data in the index file
// as index entries are added. Grow the file to the max index size before
// memory-mapping the file and then return the index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Takes in an offset and returns its associated record's position in the store.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Appends the given offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	// Validate there is space to write the entry.
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Encode the offset and position and write them to the memory-mapped file.
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Increment the position where the next write will go.
	i.size += uint64(entWidth)

	return nil
}

// Returns the index's file path.
func (i *index) Name() string {
	return i.file.Name()
}

// This ensures the memory-mapped file has synced its data to the
// persisted file and that the persisted file has flushed its contents
// to stable storage. Then, it truncates the persisted file to the
// ammount of data actaully needed before finally closing the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
