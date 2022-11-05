package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// Stores most significant byte (the "big end") of the data
	// at the byte with the lowest address. The rest of the data
	// is placed in order in the next three bytes in memory.
	// Defnition: https://bit.ly/3Wzrnm8

	// This defines the encoding that persists record sizes and index entries in.
	enc = binary.BigEndian
)

const (
	// This defines the number of bytes used to store the
	// record's (the data stored in the log) length.
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// This function creates a store (the file records are kept in) for a given file.
func newStore(f *os.File) (*store, error) {
	// Get the file's current size.
	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// This method persists the given bytes to the store. Write the length
// of the record so that, when reading it, the number of bytes needed to
// be read is known. It writes to the buffered wrtiter instead of directly
// to the file to reduce the number of system calls and improve performance.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)

	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	// Return the number of bytes written.
	return uint64(w), pos, nil
}

// This method returns the record at the given position. It finds how many bytes
// it needs to read to get the whole record, then fetches and returns said record
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the writer buffer in case it tries to read a record that
	// hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))

	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// This method reads len(p) bytes into p beginning at the offset in the
// store's file. It implements io.ReaderAt on the store type.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// This method persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()

	if err != nil {
		return err
	}

	return s.File.Close()
}
