package main

import (
	"bytes"
	"sync"
)

// safeBuffer is a goroutine safe bytes.safeBuffer
type safeBuffer struct {
	buffer bytes.Buffer
	sync.Mutex
}

func (s *safeBuffer) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	return s.buffer.Read(p)
}

// Write appends the contents of p to the buffer, growing the buffer as needed. It returns
// the number of bytes written.
func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	return s.buffer.Write(p)
}

// String returns the contents of the unread portion of the buffer
// as a string.  If the Buffer is a nil pointer, it returns "<nil>".
func (s *safeBuffer) String() string {
	s.Lock()
	defer s.Unlock()

	return s.buffer.String()
}
