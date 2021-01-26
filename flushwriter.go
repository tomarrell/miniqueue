package main

import (
	"io"
	"net/http"
)

type flushWriter struct {
	f http.Flusher
	w io.Writer
}

func newFlushWriter(w io.Writer) flushWriter {
	fw := flushWriter{w: w}
	if f, ok := w.(http.Flusher); ok {
		fw.f = f
	}

	return fw
}

func (fw flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.w.Write(p)
	if fw.f != nil {
		fw.f.Flush()
	}

	return
}
