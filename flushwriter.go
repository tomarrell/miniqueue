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

func (f flushWriter) Write(p []byte) (n int, err error) {
	n, err = f.w.Write(p)
	if f.f != nil {
		f.f.Flush()
	}

	return
}
