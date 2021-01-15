package main

import (
	"encoding/json"
	"errors"
	"io"
	"time"
)

const timeoutSeconds = time.Second

// DecodeWaiter adds a method to wait a specific amount of time trying to decode
// before returning a timeout.
type DecodeWaiter struct {
	*json.Decoder
}

func NewDecodeWaiter(r io.Reader) *DecodeWaiter {
	return &DecodeWaiter{Decoder: json.NewDecoder(r)}
}

func (d *DecodeWaiter) WaitAndDecode(v interface{}) error {
	timer := time.After(timeoutSeconds)

	for {
		select {
		case <-timer:
			return errors.New("timed out waiting for decode")
		default:
			if !d.More() {
				time.Sleep(time.Millisecond)
			} else {
				return d.Decode(v)
			}
		}
	}

	return nil
}
