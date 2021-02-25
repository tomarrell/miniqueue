package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type value struct {
	DackCount int
	Raw       []byte
}

func newValue(b []byte) *value {
	return &value{
		DackCount: 0,
		Raw:       b,
	}
}

func (v *value) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("gob encoding value: %v", err)
	}

	return buf.Bytes(), nil
}

func decodeValue(b []byte) (*value, error) {
	var v value
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err != nil {
		return nil, fmt.Errorf("gob decoding value: %v", err)
	}

	return &v, nil
}
