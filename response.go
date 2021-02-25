package main

import (
	"encoding/json"

	"github.com/rs/zerolog"
)

type subResponse struct {
	Msg       []byte `json:"msg,omitempty"`
	DackCount int    `json:"dackCount,omitempty"`
	Error     string `json:"error,omitempty"`
}

func respondMsg(log zerolog.Logger, e *json.Encoder, val *value) {
	res := subResponse{
		Msg:       val.Raw,
		DackCount: val.DackCount,
	}

	if err := e.Encode(res); err != nil {
		log.Err(err).Msg("failed to write response to client")
	}
}

func respondError(log zerolog.Logger, e *json.Encoder, errMsg string) {
	res := subResponse{
		Error: errMsg,
	}

	if err := e.Encode(res); err != nil {
		log.Err(err).Msg("writing response to client")
	}
}
