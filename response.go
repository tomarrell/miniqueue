package main

import (
	"encoding/json"

	"github.com/rs/zerolog"
)

type subResponse struct {
	Msg   string `json:"msg,omitempty"`
	Error string `json:"error,omitempty"`
}

func respondMsg(log zerolog.Logger, e *json.Encoder, msg []byte) {
	res := subResponse{
		Msg: string(msg),
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
