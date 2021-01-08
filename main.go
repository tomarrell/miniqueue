package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// If the binary is run with ENV=PRODUCTION, use JSON formatted logging.
	if os.Getenv("ENV") != "PRODUCTION" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	port := flag.String("port", "8080", "port used to run the server")
	flag.Parse()

	// Start the server
	srv := server{}
	p := fmt.Sprintf(":%s", *port)

	log.Info().Str("port", p).Msg("starting miniqueue")

	if err := http.ListenAndServe(p, srv); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("server closed")
	}
}
