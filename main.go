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

const (
	defaultHumanReadable = false
	defaultPort          = 8080
	defaultCertPath      = "./testdata/localhost.pem"
	defaultKeyPath       = "./testdata/localhost-key.pem"
	defaultDBPath        = "/tmp/miniqueue"
)

func main() {
	var (
		humanReadable = flag.Bool("human", defaultHumanReadable, "human readable logging output")
		port          = flag.Int("port", defaultPort, "port used to run the server")
		tlsCertPath   = flag.String("cert", defaultCertPath, "path to TLS certificate")
		tlsKeyPath    = flag.String("key", defaultKeyPath, "path to TLS key")
		dbPath        = flag.String("db", defaultDBPath, "path to the db file")
	)

	flag.Parse()

	if *humanReadable {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	srv := newServer(newBroker(newStore(*dbPath)))

	// Start the server
	p := fmt.Sprintf(":%d", *port)

	log.Info().
		Str("port", p).
		Msg("starting miniqueue")

	if err := http.ListenAndServeTLS(p, *tlsCertPath, *tlsKeyPath, srv); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("server closed")
	}
}
