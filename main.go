package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

var (
	//go:embed VERSION
	version string
)

const (
	defaultHumanReadable = false
	defaultPort          = 8080
	defaultCertPath      = "./testdata/localhost.pem"
	defaultKeyPath       = "./testdata/localhost-key.pem"
	defaultDBPath        = "./data"
	defaultLogLevel      = "debug"
)

func main() {
	var (
		humanReadable = flag.Bool("human", defaultHumanReadable, "human readable logging output")
		port          = flag.Int("port", defaultPort, "port used to run the server")
		tlsCertPath   = flag.String("cert", defaultCertPath, "path to TLS certificate")
		tlsKeyPath    = flag.String("key", defaultKeyPath, "path to TLS key")
		dbPath        = flag.String("db", defaultDBPath, "path to the db file")
		logLevel      = flag.String("level", defaultLogLevel, "(disabled|debug|info)")
		delayPeriod   = flag.Duration("period", time.Second, "period between runs to check and restore delayed messages")
	)

	flag.Parse()

	if *humanReadable {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	switch *logLevel {
	case "debug":
		log.Logger = log.Level(zerolog.DebugLevel)
	case "info":
		log.Logger = log.Level(zerolog.InfoLevel)
	case "disabled":
		log.Logger = log.Level(zerolog.Disabled)
	default:
		log.Fatal().Msg("invalid log level, see -h")
	}

	if *dbPath == defaultDBPath {
		log.Warn().
			Msgf("no DB path specified, using default %s", defaultDBPath)
	}

	if *tlsCertPath == defaultCertPath {
		log.Warn().
			Msgf("no TLS certificate path specified, using default %s", defaultCertPath)
	}

	if *tlsKeyPath == defaultKeyPath {
		log.Warn().
			Msgf("no TLS key path specified, using default %s", defaultKeyPath)
	}

	ctx := context.Background()

	b := newBroker(newStore(*dbPath))
	go b.ProcessDelays(ctx, *delayPeriod)

	switch {
	case false:
		runHTTP(b, port, tlsCertPath, tlsKeyPath)

	case true:
		runRedis(b, tlsCertPath, tlsKeyPath)
	}
}

func runRedis(b brokerer, tlsCertPath, tlsKeyPath *string) {
	log.Info().
		Msg("starting miniqueue over redis")

	r := newRedis(b)

	err := redcon.ListenAndServe(":6379", r.handleCmd, nil, nil)
	if err != nil {
		log.Err(err).Msg("closing server")
	}
}

func runHTTP(b brokerer, port *int, tlsCertPath, tlsKeyPath *string) {
	// Start the server
	p := fmt.Sprintf(":%d", *port)

	log.Info().
		Str("port", p).
		Msg("starting miniqueue over HTTP")

	srv := newHTTPServer(b)

	if err := http.ListenAndServeTLS(p, *tlsCertPath, *tlsKeyPath, srv); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().
			Err(err).
			Msg("server closed")
	}
}
