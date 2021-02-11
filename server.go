//go:generate mockgen -source=$GOFILE -destination=server_mock.go -package=main
package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const topicVarKey = "topic"

const (
	// CmdInit is the command to be sent with the initial subscribe request to
	// indicate a new consumer should be initialised.
	CmdInit = "INIT"
	// CmdAck notifies the server that the outstanding message was processed
	// successfully and can be removed from the queue.
	CmdAck = "ACK"
)

const (
	errInvalidTopicValue = serverError("invalid topic value")
	errReadBody          = serverError("error reading the request body")
	errPublish           = serverError("error publishing to broker")
	errNextValue         = serverError("error getting next value for consumer")
	errAck               = serverError("error ACKing message")
	errDecodingCmd       = serverError("error decoding command")
	errRequestCancelled  = serverError("request context cancelled")
)

type brokerer interface {
	Publish(topic string, value value) error
	Subscribe(topic string) *consumer
}

type server struct {
	broker brokerer
}

func newServer(broker brokerer) *server {
	return &server{
		broker: broker,
	}
}

func (s server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := mux.NewRouter()

	route.HandleFunc("/publish/{topic}", publish(s.broker)).Methods(http.MethodPost)
	route.HandleFunc("/subscribe/{topic}", subscribe(s.broker)).Methods(http.MethodPost)

	route.ServeHTTP(w, r)
}

func publish(broker brokerer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log := log.With().
			Str("request_id", xid.New().String()).
			Str("handler", "publish").
			Logger()

		// Read topic
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			log.Debug().
				Msg("invalid topic in path")

			w.WriteHeader(http.StatusBadRequest)
			respondError(log, json.NewEncoder(w), errInvalidTopicValue.Error())

			return
		}

		log = log.With().
			Str("topic", topic).
			Logger()

		log.Info().
			Msg("publishing to topic")

		// Read body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Err(err).
				Msg("failed reading request body")

			w.WriteHeader(http.StatusInternalServerError)
			respondError(log, json.NewEncoder(w), errReadBody.Error())

			return
		}
		defer r.Body.Close()

		// Call broker to publish to topic
		if err := broker.Publish(topic, b); err != nil {
			log.Err(err).
				Msg("failed to publish to broker")

			w.WriteHeader(http.StatusInternalServerError)
			respondError(log, json.NewEncoder(w), errPublish.Error())

			return
		}

		w.WriteHeader(http.StatusCreated)

		log.Debug().
			Str("body", string(b)).
			Msg("successfully published to topic")
	}
}

func subscribe(broker brokerer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		log := log.With().
			Str("request_id", xid.New().String()).
			Str("handler", "subscribe").
			Logger()

		// Read topic from URL
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			log.Debug().
				Msg("invalid topic in path")

			w.WriteHeader(http.StatusBadRequest)
			respondError(log, json.NewEncoder(w), errInvalidTopicValue.Error())

			return
		}

		log = log.With().
			Str("topic", topic).
			Logger()

		log.Info().
			Msg("subscribing to topic")

		// Wrap the writer in a flushWriter in order to immediately flush each write
		// to the client.
		cons := broker.Subscribe(topic)
		enc := json.NewEncoder(newFlushWriter(w))
		dec := json.NewDecoder(r.Body)

		for {
			log := log

			var cmd string
			err := dec.Decode(&cmd)
			if err != nil {

				log.Err(err).
					Msg("failed decoding command")

				if err := cons.Nack(); err != nil {
					log.Err(err).
						Msg("failed to nack")
				}

				w.WriteHeader(http.StatusInternalServerError)
				respondError(log, enc, errDecodingCmd.Error())

				return
			}

			log = log.With().
				Str("cmd", cmd).
				Logger()

			switch cmd {
			case CmdInit:
				log.Debug().
					Msg("initialising consumer")

				msg, err := cons.Next(ctx)
				if errors.Is(err, errRequestCancelled) {
					log.Debug().Msg("request context cancelled while waiting for next message")

					return
				} else if err != nil {
					log.Err(err).Msg("failed to get next value for topic")
					respondError(log, enc, errNextValue.Error())

					return
				}

				respondMsg(log, enc, msg)

				log.Debug().
					Str("msg", string(msg)).
					Msg("written message to client")

			case CmdAck:
				log.Debug().
					Msg("ACKing message")

				if err := cons.Ack(); err != nil {
					log.Err(err).Msg("failed to ACK")
					w.WriteHeader(http.StatusInternalServerError)
					respondError(log, enc, errAck.Error())

					return
				}

				msg, err := cons.Next(ctx)
				if errors.Is(err, errRequestCancelled) {
					log.Debug().Msg("request context cancelled while waiting for next message")

					return
				} else if err != nil {
					log.Err(err).Msg("failed to get next value for topic")
					respondError(log, enc, errNextValue.Error())

					return
				}

				respondMsg(log, enc, msg)

				log.Debug().
					Str("msg", string(msg)).
					Msg("written message to client")

			default:
				log.Warn().
					Msg("unrecognised command received")

				respondError(log, enc, "unrecognised command received")
			}
		}
	}
}

type subResponse struct {
	Msg   string `json:"msg,omitempty"`
	Error string `json:"error,omitempty"`
}

func respondMsg(log zerolog.Logger, e *json.Encoder, msg []byte) {
	if err := e.Encode(subResponse{
		Msg: string(msg),
	}); err != nil {
		log.Err(err).
			Msg("failed to write response to client")
	}
}

func respondError(log zerolog.Logger, e *json.Encoder, errMsg string) {
	if err := e.Encode(subResponse{
		Error: errMsg,
	}); err != nil {
		log.Err(err).
			Msg("writing response to client")
	}
}

type serverError string

func (e serverError) Error() string {
	return string(e)
}
