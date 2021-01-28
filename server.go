//go:generate mockgen -source=$GOFILE -destination=server_mock.go -package=main
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

const (
	topicVarKey = "topic"

	MsgInit    = "INIT"
	MsgAck     = "ACK"
	MsgUnknown = "UNKNOWN"
)

type brokerer interface {
	Publish(topic string, value value) error
	Subscribe(topic string) consumer
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
		log := log.With().Str("request_id", xid.New().String()).Str("handler", "publish").Logger()

		// Read topic
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid topic value"))
		}

		log.Info().
			Str("topic", topic).
			Msg("request to publish to topic")

		// Read body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Err(err).Msg("failed reading in body")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		// Call broker to publish to topic
		if err := broker.Publish(topic, b); err != nil {
			http.Error(w, fmt.Sprintf("failed to publish: %v", err.Error()), http.StatusInternalServerError)
		}

		log.Debug().Str("body", spew.Sprintf(string(b))).Msg("message body")
		log.Info().Str("topic", topic).Msg("successfully published to topic")
	}
}

func subscribe(broker brokerer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log := log.With().
			Str("request_id", xid.New().String()).
			Str("handler", "subscribe").
			Logger()

		// Read topic
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid topic value"))
			return
		}

		log.Info().Str("topic", topic).Msg("request to subscribe to topic")

		consumer := broker.Subscribe(topic)

		msg, err := consumer.Next()
		if err != nil {
			log.Err(err).
				Msg("failed getting next from consumer")

			http.Error(w, fmt.Sprintf("failed to get next value for topic: %v", err), http.StatusInternalServerError)

			return
		}

		// Wrap the writer in a flushWriter in order to immediately flush each write
		// to the client.
		encoder := json.NewEncoder(newFlushWriter(w))
		// Send back the first message on the topic
		encoder.Encode(string(msg))

		log.Info().
			Str("msg", string(msg)).
			Msg("written first message back to client")

		// Listen for an ACK
		decoder := json.NewDecoder(r.Body)
		for {
			var command string
			if err := decoder.Decode(&command); err != nil {
				log.Err(err).
					Msg("failed decoding message")

				return
			}

			switch command {
			case MsgInit:
				log.Info().Msg("initialising consumer")

				continue

			case MsgAck:
				log.Info().Msg("ACKing message")

				if err := consumer.Ack(); err != nil {
					log.Err(err).Msg("failed to ACK")
					return
				}

				msg, err := consumer.Next()
				if errors.Is(err, ErrTopicEmpty) {
					// TODO wait for publish event which affects this topic
					return
				}
				if err != nil {
					log.Err(err).
						Msg("failed getting next from consumer")

					return
				}

				log.Debug().
					Str("msg", string(msg)).
					Msg("sending next message to client")

				if msg != nil {
					encoder.Encode(string(msg))
				}

			default:
				log.Error().Str("msg", string(msg)).Msg("unrecognised message received")
				encoder.Encode(MsgUnknown)
			}
		}
	}
}
