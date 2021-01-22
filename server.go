//go:generate mockgen -source=$GOFILE -destination=server_mock.go -package=main
package main

import (
	"encoding/json"
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

		log.Info().Str("topic", topic).Msg("request to publish to topic")

		// Read body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic("failed read")
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
		log := log.With().Str("request_id", xid.New().String()).Str("handler", "subscribe").Logger()

		// Read topic
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid topic value"))
			return
		}

		log.Info().Str("topic", topic).Msg("request to subscribe to topic")

		c := broker.Subscribe(topic)

		msg, err := c.Next()
		if err != nil {
			log.Err(err).Msg("getting next from consumer")
			http.Error(w, fmt.Sprintf("failed to get next value for topic: %v", err), http.StatusInternalServerError)
			return
		}

		// Send back the first message on the queue
		encoder := json.NewEncoder(w)
		encoder.Encode(string(msg))

		log.Info().Msg("written first message back to client")

		// Listen for an ACK
		decoder := json.NewDecoder(r.Body)
		for {
			var command string
			if err := decoder.Decode(&command); err != nil {
				log.Err(err).Msg("decoding message")
				return
			}

			log.Debug().Str("command", command).Msg("received command from client")

			switch command {
			case MsgInit:
				continue
			case MsgAck:
				log.Info().Msg("received ACK")
				err := c.Ack()

				msg, err := c.Next()
				if err != nil {
					log.Err(err).Msg("getting next from consumer")
					http.Error(w, fmt.Sprintf("failed to get next value for topic: %v", err), http.StatusInternalServerError)
					return
				}
				encoder.Encode(string(msg))
			default:
				log.Error().Str("msg", string(msg)).Msg("unrecognised message received")
				encoder.Encode(MsgUnknown)
			}
		}
	}
}
