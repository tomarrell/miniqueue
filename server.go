//go:generate mockgen -source=$GOFILE -destination=server_mock.go -package=main
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

const topicVarKey = "topic"

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
	route.HandleFunc("/subscribe/{topic}", subscribe(s.broker)).Methods(http.MethodGet)

	route.ServeHTTP(w, r)
}

func publish(broker brokerer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log := log.With().Str("request_id", xid.New().String()).Logger()

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

		val, err := c.Next()
		if err != nil {
			log.Err(err).Msg("getting next from consumer")
			http.Error(w, fmt.Sprintf("failed to get next value for topic: %v", err), http.StatusInternalServerError)
			return
		}

		w.Write(val)
	}
}
