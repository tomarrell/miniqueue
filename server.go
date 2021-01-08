//go:generate mockgen -source=$GOFILE -destination=server_mock.go -package=main
package main

import (
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

const topicVarKey = "topic"

type brokerer interface {
	Publish(topic string, value value) error
	Subscribe(topic string) (<-chan value, error)
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

	route.Handle("/publish/{topic}", publish(s.broker)).Methods(http.MethodPost)
	route.Handle("/subscribe/{topic}", subscribe(s.broker)).Methods(http.MethodGet)

	route.ServeHTTP(w, r)
}

func publish(broker brokerer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read topic
		vars := mux.Vars(r)
		topic, ok := vars[topicVarKey]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid topic value"))
		}

		// Read body
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic("failed read")
		}
		defer r.Body.Close()

		// Call broker to publish to topic
		// TODO handle the error
		_ = broker.Publish(topic, b)

		log.Info().Str("topic", topic).Msg("successfully published to topic")
	})
}

func subscribe(broker brokerer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info().Msg("subscribe")
	})
}
