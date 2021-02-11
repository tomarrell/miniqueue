package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/xid"
)

var (
	url           = "https://localhost:8080"
	topic         = "test_topic"
	publishPeriod = time.Second
)

func main() {
	log.SetFlags(0)

	go producer()

	// Give the producer time to create the topic
	time.Sleep(time.Second)

	go consumer(1)
	consumer(2)
}

func producer() {
	log := log.New(os.Stdout, "producer: ", 0)

	for {
		msg := xid.New().String()

		res, err := http.Post(
			fmt.Sprintf("%s/publish/%s", url, topic),
			"application/json",
			strings.NewReader(msg),
		)
		if err != nil {
			log.Printf("failed to publish: %v", err)
			continue
		}
		if res.StatusCode != http.StatusCreated {
			log.Printf("failed to publish, received status code: %d", res.StatusCode)
		}

		log.Printf("published message %s to topic %s\n", msg, topic)

		time.Sleep(publishPeriod)
	}
}

type subRes struct {
	Msg   string `json:"msg"`
	Error string `json:"error"`
}

func consumer(id int) {
restart:
	for {
		log := log.New(os.Stdout, fmt.Sprintf("consumer-%d: ", id), 0)

		reader, writer := io.Pipe()
		enc := json.NewEncoder(writer)
		go func() {
			_ = enc.Encode("INIT")
		}()

		res, err := http.Post(fmt.Sprintf("%s/subscribe/%s", url, topic), "application/json", reader)
		if err != nil {
			log.Fatalf("failed to consume: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			log.Fatalf("failed to consume, received status code: %d", res.StatusCode)
		}

		dec := json.NewDecoder(res.Body)

		for {
			time.Sleep(publishPeriod)

			var out subRes
			if _ = dec.Decode(&out); err != nil {
				log.Printf("failed decode response body: %v\n", err)
				log.Printf("restarting consumer %d\n", id)
				continue restart
			}

			if out.Error != "" {
				res.Body.Close()
				log.Printf("received error: %s\n", out.Error)
				log.Printf("restarting consumer %d\n", id)
				continue restart
			}

			time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

			_ = enc.Encode("ACK")

			log.Printf("consumed message: %s\n", out.Msg)
		}
	}
}
