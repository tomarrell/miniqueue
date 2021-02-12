package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	url   = "https://localhost:8080"
	topic = "test_topic"

	consumers    = flag.Int("consumers", 2, "number of consumers (minimum 1)")
	pubRate      = flag.Duration("rate", time.Second, "the default rate at which to publish")
	maxSleepTime = flag.Int("sleep", 5, "upper bound for consumer random sleep seconds")
	validate     = flag.Bool("validate", false, "run in validation mode, check for pub/sub consistency, must be run with only 1 consumer")
	nackChance   = flag.Int("chance", 10, "1/n change to randomly send back a nack")
)

func main() {
	flag.Parse()

	if *validate && *consumers > 1 {
		log.Fatal("validate can currently only be run with a single consumer")
	}

	log.SetFlags(0)

	go producer()

	// Give the producer time to create the topic
	time.Sleep(time.Second)

	for i := 0; i < *consumers-1; i++ {
		go consumer(i)
	}
	consumer(*consumers - 1)
}

func producer() {
	log := log.New(os.Stdout, "producer: ", 0)

	for n := 0; ; n++ {
		msg := fmt.Sprintf("%d", n)

		res, err := http.Post(
			fmt.Sprintf("%s/publish/%s", url, topic),
			"application/json",
			strings.NewReader(msg),
		)
		if err != nil {
			log.Printf("failed to publish: %v\n", err)
			continue
		}
		if res.StatusCode != http.StatusCreated {
			log.Printf("failed to publish, received status code: %d\n", res.StatusCode)
		}

		log.Printf("published message %s\n", msg)

		time.Sleep(*pubRate)
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

		n := 0
		for {
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

			log.Printf("consumed message: %s\n", out.Msg)

			if !*validate {
				t := time.Duration(rand.Intn(5)) * time.Second
				log.Println("doing some work for", t)
				time.Sleep(t)
			}

			if *validate {
				c, _ := strconv.Atoi(out.Msg)
				if c != n {
					panic("uh oh")
				}

				// Randomly choose to ack or nack
				if rand.Intn(*nackChance) == 0 {
					_ = enc.Encode("NACK")
					continue
				}

				n++
			}

			_ = enc.Encode("ACK")
		}
	}
}
