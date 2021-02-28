package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
)

var (
	url   = "https://localhost:8080"
	topic = "test_topic"
)

func main() {
	flag.Parse()

	publish()
	consume(0)
}

func publish() {
	log := log.New(os.Stdout, "producer: ", 0)

	msg := "hello_world"
	res, err := http.Post(fmt.Sprintf("%s/publish/%s", url, topic), "application/json", strings.NewReader(msg))
	if err != nil {
		log.Fatalf("failed to publish: %v\n", err)
	}
	if res.StatusCode != http.StatusCreated {
		log.Fatalf("failed to publish, received status code: %d\n", res.StatusCode)
	}

	log.Printf("published message %s\n", msg)
}

type subRes struct {
	Msg       string `json:"msg"`
	Error     string `json:"error"`
	DackCount int    `json:"dackCount"`
}

func consume(id int) {
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
			var out subRes
			if _ = dec.Decode(&out); err != nil {
				log.Fatalf("failed decode response body: %v\n", err)
			}

			if out.Error != "" {
				log.Fatalf("received error: %s\n", out.Error)
			}

			delay := int(math.Pow(2, float64(out.DackCount)))

			strMsg := mustBase64Decode(out.Msg)
			log.Printf("consumed message: %s\n", strMsg)
			log.Printf("delaying message: %s by %ds\n", strMsg, delay)

			_ = enc.Encode(fmt.Sprintf("DACK %d", delay))
		}
	}
}

func mustBase64Decode(b string) string {
	s, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		log.Fatal(err)
	}

	return string(s)
}
