package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	topic = "test"
	url   = "https://localhost:8080"

	duration = flag.Duration("duration", 10*time.Second, "duration of the benchmark")
)

func main() {
	flag.Parse()

	// Bail out if necessary
	go func() {
		<-time.After(*duration + time.Second)
		log.Fatal("blocked reading, maybe ran out of things to consume?")
	}()

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

	timer := time.After(*duration)
	count := 0
	for {
		select {
		case <-timer:
			fmt.Printf("consumed %d times in %s\n", count, *duration)
			fmt.Printf("%d (consume+ack)/second\n", count/int(*duration/time.Second))
			_ = writer.Close()
			os.Exit(0)
		default:
		}

		var out struct {
			Msg   string `json:"msg"`
			Error string `json:"error"`
		}
		if _ = dec.Decode(&out); err != nil {
			log.Fatalf("failed decode response body: %v\n", err)
		}

		if out.Error != "" {
			log.Fatalf("received error: %s\n", out.Error)
		}

		if err := enc.Encode("ACK"); err != nil {
			log.Fatalf("ACK failed: %v\n", err)
		}

		count++
	}
}
