package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	redcon "github.com/tidwall/redcon"
)

func TestRedisPublish(t *testing.T) {
	t.Run("publish publishes to the respective queue", func(t *testing.T) {
		// require := require.New(t)

		helperNewTestRedisServer(t)

		publishOne(t, "test", "test")

		consumeOne(t, "test")
	})
}

func TestRedisSubscribe(t *testing.T) {
	t.Run("subscribe returns waiting message", func(t *testing.T) {
	})
}

// Helpers

func publishOne(t *testing.T, topic, value string) {
	t.Helper()
	cmd := exec.Command("./testdata/cmd/redcli", "publish", topic, value)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}

	fmt.Println(string(out))
}

func consumeOne(t *testing.T, topic string) string {
	t.Helper()
	cmd := exec.Command("./testdata/cmd/redcli", "subscribe", "-c", "1", topic)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}

	fmt.Println(string(out))

	return string(out)
}

func helperNewTestRedisServer(t *testing.T) *redis {
	r := newRedis(newBroker(newStore(os.TempDir())))

	s := redcon.NewServer("localhost:6379", r.handleCmd, nil, nil)
	t.Cleanup(func() {
		time.Sleep(10 * time.Millisecond)
		s.Close()
	})

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(10 * time.Millisecond)

	return r
}
