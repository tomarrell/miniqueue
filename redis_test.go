package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	redcon "github.com/tidwall/redcon"
)

var redcliPath string

func init() {
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		redcliPath = "./testdata/cmd/redcli_linux_amd64"
	} else if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		redcliPath = "./testdata/cmd/redcli_darwin_arm64"
	} else {
		panic("unsupported test platform")
	}
}

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
	cmd := exec.Command(redcliPath, "publish", topic, value)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}

	fmt.Println(string(out))
}

func consumeOne(t *testing.T, topic string) string {
	t.Helper()
	cmd := exec.Command(redcliPath, "subscribe", "-c", "1", topic)

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
