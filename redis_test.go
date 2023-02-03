package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		_ = helperNewTestRedisServer(t)

		var (
			topic = "topic"
			val   = "value"
		)

		publishOne(t, topic, val)
		res := consumeOne(t, topic)
		require.Equal(t, val, res)
	})
}

func TestRedisSubscribe(t *testing.T) {
	t.Run("subscriber waits for message", func(t *testing.T) {
		_ = helperNewTestRedisServer(t)

		var (
			topic = "topic"
			val   = "value"
		)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			res := consumeOne(t, topic)
			require.Equal(t, val, res)
			wg.Done()
		}()

		publishOne(t, topic, val)
		wg.Wait()
	})

	t.Run("subscriber waiting for two messages on same topic", func(t *testing.T) {
		_ = helperNewTestRedisServer(t)

		var (
			topic = "topic"
			val1  = "value1"
			val2  = "value2"
		)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			res := consumeOne(t, topic)
			require.Equal(t, val1, res)

			res = consumeOne(t, topic)
			require.Equal(t, val2, res)
			wg.Done()
		}()

		publishOne(t, topic, val1)
		publishOne(t, topic, val2)
		wg.Wait()
	})

	t.Run("subscribers on seperate topics", func(t *testing.T) {
		_ = helperNewTestRedisServer(t)

		var (
			topic1 = "topic1"
			topic2 = "topic2"
			val1   = "value1"
			val2   = "value2"
		)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			res := consumeOne(t, topic1)
			require.Equal(t, val1, res)
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			res := consumeOne(t, topic2)
			require.Equal(t, val2, res)
			wg.Done()
		}()

		publishOne(t, topic1, val1)
		publishOne(t, topic2, val2)
		wg.Wait()
	})

	t.Run("large message quantity, same topic", func(t *testing.T) {
		_ = helperNewTestRedisServer(t)

		var (
			topic = "topic"
		)

		// Publish n messages
		n := 50
		for i := 0; i < n; i++ {
			publishOne(t, topic, strconv.Itoa(i))
		}

		for i := 0; i < n; i++ {
			res := consumeOne(t, topic)
			require.Equal(t, strconv.Itoa(i), res)
		}
	})
}

// Helpers

func publishOne(t *testing.T, topic, value string) {
	t.Helper()
	cmd := exec.Command(redcliPath, "publish", topic, value)

	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(raw))
	}

	out := strings.TrimSuffix(string(raw), "\n")

	fmt.Println(out)
}

func consumeOne(t *testing.T, topic string) string {
	t.Helper()
	cmd := exec.Command(redcliPath, "subscribe", "-c", "1", topic)

	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err, string(raw))
	}

	out := strings.TrimSuffix(string(raw), "\n")

	fmt.Println(out)

	return out
}

func helperNewTestRedisServer(t *testing.T) *redis {
	dir, err := os.MkdirTemp("", "miniqueue_")
	require.NoError(t, err)
	r := newRedis(newBroker(newStore(dir)))

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
