package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	redcon "github.com/tidwall/redcon"
)

func TestRedisPublish(t *testing.T) {
	t.Run("publish publishes to the respective queue", func(t *testing.T) {
		require := require.New(t)

		helperNewTestRedisServer(t)

		err := publishOne("test", "test")
		require.NoError(err)

		_, err = consumeOne("test")
		require.NoError(err)
	})
}

func TestRedisSubscribe(t *testing.T) {
	t.Run("subscribe returns waiting message", func(t *testing.T) {
	})
}

// Helpers

func publishOne(topic, value string) error {
	cmd := exec.Command("./testdata/cmd/redcli", "publish", topic, value)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	fmt.Println(string(out))

	return nil
}

func consumeOne(topic string) (string, error) {
	cmd := exec.Command("./testdata/cmd/redcli", "subscribe", "-c", "1", topic)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}

	fmt.Println(string(out))

	return "", nil
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

	return r
}
