package main

import (
	"testing"

	gomock "github.com/golang/mock/gomock"
	redcon "github.com/tidwall/redcon"
)

func TestRedisPublish(t *testing.T) {
	t.Run("publish publishes to the respective queue", func(t *testing.T) {
		// TODO
		t.Skip()

		ctrl := gomock.NewController(t)
		b := NewMockbrokerer(ctrl)

		mockConn := NewMockConn(ctrl)
		cmd := redcon.Command{
			Raw:  []byte{},
			Args: [][]byte{},
		}

		handleRedisPublish(b)(mockConn, cmd)
	})
}

func TestRedisSubscribe(t *testing.T) {
	t.Run("subscribe returns waiting message", func(t *testing.T) {
	})
}
