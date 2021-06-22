package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

const (
	respOK = "OK"
)

type redis struct {
	broker brokerer
}

func newRedis(b brokerer) *redis {
	return &redis{
		broker: b,
	}
}

func (r *redis) handleCmd(conn redcon.Conn, rcmd redcon.Command) {
	cmd := string(rcmd.Args[0])
	switch strings.ToLower(cmd) {
	default:
		conn.WriteError(fmt.Sprintf("unknown command '%s'", cmd))

	case "info":
		conn.WriteString(fmt.Sprintf("redis_version:miniqueue_%s", version))

	case "ping":
		conn.WriteString("pong")

	case "topics":
		topics, err := r.broker.Topics()
		if err != nil {
			log.Err(err).Msg("failed to get topics")
			conn.WriteError("failed to get topics")
			return
		}

		conn.WriteString(fmt.Sprintf("%v", topics))

	case "publish":
		handleRedisPublish(r.broker)(conn, rcmd)

	case "subscribe":
		handleRedisSubscribe(r.broker)(conn, rcmd)
	}
}

func handleRedisTopics(broker brokerer) redcon.HandlerFunc {
	return func(conn redcon.Conn, rcmd redcon.Command) {
	}
}

func handleRedisSubscribe(broker brokerer) redcon.HandlerFunc {
	return func(conn redcon.Conn, rcmd redcon.Command) {
		dconn := conn.Detach()
		n := 0

		for {
			<-time.After(time.Second)
			n += 1
			dconn.WriteString(fmt.Sprintf("Hello, %d", n))
			dconn.Flush()
		}
	}
}

func handleRedisPublish(broker brokerer) redcon.HandlerFunc {
	return func(conn redcon.Conn, rcmd redcon.Command) {
		if len(rcmd.Args) != 3 {
			conn.WriteError("invalid number of args, want: 3")
			return
		}

		var (
			topic = string(rcmd.Args[1])
			value = newValue(rcmd.Args[2])
		)

		if err := broker.Publish(topic, value); err != nil {
			log.Err(err).Msg("failed to publish")
			conn.WriteError("failed to publish")
			return
		}

		conn.WriteString(respOK)
	}
}
