package main

import (
	"context"
	"fmt"
	"strings"

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
		handleRedisTopics(r.broker)(conn, rcmd)

	case "publish":
		handleRedisPublish(r.broker)(conn, rcmd)

	case "subscribe":
		handleRedisSubscribe(r.broker)(conn, rcmd)
	}
}

func handleRedisTopics(broker brokerer) redcon.HandlerFunc {
	return func(conn redcon.Conn, rcmd redcon.Command) {
		topics, err := broker.Topics()
		if err != nil {
			log.Err(err).Msg("failed to get topics")
			conn.WriteError("failed to get topics")
			return
		}

		conn.WriteString(fmt.Sprintf("%v", topics))
	}
}

func handleRedisSubscribe(broker brokerer) redcon.HandlerFunc {
	return func(conn redcon.Conn, rcmd redcon.Command) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dconn := conn.Detach()
		dconn.SetContext(ctx)

		if len(rcmd.Args) != 2 {
			dconn.WriteError("invalid number of args, want: 2")
			dconn.Flush()
			return
		}

		topic := string(rcmd.Args[1])
		c := broker.Subscribe(topic)

		for {
			val, err := c.Next(ctx)
			if err != nil {
				log.Err(err).Msg("getting next value")
				dconn.WriteError("failed to get next value")
				dconn.Flush()
				return
			}

			dconn.WriteAny(val.Raw)
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
