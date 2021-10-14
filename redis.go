package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
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
		log := log.With().Str("id", xid.New().String()).Logger()

		log.Debug().Msg("new connection")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dconn := conn.Detach()
		dconn.SetContext(ctx)
		defer func() {
			log.Debug().Msg("closing connection")
			dconn.Close()
		}()

		if len(rcmd.Args) != 2 {
			dconn.WriteError("invalid number of args, want: 2")
			flush(log, dconn)
			return
		}

		topic := string(rcmd.Args[1])
		c := broker.Subscribe(topic)

		for {
			val, err := c.Next(ctx)
			if err != nil {
				log.Err(err).Msg("getting next value")
				dconn.WriteError("failed to get next value")
				flush(log, dconn)
				return
			}

			log.Debug().Str("msg", string(val.Raw)).Msg("sending msg")

			dconn.WriteAny(val.Raw)
			if err := flush(log, dconn); err != nil {
				dconn.WriteError("failed to flush msg")
				return
			}

			// TODO Wait for the acknowledgement
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

// Flush flushes pending messages to the client, handling any errors.
func flush(log zerolog.Logger, dconn redcon.DetachedConn) error {
	if err := dconn.Flush(); err != nil {
		log.Err(err).Msg("flushing msg")
		return err
	}

	return nil
}
