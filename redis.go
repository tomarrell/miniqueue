package main

import (
	"context"
	"fmt"
	"strings"

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
		topic := string(rcmd.Args[1])
		c := broker.Subscribe(topic)
		defer broker.Unsubscribe(topic, c.id)

		log := log.With().Str("id", c.id).Logger()

		log.Debug().Str("topic", topic).Msg("new connection")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Detach the connection from the client so that we can control its
		// lifecycle independently.
		dconn := conn.Detach()
		dconn.SetContext(ctx)
		defer func() {
			log.Debug().Msg("closing connection")
			flush(log, dconn)
			dconn.Close()
		}()

		if len(rcmd.Args) != 2 {
			dconn.WriteError("invalid number of args, want: 2")
			return
		}

		for {
			val, err := c.Next(ctx)
			if err != nil {
				log.Err(err).Msg("getting next value")
				dconn.WriteError("failed to get next value")
				return
			}

			log.Debug().Str("msg", string(val.Raw)).Msg("sending msg")

			dconn.WriteAny(val.Raw)
			if err := flush(log, dconn); err != nil {
				dconn.WriteError("failed to flush msg")
				return
			}

			log.Debug().Msg("awaiting ack")

			cmd, err := dconn.ReadCommand()
			if err != nil {
				log.Err(err).Msg("reading ack")
				dconn.WriteError("failed to get next value")
				return
			}

			if len(cmd.Args) != 1 {
				log.Error().Str("cmd", string(cmd.Raw)).Int("len", len(cmd.Args)).Msg("invalid cmd length")
				dconn.WriteError("invalid command")
				return
			}

			ackCmd := string(cmd.Args[0])

			log.Debug().Str("cmd", ackCmd).Msg("received ack cmd")

			switch strings.ToUpper(ackCmd) {
			case CmdAck:
				if err := c.Ack(); err != nil {
					log.Err(err).Msg("acking")
					dconn.WriteError("failed to ack")
					return
				}
			case CmdBack:
				if err := c.Back(); err != nil {
					log.Err(err).Msg("backing")
					dconn.WriteError("failed to back")
					return
				}
			case CmdNack:
				if err := c.Nack(); err != nil {
					log.Err(err).Msg("Nacking")
					dconn.WriteError("failed to nack")
					return
				}
			case CmdDack:
				// TODO read extra arg
				if err := c.Dack(1); err != nil {
					log.Err(err).Msg("Nacking")
					dconn.WriteError("failed to nack")
					return
				}
			default:
				log.Error().Str("cmd", ackCmd).Msg("invalid ack command")
				dconn.WriteError("invalid ack command")
				return
			}
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

		log.Debug().
			Str("topic", topic).
			Str("msg", string(value.Raw)).
			Msg("published msg")

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
