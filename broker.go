package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

type broker struct {
	meta      *metadata
	store     storer
	consumers map[string][]consumer
	sync.RWMutex
}

func newBroker(store storer) *broker {
	meta, err := store.Meta()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialise broker")
	}

	return &broker{
		meta:      meta,
		store:     store,
		consumers: map[string][]consumer{},
	}
}

// ProcessDelays is a blocking function which starts a loop to check and return
// delayed messages which have completed their designated delay back to the main
// queue.
func (b *broker) ProcessDelays(ctx context.Context, period time.Duration) {
	log.Debug().Msg("starting delay queue processing")

	for {
		now := time.Now()

		b.RLock()
		topics := b.meta.topics
		b.RUnlock()
		for _, t := range topics {
			count, err := b.store.ReturnDelayed(t, now)
			if err != nil {
				log.Err(err).Msg("returning delayed messages to main queue")
			}

			log.Debug().
				Str("topic", t).
				Int("count", count).
				Msg("returning delayed messages")

			if count >= 1 {
				b.NotifyConsumer(t, eventTypeMsgReturned)
			}
		}

		select {
		case <-time.After(period):
		case <-ctx.Done():
			log.Debug().Msg("stopping delay queue processing, context cancelled")
			return
		}
	}
}

// Publish a message to a topic.
func (b *broker) Publish(topic string, val *value) error {
	if err := b.store.Insert(topic, val); err != nil {
		return err
	}

	b.Lock()
	meta, err := b.store.Meta()
	if err != nil {
		return fmt.Errorf("updating topic metadata: %v", err)
	}
	b.meta = meta
	b.Unlock()

	b.NotifyConsumer(topic, eventTypePublish)

	return nil
}

// Subscribe to a topic and return a consumer for the topic.
func (b *broker) Subscribe(topic string) *consumer {
	b.Lock()
	defer b.Unlock()

	cons := consumer{
		id:        xid.New().String(),
		topic:     topic,
		store:     b.store,
		eventChan: make(chan eventType),
		notifier:  b,
	}

	b.consumers[topic] = append(b.consumers[topic], cons)

	return &cons
}

// Purge removes the topic from the broker.
func (b *broker) Purge(topic string) error {
	if err := b.store.Purge(topic); err != nil {
		return fmt.Errorf("purging topic in store: %v", err)
	}

	return nil
}

// Shutdown the broker.
func (b *broker) Shutdown() error {
	return b.store.Close()
}

// NotifyConsumers notifies a waiting consumer of a topic that an event has
// occurred.
func (b *broker) NotifyConsumer(topic string, ev eventType) {
	b.RLock()
	defer b.RUnlock()

	for _, c := range b.consumers[topic] {
		select {
		case c.eventChan <- ev:
			return
		default: // If there is noone listening noop
		}
	}
}
