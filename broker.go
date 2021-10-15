package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

//go:generate mockgen -source=$GOFILE -destination=broker_mock.go -package=main
type brokerer interface {
	Publish(topic string, value *value) error
	Subscribe(topic string) *consumer
	Unsubscribe(topic, id string) error
	Purge(topic string) error
	Topics() ([]string, error)
}

type broker struct {
	store     storer
	consumers map[string][]consumer
	sync.RWMutex
}

func newBroker(store storer) *broker {
	return &broker{
		store:     store,
		consumers: map[string][]consumer{},
	}
}

func (b *broker) Topics() ([]string, error) {
	meta, err := b.store.Meta()

	return meta.topics, err
}

// ProcessDelays is a blocking function which starts a loop to check and return
// delayed messages which have completed their designated delay back to the main
// queue.
func (b *broker) ProcessDelays(ctx context.Context, period time.Duration) {
	log.Debug().Msg("starting delay queue processing")

	for {
		meta, err := b.store.Meta()
		if err != nil {
			continue
		}

		if err := processTopics(b, meta.topics); err != nil {
			log.Err(err).Msg("failed to process topics")
		}

		select {
		case <-time.After(period):
		case <-ctx.Done():
			log.Debug().Msg("stopping delay queue processing, context cancelled")
			return
		}
	}
}

func processTopics(b *broker, topics []string) error {
	now := time.Now()

	for _, t := range topics {
		count, err := b.store.ReturnDelayed(t, now)
		if err != nil {
			log.Err(err).Msg("returning delayed messages to main queue")
			continue
		}

		if count >= 1 {
			log.Debug().
				Str("topic", t).
				Int("count", count).
				Msg("returning delayed messages")

			b.NotifyConsumer(t, eventTypeMsgReturned)
		}
	}

	return nil
}

// Publish a message to a topic.
func (b *broker) Publish(topic string, val *value) error {
	if err := b.store.Insert(topic, val); err != nil {
		return err
	}

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

// Unsubscribe removes the consumer from the available pool for the topic.
func (b *broker) Unsubscribe(topic, id string) error {
	b.Lock()
	defer b.Unlock()

	consumers := b.consumers[topic]
	for i, v := range consumers {
		if v.id == id {
			b.consumers[topic] = append(consumers[:i], consumers[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("consumer ID %s not found for topic %s", id, topic)
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
		default:
			// TODO if it fails to send to the consumer, find another consumer to send
			// the message to and possibly remove this consumer from the pool.
		}
	}
}
