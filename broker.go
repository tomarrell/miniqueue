package main

import "github.com/rs/xid"

type value = []byte

type broker struct {
	store     storer
	consumers map[string][]consumer
}

func newBroker(store storer) *broker {
	return &broker{
		store:     store,
		consumers: map[string][]consumer{},
	}
}

// Publish a message to a topic.
func (b *broker) Publish(topic string, val value) error {
	if err := b.store.Insert(topic, val); err != nil {
		return err
	}

	b.NotifyConsumers(topic, eventTypePublish)

	return nil
}

// Subscribe to a topic and return a consumer for the topic.
func (b *broker) Subscribe(topic string) consumer {
	cons := consumer{
		id:        xid.New().String(),
		topic:     topic,
		store:     b.store,
		eventChan: make(chan eventType),
		notifier:  b,
	}

	b.consumers[topic] = append(b.consumers[topic], cons)

	return cons
}

// Shutdown the broker.
func (b *broker) Shutdown() error {
	return b.store.Close()
}

// NotifyConsumers notifies the consumers of a topic that an event has occurred.
func (b *broker) NotifyConsumers(topic string, ev eventType) {
	for _, c := range b.consumers[topic] {
		select {
		case c.eventChan <- ev:
		default: // If there is noone listening noop
		}
	}
}
