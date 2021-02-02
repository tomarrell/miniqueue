//go:generate mockgen -source=$GOFILE -destination=broker_mock.go -package=main
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
func (b *broker) Publish(topicName string, value value) error {
	if err := b.store.Insert(topicName, value); err != nil {
		return err
	}

	// Notify the consumers of the topic of the new event
	for _, c := range b.consumers[topicName] {
		c.eventChan <- eventTypePublish
	}

	return nil
}

// Subscribe to a topic and return a consumer for the topic.
func (b *broker) Subscribe(topic string) consumer {
	cons := consumer{
		id:        xid.New().String(),
		topic:     topic,
		store:     b.store,
		eventChan: make(chan eventType),
	}

	b.consumers[topic] = append(b.consumers[topic], cons)

	return cons
}

// Shutdown the broker.
func (b *broker) Shutdown() error {
	return b.store.Close()
}
