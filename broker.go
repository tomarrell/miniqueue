//go:generate mockgen -source=$GOFILE -destination=broker_mock.go -package=main
package main

import "github.com/rs/xid"

type value = []byte

// storer should be safe for concurrent use.
type storer interface {
	Insert(topic string, value value) error
	GetNext(topic string) (value, error)
	IncHead(topic string) error
	Close() error
}

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
	return b.store.Insert(topicName, value)
}

// Subscribe to a topic and return a consumer for the topic.
func (b *broker) Subscribe(topic string) consumer {
	cons := consumer{
		id:    xid.New().String(),
		topic: topic,
		store: b.store,
	}

	b.consumers[topic] = append(b.consumers[topic], cons)

	return cons
}

// Shutdown the broker.
func (b *broker) Shutdown() error {
	return b.store.Close()
}

type consumer struct {
	id    string
	topic string
	store storer
}

func (c *consumer) Next() (value, error) {
	return c.store.GetNext(c.topic)
}

func (c *consumer) Ack() error {
	return c.store.IncHead(c.topic)
}
