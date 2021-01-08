//go:generate mockgen -source=$GOFILE -destination=broker_mock.go -package=main
package main

type value = []byte

type storer interface {
	Insert(topic string, value []byte) error
	Close() error
}

type broker struct {
	store storer
}

func newBroker(store storer) *broker {
	return &broker{store: store}
}

// Publish a message to a topic.
func (b *broker) Publish(topicName string, value value) error {
	b.store.Insert(topicName, value)

	return nil
}

// Subscribe to a topic and return a channel which will receive messages
// published to the topic.
func (b *broker) Subscribe() <-chan value {
	c := make(<-chan value)
	return c
}

// Shutdown the broker.
func (b *broker) Shutdown() error {
	return b.store.Close()
}
