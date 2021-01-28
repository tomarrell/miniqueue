package main

const (
	eventTypePublish = eventType("PUBLISH")
)

type eventType string

// consumer handles providing values iteratively to a single consumer.
type consumer struct {
	id        string
	topic     string
	store     storer
	eventChan chan eventType
}

// Next requests the next value in the series.
func (c *consumer) Next() (value, error) {
	return c.store.GetNext(c.topic)
}

// Ack acknowledges the previously consumed value.
func (c *consumer) Ack() error {
	return c.store.IncHead(c.topic)
}

// EventChan returns a channel to notify the consumer of events occurring on the
// topic.
func (c *consumer) EventChan() <-chan eventType {
	return c.eventChan
}
