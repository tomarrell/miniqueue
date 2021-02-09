package main

import "fmt"

const (
	eventTypePublish = eventType("PUBLISH")
)

type eventType string

// consumer handles providing values iteratively to a single consumer.
type consumer struct {
	id        string
	topic     string
	ackOffset int
	store     storer
	eventChan chan eventType
}

// Next requests the next value in the series.
func (c *consumer) Next() (val value, err error) {
	val, ao, err := c.store.GetNext(c.topic)
	if err != nil {
		return nil, fmt.Errorf("getting next from store: %w", err)
	}

	c.ackOffset = ao

	return val, err
}

// Ack acknowledges the previously consumed value.
func (c *consumer) Ack() error {
	if err := c.store.Ack(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("acking topic %s with offset %d: %v", c.topic, c.ackOffset, err)
	}

	return nil
}

// Nack negatively acknowledges a message, returning it for consumption by other
// consumers.
func (c *consumer) Nack() error {
	if err := c.store.Nack(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("nacking topic %s with offset %d: %v", c.topic, c.ackOffset, err)
	}

	return nil
}

// EventChan returns a channel to notify the consumer of events occurring on the
// topic.
func (c *consumer) EventChan() <-chan eventType {
	return c.eventChan
}
