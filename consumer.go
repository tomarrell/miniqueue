package main

import (
	"context"
	"errors"
	"fmt"
)

const (
	eventTypePublish eventType = iota
	eventTypeNack
	eventTypeBack
)

type eventType int

type notifier interface {
	NotifyConsumer(topic string, ev eventType)
}

// consumer handles providing values iteratively to a single consumer.
type consumer struct {
	id        string
	topic     string
	ackOffset int
	store     storer
	eventChan chan eventType
	notifier  notifier
}

// Next will attempt to retrieve the next value on the topic, or it will
// block waiting for a msg indicating there is a new value available.
func (c *consumer) Next(ctx context.Context) (val value, err error) {
	val, ao, err := c.store.GetNext(c.topic)
	if errors.Is(err, errTopicEmpty) {
		select {
		case <-c.eventChan:
		case <-ctx.Done():
			return nil, errRequestCancelled
		}

		return c.Next(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("getting next from store: %v", err)
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

	c.notifier.NotifyConsumer(c.topic, eventTypeNack)

	return nil
}

// Back negatively acknowleges a message, returning it to the back of the queue
// for consumption.
func (c *consumer) Back() error {
	if err := c.store.Back(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("nacking topic %s with offset %d: %v", c.topic, c.ackOffset, err)
	}

	c.notifier.NotifyConsumer(c.topic, eventTypeBack)

	return nil
}

// EventChan returns a channel to notify the consumer of events occurring on the
// topic.
func (c *consumer) EventChan() <-chan eventType {
	return c.eventChan
}
