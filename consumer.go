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
	eventTypeMsgReturned
)

type eventType int

type notifier interface {
	NotifyConsumer(topic string, ev eventType)
}

// consumer handles providing values iteratively to a single consumer. Methods
// on a consumer are not thread safe as operations should occur serially.
type consumer struct {
	id          string
	topic       string
	ackOffset   int
	store       storer
	eventChan   chan eventType
	notifier    notifier
	outstanding bool // indicates whether the consumer has an outstanding message to ack
}

// Next will attempt to retrieve the next value on the topic, or it will
// block waiting for a msg indicating there is a new value available.
func (c *consumer) Next(ctx context.Context) (val *value, err error) {
	// Prevent Next from being called if the consumer already has one outstanding
	// unacknowledged message.
	if c.outstanding {
		return nil, errors.New("unacknowledged message outstanding")
	}

	var ao int

	// Repeat trying to get the next value while the topic is either empty or not
	// created yet. It may exist sometime in the future.
	for {
		val, ao, err = c.store.GetNext(c.topic)
		if !errors.Is(err, errTopicEmpty) && !errors.Is(err, errTopicNotExist) {
			break
		}

		select {
		case <-c.eventChan:
		case <-ctx.Done():
			return nil, errRequestCancelled
		}
	}
	if err != nil {
		return nil, fmt.Errorf("getting next from store: %v", err)
	}

	c.ackOffset = ao
	c.outstanding = true

	return val, err
}

// Ack acknowledges the previously consumed value.
func (c *consumer) Ack() error {
	if err := c.store.Ack(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("acking topic %s with offset %d: %v", c.topic, c.ackOffset, err)
	}

	c.outstanding = false

	return nil
}

// Nack negatively acknowledges a message, returning it for consumption by other
// consumers.
func (c *consumer) Nack() error {
	if err := c.store.Nack(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("nacking topic %s with offset %d: %w", c.topic, c.ackOffset, err)
	}

	c.outstanding = false
	c.notifier.NotifyConsumer(c.topic, eventTypeNack)

	return nil
}

// Back negatively acknowleges a message, returning it to the back of the queue
// for consumption.
func (c *consumer) Back() error {
	if err := c.store.Back(c.topic, c.ackOffset); err != nil {
		return fmt.Errorf("backing topic %s with offset %d: %v", c.topic, c.ackOffset, err)
	}

	c.outstanding = false
	c.notifier.NotifyConsumer(c.topic, eventTypeBack)

	return nil
}

func (c *consumer) Dack(delaySeconds int) error {
	if err := c.store.Dack(c.topic, c.ackOffset, delaySeconds); err != nil {
		return fmt.Errorf("dacking topic %s with offset %d and delay %ds: %v", c.topic, c.ackOffset, delaySeconds, err)
	}

	c.outstanding = false

	return nil
}

// EventChan returns a channel to notify the consumer of events occurring on the
// topic.
func (c *consumer) EventChan() <-chan eventType {
	return c.eventChan
}
