package main

// consumer handles providing values iteratively to a single consumer.
type consumer struct {
	id    string
	topic string
	store storer
}

// Next requests the next value in the series.
func (c *consumer) Next() (value, error) {
	return c.store.GetNext(c.topic)
}

// Ack acknowledges the previously consumed value.
func (c *consumer) Ack() error {
	return c.store.IncHead(c.topic)
}
