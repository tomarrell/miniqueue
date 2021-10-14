package main

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	assert "github.com/stretchr/testify/require"
)

func TestConsumerNext(t *testing.T) {
	t.Run("next, ack, next", func(t *testing.T) {
		assert := assert.New(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			topic = "test_topic"
			msg1  = newValue([]byte("message1"))
			msg2  = newValue([]byte("message2"))
		)

		mockStore := NewMockstorer(ctrl)
		mockStore.EXPECT().GetNext(topic).Return(msg1, 0, nil)
		mockStore.EXPECT().Ack(topic, 0).Return(nil)
		mockStore.EXPECT().GetNext(topic).Return(msg2, 1, nil)

		b := newBroker(mockStore)
		c := b.Subscribe(topic)

		msg, err := c.Next(context.Background())
		assert.NoError(err)
		assert.Equal(msg1, msg)
		assert.Equal(c.ackOffset, 0)

		assert.NoError(c.Ack())

		msg, err = c.Next(context.Background())
		assert.NoError(err)
		assert.Equal(msg2, msg)
		assert.Equal(c.ackOffset, 1)
	})

	t.Run("next next, fails due to outstanding ack", func(t *testing.T) {
		assert := assert.New(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			topic = "test_topic"
			msg1  = newValue([]byte("message1"))
		)

		mockStore := NewMockstorer(ctrl)
		mockStore.EXPECT().GetNext(topic).Return(msg1, 0, nil)

		b := newBroker(mockStore)
		c := b.Subscribe(topic)

		msg, err := c.Next(context.Background())
		assert.NoError(err)
		assert.Equal(msg1, msg)
		assert.Equal(c.ackOffset, 0)

		msg, err = c.Next(context.Background())
		assert.Error(err)
	})
}
