package main

import (
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBroker_Publish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		topic = "test_topic"
		value = newValue([]byte("test_value"))
	)

	mockStore := NewMockstorer(ctrl)
	mockStore.EXPECT().Insert(topic, value)

	b := newBroker(mockStore)

	require.NoError(t, b.Publish(topic, value))
}

func TestBroker_Subscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		topic = "test_topic"
	)

	mockStore := NewMockstorer(ctrl)

	b := newBroker(mockStore)
	c := b.Subscribe(topic)

	require.IsType(t, &consumer{}, c)
}

func TestBroker_Unsubscribe(t *testing.T) {
	t.Run("removes consumer from the topic", func(t *testing.T) {
		b := broker{
			consumers: map[string][]consumer{},
		}

		topic := "test_topic"

		c := b.Subscribe(topic)
		err := b.Unsubscribe(topic, c.id)
		require.NoError(t, err)
		require.Len(t, b.consumers[topic], 0)
	})

	t.Run("returns an error if the consumer doesn't exist", func(t *testing.T) {
		b := broker{
			consumers: map[string][]consumer{},
		}

		topic := "test_topic"

		err := b.Unsubscribe(topic, "test_id")
		require.Error(t, err)
	})
}
