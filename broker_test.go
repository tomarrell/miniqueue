package main

import (
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestBrokerPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		topic = "test_topic"
		value = newValue([]byte("test_value"))
	)

	mockStore := NewMockstorer(ctrl)
	mockStore.EXPECT().Meta().Times(2).Return(&metadata{}, nil)
	mockStore.EXPECT().Insert(topic, value)

	b := newBroker(mockStore)

	assert.NoError(t, b.Publish(topic, value))
}

func TestBrokerSubscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		topic = "test_topic"
	)

	mockStore := NewMockstorer(ctrl)
	mockStore.EXPECT().Meta().Return(&metadata{}, nil)

	b := newBroker(mockStore)
	c := b.Subscribe(topic)

	assert.IsType(t, &consumer{}, c)
}
