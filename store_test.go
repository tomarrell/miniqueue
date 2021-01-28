package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const tmpDBPath = "/tmp/miniqueue_test_db"

// INSERT

func TestInsert_Single(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
		value = []byte("test_value")
	)

	assert.NoError(t, s.Insert(topic, value))

	val, err := s.GetNext(topic)
	assert.NoError(t, err)
	assert.Equal(t, value, val)
}

func TestInsert_TwoSameTopic(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
	)

	assert.NoError(t, s.Insert(topic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(topic, []byte("test_value_2")))

	val, err := s.GetOffset(topic, 0)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	val, err = s.GetOffset(topic, 1)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))
}

func TestInsert_ThreeSameTopic(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
	)

	assert.NoError(t, s.Insert(topic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(topic, []byte("test_value_2")))
	assert.NoError(t, s.Insert(topic, []byte("test_value_3")))

	val, err := s.GetOffset(topic, 0)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	val, err = s.GetOffset(topic, 1)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))

	val, err = s.GetOffset(topic, 2)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_3", string(val))
}

// GETNEXT

func TestGetNext(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
	)

	assert.NoError(t, s.Insert(topic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(topic, []byte("test_value_2")))

	val, err := s.GetNext(topic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))
}

func TestGetNext_TopicNotInitialised(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
	)

	val, err := s.GetNext(topic)
	assert.Equal(t, errTopicNotExist, err)
	assert.Equal(t, "", string(val))
}

// GETOFFSET

func TestGetOffset(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		topic = "test_topic"
	)

	assert.NoError(t, s.Insert(topic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(topic, []byte("test_value_2")))

	val, err := s.GetOffset(topic, 1)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))
}
