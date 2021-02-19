package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const tmpDBPath = "/tmp/miniqueue_test_db"

// Insert
func TestInsert_Single(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value")))

	val, _, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value", string(val))
}

func TestInsert_TopicMeta(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))

	var topics []string
	val, err := s.db.Get([]byte(metaTopics), nil)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(val, &topics))
	assert.Contains(t, topics, defaultTopic)

	assert.NoError(t, s.Insert("other_topic", []byte("test_value_2")))
	val, err = s.db.Get([]byte(metaTopics), nil)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(val, &topics))
	assert.Contains(t, topics, "other_topic")
}

func TestInsert_TwoSameTopic(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_2")))

	val, err := getOffset(s.db, topicFmt, defaultTopic, 0)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	val, err = getOffset(s.db, topicFmt, defaultTopic, 1)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))
}

func TestInsert_ThreeSameTopic(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_2")))
	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_3")))

	val, err := getOffset(s.db, topicFmt, defaultTopic, 0)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	val, err = getOffset(s.db, topicFmt, defaultTopic, 1)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))

	val, err = getOffset(s.db, topicFmt, defaultTopic, 2)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_3", string(val))
}

// GetNext
func TestGetNext(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))
	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_2")))
	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_3")))

	val, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))
	assert.Equal(t, 0, offset)

	val, offset, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(val))
	assert.Equal(t, 1, offset)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_4")))

	val, offset, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_3", string(val))
	assert.Equal(t, 2, offset)

	val, offset, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_4", string(val))
	assert.Equal(t, 3, offset)
}

func TestGetNext_TopicNotInitialised(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	val, _, err := s.GetNext(defaultTopic)
	assert.Equal(t, errTopicNotExist, err)
	assert.Equal(t, "", string(val))
}

// Ack
func TestAck(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	ackOffset := 1
	key := []byte(fmt.Sprintf(ackTopicFmt, defaultTopic, ackOffset))
	assert.NoError(t, s.db.Put(key, []byte("hello_world"), nil))

	assert.NoError(t, s.Ack(defaultTopic, ackOffset))

	has, err := s.db.Has(key, nil)
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestAck_WithPos(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	err := s.Insert(defaultTopic, []byte("test_value_1"))
	assert.NoError(t, err)

	val, ackOffset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	val, err = getOffset(s.db, ackTopicFmt, defaultTopic, ackOffset)
	assert.NoError(t, err)
	assert.Equal(t, "test_value_1", string(val))

	assert.NoError(t, s.Ack(defaultTopic, ackOffset))

	_, err = getOffset(s.db, ackTopicFmt, defaultTopic, ackOffset)
	assert.Error(t, err)
}

// Nack
func TestNack(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))

	_, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)

	assert.NoError(t, s.Nack(defaultTopic, offset))
}

func TestNack_Twice(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))

	_, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)

	// First Nack
	assert.NoError(t, s.Nack(defaultTopic, offset))

	// Second Nack
	err = s.Nack(defaultTopic, offset)
	assert.Error(t, err)
	assert.Equal(t, err, errNackMsgNotExist)
}

func TestNack_AndGet(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		msg1 = "test_value_1"
		msg2 = "test_value_2"
	)

	assert.NoError(t, s.Insert(defaultTopic, []byte(msg1)))
	assert.NoError(t, s.Insert(defaultTopic, []byte(msg2)))

	val, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg1, string(val))

	assert.NoError(t, s.Nack(defaultTopic, offset))

	val, _, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg1, string(val))
}

// Back
func TestBack(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	assert.NoError(t, s.Insert(defaultTopic, []byte("test_value_1")))

	_, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)

	assert.NoError(t, s.Back(defaultTopic, offset))
}

func TestBack_Get(t *testing.T) {
	s := newStore(tmpDBPath)
	t.Cleanup(s.Destroy)

	var (
		msg1 = []byte("test_value_1")
		msg2 = []byte("test_value_2")
	)

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)

	assert.NoError(t, s.Back(defaultTopic, offset))

	v, _, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg2, v)
}

// Dack
func TestDack(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset, _ := s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset, 1))

	_, offset, _ = s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset, 3))
}

func TestDack_SameTime(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset1, _ := s.GetNext(defaultTopic)
	_, offset2, _ := s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset1, 1))
	assert.NoError(t, s.Dack(defaultTopic, offset2, 1))
}

// GetDelayed
func TestGetDelayed(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	startTime := time.Now()

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, insertDelay(s.db, defaultTopic, msg1, 1))
	assert.NoError(t, insertDelay(s.db, defaultTopic, msg2, 3))

	iter, closer := s.GetDelayed(defaultTopic)

	assert.True(t, iter.Next())
	timestamp, _ := strconv.Atoi(strings.Split(string(iter.Key()), "-")[3])
	delayToTime := time.Unix(int64(timestamp), 0)
	assert.True(t, startTime.Before(delayToTime), "expected delay timestamp to be after now")

	assert.True(t, iter.Next())
	timestamp, _ = strconv.Atoi(strings.Split(string(iter.Key()), "-")[3])
	delayToTime = time.Unix(int64(timestamp), 0)
	assert.True(t, startTime.Before(delayToTime), "expected delay timestamp to be after now")

	assert.NoError(t, closer())
}

func TestGetDelayed_SameTimestamp(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	startTime := time.Now()

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset1, _ := s.GetNext(defaultTopic)
	_, offset2, _ := s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset1, 1))
	assert.NoError(t, s.Dack(defaultTopic, offset2, 1))

	iter, closer := s.GetDelayed(defaultTopic)

	assert.True(t, iter.Next())
	localOffset := strings.Split(string(iter.Key()), "-")[4]
	timestamp, _ := strconv.Atoi(strings.Split(string(iter.Key()), "-")[3])
	delayToTime := time.Unix(int64(timestamp), 0)
	assert.True(t, startTime.Before(delayToTime), "expected delay timestamp to be after now")
	assert.Equal(t, "0", localOffset)

	assert.True(t, iter.Next())
	localOffset = strings.Split(string(iter.Key()), "-")[4]
	timestamp, _ = strconv.Atoi(strings.Split(string(iter.Key()), "-")[3])
	delayToTime = time.Unix(int64(timestamp), 0)
	assert.True(t, startTime.Before(delayToTime), "expected delay timestamp to be after now")
	assert.Equal(t, "1", localOffset)

	assert.NoError(t, closer())
}

// ReturnDelayed
func TestReturnDelayed(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	s.Insert(defaultTopic, msg1)
	s.Insert(defaultTopic, msg2)

	_, offset, _ := s.GetNext(defaultTopic)
	s.Dack(defaultTopic, offset, 1)
	_, offset, _ = s.GetNext(defaultTopic)
	s.Dack(defaultTopic, offset, 3)

	now := time.Now().Add(time.Minute)
	assert.NoError(t, s.ReturnDelayed(defaultTopic, now))
}

func TestReturnDelayed_ReturnToMainQueue(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset, _ := s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset, 1))

	_, offset, _ = s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset, 3))

	assert.NoError(t, s.ReturnDelayed(defaultTopic, time.Now().Add(time.Minute)))

	b, _, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg2, b)

	b, _, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg1, b)
}

func TestReturnDelayed_ReturnSameTimeToMainQueue(t *testing.T) {
	s := newStore(tmpDBPath).(*store)
	t.Cleanup(s.Destroy)

	msg1 := []byte("test_value_1")
	msg2 := []byte("test_value_2")

	assert.NoError(t, s.Insert(defaultTopic, msg1))
	assert.NoError(t, s.Insert(defaultTopic, msg2))

	_, offset1, _ := s.GetNext(defaultTopic)
	_, offset2, _ := s.GetNext(defaultTopic)
	assert.NoError(t, s.Dack(defaultTopic, offset1, 1))
	assert.NoError(t, s.Dack(defaultTopic, offset2, 1))

	assert.NoError(t, s.ReturnDelayed(defaultTopic, time.Now().Add(time.Minute)))

	b, _, err := s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg2, b)

	b, _, err = s.GetNext(defaultTopic)
	assert.NoError(t, err)
	assert.Equal(t, msg1, b)
}

// Close
func TestClose(t *testing.T) {
	// TODO
}
