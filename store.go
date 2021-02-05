//go:generate mockgen -source=$GOFILE -destination=store_mock.go -package=main
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// storer should be safe for concurrent use.
type storer interface {
	// Insert inserts a new record for a given topic.
	Insert(topic string, value value) error

	// GetNext will retrieve the next value in the topic, as well as the AckKey
	// allowing future acking/nacking of the value.
	GetNext(topic string) (val value, ackOffset int, err error)

	// Ack will acknowledge the processing of a value, removing it from the topic
	// entirely.
	Ack(topic string, ackOffset int) error

	// Nack will negatively acknowledge the value, on a given topic, returning it
	// to the front of the consumption queue.
	Nack(topic string, ackOffset int) error

	// Close closes the store.
	Close() error

	// Destroy removes the store from persistence. This is a destructive
	// operation.
	Destroy()
}

const (
	errTopicEmpty    = storeError("topic is empty")
	errTopicNotExist = storeError("topic does not exist")
)

type storeError string

func (s storeError) Error() string {
	return string(s)
}

const (
	topicFmt      = "%s-%d"
	headPosKeyFmt = "%s-head"
	tailPosKeyFmt = "%s-tail"

	ackTopicFmt      = "%s-ack-%d"
	ackTailPosKeyFmt = "%s-ack-head"
)

// store handles the the underlying leveldb implementation.
type store struct {
	path string
	db   *leveldb.DB
}

func newStore(dbPath string) storer {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open levelDB")
	}

	return &store{
		path: dbPath,
		db:   db,
	}
}

// Ack will acknowledge the processing of a value, removing it from the topic
// entirely.
func (s *store) Ack(topic string, ackOffset int) error {
	// Delete the used value
	key := fmt.Sprintf(ackTopicFmt, topic, ackOffset)
	if err := s.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("deleting from ack topic: %v", err)
	}

	return nil
}

// Nack will negatively acknowledge the value, on a given topic, returning it
// to the front of the consumption queue.
func (s *store) Nack(topic string, ackOffset int) error {
	return nil
}

// Insert creates a new record for a given topic, creating the topic in the
// store if it doesn't already exist. If it does, the record is placed at the
// end of the queue.
func (s *store) Insert(topic string, value value) error {
	headPosKey := []byte(fmt.Sprintf(headPosKeyFmt, topic))
	tailPosKey := []byte(fmt.Sprintf(tailPosKeyFmt, topic))
	ackTailPosKey := []byte(fmt.Sprintf(ackTailPosKeyFmt, topic))

	exists, err := s.db.Has(tailPosKey, nil)
	if err != nil {
		return fmt.Errorf("checking for has: %v", err)
	}

	// The key already exists
	if exists {
		if _, err := appendValue(s.db, tailPosKeyFmt, topicFmt, topic, value); err != nil {
			return err
		}

		return nil
	}

	// Write initial head position
	headPos := make([]byte, 8)
	binary.PutVarint(headPos, 0)

	if err := s.db.Put(headPosKey, headPos, nil); err != nil {
		return fmt.Errorf("putting head position value: %v", err)
	}

	// Write initial ack topic head position
	ackTailPos := make([]byte, 8)
	binary.PutVarint(ackTailPos, 0)

	if err := s.db.Put(ackTailPosKey, ackTailPos, nil); err != nil {
		return fmt.Errorf("putting ack head position value: %v", err)
	}

	// Write initial tail position
	tailPos := make([]byte, 8)
	binary.PutVarint(tailPos, 1)

	if err := s.db.Put(tailPosKey, tailPos, nil); err != nil {
		return fmt.Errorf("putting tail position value: %v", err)
	}

	// Write new message to head
	newKey := []byte(fmt.Sprintf(topicFmt, topic, 0))
	if err := s.db.Put(newKey, value, nil); err != nil {
		return fmt.Errorf("putting first value for topic: %v", err)
	}

	return nil
}

// GetNext retrieves the first record for a topic, incrementing the head pointer
// of the main array and pushing the value onto the ack array.
func (s *store) GetNext(topic string) (value, int, error) {
	headOffset, err := getPos(s.db, headPosKeyFmt, topic)
	if err != nil {
		return nil, 0, err
	}

	val, err := getValue(s.db, topicFmt, topic, headOffset)
	if err != nil {
		return nil, 0, err
	}

	insertedOffset, err := appendValue(s.db, ackTailPosKeyFmt, ackTopicFmt, topic, val)
	if err != nil {
		return nil, 0, err
	}

	if _, _, err := incPos(s.db, headPosKeyFmt, topic); err != nil {
		return nil, 0, err
	}

	return val, insertedOffset, nil
}

// Close the store.
func (s *store) Close() error {
	return s.db.Close()
}

// Destroy the underlying store.
func (s *store) Destroy() {
	_ = s.Close()
	_ = os.RemoveAll(s.path)
}

// getOffset retrieves a record for a topic with a specific offset.
func getOffset(db *leveldb.DB, topicFmt string, topic string, offset int) (value, error) {
	key := fmt.Sprintf(topicFmt, topic, offset)

	val, err := db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func getPos(db *leveldb.DB, keyFmt string, topic string) (int, error) {
	key := []byte(fmt.Sprintf(keyFmt, topic))

	pos, err := db.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return 0, errTopicNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("getting offset position position: %v", err)
	}

	i, err := binary.ReadVarint(bytes.NewReader(pos))
	if err != nil {
		return 0, fmt.Errorf("reading offset position varint: %v", err)
	}

	return int(i), nil
}

func getValue(db *leveldb.DB, keyFmt string, topic string, offset int) (value, error) {
	key := fmt.Sprintf(keyFmt, topic, offset)

	val, err := db.Get([]byte(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, errTopicEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("getting value with fmt [%s] from topic %s at offset %d: %v", keyFmt, topic, offset, err)
	}

	return val, nil
}

func appendValue(db *leveldb.DB, tailPosKeyFmt, keyFmt, topic string, val value) (offset int, err error) {
	tailPosKey := []byte(fmt.Sprintf(tailPosKeyFmt, topic))

	// Fetch the current tail position
	tailPosVal, err := db.Get(tailPosKey, nil)
	if err != nil {
		return 0, fmt.Errorf("getting tail position from db: %v", err)
	}

	origOffset, err := binary.ReadVarint(bytes.NewReader(tailPosVal))
	if err != nil {
		return 0, fmt.Errorf("reading tail pos varint: %v", err)
	}

	// Write new record to next tail position
	newKey := []byte(fmt.Sprintf(keyFmt, topic, origOffset))

	if err := db.Put(newKey, val, nil); err != nil {
		return 0, fmt.Errorf("putting value: %v", err)
	}

	// Update tail position
	tail := make([]byte, 8)
	binary.PutVarint(tail, origOffset+1)
	if err := db.Put(tailPosKey, tail, nil); err != nil {
		return 0, fmt.Errorf("putting new tail position: %v", err)
	}

	return int(origOffset), nil
}

func incPos(db *leveldb.DB, posKeyFmt string, topic string) (oldPosition, newPosition int, err error) {
	oldPos, err := getPos(db, posKeyFmt, topic)
	if err != nil {
		return 0, 0, err
	}

	newPos := oldPos + 1
	newPosBytes := make([]byte, 8)
	binary.PutVarint(newPosBytes, int64(newPos))

	key := []byte(fmt.Sprintf(posKeyFmt, topic))

	if err := db.Put(key, newPosBytes, nil); err != nil {
		return 0, 0, fmt.Errorf("putting new increment position: %v", err)
	}

	return oldPos, newPos, nil
}
