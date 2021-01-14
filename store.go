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

const (
	ErrTopicEmpty    = storeError("topic is empty")
	ErrTopicNotExist = storeError("topic does not exist")
)

type storeError string

func (s storeError) Error() string {
	return string(s)
}

type store struct {
	path string
	db   *leveldb.DB
}

func newStore(dbPath string) *store {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open levelDB")
	}

	return &store{
		path: dbPath,
		db:   db,
	}
}

// Insert creates a new record for a given topic, creating the topic in the
// store if it doesn't already exist. If it does, the record is placed at the
// end of the queue.
func (s *store) Insert(topic string, value value) error {
	headKey := []byte(fmt.Sprintf("%s-head", topic))
	tailKey := []byte(fmt.Sprintf("%s-tail", topic))
	exists, err := s.db.Has(tailKey, nil)
	if err != nil {
		return err
	}

	// The key already exists
	if exists {
		// Fetch the current tail position
		val, err := s.db.Get(tailKey, nil)
		if err != nil {
			return err
		}

		i, err := binary.ReadVarint(bytes.NewReader(val))
		if err != nil {
			return err
		}

		// Write new record to next tail position
		newKey := []byte(fmt.Sprintf("%s-%d", topic, i))
		if err := s.db.Put(newKey, value, nil); err != nil {
			return err
		}

		// Update tail position
		tail := make([]byte, 8)
		binary.PutVarint(tail, i+1)
		if err := s.db.Put(tailKey, tail, nil); err != nil {
			return err
		}
	} else {
		// Write initial head position
		head := make([]byte, 8)
		binary.PutVarint(head, 0)

		if err := s.db.Put(headKey, head, nil); err != nil {
			return err
		}

		// Write initial tail position
		tail := make([]byte, 8)
		binary.PutVarint(tail, 1)

		if err := s.db.Put(tailKey, tail, nil); err != nil {
			return err
		}

		// Write new message to head
		newKey := []byte(fmt.Sprintf("%s-%d", topic, 0))
		if err := s.db.Put(newKey, value, nil); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves the first record for a topic.
func (s *store) GetNext(topic string) (value, error) {
	headKey := []byte(fmt.Sprintf("%s-head", topic))

	// Fetch the current head position
	head, err := s.db.Get(headKey, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, ErrTopicNotExist
	}
	if err != nil {
		return nil, err
	}

	i, err := binary.ReadVarint(bytes.NewReader(head))
	if err != nil {
		return nil, err
	}

	return s.GetOffset(topic, i)
}

// GetOffset retrieves a record for a topic with a specific offset.
func (s *store) GetOffset(topic string, offset int64) (value, error) {
	key := fmt.Sprintf("%s-%d", topic, offset)

	log.Debug().Str("key", key).Msg("fetching key")

	val, err := s.db.Get([]byte(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return val, ErrTopicEmpty
	}
	if err != nil {
		return nil, err
	}

	return val, nil
}

// TODO
func (s *store) Ack(topic string) error {
	return nil
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
