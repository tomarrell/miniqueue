//go:generate mockgen -source=$GOFILE -destination=store_mock.go -package=main
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type metadata struct {
	topics []string
}

// storer should be safe for concurrent use.
type storer interface {
	// Insert inserts a new record for a given topic.
	Insert(topic string, val *value) error

	// GetNext will retrieve the next value in the topic, as well as the AckKey
	// allowing future acking/nacking of the value.
	GetNext(topic string) (val *value, ackOffset int, err error)

	// Ack will acknowledge the processing of a message, removing it from the
	// topic entirely.
	Ack(topic string, ackOffset int) error

	// Nack will negatively acknowledge the message on a given topic, returning it
	// to the *front* of the consumption queue.
	Nack(topic string, ackOffset int) error

	// Back will negatively acknowledge the message on a given topic, returning it
	// to the *back* of the consumption queue.
	Back(topic string, ackOffset int) error

	// Dack will negatively acknowledge the message on a given topic, placing on
	// the delay queue with a given timestamp as part of the key for later
	// retrieval.
	Dack(topic string, ackOffset int, delaySeconds int) error

	// GetDelayed returns an iterator and a closer function allowing the caller to
	// iterate over the currently waiting messages for a given topic in
	// chronological delay order (those with soonest "done" time first).
	GetDelayed(topic string) (delayedIterator, func() error)

	// ReturnDelayed returns delayed messages with done times before the given
	// time back to the main queue returning the number of messages returned or an
	// error.
	ReturnDelayed(topic string, before time.Time) (count int, err error)

	// Meta returns the metadata of the database.
	Meta() (*metadata, error)

	// Close closes the store.
	Close() error

	// Purge deletes all data associated with a topic.
	Purge(topic string) error

	// Destroy removes the store from persistence. This is a destructive
	// operation.
	Destroy()
}

const (
	errTopicEmpty      = storeError("topic is empty")
	errTopicNotExist   = storeError("topic does not exist")
	errNackMsgNotExist = storeError("msg to nack does not exist")
	errBackMsgNotExist = storeError("msg to back does not exist")
	errDackMsgNotExist = storeError("msg to dack does not exist")
)

type storeError string

func (s storeError) Error() string {
	return string(s)
}

const (
	// metaTopics is a key which contains a JSON encoded slice
	metaTopics = "m-topics"

	// The topic queue is the primary queue containing the records to be
	// processed. We need to keep track of the head and the tail offsets of the
	// queue in their respective keys in order to quickly append/pop messages from
	// the queue.
	topicFmt      = "t-%s-%d"   // topic: [topic]-[offset]
	headPosKeyFmt = "t-%s-head" // key: [topic]-head
	tailPosKeyFmt = "t-%s-tail" // key: [topic]-tail

	// The ack topic queue is an auxillary queue which allows keeping track of
	// outstanding messages which are waiting on a consumer acknowledgement
	// command. We only ever append to the end of this queue, delete records once
	// they have been ACK'ed or moved back to the primary queue for reprocessing.
	ackTopicFmt      = "t-%s-ack-%d"   // topic: [topic]-ack-[offset]
	ackTailPosKeyFmt = "t-%s-ack-tail" // key: [topic]-ack-tail

	// The delay topic contains messages in buckets with their designated return
	// time as a unix timestamp. This provides strict ordering, allowing iteration
	// over the items in prefixed byte-order.
	delayTopicPrefix = "t-%s-delay-"              // topic: [topic]-delay-
	delayTopicFmt    = delayTopicPrefix + "%d-%d" // topic: [topic]-delay-[until_unix_timestamp]-[local_index]
)

// store handles the the underlying leveldb implementation.
type store struct {
	path string
	db   *leveldb.DB
	sync.Mutex
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
	s.Lock()
	defer s.Unlock()

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
	s.Lock()
	defer s.Unlock()

	nackKey := []byte(fmt.Sprintf(ackTopicFmt, topic, ackOffset))

	tx, err := s.db.OpenTransaction()
	if err != nil {
		return fmt.Errorf("opening transaction: %v", err)
	}

	exists, err := tx.Has(nackKey, nil)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("checking has %s: %v", nackKey, err)
	}
	if !exists {
		tx.Discard()
		return errNackMsgNotExist
	}

	val, err := getOffset(tx, ackTopicFmt, topic, ackOffset)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("getting ack msg from topic %s at offset %d: %v", topic, ackOffset, err)
	}

	if _, err := prependValue(tx, topicFmt, headPosKeyFmt, topic, val); err != nil {
		tx.Discard()
		return fmt.Errorf("prepending value to topic %s: %v", topic, err)
	}

	if err := tx.Delete(nackKey, nil); err != nil {
		tx.Discard()
		return fmt.Errorf("deleting ackKey %s: %v", nackKey, err)
	}

	if err := tx.Commit(); err != nil {
		tx.Discard()
		return fmt.Errorf("committing nack transaction: %v", err)
	}

	return nil
}

// Back will negatively acknowledge the value, on a given topic, returning it
// to the back of the consumption queue.
func (s *store) Back(topic string, ackOffset int) error {
	s.Lock()
	defer s.Unlock()

	backKey := []byte(fmt.Sprintf(ackTopicFmt, topic, ackOffset))

	tx, err := s.db.OpenTransaction()
	if err != nil {
		return fmt.Errorf("opening transaction: %v", err)
	}

	exists, err := tx.Has(backKey, nil)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("checking has %s: %v", backKey, err)
	}
	if !exists {
		tx.Discard()
		return errBackMsgNotExist
	}

	val, err := getOffset(tx, ackTopicFmt, topic, ackOffset)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("getting ack msg from topic %s at offset %d: %v", topic, ackOffset, err)
	}

	if _, err := appendValue(tx, topicFmt, tailPosKeyFmt, topic, val); err != nil {
		tx.Discard()
		return fmt.Errorf("appending value to topic %s: %v", topic, err)
	}

	if err := tx.Delete(backKey, nil); err != nil {
		tx.Discard()
		return fmt.Errorf("deleting ackKey %s: %v", backKey, err)
	}

	if err := tx.Commit(); err != nil {
		tx.Discard()
		return fmt.Errorf("committing back transaction: %v", err)
	}

	return nil
}

// Insert creates a new record for a given topic, creating the topic in the
// store if it doesn't already exist. If it does, the record is placed at the
// end of the queue.
func (s *store) Insert(topic string, val *value) error {
	s.Lock()
	defer s.Unlock()

	headPosKey := []byte(fmt.Sprintf(headPosKeyFmt, topic))
	tailPosKey := []byte(fmt.Sprintf(tailPosKeyFmt, topic))
	ackTailPosKey := []byte(fmt.Sprintf(ackTailPosKeyFmt, topic))

	exists, err := s.db.Has(tailPosKey, nil)
	if err != nil {
		return fmt.Errorf("checking has %s: %v", tailPosKey, err)
	}

	// The key already exists
	if exists {
		if _, err := appendValue(s.db, topicFmt, tailPosKeyFmt, topic, val); err != nil {
			return err
		}

		return nil
	}

	// Add the topic to the list of topics
	if err := addTopicMeta(s.db, topic); err != nil {
		return fmt.Errorf("adding topic to meta: %v", err)
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

	b, err := val.Encode()
	if err != nil {
		return fmt.Errorf("encoding value: %v", err)
	}

	if err := s.db.Put(newKey, b, nil); err != nil {
		return fmt.Errorf("putting first value for topic: %v", err)
	}

	return nil
}

// GetNext retrieves the first record for a topic, incrementing the head
// position of the main array and pushing the value onto the ack array.
func (s *store) GetNext(topic string) (*value, int, error) {
	s.Lock()
	defer s.Unlock()

	headOffset, err := getPos(s.db, headPosKeyFmt, topic)
	if err != nil {
		return nil, 0, err
	}

	val, err := getValue(s.db, topicFmt, topic, headOffset)
	if err != nil {
		return nil, 0, err
	}

	insertedOffset, err := appendValue(s.db, ackTopicFmt, ackTailPosKeyFmt, topic, val)
	if err != nil {
		return nil, 0, err
	}

	if _, _, err := addPos(s.db, headPosKeyFmt, topic, 1); err != nil {
		return nil, 0, err
	}

	return val, insertedOffset, nil
}

// Dack will negatively acknowledge the message on a given topic, placing on
// the delay queue with a given timestamp as part of the key for later
// retrieval.
func (s *store) Dack(topic string, ackOffset int, delaySeconds int) error {
	s.Lock()
	defer s.Unlock()

	dackKey := []byte(fmt.Sprintf(ackTopicFmt, topic, ackOffset))

	tx, err := s.db.OpenTransaction()
	if err != nil {
		return fmt.Errorf("opening transaction: %v", err)
	}

	exists, err := tx.Has(dackKey, nil)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("checking has %s: %v", dackKey, err)
	}
	if !exists {
		tx.Discard()
		return errDackMsgNotExist
	}

	val, err := getOffset(tx, ackTopicFmt, topic, ackOffset)
	if err != nil {
		tx.Discard()
		return fmt.Errorf("getting ack msg from topic %s at offset %d: %v", topic, ackOffset, err)
	}

	val.DackCount++

	if err := insertDelay(tx, topic, val, delaySeconds); err != nil {
		tx.Discard()
		return fmt.Errorf("inserting ack msg into delay topic from topic %s at offset %d: %v", topic, ackOffset, err)
	}

	if err := tx.Delete(dackKey, nil); err != nil {
		tx.Discard()
		return fmt.Errorf("deleting ackKey %s: %v", dackKey, err)
	}

	if err := tx.Commit(); err != nil {
		tx.Discard()
		return fmt.Errorf("committing dack transaction: %v", err)
	}

	return nil
}

// Requires version of mockgen with https://github.com/golang/mock/pull/405 due
// to exposing a bug in previous versions. Unfortunately for now-
// $ go get github.com/golang/mock/mockgen@HEAD
// until a new release made.
type delayedIterator interface {
	iterator.Iterator
}

// GetDelayed returns an iterator and a closer function allowing the caller to
// iterate over the currently waiting messages for a given topic in
// chronological delay order (those with soonest "done" time first).
func (s *store) GetDelayed(topic string) (iterator delayedIterator, closer func() error) {
	s.Lock()
	defer s.Unlock()

	key := fmt.Sprintf(delayTopicPrefix, topic)
	prefix := util.BytesPrefix([]byte(key))
	iter := s.db.NewIterator(prefix, nil)

	closer = func() error {
		iter.Release()
		return iter.Error()
	}

	return iter, closer
}

// ReturnDelayed returns delayed messages with done times before the given time
// back to the main queue.
func (s *store) ReturnDelayed(topic string, before time.Time) (int, error) {
	s.Lock()
	defer s.Unlock()

	key := fmt.Sprintf(delayTopicPrefix, topic)
	prefix := util.BytesPrefix([]byte(key))
	iter := s.db.NewIterator(prefix, nil)
	defer iter.Release() // In case we return early, it is safe to call multiple times.

	tx, err := s.db.OpenTransaction()
	if err != nil {
		return 0, fmt.Errorf("opening transaction: %v", err)
	}

	count := 0

	// For each record, check if the timestamp is earlier than the given cutoff,
	// returning the record to the front of the main queue if it is.
	//
	// TODO fix returned messages being in reverse chronological order if there
	// are multiple being done at once
	for iter.Next() {
		key := iter.Key()
		delayTime, err := timeFromDelayKey(string(key))
		if err != nil {
			tx.Discard()
			return 0, err
		}

		// The message has passed the delay point so should be returned to the front
		// of the main queue to be processed again.
		if delayTime.Before(before) {
			count++
			val := iter.Value()

			v, err := decodeValue(val)
			if err != nil {
				return 0, err
			}

			if _, err := prependValue(tx, topicFmt, headPosKeyFmt, topic, v); err != nil {
				tx.Discard()
				return 0, err
			}
			if err := tx.Delete(key, nil); err != nil {
				tx.Discard()
				return 0, err
			}
		} else {
			// We've already reached a timestamp that is in the future, no need to
			// continue.
			break
		}
	}

	iter.Release()
	if err := iter.Error(); err != nil {
		tx.Discard()
		return 0, fmt.Errorf("iterating over delayed messages for topic %s: %v", topic, err)
	}

	if err := tx.Commit(); err != nil {
		tx.Discard()
		return 0, fmt.Errorf("committing nack transaction: %v", err)
	}

	return count, nil
}

func (s *store) Meta() (*metadata, error) {
	s.Lock()
	defer s.Unlock()

	topics, err := getTopicMeta(s.db)
	if err != nil {
		return nil, err
	}

	return &metadata{
		topics: topics,
	}, nil
}

// Purge deletes all data associated with a topic.
func (s *store) Purge(topic string) error {
	s.Lock()
	defer s.Unlock()

	batch := new(leveldb.Batch)

	prefix := util.BytesPrefix([]byte(fmt.Sprintf("t-%s", topic)))
	iter := s.db.NewIterator(prefix, nil)

	for iter.Next() {
		key := iter.Key()
		batch.Delete(key)
	}

	iter.Release()
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating over purge prefix: %v", err)
	}

	if err := s.db.Write(batch, nil); err != nil {
		return fmt.Errorf("writing purge batch: %v", err)
	}

	// TODO measure performance impact of immediate compaction
	// if err := s.db.CompactRange(*prefix); err != nil {
	// return fmt.Errorf("compacting purged range: %v", err)
	// }

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

// leveldber describes methods available on both a leveldb.DB and a
// leveldb.Transaction.
type leveldber interface {
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	Put(key, value []byte, wo *opt.WriteOptions) error
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
}

func insertDelay(db leveldber, topic string, val *value, delaySeconds int) error {
	delayTo := time.Now().Unix() + int64(delaySeconds)

	var (
		key string
		// localOffset to enable inserting multiple records at a given timestamp
		localOffset = 0
	)

	for {
		key = fmt.Sprintf(delayTopicFmt, topic, delayTo, localOffset)
		exists, err := db.Has([]byte(key), nil)
		if err != nil {
			return fmt.Errorf("checking has %s: %v", key, err)
		}
		// A value in this position already exists, we need to combine them.
		if !exists {
			break
		}
		localOffset++
	}

	b, err := val.Encode()
	if err != nil {
		return fmt.Errorf("encoding value: %v", err)
	}

	if err := db.Put([]byte(key), b, nil); err != nil {
		return fmt.Errorf("putting value: %v", err)
	}

	return nil
}

// getOffset retrieves a record for a topic with a specific offset.
func getOffset(db leveldber, topicFmt string, topic string, offset int) (*value, error) {
	key := fmt.Sprintf(topicFmt, topic, offset)

	val, err := db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	v, err := decodeValue(val)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// getPos gets the integer position value (aka offset) for topic and key format.
func getPos(db leveldber, topicFmt string, topic string) (int, error) {
	key := []byte(fmt.Sprintf(topicFmt, topic))

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

// getValue returns the raw value stored given a key format, topic and offset.
func getValue(db leveldber, topicFmt string, topic string, offset int) (*value, error) {
	key := fmt.Sprintf(topicFmt, topic, offset)

	val, err := db.Get([]byte(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, errTopicEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("getting value with fmt [%s] from topic %s at offset %d: %v", topicFmt, topic, offset, err)
	}

	v, err := decodeValue(val)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// appendValue returns inserts a new value to the end of a topic given,
// returning the inserted offset.
func appendValue(db leveldber, topicFmt, tailPosKeyFmt, topic string, val *value) (offset int, err error) {
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
	newKey := []byte(fmt.Sprintf(topicFmt, topic, origOffset))

	b, err := val.Encode()
	if err != nil {
		return 0, fmt.Errorf("encoding value: %v", err)
	}

	if err := db.Put(newKey, b, nil); err != nil {
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

// prependValue inserts a value to the head of a topic, decrementing the head
// position and returning the offset of the prepended value.
func prependValue(tx leveldber, topicFmt, headPosKeyFmt, topic string, val *value) (offset int, err error) {
	headPosKey := []byte(fmt.Sprintf(headPosKeyFmt, topic))

	// Fetch the current head position
	headPosVal, err := tx.Get(headPosKey, nil)
	if err != nil {
		return 0, fmt.Errorf("getting head position from db: %v", err)
	}

	headOffset, err := binary.ReadVarint(bytes.NewReader(headPosVal))
	if err != nil {
		return 0, fmt.Errorf("reading head pos varint: %v", err)
	}

	// Write new record to lower neighbouring position
	newHeadOffset := headOffset - 1
	newKey := []byte(fmt.Sprintf(topicFmt, topic, newHeadOffset))

	b, err := val.Encode()
	if err != nil {
		return 0, fmt.Errorf("encoding value: %v", err)
	}

	if err := tx.Put(newKey, b, nil); err != nil {
		return 0, fmt.Errorf("putting value: %v", err)
	}

	// Update head position
	_, newPosition, err := addPos(tx, headPosKeyFmt, topic, -1)
	if err != nil {
		return 0, fmt.Errorf("decrementing head pos by 1: %v", err)
	}

	return int(newPosition), nil
}

// addPos adds the an integer to a given position pointer.
func addPos(db leveldber, posKeyFmt string, topic string, sum int) (oldPosition, newPosition int, err error) {
	oldPos, err := getPos(db, posKeyFmt, topic)
	if err != nil {
		return 0, 0, err
	}

	newPos := oldPos + sum
	newPosBytes := make([]byte, 8)
	binary.PutVarint(newPosBytes, int64(newPos))

	key := []byte(fmt.Sprintf(posKeyFmt, topic))

	if err := db.Put(key, newPosBytes, nil); err != nil {
		return 0, 0, fmt.Errorf("putting new increment position: %v", err)
	}

	return oldPos, newPos, nil
}

// timeFromDelayKey returns the "done" timestamp from the key of a message in a
// delay queue.
func timeFromDelayKey(key string) (time.Time, error) {
	splitKey := strings.Split(key, "-")
	if len(splitKey) < 3 {
		return time.Time{}, fmt.Errorf("invalid delay key format: %s", key)
	}

	timestampInt, err := strconv.Atoi(splitKey[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("converting timestamp to int: %v", err)
	}

	timestampTime := time.Unix(int64(timestampInt), 0)

	return timestampTime, nil
}

func getTopicMeta(db leveldber) ([]string, error) {
	var topics []string

	key := []byte(metaTopics)
	exists, err := db.Has(key, nil)
	if err != nil {
		return nil, fmt.Errorf("checking has %s: %v", key, err)
	}
	if !exists {
		return []string{}, nil
	}

	val, err := db.Get(key, nil)
	if err != nil {
		return nil, fmt.Errorf("getting topics meta: %v", err)
	}

	if err := json.Unmarshal(val, &topics); err != nil {
		return nil, fmt.Errorf("unmarshalling topics meta: %v", err)
	}

	return topics, nil
}

func addTopicMeta(db leveldber, topic string) error {
	key := []byte(metaTopics)
	topics := []string{topic}

	exists, err := db.Has(key, nil)
	if err != nil {
		return fmt.Errorf("checking has %s: %v", key, err)
	}
	if exists {
		val, err := db.Get(key, nil)
		if err != nil {
			return fmt.Errorf("getting key %s: %v", key, err)
		}

		var existingTopics []string
		if err := json.Unmarshal(val, &topics); err != nil {
			return fmt.Errorf("unmarshalling topics meta: %v", err)
		}

		topics = append(existingTopics, topic)
	}

	val, err := json.Marshal(topics)
	if err != nil {
		return fmt.Errorf("marshalling topics meta: %v", err)
	}

	if err := db.Put(key, val, nil); err != nil {
		return fmt.Errorf("putting topics meta %s: %v", key, err)
	}

	return nil
}
