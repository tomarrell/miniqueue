package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func TestPublish_SingleMessage(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		topic = "test_topic"
		value = "test_value"
	)

	mockBroker := NewMockbrokerer(ctrl)
	mockBroker.EXPECT().Publish(topic, []byte(value))

	rec := NewRecorder()
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/publish/%s", topic), strings.NewReader(value))

	srv := newServer(mockBroker)
	srv.ServeHTTP(rec, req)

	assert.Equal(http.StatusOK, rec.Code)
}

func TestSubscribe_SingleMessage(t *testing.T) {
	assert := assert.New(t)

	var (
		topic = "test_topic"
		msg   = "test_message"
	)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)

	b := newBroker(&store{db: db})

	// Publish to the topic
	pubW := NewRecorder()
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", topic), strings.NewReader(msg))
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Subscribe to the same topic
	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", topic), nil)
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	go subscribe(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message
	var out string
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg, out)
}

func TestSubscribe_Ack(t *testing.T) {
	assert := assert.New(t)

	var (
		topic = "test_topic"
		msg1  = "test_message_1"
		msg2  = "test_message_2"
	)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)

	b := newBroker(&store{db: db})

	// Publish to the topic
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", topic), strings.NewReader(msg1))
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	// Publish twice
	pubW := NewRecorder()

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Publish a second time to the topic with a different body
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", topic), strings.NewReader(msg2))
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Subscribe to the same topic
	buf := &safeBuffer{}
	encoder := json.NewEncoder(buf)
	encoder.Encode(MsgInit)

	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", topic), buf)
	r = mux.SetURLVars(r, map[string]string{"topic": topic})
	r.Header.Add("Expect", "100-continue")

	go subscribe(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message, expect the first item published to the queue
	var out string
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg1, out)

	// Send an ACK back to the server, expect it to reply with next msg
	assert.NoError(encoder.Encode(MsgAck))
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg2, out)
}

func TestServerIntegration(t *testing.T) {
	assert := assert.New(t)

	var (
		queueName = "test_queue"
	)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)
	b := newBroker(&store{db: db})

	srv := httptest.NewUnstartedServer(newServer(b))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	// Publish
	msg := "test_value"
	publishPath := fmt.Sprintf("%s/publish/%s", srv.URL, queueName)
	req, err := http.NewRequest(http.MethodPost, publishPath, strings.NewReader(msg))
	assert.NoError(err)

	res, err := srv.Client().Do(req)
	assert.NoError(err)
	defer res.Body.Close()

	assert.Equal(http.StatusOK, res.StatusCode)

	// Setup a subscriber
	var buf safeBuffer
	encoder := json.NewEncoder(&buf)
	encoder.Encode(MsgInit)

	subscribePath := fmt.Sprintf("%s/subscribe/%s", srv.URL, queueName)
	req, err = http.NewRequest(http.MethodPost, subscribePath, &buf)

	res, err = srv.Client().Do(req)
	assert.NoError(err)
	assert.Equal(http.StatusOK, res.StatusCode)
}
