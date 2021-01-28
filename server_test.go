package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
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
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", topic), mustEncodeString(CmdInit))
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	go subscribe(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message
	var out subResponse
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg, out.Msg)
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
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	go func() {
		encoder.Encode(CmdInit)
	}()

	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", topic), reader)
	r = mux.SetURLVars(r, map[string]string{"topic": topic})

	go subscribe(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message, expect the first item published to the topic
	var out subResponse
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg1, out.Msg)

	// Send an ACK back to the server, expect it to reply with next msg
	assert.NoError(encoder.Encode(CmdAck))
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg2, out.Msg)
}

func TestServerIntegration(t *testing.T) {
	assert := assert.New(t)

	var (
		topicName = "test_topic"
	)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)
	b := newBroker(&store{db: db})

	srv := httptest.NewUnstartedServer(newServer(b))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	// Publish
	msg1 := "test_msg_1"
	res := publishMsg(t, srv, topicName, msg1)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	// Setup a subscriber
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	go func() {
		encoder.Encode(CmdInit)
	}()

	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/subscribe/%s", srv.URL, topicName),
		reader,
	)
	assert.NoError(err)

	// Subscribe to topic
	res, err = srv.Client().Do(req)
	assert.NoError(err)
	assert.Equal(http.StatusOK, res.StatusCode)

	decoder := json.NewDecoder(res.Body)

	// Consume message
	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, out.Msg)

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))

	// Simulate the next publish coming in slightly later
	// i.e. the next record may not be available already on the topic
	time.Sleep(100 * time.Millisecond)

	// Publish a new message to the same topic
	msg2 := "test_msg_2"
	res = publishMsg(t, srv, topicName, msg2)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	// Read again from the queue, expect the new message
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg2, out.Msg)

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))
}

// Benchmarking

func BenchmarkPublish(b *testing.B) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	const (
		topic = "test_topic"
		msg   = "test_value"
	)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(b, err)

	srv := httptest.NewUnstartedServer(newServer(newBroker(&store{db: db})))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	var (
		publishPath = fmt.Sprintf("%s/publish/%s", srv.URL, topic)
	)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		req, _ := http.NewRequest(http.MethodPost, publishPath, strings.NewReader(msg))
		_, err := srv.Client().Do(req)
		assert.NoError(b, err)
	}
}

func publishMsg(t *testing.T, srv *httptest.Server, topicName, msg string) *http.Response {
	publishPath := fmt.Sprintf("%s/publish/%s", srv.URL, topicName)
	req, err := http.NewRequest(http.MethodPost, publishPath, strings.NewReader(msg))
	assert.NoError(t, err)

	res, err := srv.Client().Do(req)
	assert.NoError(t, err)

	return res
}

func mustEncodeString(str string) io.Reader {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(str); err != nil {
		panic(err)
	}

	return &buf
}
