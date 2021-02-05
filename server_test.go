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

const defaultTopic = "test_topic"

func TestPublish_SingleMessage(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := "test_value"

	mockBroker := NewMockbrokerer(ctrl)
	mockBroker.EXPECT().Publish(defaultTopic, []byte(msg))

	rec := NewRecorder()
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg))

	srv := newServer(mockBroker)
	srv.ServeHTTP(rec, req)

	assert.Equal(http.StatusOK, rec.Code)
}

func TestSubscribe_SingleMessage(t *testing.T) {
	assert := assert.New(t)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)

	b := newBroker(&store{db: db})

	// Publish to the topic
	pubW := NewRecorder()
	msg := "test_message"
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Subscribe to the same topic
	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", defaultTopic), helperMustEncodeString(CmdInit))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

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

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)

	b := newBroker(&store{db: db})

	// Publish to the topic
	msg1 := "test_message_1"
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg1))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	// Publish twice
	pubW := NewRecorder()

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Publish a second time to the topic with a different body
	msg2 := "test_message_2"
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg2))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	publish(b)(pubW, r)
	assert.Equal(http.StatusOK, pubW.Code)

	// Subscribe to the same topic
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	go func() {
		assert.NoError(encoder.Encode(CmdInit))
	}()

	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", defaultTopic), reader)
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

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

func TestServer(t *testing.T) {
	assert := assert.New(t)

	srv, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish
	msg1 := "test_msg_1"
	res := helperPublishMessage(t, srv, defaultTopic, msg1)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	// Setup a subscriber
	encoder, decoder, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Consume message
	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, out.Msg)

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))

	// Simulate the next publish coming in slightly later
	// i.e. the next record may not be available already on the topic to
	// immediately send back
	time.Sleep(100 * time.Millisecond)

	// Publish a new message to the same topic
	msg2 := "test_msg_2"
	res = helperPublishMessage(t, srv, defaultTopic, msg2)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	// Read again from the queue, expect the new message
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg2, out.Msg)

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))
}

func TestServer_MultiConsumer(t *testing.T) {
	assert := assert.New(t)

	srv, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish
	msg1 := "test_msg_1"
	res := helperPublishMessage(t, srv, defaultTopic, msg1)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	msg2 := "test_msg_2"
	res = helperPublishMessage(t, srv, defaultTopic, msg2)
	assert.Equal(http.StatusOK, res.StatusCode)
	defer res.Body.Close()

	// Set up consumer 1
	_, decoder1, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Set up consumer 2
	_, decoder2, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Read from consumer 1
	var out1 subResponse
	assert.NoError(decoder1.Decode(&out1))
	assert.Equal(msg1, out1.Msg)

	// Read from consumer 2
	var out2 subResponse
	assert.NoError(decoder2.Decode(&out2))
	assert.Equal(msg2, out2.Msg)
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

//
// Helpers
//

// Returns a new, started, httptest server and a corresponding function which
// will force close connections and close the server when called.
func helperNewTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(t, err)

	srv := httptest.NewUnstartedServer(newServer(newBroker(&store{
		path: "",
		db:   db,
	})))

	srv.EnableHTTP2 = true
	srv.StartTLS()

	return srv, func() {
		srv.CloseClientConnections()
		srv.Close()
	}
}

func helperSubscribeTopic(t *testing.T, srv *httptest.Server, topicName string) (*json.Encoder, *json.Decoder, func()) {
	t.Helper()

	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	go func() {
		assert.NoError(t, encoder.Encode(CmdInit))
	}()

	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/subscribe/%s", srv.URL, topicName),
		reader,
	)
	assert.NoError(t, err)

	// Subscribe to topic
	res, err := srv.Client().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)

	decoder := json.NewDecoder(res.Body)
	return encoder, decoder, func() {
		res.Body.Close()
	}
}

func helperPublishMessage(t *testing.T, srv *httptest.Server, topicName, msg string) *http.Response {
	t.Helper()

	publishPath := fmt.Sprintf("%s/publish/%s", srv.URL, topicName)
	req, err := http.NewRequest(http.MethodPost, publishPath, strings.NewReader(msg))
	assert.NoError(t, err)

	res, err := srv.Client().Do(req)
	assert.NoError(t, err)

	return res
}

func helperMustEncodeString(str string) io.Reader {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(str); err != nil {
		panic(err)
	}

	return &buf
}
