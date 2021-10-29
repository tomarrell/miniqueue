package main

import (
	"bytes"
	"context"
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

func TestPublishSingleMessage(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := newValue([]byte("test_value"))

	mockBroker := NewMockbrokerer(ctrl)
	mockBroker.EXPECT().Publish(defaultTopic, msg)

	rec := NewRecorder()
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/publish/%s", defaultTopic), bytes.NewReader(msg.Raw))

	srv := newHTTPServer(mockBroker)
	srv.ServeHTTP(rec, req)

	assert.Equal(http.StatusCreated, rec.Code)
}

func TestSubscribeSingleMessage(t *testing.T) {
	assert := assert.New(t)

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(err)

	b := newBroker(&store{db: db})

	// Publish to the topic
	pubW := NewRecorder()
	msg := "test_message"
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	publishHandler(b)(pubW, r)
	assert.Equal(http.StatusCreated, pubW.Code)

	// Subscribe to the same topic
	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", defaultTopic), helperMustEncodeString(CmdInit))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	go subscribeHandler(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message
	var out subResponse
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg, string(out.Msg))
}

func TestSubscribeAck(t *testing.T) {
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

	publishHandler(b)(pubW, r)
	assert.Equal(http.StatusCreated, pubW.Code)

	// Publish a second time to the topic with a different body
	msg2 := "test_message_2"
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/publish/%s", defaultTopic), strings.NewReader(msg2))
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	publishHandler(b)(pubW, r)
	assert.Equal(http.StatusCreated, pubW.Code)

	// Subscribe to the same topic
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	go func() {
		assert.NoError(encoder.Encode(CmdInit))
	}()

	subW := NewRecorder()
	r = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/subscribe/%s", defaultTopic), reader)
	r = mux.SetURLVars(r, map[string]string{"topic": defaultTopic})

	go subscribeHandler(b)(subW, r)

	// Wait for the first message to be written
	decoder := NewDecodeWaiter(subW)

	// Read the first message, expect the first item published to the topic
	var out subResponse
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg1, string(out.Msg))

	// Send an ACK back to the server, expect it to reply with next msg
	assert.NoError(encoder.Encode(CmdAck))
	assert.NoError(decoder.WaitAndDecode(&out))
	assert.Equal(msg2, string(out.Msg))
}

func TestServerPublishSubscribeAck(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish
	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	// Setup a subscriber
	encoder, decoder, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Consume message
	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))

	// Simulate the next publish coming in slightly later
	// i.e. the next record may not be available already on the topic to
	// immediately send back
	time.Sleep(100 * time.Millisecond)

	// Publish a new message to the same topic
	msg2 := "test_msg_2"
	helperPublishMessage(t, srv, defaultTopic, msg2)

	// Read again from the queue, expect the new message
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg2, string(out.Msg))

	// Send back and ACK
	assert.NoError(encoder.Encode(CmdAck))
}

func TestServerNack(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	enc, decoder, _ := helperSubscribeTopic(t, srv, defaultTopic)

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	assert.NoError(enc.Encode("NACK"))

	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))
}

func TestServerBack(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	msg2 := "test_msg_2"
	helperPublishMessage(t, srv, defaultTopic, msg2)

	enc, decoder, _ := helperSubscribeTopic(t, srv, defaultTopic)

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	assert.NoError(enc.Encode("BACK"))

	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg2, string(out.Msg))
}

func TestServerDack_MissingArg(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	enc, decoder, _ := helperSubscribeTopic(t, srv, defaultTopic)

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	assert.NoError(enc.Encode("DACK"))

	assert.NoError(decoder.Decode(&out))
	assert.Contains(out.Error, "too few arguments")
}

func TestServerDack_InvalidArg(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	enc, decoder, _ := helperSubscribeTopic(t, srv, defaultTopic)

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	assert.NoError(enc.Encode("DACK oops"))

	assert.NoError(decoder.Decode(&out))
	assert.Contains(out.Error, "invalid DACK duration argument")
}

func TestServerDack(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	srv, hooks, srvCloser := helperNewTestServer(t)
	defer srvCloser()
	go hooks.b.ProcessDelays(ctx, 100*time.Millisecond)

	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	enc, decoder, _ := helperSubscribeTopic(t, srv, defaultTopic)

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))
	assert.Equal(0, out.DackCount)

	assert.NoError(enc.Encode("DACK 1"))

	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))
	assert.Equal(1, out.DackCount)
}

func TestServerConnectionLost(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish twice
	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	msg2 := "test_msg_2"
	helperPublishMessage(t, srv, defaultTopic, msg2)

	// Setup a subscriber
	_, decoder, closeSub := helperSubscribeTopic(t, srv, defaultTopic)

	// Consume message
	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	// *Unexpectedly* close the connection
	closeSub()

	// We need to give it some time to Nack and be put back on the queue to
	// consume
	time.Sleep(100 * time.Millisecond)

	// Setup a new subscriber
	_, decoder, closeSub = helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Expect the first message to be sent again as it was not acked
	out = subResponse{}
	assert.NoError(decoder.Decode(&out))
	assert.Equal(msg1, string(out.Msg))
}

func TestServerMultiConsumer(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish
	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	msg2 := "test_msg_2"
	helperPublishMessage(t, srv, defaultTopic, msg2)

	// Set up consumer 1
	encoder1, decoder1, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Set up consumer 2
	encoder2, decoder2, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	// Read from consumer 1
	var out1 subResponse
	assert.NoError(decoder1.Decode(&out1))
	assert.Equal(msg1, string(out1.Msg))

	// Read from consumer 2
	var out2 subResponse
	assert.NoError(decoder2.Decode(&out2))
	assert.Equal(msg2, string(out2.Msg))

	// Publish again
	msg3 := "test_msg_3"
	helperPublishMessage(t, srv, defaultTopic, msg3)

	msg4 := "test_msg_4"
	helperPublishMessage(t, srv, defaultTopic, msg4)

	// Consumer 1 sends back an ACK for its message
	assert.NoError(encoder1.Encode(CmdAck))

	// It should receive the first message which has just been published
	var out3 subResponse
	assert.NoError(decoder1.Decode(&out3))
	assert.Equal(msg3, string(out3.Msg))

	// Consumer 2 sends back an ACK for its message
	assert.NoError(encoder2.Encode(CmdAck))

	// It should receive the second message that was published
	var out4 subResponse
	assert.NoError(decoder2.Decode(&out4))
	assert.Equal(msg4, string(out4.Msg))
}

func TestServerMultiConsumerConnectionLost(t *testing.T) {
	assert := assert.New(t)

	srv, hooks, srvCloser := helperNewTestServer(t)
	defer srvCloser()

	// Publish once
	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	// Setup a subscriber
	_, decoder1, closeSub1 := helperSubscribeTopic(t, srv, defaultTopic)

	// Setup a second subscriber, which will be blocked until the message is
	// Nacked due to the first subscriber disconnecting.
	done := make(chan struct{})
	go func() {
		_, decoder2, closeSub2 := helperSubscribeTopic(t, srv, defaultTopic)
		defer closeSub2()

		// Expect the first message to be sent again as it was not acked
		var out subResponse
		assert.NoError(decoder2.Decode(&out))
		assert.Equal(msg1, string(out.Msg))
		done <- struct{}{}
	}()

	// Consume message
	var out subResponse
	assert.NoError(decoder1.Decode(&out))
	assert.Equal(msg1, string(out.Msg))

	// *Unexpectedly* close the connection
	closeSub1()

	select {
	case <-time.After(time.Second):
		assert.FailNow("timed out waiting for second decode")
	case <-done:
		consumers := hooks.b.consumers[defaultTopic]
		assert.Len(consumers, 1)
	}
}

func TestServerDelete(t *testing.T) {
	assert := assert.New(t)

	srv, _, srvCloser := helperNewTestServer(t)
	t.Cleanup(srvCloser)

	// Publish twice
	msg1 := "test_msg_1"
	helperPublishMessage(t, srv, defaultTopic, msg1)

	msg2 := "test_msg_2"
	helperPublishMessage(t, srv, defaultTopic, msg2)

	// Setup a subscriber
	encoder, decoder, closeSub := helperSubscribeTopic(t, srv, defaultTopic)
	defer closeSub()

	var out subResponse
	assert.NoError(decoder.Decode(&out))
	assert.Equal(out.Msg, []byte(msg1))

	// Purge the topic
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", srv.URL, defaultTopic), nil)
	res, err := srv.Client().Do(req)
	assert.NoError(err)
	res.Body.Close()
	assert.Equal(http.StatusOK, res.StatusCode)

	assert.NoError(encoder.Encode("ACK"))

	// Publish again after the purge
	msg3 := "test_msg_3"
	helperPublishMessage(t, srv, defaultTopic, msg3)

	// Expect that it consumes the most recently published message
	out = subResponse{}
	assert.NoError(decoder.Decode(&out))
	assert.Equal("", out.Error)
	assert.Equal([]byte(msg3), out.Msg)

	time.Sleep(time.Second)
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

	srv := httptest.NewUnstartedServer(newHTTPServer(newBroker(&store{db: db})))
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

type hooks struct {
	b *broker
}

// Returns a new, started, httptest server and a corresponding function which
// will force close connections and close the server when called.
func helperNewTestServer(t *testing.T) (*httptest.Server, hooks, func()) {
	t.Helper()

	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	assert.NoError(t, err)

	b := newBroker(&store{path: "", db: db})
	srv := httptest.NewUnstartedServer(newHTTPServer(b))

	srv.EnableHTTP2 = true
	srv.StartTLS()

	return srv, hooks{b}, func() {
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
	assert.Equal(t, http.StatusCreated, res.StatusCode)

	t.Cleanup(func() {
		res.Body.Close()
	})

	return res
}

func helperMustEncodeString(str string) io.Reader {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(str); err != nil {
		panic(err)
	}

	return &buf
}
