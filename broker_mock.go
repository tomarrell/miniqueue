// Code generated by MockGen. DO NOT EDIT.
// Source: broker.go

// Package main is a generated GoMock package.
package main

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// Mockbrokerer is a mock of brokerer interface.
type Mockbrokerer struct {
	ctrl     *gomock.Controller
	recorder *MockbrokererMockRecorder
}

// MockbrokererMockRecorder is the mock recorder for Mockbrokerer.
type MockbrokererMockRecorder struct {
	mock *Mockbrokerer
}

// NewMockbrokerer creates a new mock instance.
func NewMockbrokerer(ctrl *gomock.Controller) *Mockbrokerer {
	mock := &Mockbrokerer{ctrl: ctrl}
	mock.recorder = &MockbrokererMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockbrokerer) EXPECT() *MockbrokererMockRecorder {
	return m.recorder
}

// Publish mocks base method.
func (m *Mockbrokerer) Publish(topic string, value *value) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", topic, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockbrokererMockRecorder) Publish(topic, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*Mockbrokerer)(nil).Publish), topic, value)
}

// Purge mocks base method.
func (m *Mockbrokerer) Purge(topic string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Purge", topic)
	ret0, _ := ret[0].(error)
	return ret0
}

// Purge indicates an expected call of Purge.
func (mr *MockbrokererMockRecorder) Purge(topic interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Purge", reflect.TypeOf((*Mockbrokerer)(nil).Purge), topic)
}

// Subscribe mocks base method.
func (m *Mockbrokerer) Subscribe(topic string) *consumer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", topic)
	ret0, _ := ret[0].(*consumer)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockbrokererMockRecorder) Subscribe(topic interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*Mockbrokerer)(nil).Subscribe), topic)
}

// Topics mocks base method.
func (m *Mockbrokerer) Topics() ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Topics")
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Topics indicates an expected call of Topics.
func (mr *MockbrokererMockRecorder) Topics() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Topics", reflect.TypeOf((*Mockbrokerer)(nil).Topics))
}

// Unsubscribe mocks base method.
func (m *Mockbrokerer) Unsubscribe(topic, id string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unsubscribe", topic, id)
	ret0, _ := ret[0].(error)
	return ret0
}

// Unsubscribe indicates an expected call of Unsubscribe.
func (mr *MockbrokererMockRecorder) Unsubscribe(topic, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unsubscribe", reflect.TypeOf((*Mockbrokerer)(nil).Unsubscribe), topic, id)
}
