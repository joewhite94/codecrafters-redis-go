package main

import (
	"sync"
)

var db = map[string]dbEntry{}

type dbEntry interface {
	Lock()
	ToResp() respElement
	Type() string
	Unlock()
}

type dbBaseEntry struct {
	dbType string
	mu     sync.Mutex
	value  any
}

func (e *dbBaseEntry) Lock() {
	e.mu.Lock()
}

func (e *dbBaseEntry) Type() string {
	return e.dbType
}

func (e *dbBaseEntry) Unlock() {
	e.mu.Unlock()
}

type dbList struct {
	dbBaseEntry
	value []dbEntry
}

func (l *dbList) ToResp() respElement {
	var list []respElement = make([]respElement, len(l.value))
	for i, e := range l.value {
		list[i] = e.ToResp()
	}
	return &respArray{
		value: list,
	}
}

func NewList(value []dbEntry) *dbList {
	return &dbList{
		dbBaseEntry: dbBaseEntry{
			dbType: "list",
		},
		value: value,
	}
}

type dbStream struct {
	dbBaseEntry
	value []dbStreamEntry
}

type dbStreamEntry struct {
	id     string
	values map[string]string
}

func (s *dbStream) ToResp() respElement {
	// var stream []respElement = make([]respElement, len(s.value))
	// for i, e := range s.value {
	// 	stream[i] = e.ToResp()
	// }
	return &respArray{}
}

func NewStream(value []dbStreamEntry) *dbStream {
	return &dbStream{
		dbBaseEntry: dbBaseEntry{
			dbType: "stream",
		},
		value: value,
	}
}

type dbString struct {
	dbBaseEntry
	value string
}

func (s *dbString) ToResp() respElement {
	return &respBulkString{
		value: s.value,
	}
}

func NewString(value string) *dbString {
	return &dbString{
		dbBaseEntry: dbBaseEntry{
			dbType: "string",
		},
		value: value,
	}
}
