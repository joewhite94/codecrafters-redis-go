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

type dbString struct {
	dbBaseEntry
	value string
}

func (s *dbString) ToResp() respElement {
	return &respBulkString{
		value: s.value,
	}
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
