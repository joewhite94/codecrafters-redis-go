package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type redisDb struct {
	m sync.Map
}

func (d *redisDb) Load(key string) (dbEntry, bool) {
	val, ok := d.m.Load(key)
	if !ok {
		return nil, ok
	}
	entry, ok := val.(dbEntry)
	if !ok {
		return nil, ok
	}
	return entry, ok
}

func (d *redisDb) Store(key string, value dbEntry) {
	d.m.Store(key, value)
}

var db = redisDb{
	m: sync.Map{},
}

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

func NewDbList(value []dbEntry) *dbList {
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
	id     dbStreamEntryId
	values map[string]string
}

type dbStreamEntryId struct {
	value string
}

func (s *dbStream) ToResp() respElement {
	var res []respElement = make([]respElement, len(s.value))
	for i, e := range s.value {
		res[i] = e.ToResp()
	}
	return &respArray{
		value: res,
	}
}

func (e *dbStreamEntry) ToResp() respElement {
	var res []respElement = make([]respElement, 2)
	res[0] = &respBulkString{
		value: e.id.value,
	}
	arr := &respArray{
		value: make([]respElement, len(e.values)*2),
	}
	var i = 0
	for k, v := range e.values {
		key := &respBulkString{
			value: k,
		}
		value := &respBulkString{
			value: v,
		}
		arr.value[i] = key
		arr.value[i+1] = value
		i += 2
	}
	res[1] = arr
	return &respArray{
		value: res,
	}
}

func (i *dbStreamEntryId) GetTimestampAndSequence() (int, int, error) {
	splitId := strings.Split(i.value, "-")

	if len(splitId) > 2 {
		return 0, 0, fmt.Errorf("Invalid stream id %s", i.value)
	}

	timestamp, err := strconv.Atoi(splitId[0])
	if err != nil {
		return 0, 0, fmt.Errorf("Invalid stream id %s", i.value)
	}

	sequence := 0
	if len(splitId) == 2 {
		var err error
		sequence, err = strconv.Atoi(splitId[1])
		if err != nil {
			return 0, 0, fmt.Errorf("Invalid stream id %s", i.value)
		}
	}

	return timestamp, sequence, nil
}

func NewDbStream(value []dbStreamEntry) *dbStream {
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

func NewDbString(value string) *dbString {
	return &dbString{
		dbBaseEntry: dbBaseEntry{
			dbType: "string",
		},
		value: value,
	}
}
