package main

import (
	"fmt"
	"strconv"
	"strings"
)

// RESP Parser: https://redis.io/docs/latest/develop/reference/protocol-spec

type respElement interface {
	ToDbEntry() (dbEntry, error)
	ToString() string
}

// other types which may be returned to clients
type respArray struct {
	value []respElement
}

func (a *respArray) ToDbEntry() (dbEntry, error) {
	var res = make([]dbEntry, len(a.value))
	for _, subElem := range a.value {
		subEntry, err := subElem.ToDbEntry()
		if err != nil {
			return nil, err
		}
		res = append(res, subEntry)
	}
	return &dbList{
		dbBaseEntry: dbBaseEntry{
			dbType: "list",
		},
		value: res,
	}, nil
}

func (a *respArray) ToString() string {
	// array: *<number-of-elements>\r\n<element-1>...<element-n>
	// TODO: implement null array
	length := len(a.value)
	res := fmt.Sprintf("*%v\r\n", length)
	for _, e := range a.value {
		res += e.ToString()
	}
	return res
}

type respBulkString struct {
	value string
}

func (s *respBulkString) ToDbEntry() (dbEntry, error) {
	return NewDbString(s.value), nil
}

func (s *respBulkString) ToString() string {
	// bulk string: $<length>\r\n<data>\r\n
	length := len(s.value)
	var res string
	if length == 0 {
		// null bulk string
		res = "$-1\r\n"
	} else {
		res = fmt.Sprintf("$%v\r\n%s\r\n", length, s.value)
	}
	return res
}

type respError struct {
	value string
}

func (e *respError) ToDbEntry() (dbEntry, error) {
	return NewDbString(e.value), nil
}

func (e *respError) ToString() string {
	// error: -ERROR\r\n
	return fmt.Sprintf("-%s\r\n", e.value)
}

type respInteger struct {
	value int
}

func (i *respInteger) ToDbEntry() (dbEntry, error) {
	return NewDbString(strconv.Itoa(i.value)), nil
}

func (i *respInteger) ToString() string {
	// integer: :[<+|->]<value>\r\n
	return fmt.Sprintf(":%v\r\n", i.value)
}

type respSimpleString struct {
	value string
}

func (s *respSimpleString) ToDbEntry() (dbEntry, error) {
	return NewDbString(s.value), nil
}

func (s *respSimpleString) ToString() string {
	// simple string: +STR\r\n
	return fmt.Sprintf("+%s\r\n", s.value)
}

func readRespInput(elems string) ([]string, error) {
	if string(elems[0]) != "*" {
		return nil, fmt.Errorf("Client input must be an array of bulk strings")
	}

	// array: *<number-of-elements>\r\n<element-1>...<element-n>
	countStr, _, _ := strings.Cut(elems[1:], "\r\n")
	elemCount, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("Error parsing resp input: %v", err)
	}

	args := make([]string, elemCount)

	bulkStrings := strings.Split(elems, "\r\n$")[1:]

	for i, s := range bulkStrings {
		_, bulkStr, _ := strings.Cut(s, "\r\n")
		args[i] = bulkStr
	}
	return args, nil
}
