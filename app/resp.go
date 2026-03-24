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
	return NewDbList(res), nil
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

type respNullArray struct{}

func (a *respNullArray) ToDbEntry() (dbEntry, error) {
	return nil, nil
}

func (a *respNullArray) ToString() string {
	return "*-1\r\n"
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

	var i int = len(countStr) + 3
	var j int = 0
	for j < len(args) {
		if string(elems[i]) == "$" {
			// start new bulk string
			length, bulkStr, _ := strings.Cut(elems[i+1:], "\r\n")
			stringLen, err := strconv.Atoi(length)
			if err != nil {
				return nil, fmt.Errorf("Error parsing resp input: %v", err)
			}
			bulkStr = bulkStr[:stringLen]
			args[j] = bulkStr
			j++
			// $length\r\nstring\r\n
			i += len(length) + stringLen + 5
		} else {
			i++
		}
	}

	return args, nil
}

// func readRespRepl(elems string) ([]respElement, error) {
// 	if string(elems[0]) != "+" {
// 		return nil, fmt.Errorf("Unimplemented")
// 	}

// 	val, _, _ := strings.Cut(elems[1:], "\r\n")

// 	return []respElement{
// 		&respSimpleString{
// 			value: val,
// 		},
// 	}, nil
// }
