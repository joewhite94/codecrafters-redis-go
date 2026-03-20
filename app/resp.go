package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// RESP Parser: https://redis.io/docs/latest/develop/reference/protocol-spec

type respElement interface {
	ToDbEntry() (dbEntry, error)
	ToString() string
}

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

func readResp(elems string, index int) (respElement, int, error) {
	respType := string(elems[index])
	elem := strings.TrimPrefix(elems[index:], respType)
	index += 1

	switch respType {
	case "+":
		// simple string: +STR\r\n
		simpleStr, _, _ := strings.Cut(elem, "\r\n")
		index += len(simpleStr) + 2
		return &respSimpleString{
			value: simpleStr,
		}, index, nil
	case ":":
		// integer: :[<+|->]<value>\r\n
		intStr, _, _ := strings.Cut(elem, "\r\n")
		index += len(intStr) + 2
		value, err := strconv.Atoi(intStr)
		if err != nil {
			return nil, 0, err
		}
		return &respInteger{
			value: value,
		}, index, nil
	case "$":
		// bulk string: $<length>\r\n<data>\r\n
		lenStr, _, _ := strings.Cut(elem, "\r\n")
		stringStart := len(lenStr) + 2
		length, err := strconv.Atoi(lenStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing resp input: %v", err)
			break
		}
		if length == -1 {
			// null bulk string
			index += stringStart + 2
			return &respBulkString{
				value: "",
			}, index, nil
		} else {
			bulkStr := elem[stringStart : stringStart+length]
			index += stringStart + len(bulkStr) + 2
			return &respBulkString{
				value: bulkStr,
			}, index, nil
		}
	case "*":
		// array: *<number-of-elements>\r\n<element-1>...<element-n>
		// TODO: implement null array
		firstElem, _, _ := strings.Cut(elem, "\r\n")
		elemCount, err := strconv.Atoi(firstElem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing resp input: %v", err)
			break
		}
		index += len(firstElem) + 2

		array := make([]respElement, elemCount)
		var valuesAdded int
		for valuesAdded < len(array) {
			var subElem respElement
			subElem, index, err = readResp(elems, index)
			if err != nil {
				break
			}
			array[valuesAdded] = subElem
			valuesAdded++
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing resp input: %v", err)
			break
		}
		return &respArray{
			value: array,
		}, index, nil
	case "_":
		// null
		return nil, 0, fmt.Errorf("Unimplemented")
	case "#":
		// boolean
		return nil, 0, fmt.Errorf("Unimplemented")
	case ",":
		// double
		return nil, 0, fmt.Errorf("Unimplemented")
	case "(":
		// bignum
		return nil, 0, fmt.Errorf("Unimplemented")
	case "!":
		// bulk error
		return nil, 0, fmt.Errorf("Unimplemented")
	case "=":
		// verbatim string
		return nil, 0, fmt.Errorf("Unimplemented")
	case "%":
		// map
		return nil, 0, fmt.Errorf("Unimplemented")
	case "|":
		// attribute
		return nil, 0, fmt.Errorf("Unimplemented")
	case "~":
		// set
		return nil, 0, fmt.Errorf("Unimplemented")
	case ">":
		// push
		return nil, 0, fmt.Errorf("Unimplemented")
	case "-":
		// error
		return nil, 0, fmt.Errorf("Received error in input")
	default:
		// unknown
	}
	return nil, 0, fmt.Errorf("Unknown RESP type %s", respType)
}
