package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// RESP Parser: https://redis.io/docs/latest/develop/reference/protocol-spec

type respElement struct {
	respType string
	mu       sync.Mutex
	value    any
}

func writeResp(elem *respElement) (string, error) {
	var err error
	var res string

	switch elem.respType {
	case "+":
		// simple string: +STR\r\n
		value, ok := elem.value.(string)
		if !ok {
			err = fmt.Errorf("Error encoding string %v to resp", elem.value)
			break
		}
		res = fmt.Sprintf("+%s\r\n", value)
	case ":":
		// integer: :[<+|->]<value>\r\n
		value, ok := elem.value.(int)
		if !ok {
			err = fmt.Errorf("Error encoding int %v to resp", elem.value)
		}
		res = fmt.Sprintf(":%v\r\n", value)
	case "$":
		// bulk string: $<length>\r\n<data>\r\n
		value, ok := elem.value.(string)
		if !ok {
			err = fmt.Errorf("Error encoding string %v to resp", elem.value)
			break
		}
		length := len(value)
		if length == 0 {
			// null bulk string
			res = "$-1\r\n"
		} else {
			res = fmt.Sprintf("$%v\r\n%s\r\n", length, value)
		}
	case "*":
		// array: *<number-of-elements>\r\n<element-1>...<element-n>
		value, ok := elem.value.([]*respElement)
		if !ok {
			err = fmt.Errorf("Error encoding array %v to resp", elem.value)
			break
		}
		length := len(value)
		res = fmt.Sprintf("*%v\r\n", length)
		for _, elem := range value {
			var elemRes string
			elemRes, err = writeResp(elem)
			if err != nil {
				break
			}
			res += elemRes
		}
	case "_":
		// null
		err = fmt.Errorf("Unimplemented")
	case "#":
		// boolean
		err = fmt.Errorf("Unimplemented")
	case ",":
		// double
		err = fmt.Errorf("Unimplemented")
	case "(":
		// bignum
		err = fmt.Errorf("Unimplemented")
	case "!":
		// bulk error
		err = fmt.Errorf("Unimplemented")
	case "=":
		// verbatim string
		err = fmt.Errorf("Unimplemented")
	case "%":
		// map
		err = fmt.Errorf("Unimplemented")
	case "|":
		// attribute
		err = fmt.Errorf("Unimplemented")
	case "~":
		// set
		err = fmt.Errorf("Unimplemented")
	case ">":
		// push
		err = fmt.Errorf("Unimplemented")
	case "-":
		// error
		err = fmt.Errorf("Received error in input")
	default:
		// unknown
		err = fmt.Errorf("Unknown RESP type %s", elem.respType)
	}

	return res, err
}

func readResp(elems string, index int) (*respElement, int, error) {
	respType := string(elems[index])
	elem := strings.TrimPrefix(elems[index:], respType)
	index += 1
	var value any
	var err error

	switch respType {
	case "+":
		// simple string: +STR\r\n
		simpleStr, _, _ := strings.Cut(elem, "\r\n")
		index += len(simpleStr) + 2
		value = simpleStr
	case ":":
		// integer: :[<+|->]<value>\r\n
		intStr, _, _ := strings.Cut(elem, "\r\n")
		index += len(intStr) + 2
		value, err = strconv.Atoi(intStr)
	case "$":
		// bulk string: $<length>\r\n<data>\r\n
		lenStr, _, _ := strings.Cut(elem, "\r\n")
		stringStart := len(lenStr) + 2
		var length int
		length, err = strconv.Atoi(lenStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing resp input: %v", err)
			break
		}
		if length == -1 {
			// null bulk string
			index += stringStart + 2
			value = ""
		} else {
			bulkStr := elem[stringStart : stringStart+length]
			index += stringStart + len(bulkStr) + 2
			value = bulkStr
		}
	case "*":
		// array: *<number-of-elements>\r\n<element-1>...<element-n>
		var elemCount int
		firstElem, _, _ := strings.Cut(elem, "\r\n")
		elemCount, err = strconv.Atoi(firstElem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing resp input: %v", err)
			break
		}
		index += len(firstElem) + 2

		array := make([]*respElement, elemCount)
		var valuesAdded int
		for valuesAdded < len(array) {
			var subElem *respElement
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
		value = array
	case "_":
		// null
		err = fmt.Errorf("Unimplemented")
	case "#":
		// boolean
		err = fmt.Errorf("Unimplemented")
	case ",":
		// double
		err = fmt.Errorf("Unimplemented")
	case "(":
		// bignum
		err = fmt.Errorf("Unimplemented")
	case "!":
		// bulk error
		err = fmt.Errorf("Unimplemented")
	case "=":
		// verbatim string
		err = fmt.Errorf("Unimplemented")
	case "%":
		// map
		err = fmt.Errorf("Unimplemented")
	case "|":
		// attribute
		err = fmt.Errorf("Unimplemented")
	case "~":
		// set
		err = fmt.Errorf("Unimplemented")
	case ">":
		// push
		err = fmt.Errorf("Unimplemented")
	case "-":
		// error
		err = fmt.Errorf("Received error in input")
	default:
		// unknown
		err = fmt.Errorf("Unknown RESP type %s", respType)
	}

	if err != nil {
		return &respElement{}, 0, err
	}
	return &respElement{
		respType: respType,
		value:    value,
	}, index, nil
}
