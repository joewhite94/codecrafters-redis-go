package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

func runCmd(args []respElement) string {
	cmd, ok := args[0].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert command arg to bulk string",
		}
		return err.ToString()
	}
	switch cmd.value {
	case "BLPOP":
		return cmdBlpop(args)
	case "ECHO":
		return cmdEcho(args)
	case "GET":
		return cmdGet(args)
	case "LLEN":
		return cmdLlen(args)
	case "LPOP":
		return cmdLpop(args)
	case "LPUSH":
		return cmdLpush(args)
	case "LRANGE":
		return cmdLrange(args)
	case "PING":
		return cmdPing()
	case "RPUSH":
		return cmdRpush(args)
	case "SET":
		return cmdSet(args)
	case "TYPE":
		return cmdType(args)
	case "XADD":
		return cmdXadd(args)
	default:
		return ""
	}
}

func cmdBlpop(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert BLPOP key to string",
		}
		return err.ToString()
	}

	var deadline time.Time
	var timeoutDuration time.Duration
	if len(args) > 2 {
		countStr, ok := args[2].(*respBulkString)
		if !ok {
			err := &respError{
				value: "ERR Unable to convert BLPOP count to string",
			}
			return err.ToString()
		}
		timeFloat, err := strconv.ParseFloat(countStr.value, 64)
		if err != nil {
			err := &respError{
				value: "ERR Unable to convert BLPOP count to float",
			}
			return err.ToString()
		}
		timeoutDuration = time.Millisecond * time.Duration(timeFloat*1000)
		deadline = time.Now().Add(timeoutDuration)
	}

	entry, ok := db.Load(key.value)
	if !ok {
		entry = NewDbList([]dbEntry{})
		db.Store(key.value, entry)
	}

	entry.Lock()
	defer entry.Unlock()

	var result respElement

	for result == nil {
		if timeoutDuration > 0 && time.Now().After(deadline) {
			// TODO: remove hard coded null array when parser supports it
			return "*-1\r\n"
		}

		list, ok := entry.(*dbList)
		if !ok {
			err := &respError{
				value: fmt.Sprintf("ERR Value at key %s is not list for BLPOP", key.value),
			}
			return err.ToString()
		}
		if len(list.value) > 0 {
			result = &respArray{
				value: []respElement{
					args[1],
					list.value[0].ToResp(),
				},
			}
			list.value = list.value[1:]
			db.Store(key.value, list)
		}
	}

	return result.ToString()
}

func cmdEcho(args []respElement) string {
	return args[1].ToString()
}

func cmdGet(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert GET key to string",
		}
		return err.ToString()
	}

	var res respElement
	val, ok := db.Load(key.value)
	if ok {
		res = val.ToResp()
	} else {
		res = &respBulkString{
			value: "",
		}
	}

	return res.ToString()
}

func cmdLlen(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert LLEN key to string",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	arr, ok := val.(*dbList)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("Value at key %s is not list for LLEN", key.value),
		}
		return err.ToString()
	}

	res := &respInteger{
		value: len(arr.value),
	}

	return res.ToString()
}

func cmdLpop(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert LPOP key to string",
		}
		return err.ToString()
	}

	var count int = 1
	var err error
	if len(args) > 2 {
		countStr, ok := args[2].(*respBulkString)
		if !ok {
			err := &respError{
				value: "Unable to convert LPOP count to string",
			}
			return err.ToString()
		}
		count, err = strconv.Atoi(countStr.value)
		if err != nil {
			err := &respError{
				value: "Unable to convert LPOP count to int",
			}
			return err.ToString()
		}
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("Value at key %s is not list for LPOP", key.value),
		}
		return err.ToString()
	}

	var result respElement
	if len(list.value) == 0 {
		result = &respBulkString{
			value: "",
		}
	} else {
		if count > 1 {
			var res []respElement = make([]respElement, count)
			for i, e := range list.value[:count] {
				res[i] = e.ToResp()
			}
			result = &respArray{
				value: res,
			}
		} else {
			result = list.value[0].ToResp()
		}
		list.value = list.value[count:]
		db.Store(key.value, list)
	}

	return result.ToString()
}

func cmdLpush(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert LPUSH key to string",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("Value at key %s is not list for LPUSH", key.value),
		}
		return err.ToString()
	}

	var prepend []dbEntry
	for _, a := range args[2:] {
		e, err := a.ToDbEntry()
		if err != nil {
			err := &respError{
				value: "ERR" + err.Error(),
			}
			return err.ToString()
		}
		prepend = append(prepend, e)
	}
	slices.Reverse(prepend)

	list.value = append(prepend, list.value...)
	db.Store(key.value, list)

	res := &respInteger{
		value: len(list.value),
	}

	return res.ToString()
}

func cmdLrange(args []respElement) string {
	if len(args) < 4 {
		err := &respError{
			value: "LRANGE requires a key, start index and stop index",
		}
		return err.ToString()
	}

	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert LRANGE key to string",
		}
		return err.ToString()
	}

	startStr, ok := args[2].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert LRANGE start index to string",
		}
		return err.ToString()
	}
	start, err := strconv.Atoi(startStr.value)
	if err != nil {
		err := &respError{
			value: "Unable to convert LRANGE start index to int",
		}
		return err.ToString()
	}

	stopStr, ok := args[3].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert LRANGE stop index to string",
		}
		return err.ToString()
	}
	stop, err := strconv.Atoi(stopStr.value)
	if err != nil {
		err := &respError{
			value: "Unable to convert LRANGE stop index to int",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("Unable to convert value at %s to list", key.value),
		}
		return err.ToString()
	}

	// negative indexes - values are negative to adding them to array length works as subtraction
	if start < 0 {
		start = len(list.value) + start
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop = len(list.value) + stop
		if stop < 0 {
			stop = 0
		}
	}

	if start > len(list.value) || (start > stop) {
		res := &respArray{
			value: []respElement{},
		}
		return res.ToString()
	}

	// stop is inclusive
	if stop > len(list.value) {
		stop = len(list.value)
	} else {
		stop++
	}

	res := &respArray{
		value: make([]respElement, len(list.value[start:stop])),
	}

	for i, li := range list.value[start:stop] {
		res.value[i] = li.ToResp()
	}

	return res.ToString()
}

func cmdPing() string {
	res := &respSimpleString{
		value: "PONG",
	}
	return res.ToString()
}

func cmdRpush(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "Unable to convert RPUSH key to string",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbList([]dbEntry{})
		db.Store(key.value, val)
	}

	list, ok := val.(*dbList)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("Value at key %s is not list for RPUSH", key.value),
		}
		return err.ToString()
	}

	var toAppend []dbEntry
	for _, a := range args[2:] {
		e, err := a.ToDbEntry()
		if err != nil {
			err := &respError{
				value: "ERR" + err.Error(),
			}
			return err.ToString()
		}
		toAppend = append(toAppend, e)
	}

	list.value = append(list.value, toAppend...)
	db.Store(key.value, list)

	res := &respInteger{
		value: len(list.value),
	}

	return res.ToString()
}

func cmdSet(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert SET key to string",
		}
		return err.ToString()
	}

	e, err := args[2].ToDbEntry()
	if err != nil {
		err := &respError{
			value: "ERR" + err.Error(),
		}
		return err.ToString()
	}

	db.Store(key.value, e)

	if len(args) > 3 {
		expiryCmd, ok := args[3].(*respBulkString)
		if !ok {
			err := &respError{
				value: "ERR Unable to convert SET expiry command to string",
			}
			return err.ToString()
		}
		switch expiryCmd.value {
		case "EX":
			expiryStr, ok := args[4].(*respBulkString)
			if !ok {
				err := &respError{
					value: "ERR Unable to convert SET expiry to string",
				}
				return err.ToString()
			}
			duration, err := time.ParseDuration(expiryStr.value + "s")
			if err != nil {
				err := &respError{
					value: fmt.Sprintf("ERR Unable to parse duration: %s", err.Error()),
				}
				return err.ToString()
			}
			time.AfterFunc(duration, func() {
				db.m.Delete(key.value)
			})
		case "PX":
			expiryStr, ok := args[4].(*respBulkString)
			if !ok {
				err := &respError{
					value: "ERR Unable to convert SET expiry to string",
				}
				return err.ToString()
			}
			duration, err := time.ParseDuration(expiryStr.value + "ms")
			if err != nil {
				err := &respError{
					value: fmt.Sprintf("ERR Unable to parse duration: %s", err.Error()),
				}
				return err.ToString()
			}
			time.AfterFunc(duration, func() {
				db.m.Delete(key.value)
			})
		default:
		}
	}

	res := &respSimpleString{
		value: "OK",
	}

	return res.ToString()
}

func cmdType(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert TYPE key to string",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		res := &respSimpleString{
			value: "none",
		}
		return res.ToString()
	}

	res := &respSimpleString{
		value: val.Type(),
	}
	return res.ToString()
}

func cmdXadd(args []respElement) string {
	key, ok := args[1].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert XADD key to string",
		}
		return err.ToString()
	}

	val, ok := db.Load(key.value)
	if !ok {
		val = NewDbStream([]dbStreamEntry{})
		db.Store(key.value, val)
	}

	stream, ok := val.(*dbStream)
	if !ok {
		err := &respError{
			value: fmt.Sprintf("ERR Value at key %s is not stream for XADD", key.value),
		}
		return err.ToString()
	}

	id, ok := args[2].(*respBulkString)
	if !ok {
		err := &respError{
			value: "ERR Unable to convert XADD id to string",
		}
		return err.ToString()
	}

	var prevEntry dbStreamEntry
	var prevTimestamp, prevSequence int
	if len(stream.value) > 0 {
		prevEntry = stream.value[len(stream.value)-1]
		var err error
		prevTimestamp, prevSequence, err = prevEntry.GetTimestampAndSequence()
		if err != nil {
			err := &respError{
				value: fmt.Sprintf("ERR The ID in stream %s is an invalid format", key.value),
			}
			return err.ToString()
		}
	}

	var timestamp int
	var sequence int
	switch id.value {
	case "0-0":
		err := &respError{
			value: "ERR The ID specified in XADD must be greater than 0-0",
		}
		return err.ToString()
	case "*":
		timestamp = int(time.Now().UnixMilli())
		sequence = 0
		if timestamp == prevTimestamp {
			sequence = prevSequence + 1
		}

		id.value = fmt.Sprintf("%d-%d", timestamp, sequence)
	default:
		splitId := strings.Split(id.value, "-")

		if len(splitId) != 2 {
			err := &respError{
				value: "ERR The ID specified in XADD is an invalid format",
			}
			return err.ToString()
		}

		var err error
		timestamp, err = strconv.Atoi(splitId[0])
		if err != nil {
			err := &respError{
				value: "ERR The ID specified in XADD is an invalid format",
			}
			return err.ToString()
		}

		if splitId[1] == "*" {
			sequence = 0
			if timestamp == prevTimestamp {
				sequence = prevSequence + 1
			}
		} else {
			var err error
			sequence, err = strconv.Atoi(splitId[1])
			if err != nil {
				err := &respError{
					value: "ERR The ID specified in XADD is an invalid format",
				}
				return err.ToString()
			}
		}
		id.value = fmt.Sprintf("%d-%d", timestamp, sequence)
	}

	entry := dbStreamEntry{
		id:     id.value,
		values: map[string]string{},
	}

	if prevTimestamp > timestamp {
		err := &respError{
			value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		}
		return err.ToString()
	}

	if prevTimestamp == timestamp {
		if prevSequence >= sequence {
			err := &respError{
				value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
			}
			return err.ToString()
		}
	}

	// TODO: guard against potential out of range panic caused by supplying insufficient params
	for i := 3; i < len(args); i += 2 {
		k, ok := args[i].(*respBulkString)
		if !ok {
			err := &respError{
				value: "ERR Unable to convert XADD map key to string",
			}
			return err.ToString()
		}
		v, ok := args[i+1].(*respBulkString)
		if !ok {
			err := &respError{
				value: "ERR Unable to convert XADD map key to string",
			}
			return err.ToString()
		}
		entry.values[k.value] = v.value
	}

	stream.value = append(stream.value, entry)
	db.Store(key.value, stream)

	res := &respBulkString{
		value: entry.id,
	}

	return res.ToString()
}
