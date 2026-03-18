package main

import (
	"fmt"
	"slices"
	"strconv"
	"time"
)

func runCmd(args []respElement) (string, error) {
	cmd, ok := args[0].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert command arg to bulk string")
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
		return "", nil
	}
}

func cmdBlpop(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert BLPOP key to string")
	}

	var deadline time.Time
	var timeoutDuration time.Duration
	if len(args) > 2 {
		countStr, ok := args[2].(*respBulkString)
		if !ok {
			return "", fmt.Errorf("Unable to convert BLPOP count to string")
		}
		timeFloat, err := strconv.ParseFloat(countStr.value, 64)
		if err != nil {
			return "", err
		}
		timeoutDuration = time.Millisecond * time.Duration(timeFloat*1000)
		deadline = time.Now().Add(timeoutDuration)
	}

	entry, ok := db[key.value]
	if !ok {
		entry = NewDbList([]dbEntry{})
		db[key.value] = entry
	}

	entry.Lock()
	defer entry.Unlock()

	var result respElement

	for result == nil {
		if timeoutDuration > 0 && time.Now().After(deadline) {
			// TODO: remove hard coded null array when parser supports it
			return "*-1\r\n", nil
		}

		list, ok := entry.(*dbList)
		if !ok {
			return "", fmt.Errorf("Value at key %s is not list for BLPOP", key)
		}
		if len(list.value) > 0 {
			result = &respArray{
				value: []respElement{
					args[1],
					list.value[0].ToResp(),
				},
			}
			list.value = list.value[1:]
			db[key.value] = list
		}
	}

	return result.ToString(), nil
}

func cmdEcho(args []respElement) (string, error) {
	return args[1].ToString(), nil
}

func cmdGet(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert GET key to string")
	}

	var res respElement
	val, ok := db[key.value]
	if ok {
		res = val.ToResp()
	} else {
		res = &respBulkString{
			value: "",
		}
	}

	return res.ToString(), nil
}

func cmdLlen(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LLEN key to string")
	}

	val, ok := db[key.value]
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	arr, ok := val.(*dbList)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not list for LLEN", key)
	}

	res := &respInteger{
		value: len(arr.value),
	}

	return res.ToString(), nil
}

func cmdLpop(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LPOP key to string")
	}

	var count int = 1
	var err error
	if len(args) > 2 {
		countStr, ok := args[2].(*respBulkString)
		if !ok {
			return "", fmt.Errorf("Unable to convert LPOP count to string")
		}
		count, err = strconv.Atoi(countStr.value)
		if err != nil {
			return "", err
		}
	}

	val, ok := db[key.value]
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not list for LPOP", key)
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
		db[key.value] = list
	}

	return result.ToString(), nil
}

func cmdLpush(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LPUSH key to string")
	}

	val, ok := db[key.value]
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not list for LPUSH", key)
	}

	var prepend []dbEntry
	for _, a := range args[2:] {
		e, err := a.ToDbEntry()
		if err != nil {
			return "", err
		}
		prepend = append(prepend, e)
	}
	slices.Reverse(prepend)

	list.value = append(prepend, list.value...)
	db[key.value] = list

	res := &respInteger{
		value: len(list.value),
	}

	return res.ToString(), nil
}

func cmdLrange(args []respElement) (string, error) {
	if len(args) < 4 {
		return "", fmt.Errorf("LRANGE requires a key, start index and stop index")
	}

	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE key to string")
	}

	startStr, ok := args[2].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE start index to string")
	}
	start, err := strconv.Atoi(startStr.value)
	if err != nil {
		return "", err
	}

	stopStr, ok := args[3].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE stop index to string")
	}
	stop, err := strconv.Atoi(stopStr.value)
	if err != nil {
		return "", err
	}

	val, ok := db[key.value]
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		return "", fmt.Errorf("Unable to convert value at %s to list", key)
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
		return res.ToString(), nil
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

	return res.ToString(), nil
}

func cmdPing() (string, error) {
	res := &respSimpleString{
		value: "PONG",
	}
	return res.ToString(), nil
}

func cmdRpush(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert RPUSH key to string")
	}

	val, ok := db[key.value]
	if !ok {
		db[key.value] = NewDbList([]dbEntry{})
		val = db[key.value]
	}

	list, ok := val.(*dbList)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not list for RPUSH", key.value)
	}

	var toAppend []dbEntry
	for _, a := range args[2:] {
		e, err := a.ToDbEntry()
		if err != nil {
			return "", err
		}
		toAppend = append(toAppend, e)
	}

	list.value = append(list.value, toAppend...)
	db[key.value] = list

	res := &respInteger{
		value: len(list.value),
	}

	return res.ToString(), nil
}

func cmdSet(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert SET key to string")
	}

	e, err := args[2].ToDbEntry()
	if err != nil {
		return "", err
	}

	db[key.value] = e

	if len(args) > 3 {
		expiryCmd, ok := args[3].(*respBulkString)
		if !ok {
			return "", fmt.Errorf("Unable to convert SET expiry command to string")
		}
		switch expiryCmd.value {
		case "EX":
			expiryStr, ok := args[4].(*respBulkString)
			if !ok {
				return "", fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr.value + "s")
			if err != nil {
				return "", fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key.value)
			})
		case "PX":
			expiryStr, ok := args[4].(*respBulkString)
			if !ok {
				return "", fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr.value + "ms")
			if err != nil {
				return "", fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key.value)
			})
		default:
		}
	}

	res := &respSimpleString{
		value: "OK",
	}

	return res.ToString(), nil
}

func cmdType(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert TYPE key to string")
	}

	val, ok := db[key.value]
	if !ok {
		res := &respSimpleString{
			value: "none",
		}
		return res.ToString(), nil
	}

	res := &respSimpleString{
		value: val.Type(),
	}
	return res.ToString(), nil
}

func cmdXadd(args []respElement) (string, error) {
	key, ok := args[1].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert XADD key to string")
	}

	val, ok := db[key.value]
	if !ok {
		db[key.value] = NewDbStream([]dbStreamEntry{})
		val = db[key.value]
	}

	stream, ok := val.(*dbStream)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not stream for XADD", key.value)
	}

	id, ok := args[2].(*respBulkString)
	if !ok {
		return "", fmt.Errorf("Unable to convert XADD id to string")
	}

	if id.value == "0-0" {
		err := &respError{
			value: "ERR The ID specified in XADD must be greater than 0-0",
		}
		return err.ToString(), nil
	}

	entry := dbStreamEntry{
		id:     id.value,
		values: map[string]string{},
	}

	timestamp, sequence, err := entry.GetTimestampAndSequence()
	if err != nil {
		return "", err
	}

	if len(stream.value) > 0 {
		prevEntry := stream.value[len(stream.value)-1]
		prevTimestamp, prevSequence, err := prevEntry.GetTimestampAndSequence()
		if err != nil {
			return "", err
		}

		if prevTimestamp > timestamp {
			err := &respError{
				value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
			}
			return err.ToString(), nil
		}

		if prevTimestamp == timestamp {
			if prevSequence >= sequence {
				err := &respError{
					value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
				}
				return err.ToString(), nil
			}
		}
	}

	// TODO: guard against potential out of range panic caused by supplying insufficient params
	for i := 3; i < len(args); i += 2 {
		k, ok := args[i].(*respBulkString)
		if !ok {
			return "", fmt.Errorf("Unable to convert XADD map key to string")
		}
		v, ok := args[i+1].(*respBulkString)
		if !ok {
			return "", fmt.Errorf("Unable to convert XADD map key to string")
		}
		entry.values[k.value] = v.value
	}

	stream.value = append(stream.value, entry)
	db[key.value] = stream

	res := &respBulkString{
		value: entry.id,
	}

	return res.ToString(), nil
}
