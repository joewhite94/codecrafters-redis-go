package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

var transactionalCmds = []string{"DISCARD", "EXEC"}

func cmd(rc *redisConn, args []string) string {
	if rc.multi && !slices.Contains(transactionalCmds, args[0]) {
		queueCmd(rc, args)
		res := &respSimpleString{
			value: "QUEUED",
		}
		return res.ToString()
	} else {
		return runCmd(rc, args).ToString()
	}
}

func queueCmd(rc *redisConn, args []string) {
	rc.queue = append(rc.queue, args)
}

func runCmd(rc *redisConn, args []string) respElement {
	cmd := args[0]
	switch cmd {
	case "BLPOP":
		return cmdBlpop(args)
	case "DISCARD":
		return cmdDiscard(rc)
	case "ECHO":
		return cmdEcho(args)
	case "EXEC":
		return cmdExec(rc)
	case "GET":
		return cmdGet(args)
	case "INCR":
		return cmdIncr(args)
	case "LLEN":
		return cmdLlen(args)
	case "LPOP":
		return cmdLpop(args)
	case "LPUSH":
		return cmdLpush(args)
	case "LRANGE":
		return cmdLrange(args)
	case "MULTI":
		return cmdMulti(rc)
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
	case "XRANGE":
		return cmdXrange(args)
	case "XREAD":
		return cmdXread(args)
	default:
		return &respError{
			value: "ERR Unknown command",
		}
	}
}

func cmdBlpop(args []string) respElement {
	key := args[1]

	var deadline time.Time
	var timeoutDuration time.Duration
	if len(args) > 2 {
		timeout := args[2]
		timeFloat, err := strconv.ParseFloat(timeout, 64)
		if err != nil {
			res := &respError{
				value: "ERR Unable to convert BLPOP timeout to float",
			}
			return res
		}
		timeoutDuration = time.Millisecond * time.Duration(timeFloat*1000)
		deadline = time.Now().Add(timeoutDuration)
	}

	entry, ok := db.Load(key)
	if !ok {
		entry = NewDbList([]dbEntry{})
		db.Store(key, entry)
	}

	entry.Lock()
	defer entry.Unlock()

	var res respElement

	for res == nil {
		if timeoutDuration > 0 && time.Now().After(deadline) {
			// TODO: remove hard coded null array when parser supports it
			return &respNullArray{}
		}

		list, ok := entry.(*dbList)
		if !ok {
			res = &respError{
				value: fmt.Sprintf("ERR Value at key %s is not list for BLPOP", key),
			}
		}
		if len(list.value) > 0 {
			res = &respArray{
				value: []respElement{
					&respBulkString{
						value: args[1],
					},
					list.value[0].ToResp(),
				},
			}
			list.value = list.value[1:]
			db.Store(key, list)
		}
	}

	return res
}

func cmdEcho(args []string) respElement {
	res := &respBulkString{
		value: args[1],
	}
	return res
}

func cmdDiscard(rc *redisConn) respElement {
	if !rc.multi {
		res := &respError{
			value: "ERR DISCARD without MULTI",
		}
		return res
	}
	rc.multi = false
	rc.queue = [][]string{}
	res := &respSimpleString{
		value: "OK",
	}
	return res
}

func cmdExec(rc *redisConn) respElement {
	if !rc.multi {
		res := &respError{
			value: "ERR EXEC without MULTI",
		}
		return res
	}
	rc.multi = false
	res := &respArray{}
	for _, c := range rc.queue {
		r := runCmd(rc, c)
		res.value = append(res.value, r)
	}
	return res
}

func cmdGet(args []string) respElement {
	key := args[1]

	var res respElement
	val, ok := db.Load(key)
	if ok {
		res = val.ToResp()
	} else {
		res = &respBulkString{
			value: "",
		}
	}

	return res
}

func cmdIncr(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		val = NewDbString("0")
	}

	str, ok := val.(*dbString)
	if !ok {
		res := &respError{
			value: "ERR value is not an integer or out of range",
		}
		return res
	}

	i, err := strconv.Atoi(str.value)
	if err != nil {
		res := &respError{
			value: "ERR value is not an integer or out of range",
		}
		return res
	}

	i++
	db.Store(key, NewDbString(strconv.Itoa(i)))

	res := &respInteger{
		value: i,
	}

	return res
}

func cmdLlen(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	arr, ok := val.(*dbList)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("ERR Value at key %s is not list for LLEN", key),
		}
		return res
	}

	res := &respInteger{
		value: len(arr.value),
	}

	return res
}

func cmdLpop(args []string) respElement {
	key := args[1]

	var count int = 1
	var err error
	if len(args) > 2 {
		countStr := args[2]
		count, err = strconv.Atoi(countStr)
		if err != nil {
			res := &respError{
				value: "Unable to convert LPOP count to int",
			}
			return res
		}
	}

	val, ok := db.Load(key)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("Value at key %s is not list for LPOP", key),
		}
		return res
	}

	var res respElement
	if len(list.value) == 0 {
		res = &respBulkString{
			value: "",
		}
	} else {
		if count > 1 {
			var elems []respElement = make([]respElement, count)
			for i, e := range list.value[:count] {
				elems[i] = e.ToResp()
			}
			res = &respArray{
				value: elems,
			}
		} else {
			res = list.value[0].ToResp()
		}
		list.value = list.value[count:]
		db.Store(key, list)
	}

	return res
}

func cmdLpush(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("Value at key %s is not list for LPUSH", key),
		}
		return res
	}

	var prepend []dbEntry
	for _, a := range args[2:] {
		prepend = append(prepend, NewDbString(a))
	}
	slices.Reverse(prepend)

	list.value = append(prepend, list.value...)
	db.Store(key, list)

	res := &respInteger{
		value: len(list.value),
	}

	return res
}

func cmdLrange(args []string) respElement {
	if len(args) < 4 {
		res := &respError{
			value: "LRANGE requires a key, start index and stop index",
		}
		return res
	}

	key := args[1]

	startStr := args[2]
	start, err := strconv.Atoi(startStr)
	if err != nil {
		res := &respError{
			value: "Unable to convert LRANGE start index to int",
		}
		return res
	}

	stopStr := args[3]
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		res := &respError{
			value: "Unable to convert LRANGE stop index to int",
		}
		return res
	}

	val, ok := db.Load(key)
	if !ok {
		val = NewDbList([]dbEntry{})
	}

	list, ok := val.(*dbList)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("Unable to convert value at %s to list", key),
		}
		return res
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
		return res
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

	return res
}

func cmdMulti(rc *redisConn) respElement {
	rc.multi = true
	res := &respSimpleString{
		value: "OK",
	}
	return res
}

func cmdPing() respElement {
	res := &respSimpleString{
		value: "PONG",
	}
	return res
}

func cmdRpush(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		val = NewDbList([]dbEntry{})
		db.Store(key, val)
	}

	list, ok := val.(*dbList)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("Value at key %s is not list for RPUSH", key),
		}
		return res
	}

	var toAppend []dbEntry
	for _, a := range args[2:] {
		toAppend = append(toAppend, NewDbString(a))
	}

	list.value = append(list.value, toAppend...)
	db.Store(key, list)

	res := &respInteger{
		value: len(list.value),
	}

	return res
}

func cmdSet(args []string) respElement {
	key := args[1]

	db.Store(key, NewDbString(args[2]))

	if len(args) > 3 {
		expiryCmd := args[3]
		switch expiryCmd {
		case "EX":
			expiryStr := args[4]
			duration, err := time.ParseDuration(expiryStr + "s")
			if err != nil {
				res := &respError{
					value: fmt.Sprintf("ERR Unable to parse duration: %s", err.Error()),
				}
				return res
			}
			time.AfterFunc(duration, func() {
				db.m.Delete(key)
			})
		case "PX":
			expiryStr := args[4]
			duration, err := time.ParseDuration(expiryStr + "ms")
			if err != nil {
				res := &respError{
					value: fmt.Sprintf("ERR Unable to parse duration: %s", err.Error()),
				}
				return res
			}
			time.AfterFunc(duration, func() {
				db.m.Delete(key)
			})
		default:
		}
	}

	res := &respSimpleString{
		value: "OK",
	}

	return res
}

func cmdType(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		res := &respSimpleString{
			value: "none",
		}
		return res
	}

	res := &respSimpleString{
		value: val.Type(),
	}
	return res
}

func cmdXadd(args []string) respElement {
	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		val = NewDbStream([]dbStreamEntry{})
		db.Store(key, val)
	}

	stream, ok := val.(*dbStream)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("ERR Value at key %s is not stream for XADD", key),
		}
		return res
	}

	id := args[2]

	var prevEntry dbStreamEntry
	var prevTimestamp, prevSequence int
	if len(stream.value) > 0 {
		prevEntry = stream.value[len(stream.value)-1]
		var err error
		prevTimestamp, prevSequence, err = prevEntry.id.GetTimestampAndSequence()
		if err != nil {
			res := &respError{
				value: fmt.Sprintf("ERR The ID in stream %s is an invalid format", key),
			}
			return res
		}
	}

	var timestamp int
	var sequence int
	switch id {
	case "0-0":
		res := &respError{
			value: "ERR The ID specified in XADD must be greater than 0-0",
		}
		return res
	case "*":
		timestamp = int(time.Now().UnixMilli())
		sequence = 0
		if timestamp == prevTimestamp {
			sequence = prevSequence + 1
		}

		id = fmt.Sprintf("%d-%d", timestamp, sequence)
	default:
		splitId := strings.Split(id, "-")

		if len(splitId) != 2 {
			res := &respError{
				value: "ERR The ID specified in XADD is an invalid format",
			}
			return res
		}

		var err error
		timestamp, err = strconv.Atoi(splitId[0])
		if err != nil {
			res := &respError{
				value: "ERR The ID specified in XADD is an invalid format",
			}
			return res
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
				res := &respError{
					value: "ERR The ID specified in XADD is an invalid format",
				}
				return res
			}
		}
		id = fmt.Sprintf("%d-%d", timestamp, sequence)
	}

	entry := dbStreamEntry{
		id: dbStreamEntryId{
			value: id,
		},
		values: map[string]string{},
	}

	if prevTimestamp > timestamp {
		res := &respError{
			value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		}
		return res
	}

	if prevTimestamp == timestamp {
		if prevSequence >= sequence {
			res := &respError{
				value: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
			}
			return res
		}
	}

	// TODO: guard against potential out of range panic caused by supplying insufficient params
	for i := 3; i < len(args); i += 2 {
		k := args[i]
		v := args[i+1]
		entry.values[k] = v
	}

	stream.value = append(stream.value, entry)
	db.Store(key, stream)

	res := &respBulkString{
		value: entry.id.value,
	}

	return res
}

func cmdXrange(args []string) respElement {
	if len(args) < 4 {
		res := &respError{
			value: "ERR XRANGE requires key, start, and stop arguments",
		}
		return res
	}

	key := args[1]

	val, ok := db.Load(key)
	if !ok {
		res := &respArray{
			value: []respElement{},
		}
		return res
	}

	stream, ok := val.(*dbStream)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("ERR Value at %s is not stream", key),
		}
		return res
	}

	start := args[2]

	var startIndex = 0
	if start != "-" {
		startId := &dbStreamEntryId{
			value: start,
		}

		startTimestamp, startSequence, err := startId.GetTimestampAndSequence()
		if err != nil {
			res := &respError{
				value: "ERR Unable to convert XRANGE start to ID format",
			}
			return res
		}

		for i, entry := range stream.value {
			timestamp, sequence, err := entry.id.GetTimestampAndSequence()
			if err != nil {
				res := &respError{
					value: "ERR Unable to parse stream entry id",
				}
				return res
			}

			if timestamp < startTimestamp {
				continue
			}

			if timestamp == startTimestamp {
				if sequence >= startSequence {
					startIndex = i
					break
				}
			}

			if timestamp > startTimestamp {
				startIndex = i
				break
			}
		}
	}

	stop := args[3]

	var stopIndex = len(stream.value)
	if stop != "+" {
		stopId := &dbStreamEntryId{
			value: stop,
		}

		stopTimestamp, stopSequence, err := stopId.GetTimestampAndSequence()
		if err != nil {
			res := &respError{
				value: "ERR Unable to convert XRANGE stop to ID format",
			}
			return res
		}

		for j := len(stream.value) - 1; j > 0; j-- {
			entry := stream.value[j]

			timestamp, sequence, err := entry.id.GetTimestampAndSequence()
			if err != nil {
				res := &respError{
					value: "ERR Unable to parse stream entry id",
				}
				return res
			}

			if timestamp > stopTimestamp {
				continue
			}

			if timestamp == stopTimestamp {
				if sequence <= stopSequence {
					stopIndex = j + 1
					break
				}
			}

			if timestamp < stopTimestamp {
				stopIndex = j + 1
				break
			}
		}
	}

	r := stream.ToResp()
	arr, ok := r.(*respArray)
	if !ok {
		res := &respError{
			value: fmt.Sprintf("ERR Value at %s is not stream", key),
		}
		return res
	}

	arr.value = arr.value[startIndex:stopIndex]

	return arr
}

func cmdXread(args []string) respElement {
	arg := args[1]

	var deadline time.Time
	var timeoutDuration time.Duration
	if strings.ToLower(arg) == "block" {
		timeout := args[2]
		timeFloat, err := strconv.ParseFloat(timeout, 64)
		if err != nil {
			res := &respError{
				value: "ERR Unable to convert XREAD timeout to float",
			}
			return res
		}
		timeoutDuration = time.Millisecond * time.Duration(timeFloat)
		deadline = time.Now().Add(timeoutDuration)
	}

	streamsIndex := slices.IndexFunc(args, func(e string) bool {
		return strings.ToLower(e) == "streams"
	})
	keysAndStarts := args[streamsIndex+1:]

	if len(args)%2 != 0 {
		res := &respError{
			value: "ERR Insufficient arguments provided for XREAD",
		}
		return res
	}

	halfway := len(keysAndStarts) / 2

	keys := keysAndStarts[:halfway]
	starts := keysAndStarts[halfway:]

	var res = NewDbList([]dbEntry{})

	for i := 0; i < halfway; i++ {
		key := keys[i]

		val, ok := db.Load(key)
		if !ok {
			res := &respArray{
				value: []respElement{},
			}
			return res
		}

		stream, ok := val.(*dbStream)
		if !ok {
			res := &respError{
				value: fmt.Sprintf("ERR Value at %s is not stream", key),
			}
			return res
		}

		start := starts[i]

		var startIndex = 0
		if start == "$" {
			startIndex = len(stream.value)
		} else {
			startId := &dbStreamEntryId{
				value: start,
			}

			startTimestamp, startSequence, err := startId.GetTimestampAndSequence()
			if err != nil {
				res := &respError{
					value: "ERR Unable to convert XRANGE start to ID format",
				}
				return res
			}

			for i, entry := range stream.value {
				timestamp, sequence, err := entry.id.GetTimestampAndSequence()
				if err != nil {
					res := &respError{
						value: "ERR Unable to parse stream entry id",
					}
					return res
				}

				if timestamp < startTimestamp {
					continue
				}

				if timestamp == startTimestamp {
					if sequence == startSequence {
						startIndex = i + 1
						break
					}
				}

				if timestamp > startTimestamp {
					startIndex = i
					break
				}
			}
		}

		arr := NewDbStream([]dbStreamEntry{})

		for len(arr.value) == 0 {
			if timeoutDuration > 0 && time.Now().After(deadline) {
				// TODO: remove hard coded null array when parser supports it
				return &respNullArray{}
			}
			if len(stream.value) > startIndex {
				arr.value = stream.value[startIndex:]
			}
		}

		res.value = append(res.value, NewDbList([]dbEntry{
			NewDbString(key),
			arr,
		}))
	}

	return res.ToResp()
}
