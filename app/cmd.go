package main

import (
	"fmt"
	"slices"
	"strconv"
	"time"
)

func runCmd(args []*respElement) (string, error) {
	switch args[0].value {
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
	default:
		return "", nil
	}
}

func cmdBlpop(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert BLPOP key to string")
	}

	var deadline time.Time
	var timeoutDuration time.Duration
	if len(args) > 2 {
		countStr, ok := args[2].value.(string)
		if !ok {
			return "", fmt.Errorf("Unable to convert BLPOP count to string")
		}
		timeFloat, err := strconv.ParseFloat(countStr, 64)
		if err != nil {
			return "", err
		}
		timeoutDuration = time.Millisecond * time.Duration(timeFloat*1000)
		deadline = time.Now().Add(timeoutDuration)
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
		db[key] = val
	}

	val.mu.Lock()
	defer val.mu.Unlock()

	var result *respElement

	for result == nil {
		if timeoutDuration > 0 && time.Now().After(deadline) {
			// TODO: remove hard coded null array when parser supports it
			return "*-1\r\n", nil
		}

		arr, ok := val.value.([]*respElement)
		if !ok {
			return "", fmt.Errorf("Value at key %s is not an array for BLPOP", key)
		}
		if len(arr) > 0 {
			result = &respElement{
				respType: "*",
				value: []*respElement{
					args[1],
					arr[0],
				},
			}
			arr = arr[1:]
			db[key].value = arr
		}
	}

	return writeResp(result)
}

func cmdEcho(args []*respElement) (string, error) {
	return writeResp(args[1])
}

func cmdGet(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert GET key to string")
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "$",
			value:    "",
		}
	}

	return writeResp(val)
}

func cmdLlen(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LLEN key to string")
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
	}

	arr, ok := val.value.([]*respElement)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not an array for LLEN", key)
	}

	return writeResp(&respElement{
		respType: ":",
		value:    len(arr),
	})
}

func cmdLpop(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LPOP key to string")
	}

	var count int = 1
	var err error
	if len(args) > 2 {
		countStr, ok := args[2].value.(string)
		if !ok {
			return "", fmt.Errorf("Unable to convert LPOP count to string")
		}
		count, err = strconv.Atoi(countStr)
		if err != nil {
			return "", err
		}
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
	}

	arr, ok := val.value.([]*respElement)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not an array for LPOP", key)
	}

	var result *respElement
	if len(arr) == 0 {
		result = &respElement{
			respType: "$",
			value:    "",
		}
	} else {
		if count > 1 {
			result = &respElement{
				respType: "*",
				value:    arr[0:count],
			}
		} else {
			result = arr[0]
		}
		arr = arr[count:]
		db[key].value = arr
	}

	return writeResp(result)
}

func cmdLpush(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LPUSH key to string")
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
	}

	arr, ok := val.value.([]*respElement)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not an array for LPUSH", key)
	}

	prepend := args[2:]
	slices.Reverse(prepend)

	arr = append(prepend, arr...)
	val.value = arr
	db[key] = val

	return writeResp(&respElement{
		respType: ":",
		value:    len(arr),
	})
}

func cmdLrange(args []*respElement) (string, error) {
	if len(args) < 4 {
		return "", fmt.Errorf("LRANGE requires a key, start index and stop index")
	}

	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE key to string")
	}

	startStr, ok := args[2].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE start index to string")
	}
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return "", err
	}

	stopStr, ok := args[3].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert LRANGE stop index to string")
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return "", err
	}

	val, ok := db[key]
	if !ok {
		val = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
	}

	arr, ok := val.value.([]*respElement)
	if !ok {
		return "", fmt.Errorf("Unable to convert value at %s to array", key)
	}

	// negative indexes - values are negative to adding them to array length works as subtraction
	if start < 0 {
		start = len(arr) + start
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop = len(arr) + stop
		if stop < 0 {
			stop = 0
		}
	}

	if start > len(arr) || (start > stop) {
		return writeResp(&respElement{
			respType: "*",
			value:    []*respElement{},
		})
	}

	// stop is inclusive
	if stop > len(arr) {
		stop = len(arr)
	} else {
		stop++
	}

	return writeResp(&respElement{
		respType: "*",
		value:    arr[start:stop],
	})
}

func cmdPing() (string, error) {
	return writeResp(&respElement{
		respType: "+",
		value:    "PONG",
	})
}

func cmdRpush(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert RPUSH key to string")
	}

	val, ok := db[key]
	if !ok {
		db[key] = &respElement{
			respType: "*",
			value:    []*respElement{},
		}
		val = db[key]
	}

	arr, ok := val.value.([]*respElement)
	if !ok {
		return "", fmt.Errorf("Value at key %s is not an array for RPUSH", key)
	}

	arr = append(arr, args[2:]...)
	db[key].value = arr

	return writeResp(&respElement{
		respType: ":",
		value:    len(arr),
	})
}

func cmdSet(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert SET key to string")
	}

	db[key] = args[2]

	if len(args) > 3 {
		switch args[3].value {
		case "EX":
			expiryStr, ok := args[4].value.(string)
			if !ok {
				return "", fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr + "s")
			if err != nil {
				return "", fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key)
			})
		case "PX":
			expiryStr, ok := args[4].value.(string)
			if !ok {
				return "", fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr + "ms")
			if err != nil {
				return "", fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key)
			})
		default:
		}
	}

	return writeResp(&respElement{
		respType: "+",
		value:    "OK",
	})
}

func cmdType(args []*respElement) (string, error) {
	key, ok := args[1].value.(string)
	if !ok {
		return "", fmt.Errorf("Unable to convert TYPE key to string")
	}

	val, ok := db[key]
	if !ok {
		return writeResp(&respElement{
			respType: "+",
			value:    "none",
		})
	}

	var res string
	switch val.respType {
	// TODO: support additional types
	case "+", "$":
		//string
		res = "string"
	}

	return writeResp(&respElement{
		respType: "+",
		value:    res,
	})
}
