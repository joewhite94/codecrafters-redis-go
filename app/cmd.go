package main

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"
)

func runCmd(conn net.Conn, args []respElement) error {
	switch args[0].value {
	case "ECHO":
		return cmdEcho(conn, args)
	case "GET":
		return cmdGet(conn, args)
	case "LPUSH":
		return cmdLpush(conn, args)
	case "LRANGE":
		return cmdLrange(conn, args)
	case "PING":
		return cmdPing(conn)
	case "RPUSH":
		return cmdRpush(conn, args)
	case "SET":
		return cmdSet(conn, args)
	default:
		return nil
	}
}

func cmdEcho(conn net.Conn, args []respElement) error {
	res, err := writeResp(args[1])
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(res))
	return err
}

func cmdGet(conn net.Conn, args []respElement) error {
	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert GET key to string")
	}

	val, ok := db[key]
	if !ok {
		val = respElement{
			respType: "$",
			value:    "",
		}
	}

	res, err := writeResp(val)
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(res))
	return err
}

func cmdLpush(conn net.Conn, args []respElement) error {
	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert LPUSH key to string")
	}

	val, ok := db[key]
	if !ok {
		val = respElement{
			respType: "*",
			value:    []respElement{},
		}
	}

	arr, ok := val.value.([]respElement)
	if !ok {
		return fmt.Errorf("Value at key %s is not an array for LPUSH", key)
	}

	prepend := args[2:]
	slices.Reverse(prepend)

	arr = append(prepend, arr...)
	val.value = arr
	db[key] = val

	res := fmt.Sprintf(":%v\r\n", len(arr))

	_, err := conn.Write([]byte(res))
	return err
}

func cmdLrange(conn net.Conn, args []respElement) error {
	if len(args) < 4 {
		return fmt.Errorf("LRANGE requires a key, start index and stop index")
	}

	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert LRANGE key to string")
	}

	startStr, ok := args[2].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert LRANGE start index to string")
	}
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return err
	}

	stopStr, ok := args[3].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert LRANGE stop index to string")
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return err
	}

	val, ok := db[key]
	if !ok {
		val = respElement{
			respType: "*",
			value:    []respElement{},
		}
	}

	arr, ok := val.value.([]respElement)
	if !ok {
		return fmt.Errorf("Unable to convert value at %s to array", key)
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

	var res string
	if start > len(arr) || (start > stop) {
		res, err = writeResp(respElement{
			respType: "*",
			value:    []respElement{},
		})
		if err != nil {
			return err
		}
		_, err = conn.Write([]byte(res))
		return err
	}

	// stop is inclusive
	if stop > len(arr) {
		stop = len(arr)
	} else {
		stop++
	}

	res, err = writeResp(respElement{
		respType: "*",
		value:    arr[start:stop],
	})
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(res))
	return err
}

func cmdPing(conn net.Conn) error {
	res, err := writeResp(respElement{
		respType: "+",
		value:    "PONG",
	})
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(res))
	return err
}

func cmdRpush(conn net.Conn, args []respElement) error {
	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert RPUSH key to string")
	}

	val, ok := db[key]
	if !ok {
		val = respElement{
			respType: "*",
			value:    []respElement{},
		}
	}

	arr, ok := val.value.([]respElement)
	if !ok {
		return fmt.Errorf("Value at key %s is not an array for RPUSH", key)
	}

	arr = append(arr, args[2:]...)
	val.value = arr
	db[key] = val

	res := fmt.Sprintf(":%v\r\n", len(arr))

	_, err := conn.Write([]byte(res))
	return err
}

func cmdSet(conn net.Conn, args []respElement) error {
	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert SET key to string")
	}

	db[key] = args[2]

	if len(args) > 3 {
		switch args[3].value {
		case "EX":
			expiryStr, ok := args[4].value.(string)
			if !ok {
				return fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr + "s")
			if err != nil {
				return fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key)
			})
		case "PX":
			expiryStr, ok := args[4].value.(string)
			if !ok {
				return fmt.Errorf("Unable to convert SET expiry to string")
			}
			duration, err := time.ParseDuration(expiryStr + "ms")
			if err != nil {
				return fmt.Errorf("Unable to parse duration: %s", err.Error())
			}
			time.AfterFunc(duration, func() {
				delete(db, key)
			})
		default:
		}
	}

	res, err := writeResp(respElement{
		respType: "+",
		value:    "OK",
	})
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(res))
	return err
}
