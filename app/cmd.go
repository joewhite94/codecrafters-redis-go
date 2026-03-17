package main

import (
	"fmt"
	"net"
	"time"
)

func runCmd(conn net.Conn, args []respElement) error {
	switch args[0].value {
	case "ECHO":
		return cmdEcho(conn, args)
	case "GET":
		return cmdGet(conn, args)
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
		return fmt.Errorf("Unable to convert SET key to string")
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

	toPush := args[2:]

	val.value = append(arr, toPush...)
	db[key] = val

	res := fmt.Sprintf(":%v\r\n", len(toPush))

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
