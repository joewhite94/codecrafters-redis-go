package main

import (
	"fmt"
	"net"
)

func runCmd(conn net.Conn, args []respElement) error {
	switch args[0].value {
	case "ECHO":
		return cmdEcho(conn, args)
	case "GET":
		return cmdGet(conn, args)
	case "PING":
		return cmdPing(conn)
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

	res, err := writeResp(db[key])
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

func cmdSet(conn net.Conn, args []respElement) error {
	key, ok := args[1].value.(string)
	if !ok {
		return fmt.Errorf("Unable to convert SET key to string")
	}

	db[key] = args[2]

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
