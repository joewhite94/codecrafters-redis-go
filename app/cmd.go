package main

import "net"

func runCmd(conn net.Conn, args []respElement) error {
	switch args[0].value {
	case "ECHO":
		return cmdEcho(conn, args)
	case "PING":
		return cmdPing(conn)
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

func cmdPing(conn net.Conn) error {
	_, err := conn.Write([]byte("+PONG\r\n"))
	return err
}
