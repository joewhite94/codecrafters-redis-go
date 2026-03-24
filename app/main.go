package main

import (
	"fmt"
	"net"
	"os"
	"slices"
)

type redisConn struct {
	conn  net.Conn
	multi bool
	queue [][]string
}

func getArg(arg string) (string, error) {
	var args []string = os.Args
	var res string
	var err error

	if len(args) > 1 {
		index := slices.Index(args, arg)
		if index != -1 {
			if len(args) < index {
				err = fmt.Errorf("Argument %s requires a value\n", arg)
			} else {
				res = args[index+1]
			}
		}
	}
	return res, err
}

func handleConnection(conn net.Conn) {
	rc := &redisConn{
		conn: conn,
	}

	defer rc.conn.Close()

	for {
		buf := make([]byte, 1024)

		_, err := rc.conn.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}

		args, err := readRespInput(string(buf))
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
		}

		res := cmd(rc, args)
		_, err = conn.Write([]byte(res))
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}
	}
}

func main() {
	port, err := getArg("--port")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if port == "" {
		port = "6379"
	}

	replicaOf, err := getArg("--replicaof")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if replicaOf == "" {
		os.Setenv("ROLE", "master")
	} else {
		os.Setenv("ROLE", "slave")
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port" + port)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
