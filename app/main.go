package main

import (
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
)

type redisConn struct {
	conn  net.Conn
	multi bool
	queue [][]string
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
	var port string = "6379"

	var args []string = os.Args
	if len(args) > 1 {
		portIndex := slices.Index(args, "--port")
		if portIndex != -1 {
			if len(args) < portIndex {
				fmt.Println("Argument --port was not followed with a valid port")
				os.Exit(1)
			} else {
				port = args[portIndex+1]
				_, err := strconv.Atoi(port)
				if err != nil {
					fmt.Println("Invalid port: " + port)
					os.Exit(1)
				}
			}
		}
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
