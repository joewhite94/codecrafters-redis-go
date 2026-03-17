package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}

		resp, _, err := readResp(string(buf), 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
		}

		switch resp.respType {
		case "*":
			if arr, ok := resp.value.([]respElement); ok {
				if arr[0].respType == "$" {
					err := runCmd(conn, arr)
					if err != nil {
						fmt.Fprintf(os.Stderr, err.Error())
						return
					}
				}
			} else {
				fmt.Fprintf(os.Stderr, "Unable to convert array")
				return
			}
		default:
			return
		}
	}
}
