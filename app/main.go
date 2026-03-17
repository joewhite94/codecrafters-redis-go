package main

import (
	"fmt"
	"net"
	"os"
)

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

		if arr, ok := resp.(*respArray); ok {
			res, err := runCmd(arr.value)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
				return
			}
			_, err = conn.Write([]byte(res))
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
				return
			}
		} else {
			fmt.Fprintf(os.Stderr, "Unable to parse RESP input")
			return
		}
	}
}

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
