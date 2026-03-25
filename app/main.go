package main

import (
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
)

var port, replId, role string
var replOffset int

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

func handleConnection(conn net.Conn, isRepl bool) {
	rc := &redisConn{
		conn:   conn,
		isRepl: isRepl,
	}

	defer rc.conn.Close()

	for {
		buf := make([]byte, 1024)

		length, err := rc.conn.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}

		input := string(buf[:length])

		var argSets [][]string
		var index int
		for index < length {
			var args []string
			var err error
			args, index, err = readRespInput(input, index)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
			}
			argSets = append(argSets, args)
		}

		res := []respElement{}
		for _, args := range argSets {
			res = append(res, rc.cmd(args)...)
		}

		for _, e := range res {
			var w = []byte{}
			if e != nil {
				w = []byte(e.ToString())
			}
			_, err = conn.Write(w)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
				return
			}
		}
	}
}

func main() {
	var err error

	port, err = getArg("--port")
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
		role = "master"
		replId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		replOffset = 0
	} else {
		role = "slave"
		masterAddr := strings.Split(replicaOf, " ")
		masterHost := masterAddr[0]
		masterPort := masterAddr[1]

		conn, err := net.Dial("tcp", masterHost+":"+masterPort)
		if err != nil {
			fmt.Printf("Replica failed to connect to master: %s\n", err.Error())
			os.Exit(1)
		}

		err = replSendHandshake(conn)
		if err != nil {
			os.Exit(1)
		}

		err = replPsync(conn)
		if err != nil {
			os.Exit(1)
		}

		go handleConnection(conn, true)
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
		go handleConnection(conn, false)
	}
}
