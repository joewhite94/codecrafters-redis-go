package main

import (
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
)

var dbFileName, dir, port, replId, role string
var replOffset int

type redisConnection interface {
	Close()
	Cmd(args argSet) []respElement
	Init() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
}

type argSet struct {
	args  []string
	bytes int
}

func handleConnection(rc redisConnection) {
	defer rc.Close()

	if err := rc.Init(); err != nil {
		os.Exit(1)
	}

	for {
		buf := make([]byte, 4096)

		length, err := rc.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}

		input := string(buf[:length])

		var argSets []argSet
		var index int
		for index < length {
			var args []string
			var err error
			oldI := index
			args, index, err = readRespInput(input, index)
			if err != nil {
				fmt.Printf("error reading input: %s", err.Error())
				break
			}
			if len(args) > 0 {
				argSets = append(argSets, argSet{
					args:  args,
					bytes: index - oldI,
				})
			}
		}

		res := []respElement{}
		for _, args := range argSets {
			res = append(res, rc.Cmd(args)...)
		}

		for _, e := range res {
			var w = []byte{}
			if e != nil {
				w = []byte(e.ToString())
			}
			_, err = rc.Write(w)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
				return
			}
		}
	}
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

	dbFileName, err = getArg("--dbfilename")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	dir, err = getArg("--dir")
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

		rmc := &redisMasterConn{
			redisConn: redisConn{
				conn: conn,
			},
		}

		go handleConnection(rmc)
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
		rc := &redisConn{
			conn: conn,
		}
		go handleConnection(rc)
	}
}
