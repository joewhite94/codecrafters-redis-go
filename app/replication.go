package main

import (
	"fmt"
	"net"
	"strconv"
)

var replicas = []*replSlave{}

type replSlave struct {
	conn net.Conn
}

func replPropagate(args []string) error {
	resp := writeRespInput(args)
	for i, r := range replicas {
		_, err := r.conn.Write([]byte(resp.ToString()))
		if err != nil {
			fmt.Printf("Failed to propagate command %s to replica %v: %s\n", args[0], i, err.Error())
			continue
		}
	}
	return nil
}

func replPsync(conn net.Conn) error {
	var err error

	var psyncReplId string = replId
	var psyncReplOffset int = replOffset
	if psyncReplId == "" {
		psyncReplId = "?"
		psyncReplOffset = -1
	}

	var psync = &respArray{
		value: []respElement{
			&respBulkString{
				value: "PSYNC",
			},
			&respBulkString{
				value: psyncReplId,
			},
			&respBulkString{
				value: strconv.Itoa(psyncReplOffset),
			},
		},
	}

	_, err = conn.Write([]byte(psync.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to PSYNC master: %s\n", err.Error())
	}

	buf := make([]byte, 1024)

	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	return nil
}

func replSendHandshake(conn net.Conn) error {
	var err error

	var ping = &respArray{
		value: []respElement{
			&respBulkString{
				value: "PING",
			},
		},
	}
	_, err = conn.Write([]byte(ping.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to PING master: %s\n", err.Error())
	}

	buf := make([]byte, 1024)

	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	// pong, err := readRespRepl(string(buf))
	// if err != nil {
	// 	return err
	// }
	// if pong[0].ToString() != "PONG" {
	// 	return fmt.Errorf("Replica failed to receive PONG")
	// }

	var replConf1 = &respArray{
		value: []respElement{
			&respBulkString{
				value: "REPLCONF",
			},
			&respBulkString{
				value: "listening-port",
			},
			&respBulkString{
				value: port,
			},
		},
	}
	_, err = conn.Write([]byte(replConf1.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to REPLCONF master :%s\n", err.Error())
	}

	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	var replConf2 = &respArray{
		value: []respElement{
			&respBulkString{
				value: "REPLCONF",
			},
			&respBulkString{
				value: "capa",
			},
			&respBulkString{
				value: "psync2",
			},
		},
	}
	_, err = conn.Write([]byte(replConf2.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to REPLCONF master :%s\n", err.Error())
	}

	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	return nil
}
