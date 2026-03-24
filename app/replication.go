package main

import (
	"fmt"
	"net"
)

func replSendHandshake(masterAddr string) error {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return fmt.Errorf("Replica failed to connect to master: %s\n", err.Error())
	}

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

	return nil
}
