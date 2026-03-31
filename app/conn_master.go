package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// handling for connections to, and commands received from, master instance

type redisMasterConn struct {
	redisConn
}

func (rmc *redisMasterConn) Read(b []byte) (int, error) {
	i, err := rmc.conn.Read(b)
	return i, err
}

func (rmc *redisMasterConn) Cmd(args argSet) []respElement {
	return rmc.runCmd(args)
}

func (rmc *redisMasterConn) Init() error {
	err := rmc.sendHandshake()
	if err != nil {
		return err
	}

	err = rmc.sendPsync()
	if err != nil {
		return err
	}
	return nil
}

func (rmc *redisMasterConn) runCmd(as argSet) []respElement {
	var res []respElement

	args := as.args

	cmd := args[0]

	switch cmd {
	case "+FULLRESYNC":
		return nil
	case "PING":
		rmc.redisConn.cmdPing()
		replOffset += as.bytes
	case "REPLCONF":
		res = append(res, rmc.cmdReplconf(args))
		replOffset += as.bytes
	default:
		rmc.redisConn.runCmd(as)
	}
	return res
}

func (rmc *redisMasterConn) cmdReplconf(args []string) respElement {
	if slices.ContainsFunc(args, func(e string) bool {
		return strings.ToLower(e) == "getack"
	}) {
		res := &respArray{
			value: []respElement{
				&respBulkString{
					value: "REPLCONF",
				},
				&respBulkString{
					value: "ACK",
				},
				&respBulkString{
					value: strconv.Itoa(replOffset),
				},
			},
		}
		return res
	}
	res := &respSimpleString{
		value: "OK",
	}
	return res
}

func (rmc *redisMasterConn) sendHandshake() error {
	var err error

	var ping = &respArray{
		value: []respElement{
			&respBulkString{
				value: "PING",
			},
		},
	}
	_, err = rmc.Write([]byte(ping.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to PING master: %s\n", err.Error())
	}

	buf := make([]byte, 4096)

	_, err = rmc.conn.Read(buf)
	if err != nil {
		return err
	}

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
	_, err = rmc.Write([]byte(replConf1.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to REPLCONF master :%s\n", err.Error())
	}

	_, err = rmc.conn.Read(buf)
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
	_, err = rmc.Write([]byte(replConf2.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to REPLCONF master :%s\n", err.Error())
	}

	_, err = rmc.conn.Read(buf)
	if err != nil {
		return err
	}

	return nil
}

func (rmc *redisMasterConn) sendPsync() error {
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

	_, err = rmc.Write([]byte(psync.ToString()))
	if err != nil {
		return fmt.Errorf("Replica failed to PSYNC master: %s\n", err.Error())
	}

	return nil
}
