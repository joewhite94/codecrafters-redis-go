package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type redisReplicaConn struct {
	rc     *redisConn
	offset int
}

var replicas = []*redisReplicaConn{}

func propagateCmd(args []string) {
	resp := writeRespInput(args)
	for i, r := range replicas {
		_, err := r.rc.conn.Write([]byte(resp.ToString()))
		if err != nil {
			fmt.Printf("Failed to propagate command %s to replica %v: %s\n", args[0], i, err.Error())
			continue
		}
	}
}

func (rc *redisConn) cmdReplconf(args []string) respElement {
	var res respElement

	if strings.ToLower(args[1]) == "ack" {
		i := slices.IndexFunc(replicas, func(r *redisReplicaConn) bool {
			return r.rc == rc
		})

		if i == -1 {
			return &respError{
				value: "ERR Not registered as replica",
			}
		}

		rrc := replicas[i]

		offset, err := strconv.Atoi(args[2])
		if err != nil {
			return &respError{
				value: "ERR Invalid offset",
			}
		}
		rrc.offset = offset
	} else {
		res = &respSimpleString{
			value: "OK",
		}
	}

	return res
}
