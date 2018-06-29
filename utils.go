package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/semihalev/redis-balancer"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/redcon"
)

func qcmdlower(n []byte) string {
	switch len(n) {
	case 3:
		if (n[0] == 's' || n[0] == 'S') &&
			(n[1] == 'e' || n[1] == 'E') &&
			(n[2] == 't' || n[2] == 'T') {
			return "set"
		}
		if (n[0] == 'g' || n[0] == 'G') &&
			(n[1] == 'e' || n[1] == 'E') &&
			(n[2] == 't' || n[2] == 'T') {
			return "get"
		}
	case 4:
		if (n[0] == 'm' || n[0] == 'M') &&
			(n[1] == 's' || n[1] == 'S') &&
			(n[2] == 'e' || n[2] == 'E') &&
			(n[3] == 't' || n[3] == 'T') {
			return "mset"
		}
		if (n[0] == 'm' || n[0] == 'M') &&
			(n[1] == 'g' || n[1] == 'G') &&
			(n[2] == 'e' || n[2] == 'E') &&
			(n[3] == 't' || n[3] == 'T') {
			return "mget"
		}
		if (n[0] == 'e' || n[0] == 'E') &&
			(n[1] == 'v' || n[1] == 'V') &&
			(n[2] == 'a' || n[2] == 'A') &&
			(n[3] == 'l' || n[3] == 'L') {
			return "eval"
		}
	case 5:
		if (n[0] == 'p' || n[0] == 'P') &&
			(n[1] == 'l' || n[1] == 'L') &&
			(n[2] == 's' || n[2] == 'S') &&
			(n[3] == 'e' || n[3] == 'E') &&
			(n[4] == 't' || n[4] == 'T') {
			return "plset"
		}
		if (n[0] == 'p' || n[0] == 'P') &&
			(n[1] == 'l' || n[1] == 'L') &&
			(n[2] == 'g' || n[2] == 'G') &&
			(n[3] == 'e' || n[3] == 'E') &&
			(n[4] == 't' || n[4] == 'T') {
			return "plget"
		}
	case 6:
		if (n[0] == 'e' || n[0] == 'E') &&
			(n[1] == 'v' || n[1] == 'V') &&
			(n[2] == 'a' || n[2] == 'A') &&
			(n[3] == 'l' || n[3] == 'L') &&
			(n[4] == 'r' || n[4] == 'R') &&
			(n[5] == 'o' || n[5] == 'O') {
			return "evalro"
		}
	}
	return strings.ToLower(string(n))
}

func writeArray(conn redcon.Conn, val []interface{}) {
	conn.WriteArray(len(val))

	for _, r := range val {
		switch v := r.(type) {
		default:
			conn.WriteError(fmt.Sprintf("ERR invalid response '%s'", reflect.TypeOf(r)))
		case string:
			conn.WriteString(v)
		case int64:
			conn.WriteInt64(v)
		case []byte:
			conn.WriteBulk(v)
		case redis.Error:
			conn.WriteError(string(v))
		case []interface{}:
			writeArray(conn, v)
		case nil:
			conn.WriteNull()
		case []int:
			conn.WriteArray(v[0])
		}
	}
}

func pipelineCommand(conn redcon.Conn, cmd redcon.Command) (int, redcon.Command, error) {
	if conn == nil {
		return 0, cmd, nil
	}
	pcmds := conn.PeekPipeline()
	if len(pcmds) == 0 {
		return 0, cmd, nil
	}
	args := make([][]byte, 0, 64)
	switch qcmdlower(cmd.Args[0]) {
	default:
		return 0, cmd, nil
	case "get":
		if len(cmd.Args) != 2 {
			return 0, cmd, nil
		}
		// convert to an PLGET command which similar to an MGET
		for _, pcmd := range pcmds {
			if qcmdlower(pcmd.Args[0]) != "get" || len(pcmd.Args) != 2 {
				return 0, cmd, nil
			}
		}
		args = append(args, []byte("plget"))
		for _, pcmd := range append([]redcon.Command{cmd}, pcmds...) {
			args = append(args, pcmd.Args[1])
		}
	case "set":
		if len(cmd.Args) != 3 {
			return 0, cmd, nil
		}
		// convert to a PLSET command which is similar to an MSET
		for _, pcmd := range pcmds {
			if qcmdlower(pcmd.Args[0]) != "set" || len(pcmd.Args) != 3 {
				return 0, cmd, nil
			}
		}
		args = append(args, []byte("plset"))
		for _, pcmd := range append([]redcon.Command{cmd}, pcmds...) {
			args = append(args, pcmd.Args[1], pcmd.Args[2])
		}
	}

	// remove the peeked items off the pipeline
	conn.ReadPipeline()

	ncmd := buildCommand(args)
	return len(pcmds) + 1, ncmd, nil
}

func buildCommand(args [][]byte) redcon.Command {
	// build a pipeline command
	buf := make([]byte, 0, 128)
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')

	poss := make([]int, 0, len(args)*2)
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		poss = append(poss, len(buf), len(buf)+len(arg))
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}

	// reformat a new command
	var ncmd redcon.Command
	ncmd.Raw = buf
	ncmd.Args = make([][]byte, len(poss)/2)
	for i, j := 0, 0; i < len(poss); i, j = i+2, j+1 {
		ncmd.Args[j] = ncmd.Raw[poss[i]:poss[i+1]]
	}
	return ncmd
}

func respPipeline(conn redcon.Conn, pn int, err error) {
	if err != nil {
		if conn != nil {
			for i := 0; i < pn-1; i++ {
				conn.WriteError(err.Error())
			}
		}
	}
}

func modeFromString(modeString string) balancer.BalanceMode {
	switch modeString {
	case "leastconn":
		return balancer.ModeLeastConn
	case "firstup":
		return balancer.ModeFirstUp
	case "minlatency":
		return balancer.ModeMinLatency
	case "random":
		return balancer.ModeRandom
	case "weightedlatency":
		return balancer.ModeWeightedLatency
	case "roundrobin":
		return balancer.ModeRoundRobin
	default:
		return balancer.ModeLeastConn
	}
}
