package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/gomodule/redigo/redis"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/semihalev/log"
	"github.com/semihalev/summitdb-balancer/balancer"
	"github.com/tidwall/redcon"
)

// SummitDBBalancer structure
type SummitDBBalancer struct {
	balancer *balancer.Balancer
}

const (
	metricPrefix        = "sb"
	checkLeaderInterval = 1
	version             = "v0.1"
)

var (
	flagcpus   = flag.Int("C", 8, "set the maximum number of CPUs to use")
	flagLogLvl = flag.String("L", "info", "log verbosity level [crit,error,warn,info,debug]")
	flagaddr   = flag.String("l", ":7781", "balancer listen addr")
	flagconfig = flag.String("c", "sb.yaml", "config file path")
	flagpprof  = flag.Bool("pprof", false, "Debug information on http port :6060")
)

var (
	redisMonitorCh = make(chan string)

	config *Config
)

func (sb *SummitDBBalancer) onRedisConnect(conn redcon.Conn) bool {
	log.Info("Redis new connection", "remote", conn.RemoteAddr())
	return true
}

func (sb *SummitDBBalancer) onRedisCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	start := time.Now()

	sb.redisCommandNext(conn, cmd)

	commandMetric := metrics.GetOrRegisterTimer(fmt.Sprintf("%s.command.%s", metricPrefix, command), nil)
	commandMetric.UpdateSince(start)

	redisMonitor(conn, cmd)
}

// monitor middleware
func redisMonitor(conn redcon.Conn, cmd redcon.Command) {
	select {
	case redisMonitorCh <- fmt.Sprintf("- %s [%s] |%s|",
		time.Now().Format("2006/01/02 15:04:05.00"),
		conn.RemoteAddr(), string(bytes.Join(cmd.Args, []byte(" ")))):
	default:
	}
}

func (sb *SummitDBBalancer) redisCommandNext(conn redcon.Conn, cmd redcon.Command) {
	var pn int
	var err error

	pn, cmd, err = pipelineCommand(conn, cmd)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	}

	switch qcmdlower(cmd.Args[0]) {
	case "monitor":
		dc := conn.Detach()
		go func() {
			defer dc.Close()

			dc.WriteString("OK")
			dc.Flush()

			for {
				dc.WriteString(<-redisMonitorCh)

				err := dc.Flush()
				if err != nil {
					break
				}
			}
		}()
	case "metrics":
		data, err := json.Marshal(metrics.DefaultRegistry.GetAll())
		if err != nil {
			conn.WriteNull()
			return
		}

		conn.WriteBulk(data)
	case "plget":
		resp, err := sb.plget(cmd)
		if err != nil {
			respPipeline(conn, pn, err)
			return
		}

		for _, val := range resp {
			if val == nil {
				conn.WriteNull()
				continue
			}
			conn.WriteBulk(val.([]byte))
		}
	case "plset":
		err := sb.plset(cmd)
		if err != nil {
			respPipeline(conn, pn, err)
			return
		}

		for i := 1; i < len(cmd.Args); i += 2 {
			conn.WriteString("OK")
		}
	default:
		sb.Do(conn, cmd)
	}
}

func (sb *SummitDBBalancer) onRedisClose(conn redcon.Conn, err error) {
	log.Info("Redis connection closed", "remote", conn.RemoteAddr())
}

func (sb *SummitDBBalancer) plget(cmd redcon.Command) ([]interface{}, error) {
	backend := sb.balancer.Next()

	cmd.Args[0] = []byte("MGET")

	client := backend.Pool.Get()
	defer client.Close()

	backendMetric := metrics.GetOrRegisterMeter(fmt.Sprintf("%s.backend.%s", metricPrefix, backend.Addr), nil)
	backendMetric.Mark(1)

	var args []interface{}
	for _, arg := range cmd.Args[1:] {
		args = append(args, arg)
	}

	reply, err := client.Do(string(cmd.Args[0]), args...)
	if err != nil {
		return nil, errors.New("ERR " + err.Error())
	}

	switch val := reply.(type) {
	case redis.Error:
		return nil, errors.New(string(val))
	case []interface{}:
		return val, nil
	default:
		log.Debug("Invalid response from backend", "response-type", reflect.TypeOf(reply))
		return nil, errors.New("ERR invalid response")
	}
}

func (sb *SummitDBBalancer) plset(cmd redcon.Command) error {
	var backend *balancer.Backend

	if config.LoadBalancer.Routing {
		backend = sb.balancer.Leader()
	} else {
		backend = sb.balancer.Next()
	}

	cmd.Args[0] = []byte("MSET")

	client := backend.Pool.Get()
	defer client.Close()

	backendMetric := metrics.GetOrRegisterMeter(fmt.Sprintf("%s.backend.%s", metricPrefix, backend.Addr), nil)
	backendMetric.Mark(1)

	var args []interface{}
	for _, arg := range cmd.Args[1:] {
		args = append(args, arg)
	}

	reply, err := client.Do(string(cmd.Args[0]), args...)
	if err != nil {
		return errors.New("ERR " + err.Error())
	}

	switch val := reply.(type) {
	case redis.Error:
		return errors.New(string(val))
	case string:
		return nil
	default:
		log.Debug("Invalid response from backend", "response-type", reflect.TypeOf(reply))
		return errors.New("ERR invalid response")
	}
}

// Do from redis
func (sb *SummitDBBalancer) Do(conn redcon.Conn, cmd redcon.Command) {
	var backend *balancer.Backend

	switch qcmdlower(cmd.Args[0]) {
	case "set", "jset":
		if config.LoadBalancer.Routing {
			backend = sb.balancer.Leader()
		} else {
			backend = sb.balancer.Next()
		}
	default:
		backend = sb.balancer.Next()
	}

	client := backend.Pool.Get()
	defer client.Close()

	backendMetric := metrics.GetOrRegisterMeter(fmt.Sprintf("%s.backend.%s", metricPrefix, backend.Addr), nil)
	backendMetric.Mark(1)

	var args []interface{}
	for _, arg := range cmd.Args[1:] {
		args = append(args, arg)
	}

	reply, err := client.Do(string(cmd.Args[0]), args...)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	switch val := reply.(type) {
	case redis.Error:
		conn.WriteError(string(val))
	case string:
		conn.WriteString(val)
	case []byte:
		conn.WriteBulk(val)
	case int64:
		conn.WriteInt64(val)
	case []interface{}:
		writeArray(conn, val)
	case nil:
		conn.WriteNull()
	default:
		log.Debug("Invalid response from backend", "response-type", reflect.TypeOf(reply))
		conn.WriteError("ERR invalid response")
	}
}

func (sb *SummitDBBalancer) checkLeader() {
	for {
		time.Sleep(checkLeaderInterval * time.Second)
	}
}

func runBalancer() {
	sb := new(SummitDBBalancer)

	var options []*balancer.Options
	for _, backend := range config.LoadBalancer.Upstream {
		option := &balancer.Options{
			Network:       "tcp",
			Addr:          backend.Host,
			Fall:          backend.Fall,
			Rise:          backend.Rise,
			CheckInterval: backend.CheckInterval,

			MaxIdle: config.LoadBalancer.MaxIdle,
		}
		options = append(options, option)
	}

	sb.balancer = balancer.New(options, modeFromString(config.LoadBalancer.Mode))
	defer sb.balancer.Close()

	if config.LoadBalancer.Routing {
		go sb.checkLeader()
	}

	err := redcon.ListenAndServe(*flagaddr, sb.onRedisCommand, sb.onRedisConnect, sb.onRedisClose)
	if err != nil {
		log.Crit("Redis server startup failed", "error", err.Error())
	}
}

func main() {
	flag.Parse()

	if *flagcpus == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(*flagcpus)
	}

	lvl, err := log.LvlFromString(*flagLogLvl)
	if err != nil {
		log.Crit("Log verbosity level unknown")
	}

	log.Root().SetHandler(log.LvlFilterHandler(lvl, log.StdoutHandler))

	config, err = readConfig(*flagconfig)
	if err != nil {
		log.Crit("Config read failed", "error", err.Error())
	}

	go runBalancer()

	if *flagpprof {
		go func() {
			err := http.ListenAndServe(":6060", nil)
			if err != nil {
				log.Error("http listener for pprof failed", "error", err.Error())
			}
		}()
	}

	log.Info("SummitDB balancer service started", "version", version, "addr", *flagaddr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	<-c

	log.Info("SummitDB balancer service stopping")
}
