package main

import (
	"bytes"
	"os"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// Config structure
type Config struct {
	LoadBalancer loadBalancer
}

type loadBalancer struct {
	Upstream    []backend
	MaxIdle     int
	Mode        string
	HealthCheck bool
	Routing     bool
}

type backend struct {
	Host          string
	CheckInterval time.Duration
	Rise          int
	Fall          int
}

func readConfig(path string) (c *Config, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var buf bytes.Buffer

	buf.ReadFrom(f)

	c = new(Config)
	err = yaml.Unmarshal(buf.Bytes(), c)
	if err != nil {
		return nil, err
	}

	return
}
