// Package config loads config/default.yaml with built-in defaults.
package config

import (
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config mirrors config/default.yaml.
type Config struct {
	Kafka struct {
		BootstrapServers string `yaml:"bootstrap_servers"`
		Topic            string `yaml:"topic"`
		Partitions       int    `yaml:"partitions"`
		Acks             string `yaml:"acks"`
	} `yaml:"kafka"`
	Streaming struct {
		Watermark string `yaml:"watermark"`
		Window    string `yaml:"window"`
		Slide     string `yaml:"slide"`
	} `yaml:"streaming"`
	Postgres struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		DB       string `yaml:"db"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"postgres"`
	API struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"api"`
	Model struct {
		Path    string `yaml:"path"`
		Version string `yaml:"version"`
	} `yaml:"model"`
}

// Defaults match the Python implementation.
func Defaults() Config {
	c := Config{}
	c.Kafka.BootstrapServers = "localhost:9092"
	c.Kafka.Topic = "vitals.raw"
	c.Kafka.Partitions = 3
	c.Kafka.Acks = "all"
	c.Streaming.Watermark = "2 minutes"
	c.Streaming.Window = "5 minutes"
	c.Streaming.Slide = "1 minute"
	c.Postgres.Host = "localhost"
	c.Postgres.Port = 5432
	c.Postgres.DB = "vytals"
	c.Postgres.User = "vytals"
	c.Postgres.Password = "vytals"
	c.API.Host = "0.0.0.0"
	c.API.Port = 8000
	c.Model.Path = "models/xgb_risk.json"
	c.Model.Version = "v1"
	return c
}

// Load reads path (or VYTALS_CONFIG, or config/default.yaml), falling back
// to Defaults when the file is absent. YAML values override defaults.
func Load(path string) Config {
	cfg := Defaults()
	if path == "" {
		path = os.Getenv("VYTALS_CONFIG")
	}
	cands := []string{}
	if path != "" {
		cands = append(cands, path)
	} else {
		cands = append(cands, "config/default.yaml")
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		// internal/config -> repo root is two levels up
		root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
		cands = append(cands, filepath.Join(root, "config/default.yaml"))
	}
	if wd, err := os.Getwd(); err == nil {
		cands = append(cands, filepath.Join(wd, "config/default.yaml"))
	}
	for _, c := range cands {
		b, err := os.ReadFile(c)
		if err != nil {
			continue
		}
		var raw Config
		// merge: decode into a copy of defaults so missing keys keep defaults
		merged := Defaults()
		if err := yaml.Unmarshal(b, &raw); err != nil {
			continue
		}
		// overlay non-zero values
		if raw.Kafka.BootstrapServers != "" {
			merged.Kafka.BootstrapServers = raw.Kafka.BootstrapServers
		}
		if raw.Kafka.Topic != "" {
			merged.Kafka.Topic = raw.Kafka.Topic
		}
		if raw.Kafka.Partitions != 0 {
			merged.Kafka.Partitions = raw.Kafka.Partitions
		}
		if raw.Kafka.Acks != "" {
			merged.Kafka.Acks = raw.Kafka.Acks
		}
		if raw.Streaming.Watermark != "" {
			merged.Streaming.Watermark = raw.Streaming.Watermark
		}
		if raw.Streaming.Window != "" {
			merged.Streaming.Window = raw.Streaming.Window
		}
		if raw.Streaming.Slide != "" {
			merged.Streaming.Slide = raw.Streaming.Slide
		}
		if raw.Postgres.Host != "" {
			merged.Postgres.Host = raw.Postgres.Host
		}
		if raw.Postgres.Port != 0 {
			merged.Postgres.Port = raw.Postgres.Port
		}
		if raw.Postgres.DB != "" {
			merged.Postgres.DB = raw.Postgres.DB
		}
		if raw.Postgres.User != "" {
			merged.Postgres.User = raw.Postgres.User
		}
		if raw.Postgres.Password != "" {
			merged.Postgres.Password = raw.Postgres.Password
		}
		if raw.API.Host != "" {
			merged.API.Host = raw.API.Host
		}
		if raw.API.Port != 0 {
			merged.API.Port = raw.API.Port
		}
		if raw.Model.Path != "" {
			merged.Model.Path = raw.Model.Path
		}
		if raw.Model.Version != "" {
			merged.Model.Version = raw.Model.Version
		}
		return merged
	}
	return cfg
}
