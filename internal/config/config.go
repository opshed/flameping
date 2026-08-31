package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Duration time.Duration

func (d Duration) Value() time.Duration { return time.Duration(d) }
func (d Duration) String() string       { return time.Duration(d).String() }

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

type Bytes int64

func (b Bytes) Int64() int64   { return int64(b) }
func (b Bytes) String() string { return formatBytes(int64(b)) }

func (b *Bytes) UnmarshalText(text []byte) error {
	v, err := parseBytes(string(text))
	if err != nil {
		return err
	}
	*b = Bytes(v)
	return nil
}

func (b Bytes) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

type Config struct {
	Server     ServerConfig      `yaml:"server"`
	Storage    StorageConfig     `yaml:"storage"`
	Ping       PingConfig        `yaml:"ping"`
	DNS        DNSConfig         `yaml:"dns"`
	Targets    []TargetConfig    `yaml:"targets"`
	Interfaces []InterfaceConfig `yaml:"interfaces"`
	Traceroute TracerouteConfig  `yaml:"traceroute"`
	Logging    LoggingConfig     `yaml:"logging"`
}

type ServerConfig struct {
	Listen      string `yaml:"listen"`
	AllowPublic bool   `yaml:"allow_public"`
}

type StorageConfig struct {
	Path                    string   `yaml:"path"`
	Durability              string   `yaml:"durability"`
	RawRetention            Duration `yaml:"raw_retention"`
	MinuteRetention         Duration `yaml:"minute_retention"`
	HourRetention           Duration `yaml:"hour_retention"`
	TraceRetention          Duration `yaml:"trace_retention"`
	MaxBytes                Bytes    `yaml:"max_bytes"`
	FreeSpaceReserve        Bytes    `yaml:"free_space_reserve"`
	FreeSpaceReservePercent int      `yaml:"free_space_reserve_percent"`
}

type PingConfig struct {
	Interval           Duration `yaml:"interval"`
	Timeout            Duration `yaml:"timeout"`
	MinInterval        Duration `yaml:"min_interval"`
	EventQueue         int      `yaml:"event_queue"`
	SendQueue          int      `yaml:"send_queue"`
	MaxProbesPerSecond int      `yaml:"max_probes_per_second"`
}

type DNSConfig struct {
	RefreshInterval Duration `yaml:"refresh_interval"`
}

type TargetConfig struct {
	ID         string   `yaml:"id"`
	Name       string   `yaml:"name"`
	Address    string   `yaml:"address"`
	Family     string   `yaml:"family"`
	Interval   Duration `yaml:"interval"`
	Timeout    Duration `yaml:"timeout"`
	Traceroute *bool    `yaml:"traceroute"`
}

func (t TargetConfig) EffectiveInterval(c Config) time.Duration {
	if t.Interval > 0 {
		return t.Interval.Value()
	}
	return c.Ping.Interval.Value()
}

func (t TargetConfig) EffectiveTimeout(c Config) time.Duration {
	if t.Timeout > 0 {
		return t.Timeout.Value()
	}
	return c.Ping.Timeout.Value()
}

func (t TargetConfig) TraceEnabled(global bool) bool {
	if t.Traceroute == nil {
		return global
	}
	return *t.Traceroute
}

type InterfaceConfig struct {
	Name        string   `yaml:"name"`
	DisplayName string   `yaml:"display_name"`
	Interval    Duration `yaml:"interval"`
}

type TracerouteConfig struct {
	Enabled             bool     `yaml:"enabled"`
	Method              string   `yaml:"method"`
	Interval            Duration `yaml:"interval"`
	ProbesPerHop        int      `yaml:"probes_per_hop"`
	MaxHops             int      `yaml:"max_hops"`
	HopTimeout          Duration `yaml:"hop_timeout"`
	OverallTimeout      Duration `yaml:"overall_timeout"`
	PipelineHops        int      `yaml:"pipeline_hops"`
	MaxPacketsPerSecond int      `yaml:"max_packets_per_second"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Defaults() Config {
	return Config{
		Server: ServerConfig{Listen: "127.0.0.1:8080"},
		Storage: StorageConfig{
			Path:                    "./flameping.db",
			Durability:              "normal",
			RawRetention:            Duration(7 * 24 * time.Hour),
			MinuteRetention:         Duration(90 * 24 * time.Hour),
			HourRetention:           Duration(5 * 365 * 24 * time.Hour),
			TraceRetention:          Duration(30 * 24 * time.Hour),
			MaxBytes:                Bytes(5 << 30),
			FreeSpaceReserve:        Bytes(1 << 30),
			FreeSpaceReservePercent: 10,
		},
		Ping: PingConfig{
			Interval:           Duration(5 * time.Second),
			Timeout:            Duration(time.Second),
			MinInterval:        Duration(100 * time.Millisecond),
			EventQueue:         8192,
			SendQueue:          1024,
			MaxProbesPerSecond: 5000,
		},
		DNS: DNSConfig{RefreshInterval: Duration(5 * time.Minute)},
		Traceroute: TracerouteConfig{
			Enabled:             true,
			Method:              "auto",
			Interval:            Duration(15 * time.Minute),
			ProbesPerHop:        3,
			MaxHops:             30,
			HopTimeout:          Duration(time.Second),
			OverallTimeout:      Duration(10 * time.Second),
			PipelineHops:        4,
			MaxPacketsPerSecond: 50,
		},
		Logging: LoggingConfig{Level: "info", Format: "json"},
	}
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	cfg := Defaults()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("config contains multiple YAML documents")
		}
		return Config{}, fmt.Errorf("decode trailing config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

var stableIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (c Config) Validate() error {
	var errs []error
	if err := validateListen(c.Server); err != nil {
		errs = append(errs, err)
	}
	if c.Storage.Path == "" {
		errs = append(errs, errors.New("storage.path is required"))
	}
	if c.Storage.Durability != "normal" && c.Storage.Durability != "full" {
		errs = append(errs, errors.New("storage.durability must be normal or full"))
	}
	if c.Storage.RawRetention.Value() < 24*time.Hour {
		errs = append(errs, errors.New("storage.raw_retention must be at least 24h"))
	}
	const reviewFloor = 30 * 24 * time.Hour
	if c.Storage.MinuteRetention.Value() < reviewFloor {
		errs = append(errs, errors.New("storage.minute_retention must be at least 720h"))
	}
	if c.Storage.RawRetention > c.Storage.MinuteRetention {
		errs = append(errs, errors.New("storage.raw_retention must not exceed minute_retention"))
	}
	if c.Storage.HourRetention < c.Storage.MinuteRetention {
		errs = append(errs, errors.New("storage.hour_retention must be at least minute_retention"))
	}
	if c.Storage.TraceRetention <= 0 {
		errs = append(errs, errors.New("storage.trace_retention must be positive"))
	}
	if c.Storage.MaxBytes < Bytes(64<<20) {
		errs = append(errs, errors.New("storage.max_bytes must be at least 64MiB"))
	}
	if c.Storage.FreeSpaceReserve < 0 || c.Storage.FreeSpaceReservePercent < 0 || c.Storage.FreeSpaceReservePercent > 100 {
		errs = append(errs, errors.New("storage free-space reserve values are invalid"))
	}
	if c.Ping.MinInterval.Value() < 100*time.Millisecond {
		errs = append(errs, errors.New("ping.min_interval must be at least 100ms"))
	}
	if c.Ping.Interval < c.Ping.MinInterval {
		errs = append(errs, errors.New("ping.interval must be at least ping.min_interval"))
	}
	if c.Ping.Timeout <= 0 || c.Ping.Timeout >= c.Storage.RawRetention {
		errs = append(errs, errors.New("ping.timeout must be positive and shorter than raw_retention"))
	}
	if c.Ping.EventQueue < 64 || c.Ping.SendQueue < 1 || c.Ping.MaxProbesPerSecond < 1 {
		errs = append(errs, errors.New("ping queue sizes and max_probes_per_second must be positive"))
	}
	seen := make(map[string]struct{}, len(c.Targets))
	rate := 0.0
	for i, t := range c.Targets {
		prefix := fmt.Sprintf("targets[%d]", i)
		if !stableIDRE.MatchString(t.ID) {
			errs = append(errs, fmt.Errorf("%s.id must match %s", prefix, stableIDRE))
		}
		if _, ok := seen[t.ID]; ok {
			errs = append(errs, fmt.Errorf("duplicate target id %q", t.ID))
		}
		seen[t.ID] = struct{}{}
		if strings.TrimSpace(t.Address) == "" {
			errs = append(errs, fmt.Errorf("%s.address is required", prefix))
		}
		if t.Family == "" {
			t.Family = "auto"
		}
		if t.Family != "auto" && t.Family != "4" && t.Family != "6" {
			errs = append(errs, fmt.Errorf("%s.family must be auto, 4, or 6", prefix))
		}
		interval := t.EffectiveInterval(c)
		timeout := t.EffectiveTimeout(c)
		if interval < c.Ping.MinInterval.Value() {
			errs = append(errs, fmt.Errorf("%s.interval must be at least %s", prefix, c.Ping.MinInterval))
		} else {
			rate += 1 / interval.Seconds()
		}
		if timeout <= 0 || timeout >= c.Storage.RawRetention.Value() {
			errs = append(errs, fmt.Errorf("%s.timeout must be positive and shorter than raw_retention", prefix))
		}
	}
	maxTimeout := c.Ping.Timeout.Value()
	for _, target := range c.Targets {
		maxTimeout = max(maxTimeout, target.EffectiveTimeout(c))
	}
	if c.Storage.RawRetention.Value() < maxTimeout+time.Minute {
		errs = append(errs, errors.New("storage.raw_retention must exceed the maximum target timeout by at least 1m"))
	}
	if rate > float64(c.Ping.MaxProbesPerSecond) {
		errs = append(errs, fmt.Errorf("configured target rate %.2f/s exceeds ping.max_probes_per_second %d", rate, c.Ping.MaxProbesPerSecond))
	}
	ifaceSeen := make(map[string]struct{}, len(c.Interfaces))
	for i, inf := range c.Interfaces {
		if strings.TrimSpace(inf.Name) == "" || strings.ContainsAny(inf.Name, `/\`) {
			errs = append(errs, fmt.Errorf("interfaces[%d].name is invalid", i))
		}
		if _, ok := ifaceSeen[inf.Name]; ok {
			errs = append(errs, fmt.Errorf("duplicate interface name %q", inf.Name))
		}
		ifaceSeen[inf.Name] = struct{}{}
		if inf.Interval < 0 {
			errs = append(errs, fmt.Errorf("interfaces[%d].interval must be positive", i))
		}
	}
	tr := c.Traceroute
	if tr.Method != "auto" && tr.Method != "paris-udp" && tr.Method != "classic-udp" {
		errs = append(errs, errors.New("traceroute.method must be auto, paris-udp, or classic-udp"))
	}
	if tr.Enabled && (tr.Interval <= 0 || tr.HopTimeout <= 0 || tr.OverallTimeout <= 0 || tr.ProbesPerHop < 1 || tr.MaxHops < 1 || tr.MaxHops > 255 || tr.PipelineHops < 1 || tr.MaxPacketsPerSecond < 1) {
		errs = append(errs, errors.New("traceroute timing and count values must be positive (max_hops <= 255)"))
	}
	if c.DNS.RefreshInterval <= 0 {
		errs = append(errs, errors.New("dns.refresh_interval must be positive"))
	}
	if c.Logging.Level != "debug" && c.Logging.Level != "info" && c.Logging.Level != "warn" && c.Logging.Level != "error" {
		errs = append(errs, errors.New("logging.level must be debug, info, warn, or error"))
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		errs = append(errs, errors.New("logging.format must be json or text"))
	}
	return errors.Join(errs...)
}

func validateListen(s ServerConfig) error {
	host, _, err := net.SplitHostPort(s.Listen)
	if err != nil {
		return fmt.Errorf("server.listen: %w", err)
	}
	if s.AllowPublic {
		return nil
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("server.listen is not loopback; set server.allow_public: true explicitly")
	}
	return nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty byte size")
	}
	units := []struct {
		suffix string
		mul    int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1},
	}
	for _, unit := range units {
		if strings.HasSuffix(s, unit.suffix) {
			n := strings.TrimSpace(strings.TrimSuffix(s, unit.suffix))
			v, err := strconv.ParseFloat(n, 64)
			if err != nil || v < 0 {
				return 0, fmt.Errorf("invalid byte size %q", s)
			}
			result := v * float64(unit.mul)
			if result > float64(^uint64(0)>>1) {
				return 0, fmt.Errorf("byte size %q overflows int64", s)
			}
			return int64(result), nil
		}
	}
	return 0, fmt.Errorf("byte size %q needs an IEC suffix (KiB, MiB, GiB, TiB)", s)
}

func formatBytes(v int64) string {
	for _, unit := range []struct {
		suffix string
		mul    int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if v >= unit.mul && v%unit.mul == 0 {
			return fmt.Sprintf("%d%s", v/unit.mul, unit.suffix)
		}
	}
	return fmt.Sprintf("%dB", v)
}
