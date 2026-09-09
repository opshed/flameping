package config

import (
	"errors"
	"fmt"
	"io"
	"math"
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
	ID         string            `yaml:"id"`
	Name       string            `yaml:"name"`
	Address    string            `yaml:"address"`
	Family     string            `yaml:"family"`
	Interval   Duration          `yaml:"interval"`
	Timeout    Duration          `yaml:"timeout"`
	Ping       *TargetPingConfig `yaml:"ping"`
	Traceroute *bool             `yaml:"traceroute"`
	Obsess     *ObsessConfig     `yaml:"obsess"`
}

// TargetPingConfig overrides timing independently for a single target. Queue
// capacities and the aggregate probe budget belong to the shared ping engine.
// Pointers distinguish omitted settings from explicitly invalid zero values.
type TargetPingConfig struct {
	Interval    *Duration `yaml:"interval"`
	Timeout     *Duration `yaml:"timeout"`
	MinInterval *Duration `yaml:"min_interval"`
}

// MaxObsessStateSlots bounds the configured probe and latency history retained
// by all enabled adaptive targets, including queued sends that can arrive in a burst.
const MaxObsessStateSlots = 100000

type ObsessConfig struct {
	Enabled          *bool    `yaml:"enabled"`
	Interval         Duration `yaml:"interval"`
	LatencyThreshold string   `yaml:"latency_threshold"`
	BaselineWindow   Duration `yaml:"baseline_window"`
	MinSamples       int      `yaml:"min_samples"`
	RecoverAfter     Duration `yaml:"recover_after"`
	present          map[string]bool
}

func (o *ObsessConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return errors.New("obsess must be a mapping")
	}
	known := map[string]bool{"enabled": true, "interval": true, "latency_threshold": true, "baseline_window": true, "min_samples": true, "recover_after": true}
	present := make(map[string]bool, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if !known[key] {
			return fmt.Errorf("field %s not found in type config.ObsessConfig", key)
		}
		present[key] = true
	}
	type plain ObsessConfig
	if err := node.Decode((*plain)(o)); err != nil {
		return err
	}
	o.present = present
	return nil
}

// EffectiveObsess returns a default-resolved copy, or nil when this target has
// not opted in. Explicit zero values from YAML remain invalid during validation.
func (t TargetConfig) EffectiveObsess(c Config) *ObsessConfig {
	if t.Obsess == nil || (t.Obsess.Enabled != nil && !*t.Obsess.Enabled) {
		return nil
	}
	o := *t.Obsess
	if o.Interval == 0 && !o.present["interval"] {
		o.Interval = Duration(max(100*time.Millisecond, t.EffectiveMinInterval(c)))
	}
	if o.LatencyThreshold == "" && !o.present["latency_threshold"] {
		o.LatencyThreshold = "50%"
	}
	if o.BaselineWindow == 0 && !o.present["baseline_window"] {
		o.BaselineWindow = Duration(time.Minute)
	}
	if o.MinSamples == 0 && !o.present["min_samples"] {
		o.MinSamples = 3
	}
	if o.RecoverAfter == 0 && !o.present["recover_after"] {
		o.RecoverAfter = Duration(time.Minute)
	}
	return &o
}

// Threshold returns either an absolute RTT or an increase percentage. For
// example, 50% triggers above 1.5 times the preceding baseline average.
func (o ObsessConfig) Threshold() (absolute time.Duration, relativePercent float64, err error) {
	raw := strings.TrimSpace(o.LatencyThreshold)
	if strings.HasSuffix(raw, "%") {
		percent, parseErr := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(raw, "%")), 64)
		if parseErr != nil || math.IsNaN(percent) || math.IsInf(percent, 0) || percent <= 0 || math.IsInf(1+percent/100, 0) {
			return 0, 0, errors.New("latency_threshold percentage must be finite and positive")
		}
		return 0, percent, nil
	}
	absolute, err = time.ParseDuration(raw)
	if err != nil || absolute <= 0 {
		return 0, 0, errors.New("latency_threshold must be a positive duration or percentage")
	}
	return absolute, 0, nil
}

func (t TargetConfig) EffectiveInterval(c Config) time.Duration {
	if t.Ping != nil && t.Ping.Interval != nil {
		return t.Ping.Interval.Value()
	}
	if t.Interval > 0 {
		return t.Interval.Value()
	}
	return c.Ping.Interval.Value()
}

func (t TargetConfig) EffectiveTimeout(c Config) time.Duration {
	if t.Ping != nil && t.Ping.Timeout != nil {
		return t.Ping.Timeout.Value()
	}
	if t.Timeout > 0 {
		return t.Timeout.Value()
	}
	return c.Ping.Timeout.Value()
}

func (t TargetConfig) EffectiveMinInterval(c Config) time.Duration {
	if t.Ping != nil && t.Ping.MinInterval != nil {
		return t.Ping.MinInterval.Value()
	}
	return c.Ping.MinInterval.Value()
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
	obsessSlots := int64(0)
	obsessEnabled := false
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
		minimum := t.EffectiveMinInterval(c)
		if t.Interval < 0 || t.Timeout < 0 {
			errs = append(errs, fmt.Errorf("%s.interval and timeout cannot be negative", prefix))
		}
		if t.Ping != nil {
			if t.Ping.Interval != nil && t.Interval != 0 {
				errs = append(errs, fmt.Errorf("%s: specify interval either directly or under ping, not both", prefix))
			}
			if t.Ping.Timeout != nil && t.Timeout != 0 {
				errs = append(errs, fmt.Errorf("%s: specify timeout either directly or under ping, not both", prefix))
			}
		}
		if minimum < 100*time.Millisecond {
			errs = append(errs, fmt.Errorf("%s.ping.min_interval must be at least 100ms", prefix))
		}
		if interval <= 0 || interval < minimum {
			errs = append(errs, fmt.Errorf("%s ping interval must be positive and at least %s", prefix, minimum))
		}
		if timeout <= 0 || timeout >= c.Storage.RawRetention.Value() {
			errs = append(errs, fmt.Errorf("%s.timeout must be positive and shorter than raw_retention", prefix))
		}
		fastest := interval
		if o := t.EffectiveObsess(c); o != nil {
			obsessEnabled = true
			fast := o.Interval.Value()
			if fast < minimum || fast <= 0 {
				errs = append(errs, fmt.Errorf("%s.obsess.interval must be at least %s", prefix, minimum))
			}
			if fast >= interval {
				errs = append(errs, fmt.Errorf("%s.obsess.interval must be faster than the normal interval", prefix))
			}
			if fast > 0 {
				fastest = min(fastest, fast)
			}
			_, percent, thresholdErr := o.Threshold()
			if thresholdErr != nil {
				errs = append(errs, fmt.Errorf("%s.obsess: %w", prefix, thresholdErr))
			} else if percent > 0 && timeout > 0 && float64(timeout)*(1+percent/100) > float64(math.MaxInt64) {
				errs = append(errs, fmt.Errorf("%s.obsess.latency_threshold exceeds the supported duration range", prefix))
			}
			if o.BaselineWindow <= 0 || o.RecoverAfter <= 0 || o.MinSamples <= 0 {
				errs = append(errs, fmt.Errorf("%s.obsess baseline_window, recover_after and min_samples must be positive", prefix))
			}
			if percent > 0 && interval > 0 && o.BaselineWindow > 0 && int64(o.MinSamples) > int64(o.BaselineWindow.Value()/interval) {
				errs = append(errs, fmt.Errorf("%s.obsess.min_samples cannot fit in baseline_window at the normal interval", prefix))
			}
			if fast > 0 && timeout > 0 && o.BaselineWindow > 0 {
				// Four slots cover inclusive history boundaries and the two echo
				// send loops while their socket writes are still unfinished.
				obsessSlots += min(int64(MaxObsessStateSlots+1), durationSlots(timeout, fast)) + min(int64(MaxObsessStateSlots+1), durationSlots(o.BaselineWindow.Value(), fast)) + 4
				obsessSlots = min(obsessSlots, int64(MaxObsessStateSlots+1))
			}
		}
		if fastest > 0 {
			rate += 1 / fastest.Seconds()
		}
	}
	if obsessEnabled {
		// Both address families can drain their accepted queues at once.
		queueSlots := 2 * min(c.Ping.SendQueue, MaxObsessStateSlots+1)
		if obsessSlots+int64(queueSlots) > MaxObsessStateSlots {
			errs = append(errs, fmt.Errorf("configured obsess state exceeds %d probe/sample slots; shorten target timeouts or baseline windows, increase obsess intervals, or reduce ping.send_queue", MaxObsessStateSlots))
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

func durationSlots(window, interval time.Duration) int64 {
	count := int64(window / interval)
	if window%interval != 0 {
		count++
	}
	return count
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
