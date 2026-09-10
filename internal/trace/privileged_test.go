package trace

import (
	"context"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/config"
)

func TestPrivilegedLoopbackDestinationReached(t *testing.T) {
	if os.Getenv("FLAMEPING_PRIVILEGED_TESTS") != "1" {
		t.Skip("set FLAMEPING_PRIVILEGED_TESTS=1 to exercise raw ICMP traceroute")
	}
	cfg := config.Defaults().Traceroute
	cfg.Method = "classic-udp"
	cfg.MaxHops = 3
	cfg.ProbesPerHop = 1
	cfg.PipelineHops = 1
	cfg.HopTimeout = config.Duration(500 * time.Millisecond)
	cfg.OverallTimeout = config.Duration(3 * time.Second)
	runner, warnings := OpenRunner(cfg)
	defer runner.Close()
	address := netip.MustParseAddr("127.0.0.1")
	if !runner.Available(address) {
		t.Skipf("raw IPv4 receiver unavailable: %v", warnings)
	}
	result, err := runner.Trace(context.Background(), Target{ID: 1, StableID: "loopback", Endpoint: address})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Reached || result.ReachedHop != 1 || len(result.Probes) == 0 {
		t.Fatalf("loopback traceroute did not reach destination: %+v", result)
	}
}
