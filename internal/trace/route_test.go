package trace

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/opshed/flameping/internal/config"
	"github.com/opshed/flameping/internal/model"
)

func TestRouteSignatureIgnoresOrderAndMissing(t *testing.T) {
	a := []model.TraceProbe{
		{TTL: 2, Responder: netip.MustParseAddr("192.0.2.2")},
		{TTL: 1, Responder: netip.MustParseAddr("192.0.2.1")},
		{TTL: 2},
	}
	b := []model.TraceProbe{a[1], a[0]}
	if routeSignature(a) != routeSignature(b) {
		t.Fatalf("signatures differ:\n%s\n%s", routeSignature(a), routeSignature(b))
	}
}

func TestAutoFallsBackAfterParisError(t *testing.T) {
	cfg := config.Defaults().Traceroute
	cfg.Method = "auto"
	runner := &Runner{cfg: cfg}
	result, err := runner.Trace(context.Background(), Target{ID: 1, StableID: "test", Endpoint: netip.MustParseAddr("127.0.0.1")})
	if err == nil || result.Method != "classic-udp" || !strings.Contains(err.Error(), "paris-udp failed") || !strings.Contains(err.Error(), "classic-udp failed") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestRunnerRateLimitIsSharedAcrossCalls(t *testing.T) {
	runner := &Runner{cfg: config.TracerouteConfig{MaxPacketsPerSecond: 100}}
	started := time.Now()
	if err := runner.waitRate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runner.waitRate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 8*time.Millisecond {
		t.Fatalf("shared rate limiter allowed back-to-back sends in %s", elapsed)
	}
}

func TestQuotedUDPIPv4(t *testing.T) {
	packet := make([]byte, 28)
	packet[0] = 0x45
	packet[9] = 17
	packet[20], packet[21] = 0x9c, 0x40
	packet[22], packet[23] = 0x82, 0x9a
	packet[26], packet[27] = 0x12, 0x34
	source, destination, checksum, err := quotedUDP(4, packet)
	if err != nil || source != 40000 || destination != 33434 || checksum != 0x1234 {
		t.Fatalf("quotedUDP = %d, %d, %#x, %v", source, destination, checksum, err)
	}
}

func FuzzQuotedUDP(f *testing.F) {
	packet := make([]byte, 28)
	packet[0] = 0x45
	packet[9] = 17
	f.Add(4, packet)
	f.Add(6, make([]byte, 40))
	f.Add(4, []byte{1})
	f.Fuzz(func(t *testing.T, family int, data []byte) {
		if family != 4 && family != 6 {
			return
		}
		_, _, _, _ = quotedUDP(family, data)
	})
}
