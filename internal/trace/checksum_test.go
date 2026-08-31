package trace

import (
	"net/netip"
	"testing"
)

func TestParisChecksumCompensation(t *testing.T) {
	for _, test := range []struct {
		src, dst string
	}{
		{"192.0.2.10", "198.51.100.20"},
		{"2001:db8::10", "2001:db8::20"},
	} {
		payload, err := parisPayload(netip.MustParseAddr(test.src), netip.MustParseAddr(test.dst), 45000, 33434, 0x4567)
		if err != nil {
			t.Fatal(err)
		}
		got, err := udpChecksum(netip.MustParseAddr(test.src), netip.MustParseAddr(test.dst), 45000, 33434, payload)
		if err != nil || got != 0x4567 {
			t.Fatalf("checksum = %#x, %v", got, err)
		}
	}
}
