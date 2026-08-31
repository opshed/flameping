package resolver

import (
	"context"
	"net/netip"
	"testing"
)

func TestLiteralFamily(t *testing.T) {
	r := Resolver{}
	got, err := r.Resolve(context.Background(), "::1", "6", netip.Addr{})
	if err != nil || got != netip.IPv6Loopback() {
		t.Fatalf("Resolve = %v, %v", got, err)
	}
	if _, err := r.Resolve(context.Background(), "::1", "4", netip.Addr{}); err == nil {
		t.Fatal("expected family mismatch")
	}
}
