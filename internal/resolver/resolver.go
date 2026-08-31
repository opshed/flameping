package resolver

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
)

type Resolver struct {
	Net *net.Resolver
}

func (r Resolver) Resolve(ctx context.Context, address, family string, previous netip.Addr) (netip.Addr, error) {
	if addr, err := netip.ParseAddr(address); err == nil {
		if familyMatches(addr, family) {
			return addr, nil
		}
		return netip.Addr{}, fmt.Errorf("address %s does not match family %s", address, family)
	}
	resolver := r.Net
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", address)
	if err != nil {
		return netip.Addr{}, err
	}
	filtered := addresses[:0]
	for _, addr := range addresses {
		addr = addr.Unmap()
		if familyMatches(addr, family) {
			filtered = append(filtered, addr)
		}
	}
	if len(filtered) == 0 {
		return netip.Addr{}, fmt.Errorf("%s has no addresses for family %s", address, family)
	}
	if previous.IsValid() {
		for _, addr := range filtered {
			if addr == previous {
				return addr, nil
			}
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if family == "auto" && filtered[i].Is4() != filtered[j].Is4() {
			return filtered[i].Is4()
		}
		return filtered[i].Compare(filtered[j]) < 0
	})
	return filtered[0], nil
}

func familyMatches(addr netip.Addr, family string) bool {
	switch family {
	case "", "auto":
		return true
	case "4":
		return addr.Is4()
	case "6":
		return addr.Is6()
	default:
		return false
	}
}
