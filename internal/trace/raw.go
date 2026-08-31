package trace

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type response struct {
	source          netip.Addr
	sourcePort      uint16
	destinationPort uint16
	checksum        uint16
	icmpType        int
	icmpCode        int
	reached         bool
}

type rawReceiver struct {
	conn   *icmp.PacketConn
	family int
	proto  int
}

func openRawReceiver(family int) (*rawReceiver, error) {
	network, address, proto := "ip4:icmp", "0.0.0.0", 1
	if family == 6 {
		network, address, proto = "ip6:ipv6-icmp", "::", 58
	}
	conn, err := icmp.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &rawReceiver{conn: conn, family: family, proto: proto}, nil
}

func (r *rawReceiver) read(deadline time.Time) (response, error) {
	if err := r.conn.SetReadDeadline(deadline); err != nil {
		return response{}, err
	}
	buf := make([]byte, 2048)
	for {
		n, peer, err := r.conn.ReadFrom(buf)
		if err != nil {
			return response{}, err
		}
		message, err := icmp.ParseMessage(r.proto, buf[:n])
		if err != nil {
			continue
		}
		var data []byte
		reached := false
		switch body := message.Body.(type) {
		case *icmp.TimeExceeded:
			data = body.Data
		case *icmp.DstUnreach:
			data = body.Data
			reached = true
		default:
			continue
		}
		src, ok := peerAddr(peer)
		if !ok {
			continue
		}
		sourcePort, destinationPort, checksum, err := quotedUDP(r.family, data)
		if err != nil {
			continue
		}
		return response{source: src, sourcePort: sourcePort, destinationPort: destinationPort, checksum: checksum,
			icmpType: traceICMPType(message.Type), icmpCode: message.Code, reached: reached}, nil
	}
}

func quotedUDP(family int, data []byte) (uint16, uint16, uint16, error) {
	offset := 0
	if family == 4 {
		header, err := icmp.ParseIPv4Header(data)
		if err != nil {
			return 0, 0, 0, err
		}
		offset = header.Len
	} else {
		if len(data) < 40 || data[6] != 17 {
			return 0, 0, 0, fmt.Errorf("quoted IPv6 packet has no direct UDP header")
		}
		offset = 40
	}
	if len(data) < offset+8 {
		return 0, 0, 0, fmt.Errorf("quoted UDP header is truncated")
	}
	udp := data[offset : offset+8]
	return binary.BigEndian.Uint16(udp[0:2]), binary.BigEndian.Uint16(udp[2:4]), binary.BigEndian.Uint16(udp[6:8]), nil
}

func peerAddr(peer net.Addr) (netip.Addr, bool) {
	var ip net.IP
	var zone string
	switch p := peer.(type) {
	case *net.IPAddr:
		ip, zone = p.IP, p.Zone
	case *net.UDPAddr:
		ip, zone = p.IP, p.Zone
	default:
		return netip.Addr{}, false
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}
	addr = addr.Unmap()
	if zone != "" {
		addr = addr.WithZone(zone)
	}
	return addr, true
}

func traceICMPType(t icmp.Type) int {
	switch value := t.(type) {
	case ipv4.ICMPType:
		return int(value)
	case ipv6.ICMPType:
		return int(value)
	default:
		return 0
	}
}

func (r *rawReceiver) Close() error { return r.conn.Close() }
