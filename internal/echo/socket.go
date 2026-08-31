package echo

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type Packet struct {
	Payload []byte
	Source  netip.Addr
	Type    int
	Code    int
}

type PacketIO interface {
	WriteEcho(payload []byte, dst netip.Addr, sequence uint64) error
	ReadEcho(context.Context) (Packet, error)
	Close() error
}

type ICMPSocket struct {
	conn    *icmp.PacketConn
	family  int
	proto   int
	request icmp.Type
	reply   icmp.Type
	raw     bool
}

func OpenRaw(family int) (*ICMPSocket, error) {
	var network, address string
	var proto int
	var request, reply icmp.Type
	if family == 4 {
		network, address, proto = "ip4:icmp", "0.0.0.0", 1
		request, reply = ipv4.ICMPTypeEcho, ipv4.ICMPTypeEchoReply
	} else if family == 6 {
		network, address, proto = "ip6:ipv6-icmp", "::", 58
		request, reply = ipv6.ICMPTypeEchoRequest, ipv6.ICMPTypeEchoReply
	} else {
		return nil, fmt.Errorf("unsupported ICMP family %d", family)
	}
	conn, err := icmp.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &ICMPSocket{conn: conn, family: family, proto: proto, request: request, reply: reply, raw: true}, nil
}

func OpenUnprivileged(family int) (*ICMPSocket, error) {
	var network, address string
	var proto int
	var request, reply icmp.Type
	if family == 4 {
		network, address, proto = "udp4", "0.0.0.0", 1
		request, reply = ipv4.ICMPTypeEcho, ipv4.ICMPTypeEchoReply
	} else if family == 6 {
		network, address, proto = "udp6", "::", 58
		request, reply = ipv6.ICMPTypeEchoRequest, ipv6.ICMPTypeEchoReply
	} else {
		return nil, fmt.Errorf("unsupported ICMP family %d", family)
	}
	conn, err := icmp.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return &ICMPSocket{conn: conn, family: family, proto: proto, request: request, reply: reply}, nil
}

func (s *ICMPSocket) WriteEcho(payload []byte, dst netip.Addr, sequence uint64) error {
	message := icmp.Message{Type: s.request, Code: 0, Body: &icmp.Echo{ID: os.Getpid() & 0xffff, Seq: int(sequence & 0xffff), Data: payload}}
	wire, err := message.Marshal(nil)
	if err != nil {
		return err
	}
	var address net.Addr = &net.UDPAddr{IP: net.IP(dst.AsSlice()), Zone: dst.Zone()}
	if s.raw {
		address = &net.IPAddr{IP: net.IP(dst.AsSlice()), Zone: dst.Zone()}
	}
	_, err = s.conn.WriteTo(wire, address)
	return err
}

func (s *ICMPSocket) ReadEcho(ctx context.Context) (Packet, error) {
	buf := make([]byte, 1500)
	for {
		if err := s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return Packet{}, err
		}
		n, peer, err := s.conn.ReadFrom(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				select {
				case <-ctx.Done():
					return Packet{}, ctx.Err()
				default:
					continue
				}
			}
			return Packet{}, err
		}
		message, err := icmp.ParseMessage(s.proto, buf[:n])
		if err != nil || message.Type != s.reply {
			continue
		}
		echoBody, ok := message.Body.(*icmp.Echo)
		if !ok {
			continue
		}
		var ip net.IP
		var zone string
		switch value := peer.(type) {
		case *net.UDPAddr:
			ip, zone = value.IP, value.Zone
		case *net.IPAddr:
			ip, zone = value.IP, value.Zone
		default:
			continue
		}
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		addr = addr.Unmap()
		if zone != "" {
			addr = addr.WithZone(zone)
		}
		return Packet{Payload: append([]byte(nil), echoBody.Data...), Source: addr, Type: intType(message.Type), Code: message.Code}, nil
	}
}

func intType(t icmp.Type) int {
	switch v := t.(type) {
	case ipv4.ICMPType:
		return int(v)
	case ipv6.ICMPType:
		return int(v)
	default:
		return 0
	}
}

func (s *ICMPSocket) Close() error { return s.conn.Close() }
